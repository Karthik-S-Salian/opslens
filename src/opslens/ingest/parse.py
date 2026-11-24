"""Log line parsing and normalization: format sniffing, multi-line joining, schema mapping.

Deliberately dependency-free and synchronous so it can be unit-tested without any services.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

TS_KEYS = ("timestamp", "time", "ts", "@timestamp", "_time", "datetime", "timeUnixNano")
SVC_KEYS = (
    "service",
    "service_name",
    "serviceName",
    "service.name",
    "app",
    "application",
    "container_name",
)
ENV_KEYS = ("env", "environment", "deployment.environment", "stage")
SEV_KEYS = ("level", "severity", "severity_text", "severityText", "loglevel", "log_level")
TRACE_KEYS = ("trace_id", "traceId", "trace.id", "traceID")
REQ_KEYS = ("request_id", "requestId", "request.id")
MSG_KEYS = ("message", "msg", "log", "_msg", "event", "body")

SEVERITIES = {
    "trace": "TRACE",
    "debug": "DEBUG",
    "info": "INFO",
    "information": "INFO",
    "notice": "INFO",
    "warn": "WARN",
    "warning": "WARN",
    "error": "ERROR",
    "err": "ERROR",
    "fatal": "FATAL",
    "critical": "FATAL",
    "crit": "FATAL",
    "panic": "FATAL",
}

TEXT_PREFIX = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[T ][\d:.,]+(?:Z|[+-]\d{2}:?\d{2})?)\s+"
    r"(?P<level>TRACE|DEBUG|INFO|INFORMATION|NOTICE|WARN|WARNING|ERROR|ERR|FATAL|CRITICAL|CRIT)\s+"
    r"(?:\[(?P<service>[\w.\-/]+)\]\s+)?(?P<msg>.*)$",
    re.IGNORECASE,
)

# A line that continues the previous record: indented, or a stack-frame/traceback marker.
CONTINUATION = re.compile(
    r"^(\s|at\s|Caused by:|Traceback \(most recent call last\)|panic:|goroutine \d+"
    r"|\.\.\. \d+ more|File \"|[\w.]*(?:Error|Exception|Warning):)"
)

MAX_RECORD_CHARS = 64_000
MAX_CONTINUATION_LINES = 200


def sniff(head: bytes) -> str:
    """Return 'json' or 'text' from the first bytes of a file."""
    stripped = head.lstrip()
    if stripped.startswith(b"{") and b"\n" in head:
        return "json"
    return "text"


def normalize_severity(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return SEVERITIES.get(text, text.upper()[:16] or None)


def normalize_time(value: Any) -> datetime | None:
    """Accept RFC3339 strings, epoch seconds/ms/us/ns, and OTel nano strings."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) or (
        isinstance(value, str) and value.replace(".", "").isdigit()
    ):
        number = float(value)
        for scale, divisor in ((1e17, 1e9), (1e14, 1e6), (1e11, 1e3), (1e8, 1)):
            if abs(number) >= scale:
                return datetime.fromtimestamp(number / divisor, tz=UTC)
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _first(row: dict, keys: tuple[str, ...]) -> Any:
    for key in keys:
        if (value := row.get(key)) not in (None, ""):
            return value
    return None


def parse_line(raw: str, defaults: dict[str, str]) -> dict[str, Any] | None:
    """Map one line to the common log schema. Returns None for blank lines."""
    line = raw.rstrip("\r\n")
    if not line.strip():
        return None

    row: dict[str, Any] = {}
    json_failed = False
    if line.lstrip().startswith("{"):
        try:
            loaded = json.loads(line)
        except json.JSONDecodeError:
            loaded = None
            json_failed = True
        if isinstance(loaded, dict):
            row = loaded
            # OTel JSON: flatten attributes under the top-level fields.
            attributes = row.get("attributes")
            if isinstance(attributes, dict):
                row = {**attributes, **row}
            # docker json-file driver wraps the payload in "log".
            if isinstance(row.get("log"), str) and "stream" in row and "time" in row:
                row = {**row, "message": row["log"]}

    record: dict[str, Any] = {
        "service": _first(row, SVC_KEYS) or defaults.get("service"),
        "env": _first(row, ENV_KEYS) or defaults.get("env"),
        "severity": normalize_severity(_first(row, SEV_KEYS)),
        "trace_id": _first(row, TRACE_KEYS),
        "request_id": _first(row, REQ_KEYS),
        "message": _first(row, MSG_KEYS),
        "time": normalize_time(_first(row, TS_KEYS)),
    }

    if not row:
        match = TEXT_PREFIX.match(line)
        if match:
            record["time"] = normalize_time(match["ts"])
            record["severity"] = normalize_severity(match["level"])
            record["service"] = match["service"] or record["service"]
            record["message"] = match["msg"]
        else:
            record["message"] = line

    if not record["message"]:
        # Unknown shape: keep the line verbatim rather than dropping evidence.
        record["message"] = line
        record["parse_error"] = True
    elif json_failed:
        record["parse_error"] = True
    return record


@dataclass(slots=True)
class LineAssembler:
    """Joins continuation lines (stack traces) onto the record they belong to."""

    defaults: dict[str, str] = field(default_factory=dict)
    _pending: dict[str, Any] | None = None
    _continuations: int = 0

    def feed(self, raw: str) -> Iterator[dict[str, Any]]:
        if self._pending is not None and CONTINUATION.match(raw):
            pending = self._pending
            if self._continuations < MAX_CONTINUATION_LINES:
                pending["message"] = (pending["message"] or "") + "\n" + raw.rstrip("\r\n")
                self._continuations += 1
                if len(pending["message"]) > MAX_RECORD_CHARS:
                    pending["message"] = pending["message"][:MAX_RECORD_CHARS] + "\n[truncated]"
                return
            yield from self.flush()

        record = parse_line(raw, self.defaults)
        if record is None:
            return
        if self._pending is not None:
            yield self._pending
        self._pending = record
        self._continuations = 0

    def flush(self) -> Iterator[dict[str, Any]]:
        if self._pending is not None:
            yield self._pending
            self._pending = None
            self._continuations = 0
