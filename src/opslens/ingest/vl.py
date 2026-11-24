"""VictoriaLogs HTTP client: jsonline ingest and LogsQL queries."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import httpx
import orjson

# Field names are part of the query contract; changing them breaks saved LogsQL.
STREAM_FIELDS = "service,env"
TIME_FIELD = "_time"
MSG_FIELD = "message"


class VictoriaLogs:
    def __init__(self, http: httpx.AsyncClient, *, retries: int = 3) -> None:
        self.http = http
        self.retries = retries

    async def insert(self, records: Sequence[dict[str, Any]]) -> None:
        if not records:
            return
        body = b"\n".join(orjson.dumps(r) for r in records)
        params = {
            "_stream_fields": STREAM_FIELDS,
            "_time_field": TIME_FIELD,
            "_msg_field": MSG_FIELD,
        }
        last: Exception | None = None
        for attempt in range(self.retries):
            try:
                resp = await self.http.post(
                    "/insert/jsonline",
                    params=params,
                    content=body,
                    headers={"Content-Type": "application/stream+json"},
                )
                resp.raise_for_status()
                return
            except httpx.HTTPError as exc:  # retry 5xx/timeouts, fail fast on 4xx
                last = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status is not None and 400 <= status < 500:
                    raise
                await asyncio.sleep(0.5 * 2**attempt)
        raise last  # type: ignore[misc]

    async def query(
        self,
        logsql: str,
        *,
        start: str,
        end: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        resp = await self.http.get(
            "/select/logsql/query",
            params={"query": logsql, "start": start, "end": end, "limit": limit},
        )
        resp.raise_for_status()
        return [orjson.loads(line) for line in resp.text.splitlines() if line.strip()]
