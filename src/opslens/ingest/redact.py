"""Scrub credentials from log text before it is stored or embedded.

Runs at ingest, so secrets never reach VictoriaLogs, pgvector, or the LLM prompt.
Patterns fail closed: over-redacting a debugging token is cheaper than leaking a password.
"""

from __future__ import annotations

import re

_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}"), "Bearer [redacted]"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), "[redacted-jwt]"),
    (
        re.compile(
            r"(?i)\b(password|passwd|pwd|secret|api[_-]?key|access[_-]?key|client[_-]?secret)"
            r"\s*[:=]\s*['\"]?[^\s'\",&;]{4,}"
        ),
        r"\1: [redacted]",
    ),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "[redacted-aws-key]"),
    (re.compile(r"\b(ghp|gho|ghs|ghu)_[A-Za-z0-9]{20,}\b"), "[redacted-github-token]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{16,}\b"), "[redacted-api-token]"),
    # postgres://user:password@host -> postgres://user:[redacted]@host
    (re.compile(r"\b([A-Za-z][A-Za-z0-9+.-]*://[^\s:/@]+):[^\s@/]+@"), r"\1:[redacted]@"),
]


def redact(text: str) -> str:
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text
