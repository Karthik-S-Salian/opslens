"""Evidence registry: every tool output gets an id the model must cite."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any

import orjson
from sqlalchemy import text

from opslens.store.resources import Resources

CITATION = re.compile(r"\[ev:([0-9a-f-]{6,36})\]", re.IGNORECASE)


@dataclass(slots=True)
class Evidence:
    id: str
    kind: str
    excerpt: str


async def register(
    resources: Resources,
    run_id: str,
    *,
    kind: str,
    ref: dict[str, Any],
    excerpt: str,
) -> Evidence:
    evidence_id = str(uuid.uuid4())
    async with resources.db.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO evidence (id, run_id, kind, ref, excerpt)
                VALUES (:id, :run_id, :kind, CAST(:ref AS jsonb), :excerpt)
                """
            ),
            {
                "id": evidence_id,
                "run_id": run_id,
                "kind": kind,
                "ref": orjson.dumps(ref).decode(),
                "excerpt": excerpt[:4000],
            },
        )
    return Evidence(id=evidence_id, kind=kind, excerpt=excerpt[:400])


def validate_citations(text: str, known: set[str]) -> tuple[str, list[str]]:
    """Resolve citations to real evidence ids; unknown ones become `[ev:unknown]`.

    Small models abbreviate ids (8 hex chars is the common habit), so unique prefixes resolve
    to the full id and the answer text is rewritten to the canonical form.
    """
    used: list[str] = []

    def resolve(token: str) -> str | None:
        token = token.lower()
        if token in known:
            return token
        if len(token) < 6:
            return None
        matches = [evidence_id for evidence_id in known if evidence_id.startswith(token)]
        return matches[0] if len(matches) == 1 else None

    def repl(match: re.Match[str]) -> str:
        evidence_id = resolve(match.group(1))
        if evidence_id is None:
            return "[ev:unknown]"
        if evidence_id not in used:
            used.append(evidence_id)
        return f"[ev:{evidence_id}]"

    return CITATION.sub(repl, text), used
