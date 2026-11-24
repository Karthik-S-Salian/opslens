"""Incidents, evidence, and the read side of a run."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import text

from opslens.api.auth import require_token
from opslens.store.resources import Resources

router = APIRouter(dependencies=[Depends(require_token)])


class IncidentIn(BaseModel):
    title: str = Field(min_length=3, max_length=300)
    severity: str = "unknown"
    services: list[str] = []
    started_at: datetime | None = None


def _resources(request: Request) -> Resources:
    return request.app.state.resources


@router.post("/incidents", status_code=201)
async def create_incident(body: IncidentIn, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    async with resources.db.begin() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        """
                    INSERT INTO incidents (org_id, title, severity, services, started_at)
                    VALUES (:org, :title, :severity, :services, :started_at)
                    RETURNING id, created_at
                    """
                    ),
                    {
                        "org": resources.settings.org_id,
                        "title": body.title,
                        "severity": body.severity,
                        "services": body.services,
                        "started_at": body.started_at,
                    },
                )
            )
            .mappings()
            .one()
        )
    return {"id": str(row["id"]), "title": body.title, "created_at": row["created_at"]}


@router.get("/incidents")
async def list_incidents(request: Request, limit: int = 50) -> list[dict[str, Any]]:
    resources = _resources(request)
    async with resources.db.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        """
                    SELECT i.id, i.title, i.severity, i.status, i.services, i.detected_at,
                           (SELECT count(*) FROM runs r WHERE r.incident_id = i.id) AS runs
                    FROM incidents i
                    WHERE i.org_id = :org
                    ORDER BY i.detected_at DESC
                    LIMIT :limit
                    """
                    ),
                    {"org": resources.settings.org_id, "limit": max(1, min(limit, 200))},
                )
            )
            .mappings()
            .all()
        )
    return [{**dict(row), "id": str(row["id"])} for row in rows]


@router.get("/incidents/{incident_id}")
async def get_incident(incident_id: uuid.UUID, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    async with resources.db.connect() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM incidents WHERE id = :id"), {"id": incident_id}
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise HTTPException(404, "incident not found")
    return {**dict(row), "id": str(row["id"])}


@router.get("/incidents/{incident_id}/timeline")
async def timeline(incident_id: uuid.UUID, request: Request, limit: int = 200) -> dict[str, Any]:
    resources = _resources(request)
    async with resources.db.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        """
                    SELECT m.run_id, m.seq, m.role, m.content, m.created_at
                    FROM run_messages m JOIN runs r ON r.id = m.run_id
                    WHERE r.incident_id = :id
                    ORDER BY m.seq DESC LIMIT :limit
                    """
                    ),
                    {"id": incident_id, "limit": max(1, min(limit, 500))},
                )
            )
            .mappings()
            .all()
        )
        runs = (
            (
                await conn.execute(
                    text(
                        "SELECT id, status, stop_reason, usage, question, created_at FROM runs"
                        " WHERE incident_id = :id ORDER BY created_at"
                    ),
                    {"id": incident_id},
                )
            )
            .mappings()
            .all()
        )
    return {
        "messages": [{**dict(row), "run_id": str(row["run_id"])} for row in reversed(rows)],
        "runs": [{**dict(row), "id": str(row["id"])} for row in runs],
    }


@router.get("/evidence/{evidence_id}")
async def get_evidence(evidence_id: uuid.UUID, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    async with resources.db.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT id, run_id, kind, ref, excerpt, created_at"
                        " FROM evidence WHERE id = :id"
                    ),
                    {"id": evidence_id},
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise HTTPException(404, "evidence not found")
    return {**dict(row), "id": str(row["id"]), "run_id": str(row["run_id"])}
