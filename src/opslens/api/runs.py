"""Run lifecycle: create, status, cancel, steer, SSE event stream."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Annotated, Any

import orjson
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from opslens.agent.loop import cancelled, run_task
from opslens.api.auth import require_token
from opslens.store.resources import Resources

router = APIRouter(dependencies=[Depends(require_token)])

TERMINAL = {"answered", "cancelled", "failed", "budget_exceeded"}
RUN_TASKS: dict[str, asyncio.Task] = {}


class RunIn(BaseModel):
    question: str = Field(default="Diagnose this incident.", max_length=4000)
    budget: dict[str, int] = {}


class SteerIn(BaseModel):
    text: str = Field(min_length=1, max_length=4000)
    interrupt: bool = False


def _resources(request: Request) -> Resources:
    return request.app.state.resources


@router.post("/incidents/{incident_id}/runs", status_code=202)
async def create_run(incident_id: uuid.UUID, body: RunIn, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    async with resources.db.begin() as conn:
        exists = (
            await conn.execute(text("SELECT 1 FROM incidents WHERE id = :id"), {"id": incident_id})
        ).first()
        if exists is None:
            raise HTTPException(404, "incident not found")
        parent = (
            await conn.execute(
                text(
                    "SELECT id FROM runs WHERE incident_id = :id ORDER BY created_at DESC LIMIT 1"
                ),
                {"id": incident_id},
            )
        ).scalar()
        run_id = (
            await conn.execute(
                text(
                    """
                    INSERT INTO runs (incident_id, parent_run_id, question, budget, status)
                    VALUES (:incident, :parent, :question, CAST(:budget AS jsonb), 'queued')
                    RETURNING id
                    """
                ),
                {
                    "incident": incident_id,
                    "parent": parent,
                    "question": body.question,
                    "budget": orjson.dumps(body.budget).decode(),
                },
            )
        ).scalar_one()

    task = asyncio.create_task(run_task(str(run_id), resources, request.app.state.llm))
    RUN_TASKS[str(run_id)] = task
    task.add_done_callback(lambda _: RUN_TASKS.pop(str(run_id), None))
    return {"run_id": str(run_id), "status": "queued"}


@router.get("/runs/{run_id}")
async def get_run(run_id: uuid.UUID, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    async with resources.db.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT id, incident_id, parent_run_id, status, stop_reason, usage, budget,"
                        " question, error, started_at, ended_at FROM runs WHERE id = :id"
                    ),
                    {"id": run_id},
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise HTTPException(404, "run not found")
    return {
        **dict(row),
        "id": str(row["id"]),
        "incident_id": str(row["incident_id"]),
        "parent_run_id": str(row["parent_run_id"]) if row["parent_run_id"] else None,
    }


@router.post("/runs/{run_id}/cancel", status_code=202)
async def cancel_run(run_id: uuid.UUID, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    await resources.redis.set(f"cancel:{run_id}", "1", ex=3600)
    task = RUN_TASKS.get(str(run_id))
    if task is not None and not task.done():
        task.cancel()
    return {"status": "cancelling"}


@router.post("/runs/{run_id}/steer", status_code=202)
async def steer_run(run_id: uuid.UUID, body: SteerIn, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    if await cancelled(resources, str(run_id)):
        raise HTTPException(409, "run is cancelling")
    await resources.redis.rpush(
        f"steer:{run_id}",
        orjson.dumps({"text": body.text, "interrupt": body.interrupt}).decode(),
    )
    await resources.redis.expire(f"steer:{run_id}", 3600)
    if body.interrupt:
        await resources.redis.set(f"interrupt:{run_id}", "1", ex=3600)
    return {"accepted": True}


@router.get("/runs/{run_id}/events")
async def run_events(
    run_id: uuid.UUID,
    request: Request,
    last_event_id: Annotated[str | None, Header()] = None,
) -> StreamingResponse:
    resources = _resources(request)
    start = int(last_event_id) if (last_event_id or "").isdigit() else 0

    async def stream():
        cursor = start
        last_ping = time.monotonic()
        while True:
            async with resources.db.connect() as conn:
                rows = (
                    (
                        await conn.execute(
                            text(
                                "SELECT seq, type, payload FROM run_events"
                                " WHERE run_id = :run AND seq > :cursor ORDER BY seq LIMIT 200"
                            ),
                            {"run": run_id, "cursor": cursor},
                        )
                    )
                    .mappings()
                    .all()
                )
                status = (
                    await conn.execute(
                        text("SELECT status FROM runs WHERE id = :id"), {"id": run_id}
                    )
                ).scalar()

            for row in rows:
                cursor = row["seq"]
                payload = orjson.dumps(row["payload"]).decode()
                yield f"id: {row['seq']}\nevent: {row['type']}\ndata: {payload}\n\n"

            if status in TERMINAL and not rows:
                break
            if time.monotonic() - last_ping > 15:
                yield ": ping\n\n"
                last_ping = time.monotonic()
            if await request.is_disconnected():
                break
            await asyncio.sleep(0.25)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
