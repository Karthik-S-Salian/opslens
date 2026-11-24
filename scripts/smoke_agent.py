"""Agent integration smoke test against the live stack with a scripted LLM.

Covers: tools -> evidence -> citations -> run_events -> cancel.
Run: uv run python scripts/smoke_agent.py
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
from sqlalchemy import text

from opslens.agent.llm import Delta, FakeLLM, ToolCall
from opslens.agent.loop import run_task
from opslens.config import get_settings
from opslens.store.resources import Resources

API = "http://localhost:8000"


async def main() -> None:
    settings = get_settings()
    resources = Resources.build(settings)
    headers = {"Authorization": f"Bearer {settings.api_token}"}
    try:
        async with httpx.AsyncClient(base_url=API, headers=headers, timeout=60) as api:
            incident = (
                await api.post(
                    "/incidents",
                    json={
                        "title": "Payment 5xx spike (agent smoke)",
                        "severity": "sev2",
                        "services": ["payment"],
                    },
                )
            ).json()
            run = await api.post(
                f"/incidents/{incident['id']}/runs",
                json={"question": "Why is payment failing?"},
            )
            assert run.status_code == 202, run.text

            # The API started a run with the real model; cancel it and drive our own with a fake.
            await api.post(f"/runs/{run.json()['run_id']}/cancel")

            run_id = str(uuid.uuid4())
            async with resources.db.begin() as conn:
                await conn.execute(
                    text(
                        """
                        INSERT INTO runs (id, incident_id, question, status)
                        VALUES (:id, :incident, :question, 'queued')
                        """
                    ),
                    {
                        "id": run_id,
                        "incident": incident["id"],
                        "question": "Why is payment failing?",
                    },
                )

            fake = FakeLLM(
                [
                    [
                        Delta(
                            tool_calls=[
                                ToolCall(
                                    name="search_logs",
                                    arguments={
                                        "service": "payment",
                                        "minutes_ago": 120,
                                        "limit": 5,
                                    },
                                )
                            ]
                        )
                    ],
                    [
                        Delta(
                            tool_calls=[
                                ToolCall(name="get_error_spikes", arguments={"minutes_ago": 120})
                            ]
                        )
                    ],
                ]
            )
            await run_task(run_id, resources, fake)

            async with resources.db.connect() as conn:
                status = (
                    (
                        await conn.execute(
                            text("SELECT status, usage FROM runs WHERE id = :id"), {"id": run_id}
                        )
                    )
                    .mappings()
                    .one()
                )
                events = (
                    (
                        await conn.execute(
                            text("SELECT type FROM run_events WHERE run_id = :id ORDER BY seq"),
                            {"id": run_id},
                        )
                    )
                    .scalars()
                    .all()
                )
                answer = (
                    await conn.execute(
                        text(
                            "SELECT content FROM run_messages WHERE run_id = :id"
                            " AND role = 'assistant' ORDER BY seq DESC LIMIT 1"
                        ),
                        {"id": run_id},
                    )
                ).scalar()
                evidence = (
                    await conn.execute(
                        text("SELECT count(*) FROM evidence WHERE run_id = :id"), {"id": run_id}
                    )
                ).scalar()

            print("status:", status["status"], "usage:", status["usage"])
            print("events:", " ".join(events))
            print("evidence rows:", evidence)
            print("answer:\n", answer)

            assert status["status"] == "answered", status
            assert {"status", "tool_start", "tool_end", "message", "done"} <= set(events)
            assert evidence >= 4  # evidence pack (2) + two tool results
            assert "[ev:" in answer and "[ev:unknown]" not in answer
            assert status["usage"]["tool_calls"] == 2
            print("OK: loop, tools, evidence, citations and events all wired")
    finally:
        await resources.aclose()


if __name__ == "__main__":
    asyncio.run(main())
