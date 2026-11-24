"""M6 evaluation: does a real model produce grounded, resolvable answers?

Runs three questions against a live API + Ollama, then scores:
  - answered vs failed/cancelled
  - every citation resolves to stored evidence (GET /evidence/{id})
  - which evidence kinds were cited (log / spike / deploy / doc / trace)
  - answer keeps the Observed / Hypothesis / Next actions structure

Run: uv run python scripts/eval_agent.py
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx

API = "http://localhost:8000"

QUESTIONS = [
    "Why is payment failing since 10:36?",
    "Did a recent deploy cause this?",
    "What does the runbook recommend we do next?",
]


async def run_question(client: httpx.AsyncClient, question: str) -> dict:
    incident = (
        await client.post(
            "/incidents",
            json={
                "title": f"Eval: {question[:60]}",
                "severity": "sev2",
                "services": ["payment", "gateway"],
            },
        )
    ).json()
    started = time.monotonic()
    run = (
        await client.post(f"/incidents/{incident['id']}/runs", json={"question": question})
    ).json()

    tools: list[str] = []
    citations: list[str] = []
    answer = ""
    status = "unknown"

    async with client.stream("GET", f"/runs/{run['run_id']}/events", timeout=900) as stream:
        event = "message"
        async for line in stream.aiter_lines():
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                payload = json.loads(line[5:].strip() or "{}")
                if event == "tool_start":
                    tools.append(payload.get("tool", "?"))
                elif event == "message":
                    answer, citations = payload.get("text", ""), payload.get("citations", [])
                elif event == "done":
                    status = payload.get("status", "?")

    kinds: dict[str, int] = {}
    unresolved: list[str] = []
    for evidence_id in citations:
        response = await client.get(f"/evidence/{evidence_id}")
        if response.status_code != 200:
            unresolved.append(evidence_id)
            continue
        kind = response.json()["kind"]
        kinds[kind] = kinds.get(kind, 0) + 1

    detail = (await client.get(f"/runs/{run['run_id']}")).json()
    return {
        "question": question,
        "status": status,
        "tools": tools,
        "citations": len(citations),
        "kinds": kinds,
        "unresolved": unresolved,
        "structure": all(
            section in answer for section in ("Observed", "Hypothesis", "Next actions")
        ),
        "usage": detail.get("usage") or {},
        "seconds": round(time.monotonic() - started, 1),
    }


async def main() -> None:
    async with httpx.AsyncClient(
        base_url=API,
        headers={"Authorization": "Bearer dev-token", "content-type": "application/json"},
        timeout=60,
    ) as client:
        results = []
        for question in QUESTIONS:
            print(f"running: {question}")
            results.append(await run_question(client, question))

    print(f"\n{'question':44s} {'status':16s} {'tools':24s} {'cites':5s} kinds")
    for row in results:
        print(
            f"{row['question'][:44]:44s} {row['status']:16s} "
            f"{','.join(row['tools'])[:24]:24s} {row['citations']:<5} {row['kinds']}"
        )
        print(
            f"  structure={row['structure']} unresolved={len(row['unresolved'])} "
            f"tokens={row['usage'].get('tokens_out', 0)} time={row['seconds']}s"
        )

    answered = sum(1 for row in results if row["status"] == "answered")
    unresolved = sum(len(row["unresolved"]) for row in results)
    cited = sum(1 for row in results if row["citations"] > 0)
    tool_kinds = {tool for row in results for tool in row["tools"]}
    cited_kinds = {kind for row in results for kind in row["kinds"]}
    grounded = sum(
        1
        for row in results
        if {"log", "spike"} & set(row["kinds"]) and {"deploy", "doc"} & set(row["kinds"])
    )

    print(
        f"\nanswered {answered}/{len(results)} · citations resolve "
        f"{'yes' if unresolved == 0 else 'NO'} · runs with citations {cited}/{len(results)}"
        f" · tools {sorted(tool_kinds)} · cited kinds {sorted(cited_kinds)}"
    )
    print(f"runs citing both a log-family and a deploy/doc artifact: {grounded}/{len(results)}")

    failures = []
    if answered < len(results):
        failures.append("not every run answered")
    if unresolved:
        failures.append(f"{unresolved} citations did not resolve")
    if cited < len(results) - 1:
        failures.append("too few runs cited evidence")
    if len(tool_kinds) < 2:
        failures.append("only one tool kind used across the suite")
    if failures:
        print("FAIL:", "; ".join(failures))
        raise SystemExit(1)
    print("PASS")


if __name__ == "__main__":
    asyncio.run(main())
