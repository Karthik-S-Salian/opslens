"""Live model check: real Ollama through the API, streamed over SSE.

Run after `scripts/smoke_ingest.py` and `scripts/seed_docs.py`:
    uv run python scripts/smoke_live.py
Exits non-zero only if the run fails outright; tool/citation counts are reported, since
small local models vary.
"""

from __future__ import annotations

import json
import sys

import httpx

API = "http://localhost:8000"
TOKEN = "dev-token"


def main() -> None:
    headers = {"Authorization": f"Bearer {TOKEN}", "content-type": "application/json"}
    with httpx.Client(base_url=API, headers=headers, timeout=60) as api:
        incident = api.post(
            "/incidents",
            json={
                "title": "Payment 5xx spike (live model)",
                "severity": "sev2",
                "services": ["payment", "gateway"],
            },
        ).json()
        run = api.post(
            f"/incidents/{incident['id']}/runs",
            json={"question": "Why is payment failing? Find the onset and check recent deploys."},
        ).json()
        print(f"incident {incident['id']} run {run['run_id']}")

        tokens = 0
        answer = ""
        tool_calls: list[str] = []
        citations: list[str] = []
        status = "unknown"

        with api.stream("GET", f"/runs/{run['run_id']}/events", timeout=600) as stream:
            event_type = "message"
            for line in stream.iter_lines():
                if line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    payload = json.loads(line[5:].strip() or "{}")
                    if event_type == "token":
                        tokens += len(payload.get("text", ""))
                    elif event_type == "tool_start":
                        tool_calls.append(payload.get("tool", "?"))
                        print(f"  tool: {payload.get('tool')} {payload.get('args')}")
                    elif event_type == "message":
                        answer = payload.get("text", "")
                        citations = payload.get("citations", [])
                    elif event_type == "done":
                        status = payload.get("status", "done")
                        print(f"  done: {status}")

        print(f"\ntokens={tokens} tool_calls={tool_calls} citations={len(citations)}")
        print(f"status={status}\n\n{answer[:2000]}")
        if status != "answered":
            sys.exit(1)


if __name__ == "__main__":
    main()
