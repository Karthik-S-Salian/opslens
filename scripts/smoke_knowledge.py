"""Knowledge path: upload a runbook through the API, then check retrieval both ways.

Covers M5: exact error codes (lexical) and paraphrases (semantic) land in the top 3.
Run after the stack is up: uv run python scripts/smoke_knowledge.py
"""

from __future__ import annotations

import asyncio

import httpx
from sqlalchemy import text

from opslens.agent.llm import OllamaLLM
from opslens.agent.tools import ToolContext, search_runbooks
from opslens.config import get_settings
from opslens.ingest.vl import VictoriaLogs
from opslens.store.resources import Resources

API = "http://localhost:8000"

RUNBOOK = """# Search Service Runbook

## Symptoms
- `ElasticsearchError: index_not_found_exception` during query execution
- Search requests return 500 while the API gateway stays healthy
- `X-Search-Index` header reports a missing alias after a deploy

## Known causes
1. An index rotation deleted the write alias and the deploy did not recreate it.
2. A partial restore left the `search-v2` index in a red cluster state.
3. The indexer ran with `SEARCH_INDEX_PREFIX` unset and wrote to the wrong index.

## Diagnosis
- `GET _cat/aliases?v` to confirm the write alias exists
- `GET _cluster/health` for red or yellow indices
- grep the indexer logs for `index_prefix=` on the last deploy

## Remediation
1. Recreate the alias with `POST _aliases` pointing at the newest index.
2. Re-run the indexer for the window covered by the failed deploy.
3. If the cluster is red, restore the last snapshot before touching the alias.

## Escalation
Page the search on-call if the alias cannot be recreated or the cluster stays red for 10 minutes.
"""


async def main() -> None:
    settings = get_settings()
    token = settings.api_token
    headers = {"Authorization": f"Bearer {token}", "content-type": "application/json"}
    body = RUNBOOK.encode()

    async with httpx.AsyncClient(base_url=API, headers=headers, timeout=60) as api:
        upload_resp = await api.post(
            "/uploads",
            json={"filename": "search.md", "kind": "runbook", "content_type": "text/markdown"},
        )
        assert upload_resp.status_code in (200, 201), upload_resp.text
        upload = upload_resp.json()

        async with httpx.AsyncClient(timeout=60) as raw:
            put = await raw.put(
                upload["url"], content=body, headers={"Content-Type": "text/markdown"}
            )
        assert put.status_code == 200, put.text

        job_resp = await api.post(
            f"/uploads/{upload['key']}/complete",
            json={"kind": "runbook", "title": "Search Service Runbook", "service": "search"},
        )
        assert job_resp.status_code == 202, job_resp.text
        job = job_resp.json()

        for _ in range(240):
            status = (await api.get(f"/jobs/{job['job_id']}")).json()
            if status["status"] in ("done", "dead"):
                break
            await asyncio.sleep(0.5)
        print("job:", status["status"], status["stats"], status.get("error") or "")
        assert status["status"] == "done", status

    resources = Resources.build(settings)
    llm = OllamaLLM(
        httpx.AsyncClient(base_url=settings.ollama_url, timeout=settings.llm_timeout_seconds),
        settings.chat_model,
        settings.embed_model,
    )
    ctx = ToolContext(
        resources=resources,
        llm=llm,
        vl=VictoriaLogs(resources.http),
        run_id="smoke",
        org_id=settings.org_id,
    )
    try:
        checks = [
            ("lexical", "ElasticsearchError: index_not_found_exception"),
            ("semantic", "search crashes because the write alias disappeared after a release"),
            ("uploaded doc", "who do I page when the cluster stays red"),
        ]
        failures = []
        for label, query in checks:
            result = await search_runbooks(ctx, query, k=3)
            hit = "Search Service Runbook" in result.text
            print(f"{label:13s} top3 hit={hit}  query={query!r}")
            if not hit:
                failures.append(label)

        async with resources.db.connect() as conn:
            docs = (
                (
                    await conn.execute(
                        text(
                            "SELECT d.title, count(c.id) AS chunks,"
                            " min(vector_dims(c.embedding)) AS dims"
                            " FROM documents d JOIN doc_chunks c ON c.document_id = d.id"
                            " WHERE d.source_key LIKE 'runbook:search:%' GROUP BY d.title"
                        )
                    )
                )
                .mappings()
                .all()
            )
        print("stored:", [dict(row) for row in docs])
        assert docs and docs[0]["dims"] == settings.embed_dim

        assert not failures, f"retrieval missed: {failures}"
        print("OK: runbook uploaded through the API and retrievable both ways")
    finally:
        await llm.http.aclose()
        await resources.aclose()


if __name__ == "__main__":
    asyncio.run(main())
