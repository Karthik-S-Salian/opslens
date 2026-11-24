# ruff: noqa: E501  -- the document bodies below are prose, not code
"""Seed runbook + postmortem documents so retrieval and citations have something real to find.

Run against a live stack with Ollama up: uv run python scripts/seed_docs.py
"""

from __future__ import annotations

import asyncio
import hashlib

import httpx
from sqlalchemy import text

from opslens.config import get_settings
from opslens.ingest.docs import chunk
from opslens.store.resources import Resources

PAYMENT_RUNBOOK = """# Payment Service Runbook

## Symptoms
- `TimeoutError: connection pool exhausted` from `/app/payment/db.py`
- 5xx rate above 2% on the payment endpoint
- p99 latency above 2s while CPU stays low (points at DB waits, not compute)

## Known causes
1. **Connection pool exhaustion after a deploy.** The pool size is `PAYMENT_DB_POOL_SIZE`
   (default 10 per replica). A release that adds a synchronous query inside a loop can
   double the number of pooled connections per request.
2. **Postgres max_connections reached after scaling replicas.** Each replica opens
   `PAYMENT_DB_POOL_SIZE` connections. Check `select count(*) from pg_stat_activity;`
   against `show max_connections;`.
3. **Long-running transactions blocking the pool.** Look for idle-in-transaction
   sessions older than 60s.

## Diagnosis
```
kubectl -n payments logs deploy/payment --since=15m | grep -i 'pool'
psql -c "select state, count(*) from pg_stat_activity group by state;"
psql -c "select pid, now() - state_change as idle_for, query from pg_stat_activity where state = 'idle in transaction';"
```

## Remediation
1. Roll back the most recent payment deploy if onset matches the deploy time.
2. Raise `PAYMENT_DB_POOL_SIZE` only after checking Postgres headroom.
3. Kill idle-in-transaction sessions older than 5 minutes.
4. Restart payment pods one at a time; watch 5xx for 5 minutes after each.

## Escalation
If 5xx stays above 5% for 10 minutes after rollback, page the payments on-call lead
and open a bridge. Do not scale more replicas until the Postgres connection count is known.
"""

POSTMORTEM = """# Postmortem: 2026-10-09 Payment 5xx outage

## Summary
A payment deploy reduced the DB pool size from 20 to 10 per replica. Combined with a new
retry loop, the service exhausted its pool within 12 minutes of rollout. 34% of charges
failed for 41 minutes.

## Timeline (UTC)
- 10:02 deploy `payment@1.9.1` starts
- 10:36 first `TimeoutError: connection pool exhausted` in logs, error rate 3%
- 10:38 alert fires at 5xx > 2%
- 10:44 error rate 34%, notification service backs up
- 11:02 rollback to `payment@1.9.0`
- 11:17 error rate back under 0.5%

## Root cause
Pool size halved in the same release that added a retry loop. Each request held a
connection while waiting on a 3s retry.

## What worked
- Onset correlated with the deploy timestamp within 2 minutes
- Runbook step "roll back if onset matches deploy" resolved it

## Action items
- Enforce a minimum pool size in the deploy manifest
- Alert on pool saturation before 5xx (metrics existed, no alert)
- Add a canary stage that holds 10 minutes with production-like traffic

## Lesson
Connection-pool failures look like latency spikes, not resource saturation: CPU stays low.
Check pool metrics and pg_stat_activity first.
"""


async def main() -> None:
    settings = get_settings()
    resources = Resources.build(settings)
    documents = [
        ("runbooks/payment.md", "runbook", "Payment Service Runbook", "payment", PAYMENT_RUNBOOK),
        (
            "postmortems/2026-10-09-payment.md",
            "postmortem",
            "2026-10-09 Payment 5xx outage",
            "payment",
            POSTMORTEM,
        ),
    ]
    try:
        async with httpx.AsyncClient(base_url=settings.ollama_url, timeout=120) as ollama:
            for source_key, kind, title, service, content in documents:
                pieces = chunk(content)
                embeddings = (
                    await ollama.post(
                        "/api/embed", json={"model": settings.embed_model, "input": pieces}
                    )
                ).json()["embeddings"]
                assert len(embeddings[0]) == settings.embed_dim, (
                    f"{settings.embed_model} returns {len(embeddings[0])} dims,"
                    f" schema expects {settings.embed_dim}"
                )

                digest = hashlib.sha256(content.encode()).hexdigest()
                async with resources.db.begin() as conn:
                    document_id = (
                        await conn.execute(
                            text(
                                """
                                INSERT INTO documents (org_id, kind, title, service, source_key, metadata)
                                VALUES (:org, :kind, :title, :service, :key, CAST(:meta AS jsonb))
                                ON CONFLICT (org_id, source_key) DO UPDATE SET title = EXCLUDED.title
                                RETURNING id
                                """
                            ),
                            {
                                "org": settings.org_id,
                                "kind": kind,
                                "title": title,
                                "service": service,
                                "key": source_key,
                                "meta": f'{{"sha256":"{digest}"}}',
                            },
                        )
                    ).scalar_one()
                    await conn.execute(
                        text("DELETE FROM doc_chunks WHERE document_id = :id"), {"id": document_id}
                    )
                    for index, (piece, embedding) in enumerate(
                        zip(pieces, embeddings, strict=True)
                    ):
                        await conn.execute(
                            text(
                                """
                                INSERT INTO doc_chunks
                                    (document_id, org_id, chunk_index, content, embedding)
                                VALUES (:doc, :org, :idx, :content, CAST(:vec AS halfvec))
                                """
                            ),
                            {
                                "doc": document_id,
                                "org": settings.org_id,
                                "idx": index,
                                "content": piece,
                                "vec": "[" + ",".join(f"{v:.6f}" for v in embedding) + "]",
                            },
                        )
                print(f"seeded {kind}: {title} ({len(pieces)} chunks)")

            # Deployments make the "did this start after a release?" question answerable.
            now_deploys = [
                ("payment", "prod", "1.9.4", "a1b2c3d", "github-actions", "seed-payment-194"),
                ("gateway", "prod", "2.3.1", "e4f5a6b", "github-actions", "seed-gateway-231"),
            ]
            async with resources.db.begin() as conn:
                for service, env, version, sha, source, external in now_deploys:
                    await conn.execute(
                        text(
                            """
                            INSERT INTO deployments
                                (org_id, service, env, version, commit_sha, deployed_at,
                                 source, external_id, payload)
                            VALUES (:org, :service, :env, :version, :sha, now() - interval '25 minutes',
                                    :source, :external, '{}'::jsonb)
                            ON CONFLICT (org_id, source, external_id) DO NOTHING
                            """
                        ),
                        {
                            "org": settings.org_id,
                            "service": service,
                            "env": env,
                            "version": version,
                            "sha": sha,
                            "source": source,
                            "external": external,
                        },
                    )
            print(f"seeded {len(now_deploys)} deployments")
    finally:
        await resources.aclose()


if __name__ == "__main__":
    asyncio.run(main())
