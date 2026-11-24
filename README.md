# OpsLens

AI-assisted incident investigation platform. Design + milestones: [PLAN.md](PLAN.md).

## Run

```bash
cp .env.example .env
docker compose up --build -d      # api, worker, postgres(pgvector), redis, victorialogs, s3
curl -s localhost:8000/healthz | jq
```

`/healthz` returns 200 when postgres, redis, victorialogs and s3 all answer, 503 otherwise.

### Object storage

MinIO's public images no longer exist (Docker Hub 404, quay 401). Two replacements were tested
end-to-end (presign → PUT → worker read → 5k-line ingest):

| | SeaweedFS (default) | LocalStack 4.14.0 |
|---|---|---|
| Size | 280 MB | 1.65 GB |
| Survives `compose restart s3` | yes | **no** — state volume empty |
| Maintained | current tags | community line frozen; 2026.x images exit 55 without `LOCALSTACK_AUTH_TOKEN` |
| Bonus | real storage engine | emulates other AWS services later |

Both ignore presigned-URL signature mismatches (LocalStack warns and proceeds), so neither buys
you stricter local validation. To switch to LocalStack anyway, replace the `s3` service:

```yaml
  s3:
    image: localstack/localstack:4.14.0
    environment: { SERVICES: s3, AWS_DEFAULT_REGION: us-east-1 }
    ports: ["4566:4566"]
    volumes: ["s3data:/var/lib/localstack"]
```
and set `S3_ENDPOINT_URL=http://s3:4566`, `S3_PUBLIC_ENDPOINT_URL=http://localhost:4566`.
`S3_PUBLIC_ENDPOINT_URL` is the host browsers use — presigned URLs are signed against it (SigV4
covers `Host`).

## Ingest

```bash
KEY=$(curl -s -XPOST localhost:8000/uploads -H 'content-type: application/json' \
  -d '{"filename":"api.log.gz","content_type":"application/gzip"}' | jq -r .key)
# PUT the file to the returned presigned url, then:
curl -s -XPOST "localhost:8000/uploads/$KEY/complete" -H 'content-type: application/json' \
  -d '{"service":"api","env":"prod"}'
curl -s localhost:8000/jobs/<job_id> | jq
```

End-to-end check (5k lines, gzip, traceback joining, VL visibility):

```bash
uv run python scripts/smoke_ingest.py
```

CI/CD changes:

```bash
curl -s -XPOST localhost:8000/webhooks/deployments -H 'content-type: application/json' \
  -H 'X-Delivery-Id: run-123' \
  -d '{"service":"payment","env":"prod","version":"1.9.4","commit_sha":"abc123","deployed_at":"2026-11-24T10:20:00Z"}'
```

Retries are idempotent on `(org_id, source, external_id)`.

## Develop

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pytest -q
uv run alembic upgrade head       # needs postgres up
```

VictoriaLogs query UI: http://localhost:9428/select/vmui
SeaweedFS console: http://localhost:9333

## Investigate (UI + agent)

```bash
open http://localhost:8000            # workspace UI; paste the bearer token (default: dev-token)
uv run python scripts/seed_docs.py    # runbook + postmortem chunks with bge-m3 embeddings
uv run python scripts/smoke_agent.py  # scripted-LLM run through the real loop
```

The UI streams `run_events` over SSE: agent tokens, tool cards, evidence chips (click to see the
cited artifact), plus Stop and Steer while a run is active.

API equivalent:

```bash
TOKEN='Authorization: Bearer dev-token'
INC=$(curl -s -XPOST localhost:8000/incidents -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"title":"payment 5xx","severity":"sev2","services":["payment"]}' | jq -r .id)
RUN=$(curl -s -XPOST localhost:8000/incidents/$INC/runs -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"question":"Why is payment failing since 10:36?"}' | jq -r .run_id)
curl -N localhost:8000/runs/$RUN/events -H "$TOKEN"          # SSE, use Last-Event-ID to resume
curl -s -XPOST localhost:8000/runs/$RUN/steer -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"text":"skip auth retries, check the 10:36 deploy","interrupt":true}'
curl -s -XPOST localhost:8000/runs/$RUN/cancel -H "$TOKEN"
```

Local models (Ollama): `CHAT_MODEL` default `qwen3:8b`, `EMBED_MODEL` default `bge-m3`
(1024 dims — it must match `doc_chunks.embedding halfvec(1024)`). The API reaches Ollama at
`host.docker.internal:11434`; tests never call it (`FakeLLM`).

## Status

- M0 — compose stack, schema (pgvector 0.8.7, halfvec + HNSW + GIN), `/healthz`. Verified live.
- M1 — presigned upload → Stream job → worker → VictoriaLogs, deploy webhook, job status.
  5,001-line gzip file round-trips with multi-line tracebacks joined and secrets redacted.
  Verified live.
- M2 — six tools (`search_logs`, `get_error_spikes`, `fetch_trace`, `list_deployments`,
  `search_runbooks`, `search_past_incidents`) over VL + Postgres hybrid RRF.
- M3 — agent loop (evidence pack, tool calls, budget), SSE event stream, `/cancel`, `/steer`,
  bearer auth, vanilla-JS workspace UI. Deterministic `FakeLLM` drives the loop in tests.
- Next: M4 follow-up context polish + UI evidence view, M5 retrieval quality, M6 real-model eval.
