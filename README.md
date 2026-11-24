# OpsLens

OpsLens investigates production incidents. You give it an incident, it reads your logs, traces,
recent deploys and runbooks, and tells you what broke — citing the evidence it used, so you can
check its work.

It is meant for the first twenty minutes of an outage, when the error rate is climbing and nobody
has worked out yet whether this is a bad deploy, a saturated connection pool, or one noisy
dependency. You watch it think, interrupt it when it heads somewhere useless, and ask follow-ups
on the same evidence.

```
incident ──► agent loop ──► search_logs ──► VictoriaLogs
                │           fetch_trace  ──► VictoriaLogs
                │           list_deployments, search_runbooks
                │           search_past_incidents ──► Postgres (pgvector + FTS)
                ▼
          run_events ──► SSE ──► workspace UI
```

## Running it

You need Docker (OrbStack or Docker Desktop) and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
docker compose up --build -d
open http://localhost:8000
```

The UI asks for a token. The default is `dev-token`, set by `API_TOKEN` in `compose.yaml` — change
it before anyone else can reach the port.

`/healthz` tells you which dependency is unhappy:

```json
{"status":"ok","checks":{"postgres":{"ok":true},"redis":{"ok":true},
                         "victorialogs":{"ok":true},"s3":{"ok":true}}}
```

Models run locally through Ollama. Install it, then:

```bash
ollama pull qwen3:8b     # chat + tool calling
ollama pull bge-m3       # embeddings, 1024 dims
```

The API reaches Ollama at `host.docker.internal:11434`. If you change the embedding model, check its
dimension first: `doc_chunks.embedding` is `halfvec(1024)`, so a 768-dim model needs a migration.

## Using it

Create an incident in the UI and ask a question. Or do it from the shell:

```bash
TOKEN='Authorization: Bearer dev-token'

INC=$(curl -s -XPOST localhost:8000/incidents -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"title":"payment 5xx","severity":"sev2","services":["payment"]}' | jq -r .id)

RUN=$(curl -s -XPOST localhost:8000/incidents/$INC/runs -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"question":"Why is payment failing since 10:36?"}' | jq -r .run_id)

curl -N localhost:8000/runs/$RUN/events -H "$TOKEN"
```

While a run is going you can push it in a different direction:

```bash
curl -s -XPOST localhost:8000/runs/$RUN/steer -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"text":"skip auth retries, check the 10:36 deploy","interrupt":true}'

curl -s -XPOST localhost:8000/runs/$RUN/cancel -H "$TOKEN"
```

`interrupt` cuts the current generation short; the instruction is picked up before the next step.
`cancel` stops the run and keeps whatever it had already concluded.

### Getting data in

Logs go in three ways: an upload, a deploy webhook, and (eventually) an OTel collector.

```bash
# 1. ask for a presigned upload, PUT the file straight to object storage, then confirm
curl -s -XPOST localhost:8000/uploads -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"filename":"api.log.gz","content_type":"application/gzip"}'
#    → { "key": "uploads/…", "url": "http://localhost:8333/…" }
#    PUT the file to that url, then:
curl -s -XPOST "localhost:8000/uploads/<key>/complete" -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"service":"api","env":"prod"}'
curl -s localhost:8000/jobs/<job_id> -H "$TOKEN"
```

The API never touches the file body. The worker streams it out of object storage line by line —
gzip, JSON-lines, Docker's json-file format, OTel JSON or plain text — joins stack traces back into
single records, strips credentials, and pushes batches into VictoriaLogs.

```bash
# 2. deploy webhook, idempotent on (org, source, external_id)
curl -s -XPOST localhost:8000/webhooks/deployments -H "$TOKEN" -H 'content-type: application/json' \
  -H 'X-Delivery-Id: run-123' \
  -d '{"service":"payment","env":"prod","version":"1.9.4","deployed_at":"2026-11-24T10:20:00Z"}'
```

Runbooks and postmortems are the weak spot right now: retrieval works, but documents still get in
through `scripts/seed_docs.py` rather than the upload endpoint.

## What the agent actually does

Before the first model call it does two cheap queries itself — error counts per service and recent
deploys — and puts the result in context. That way a small local model doesn't burn four turns
rediscovering the onset time.

The model then calls tools. It never writes LogsQL: it passes `service`, `severity`, `trace_id`,
`minutes_ago` and so on, and the server composes and escapes the query. Logs and uploaded runbooks
are untrusted text, so instructions hidden inside a log line are treated as content, not commands.

Every tool result is registered as evidence with an id. The model has to cite it, and citations are
checked afterwards: short ids get resolved to the full uuid, ids that never existed become
`[ev:unknown]`. The UI turns each citation into a chip you can click to see the underlying rows.

Budgets are hard: 8 steps, 14 tool calls, 300 seconds per run. When one is hit the run stops and
says so instead of looping.

## Layout

```
src/opslens/
  api/      FastAPI: health, uploads, incidents, runs, SSE
  agent/    LLM client, tools, evidence registry, the loop
  ingest/   parsers, redaction, VictoriaLogs client, redis worker
  store/    shared clients
  web/      the UI (plain HTML/CSS/JS, no build step)
migrations/ Alembic — the schema's source of truth
scripts/    smoke tests and demo data
tests/      unit tests; nothing here needs Docker or a model
```

## Tests

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
```

The interesting checks need the stack up, because they exercise the real services:

```bash
uv run python scripts/smoke_ingest.py   # 5k log lines end to end, tracebacks joined, secrets redacted
uv run python scripts/seed_docs.py      # runbook/postmortem chunks + demo deploys
uv run python scripts/smoke_agent.py    # scripted LLM through the real loop, no model needed
uv run python scripts/smoke_live.py     # qwen3:8b, real tool calls, real citations
```

## Things that bit me, so they don't bite you

**Presigned URLs need a second client.** SigV4 signs the `Host` header, so a URL signed against
`s3:8333` is useless to your browser. `S3_PUBLIC_ENDPOINT_URL` is a separate boto3 client used only
for signing.

**redis-py 8 defaults to a 5-second socket timeout,** which is exactly the `XREADGROUP block=5000`
the worker uses. The worker died on its first idle read until that was raised.

**Runs live in the API process.** A restart marks in-flight runs failed instead of leaving them
stuck in `running`. Moving the loop into the worker process is the fix if this ever needs to
survive a deploy mid-investigation — right now it's a dev-grade trade for keeping cancel simple.

**SSE reads Postgres, not Redis.** `run_events.seq` is the resume cursor, so Redis would just be a
second copy of the truth. It polls every 250 ms, which is fine for one API instance and the wrong
shape for ten.

## What's missing

Follow-up runs reuse the previous run's last eight messages, which is honest but shallow. Retrieval
is fused with RRF and untuned. There is no reranker, no compaction of long investigations, no
evaluation set beyond the three smoke scripts, and no live log shipping. The UI has no
reconnect-on-reload for a run that started before you opened the page.
