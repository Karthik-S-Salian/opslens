# OpsLens

OpsLens helps you work out why production is failing. Give it an incident, and it reads your logs,
traces, recent deploys and runbooks, then reports what it found — citing the evidence so you can
check its work.

It's built for the first twenty minutes of an outage: you start it, watch which queries it runs,
and steer it if it heads somewhere useless. Follow-up questions stay on the same evidence.

```
incident ──► agent loop ──► search_logs, fetch_trace ──► VictoriaLogs
                │            list_deployments ─────────► Postgres
                │            search_runbooks ──────────► Postgres (pgvector + FTS)
                ▼
          run_events ──► SSE ──► workspace UI
```

## Requirements

- Docker (OrbStack or Docker Desktop)
- [uv](https://docs.astral.sh/uv/)
- [Ollama](https://ollama.com/) with two models:

```bash
ollama pull qwen3:8b     # chat and tool calling
ollama pull bge-m3       # embeddings, 1024 dimensions
```

## Quick start

```bash
cp .env.example .env
docker compose up --build -d
open http://localhost:8000
```

The UI asks for a bearer token. The default is `dev-token`; set `API_TOKEN` in `compose.yaml` to
change it.

`GET /healthz` returns 200 when Postgres, Redis, VictoriaLogs and object storage all answer, and 503
with the failing dependency named.

## Using it

Create an incident in the UI, then ask a question. The run streams back as it happens: the queries
the agent runs, its reasoning, and the answer with a citation chip on every claim. Click a chip to
see the rows behind it. While a run is active you can send it an instruction — it is applied before
the next step, or interrupts the current answer — or stop the run and keep what it had already
established.

Endpoints, if you want to drive it without the UI:

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/incidents` | create an incident |
| `POST` | `/incidents/{id}/runs` | start an investigation |
| `GET` | `/runs/{id}/events` | SSE stream; send `Last-Event-ID` to resume |
| `POST` | `/runs/{id}/steer` | send an instruction (`interrupt` stops generation) |
| `POST` | `/runs/{id}/cancel` | stop the run |
| `GET` | `/incidents/{id}/timeline` | messages and runs for an incident |
| `GET` | `/evidence/{id}` | the stored tool result behind a citation |
| `POST` | `/uploads` | presigned log upload |
| `POST` | `/uploads/{key}/complete` | queue the uploaded file for ingest |
| `GET` | `/jobs/{id}` | ingest progress |
| `POST` | `/webhooks/deployments` | record a deploy |

All of them except `/healthz` want `Authorization: Bearer <API_TOKEN>`.

## Getting data in

Logs are uploaded directly to object storage: the API hands out a presigned URL, you `PUT` the file
there, then confirm it. The API never reads the body. A background worker streams the file back line
by line — gzip, JSON lines, Docker's json-file format, OTel JSON or plain text — joins stack traces
into single records, removes credentials, and writes batches to VictoriaLogs.

Deploys come in through the webhook endpoint and are deduplicated on
`(org, source, external_id)`, so CI retries are harmless.

Runbooks and postmortems use the same upload flow with `"kind": "runbook"` (or `postmortem`) and a
`title`. The worker chunks and embeds them, so retrieval picks them up straight away.

## How the investigation works

Before calling the model, the server runs two queries itself — error counts per service and recent
deploys — and puts them in context.

The model then calls tools. It does not write queries: it passes fields like `service`, `severity`,
`trace_id` and `minutes_ago`, and the server builds and escapes the LogsQL. Tool output and uploaded
documents are treated as data; instructions inside a log line are reported, not followed.

Each tool result is stored as evidence with an id, and the model has to cite it. Citations are
checked afterwards: abbreviated ids resolve to the full uuid, invented ones become `[ev:unknown]`.

Runs are bounded by a budget (8 steps, 14 tool calls, 300 seconds by default) and stop with a clear
status when they hit it.

## Configuration

All settings are environment variables; see `.env.example` for the local defaults.

| Variable | Default | Notes |
|---|---|---|
| `API_TOKEN` | `dev-token` | empty string disables auth |
| `DATABASE_URL` | `postgresql+asyncpg://opslens:opslens@localhost:5432/opslens` | |
| `REDIS_URL` | `redis://localhost:6379/0` | |
| `VICTORIALOGS_URL` | `http://localhost:9428` | |
| `S3_ENDPOINT_URL` | `http://localhost:8333` | internal endpoint |
| `S3_PUBLIC_ENDPOINT_URL` | same as above | used for signing; must be reachable from the client |
| `S3_BUCKET` | `opslens` | created at startup if missing |
| `OLLAMA_URL` | `http://localhost:11434` | in compose: `host.docker.internal:11434` |
| `CHAT_MODEL` | `qwen3:8b` | |
| `EMBED_MODEL` | `bge-m3` | must match `doc_chunks.embedding halfvec(1024)` |

## Layout

```
src/opslens/
  api/      FastAPI: health, uploads, incidents, runs, SSE
  agent/    LLM client, tools, evidence registry, the run loop
  ingest/   parsers, redaction, VictoriaLogs client, Redis worker
  store/    shared clients
  web/      UI (plain HTML/CSS/JS, no build step)
migrations/ Alembic; migrations are the schema's source of truth
scripts/    smoke tests and demo data
tests/      unit tests; no Docker or model required
```

## Tests

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
```

The end-to-end checks need the stack running:

```bash
uv run python scripts/smoke_ingest.py   # 5k log lines through ingest into VictoriaLogs
uv run python scripts/seed_docs.py      # runbook and postmortem chunks, plus demo deploys
uv run python scripts/smoke_agent.py    # scripted model through the real loop
uv run python scripts/smoke_knowledge.py  # upload a runbook, then retrieve it lexically and semantically
uv run python scripts/eval_agent.py     # qwen3:8b, scores grounding and citation resolvability
uv run python scripts/smoke_live.py     # one live run, printed end to end
```
