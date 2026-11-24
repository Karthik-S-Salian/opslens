# OpsLens — Implementation Plan (v1)

Greenfield. Nothing exists yet; `docker`/`orbstack` is not installed on this machine (step 0).

The spec is sound. Six things in it are wrong, underspecified, or more work than they buy.
Fix those and the rest is a straight build.

---

## 1. Architecture decisions that change the spec

**D1 — Uploads go browser/CI → S3 directly (presigned PUT), not through FastAPI.**
`POST /uploads` returns a presigned PUT + object key; `POST /uploads/{key}/complete` creates the
job. FastAPI never holds a multi-GB body in memory, and you delete the "stream directly to object
storage while also accepting it as an HTTP body" contradiction in the spec. Raw objects get a
7-day lifecycle rule — the archive is a landing zone, not a second copy of every log forever.

Presigning needs two clients: the internal one (compose hostname) for I/O and a public-host one for
signing, because SigV4 covers `Host`. Hence `S3_ENDPOINT_URL` + `S3_PUBLIC_ENDPOINT_URL`.

Local object storage is SeaweedFS, not MinIO: MinIO's public images were removed from Docker Hub
and quay, and SeaweedFS is S3-compatible, so only the endpoint changes. Any S3 store drops in.

**D2 — One Redis primitive for both jobs and events: Streams + consumer group.**
Drop the separate "ingestion job queue (list) + retry/backoff locks (hashes)". A consumer group
gives you retry (`XAUTOCLAIM` after `idle>60s`), attempt counting (`XPENDING` delivery count),
and a dead-letter stream, with no extra keys. Fewer moving parts, same guarantees.

**D3 — Postgres is the source of truth for run state; the Redis Stream is a transport.**
Add `run_events(run_id, seq BIGSERIAL, type, payload JSONB)`. `seq` becomes the SSE event `id`.
Serve reconnects from Postgres when the stream entry has been trimmed; trim with
`XADD MAXLEN ~ 2000`. Without this, `Last-Event-ID` replay breaks after truncation and you lose the
audit trail the spec promises ("saves all completed turns").

**D4 — Steering interrupts generation, not tool calls.**
"Abort the active step" doubles cancellation complexity for almost no UX gain. Two-tier instead:
- `POST /steer` → `LPUSH steer:{run_id}`; drained at the top of every loop step (always).
- `POST /steer {interrupt: true}` → also sets `interrupt:{run_id}`; the **token stream loop** checks
  it per chunk and stops generation early, keeping partial text and pivoting next step.

Tools are never killed mid-flight by a steer (they are already time-boxed and capped). `/cancel`
still hard-cancels the whole task.

**D5 — The model never writes LogsQL. Ever.**
Logs and uploaded runbooks are untrusted input (prompt injection) *and* the query language is a
cost/DoS surface. Tool schemas take structured fields only (`service`, `time_from`, `time_to`,
`severity`, `text_contains`, `trace_id`, `limit`), and the server composes LogsQL. Tool output is
delimited and prefixed as untrusted data in the prompt. Bonus: VL's `extra_filters` /
`hidden_fields_filters` give you a second, out-of-band tenancy and secrets guard.

**D6 — Add a deterministic "evidence pack" before the first LLM call.**
Three cheap calls that no LLM should be spending turns on:
`/select/logsql/hits?field=service` → onset time per service; `list_deployments` within ±60m of
onset; `/select/logsql/facets` → top error signatures. Feed that as the briefing, then let the
agent dig. Biggest accuracy-per-dollar change in the whole design, and it makes the synthetic
benchmark runnable with small/local models.

### Gaps the spec has no answer for (must decide, not necessarily now)

| Gap | Decision for v1 |
|---|---|
| No trace backend, yet `fetch_trace` is a tool | Correlate by `trace_id` **inside logs** (VL query, all services, ±N minutes). Keep `TraceSource` as a one-method interface so VictoriaTraces/Tempo slots in at Phase 2. |
| `workspaces` table | Delete it. `incident` **is** the workspace (one timeline per incident). Follow-ups are `runs` with `parent_run_id` on the same incident. |
| Tenancy/auth | `org_id` on every row + VL `AccountID`, single static API key in env for v1. Cheap now, painful to retrofit. |
| Cost runaway | `runs.budget` = max_steps, max_tool_calls, max_tokens, wall_clock; loop enforces and records `stop_reason`. |
| Citation faithfulness | Per-run `evidence` registry with UUIDs; the model cites `[ev:UUID]`; a post-check validates every cited ID and drops/redacts unknown ones. Turns "grounded" into something you can measure. |
| Who opens an incident | `POST /webhooks/alerts` (Alertmanager shape) creates it. Not a product feature, just the entry point. |
| Secrets/PII in logs | Redaction pass at ingest (regex: bearer/JWT/`postgres://user:pass@`/AWS keys) *before* VL and *before* embedding. |
| Multi-line stack traces | Continuation-line join at parse time (`^\s`, `Traceback`, `Caused by:`, `at `, `goroutine \d+`, `panic:`), bounded group size. Otherwise every traceback is N useless log rows. |
| Ingestion is at-least-once | VL has no upsert. Checkpoint the byte offset in `ingest_jobs.stats` and re-ship at most the last batch on retry. Bounded dup, documented. |

### Explicitly cut from v1
PDF parsing, rerankers, multi-workspace UI, per-field log retention (VL retention is
node/tenant-level — plan a single 30-day policy), agent framework (LangChain/LlamaIndex), a
separate analytics store for spikes (`/hits` is fast enough), HNSW tuning beyond defaults.

---

## 2. Repo layout

```
opslens/
  compose.yaml                 # api, worker, postgres(pgvector), redis, victorialogs, minio
  pyproject.toml               # uv
  alembic/  migrations/
  src/opslens/
    api/        main.py incidents.py runs.py sse.py ingest.py webhooks.py deps.py
    agent/      loop.py prompts.py tools.py llm.py budget.py evidence.py
    ingest/     worker.py parsers.py normalize.py redact.py vl_client.py
    search/     logs.py knowledge.py fuse.py          # VLLogsClient, hybrid RRF
    store/      models.py db.py storage.py redisx.py
    obs/        otel.py
  tests/        unit/ integration/ fault/ bench/
```

Deps (v1 list, nothing else): `fastapi`, `uvicorn[standard]`, `sqlalchemy[asyncio]`, `asyncpg`,
`pgvector`, `alembic`, `redis`, `httpx`, `boto3`, `pydantic-settings`, `orjson`, `pytest`,
`pytest-asyncio`, `ruff`. LLM via one provider SDK behind `agent/llm.py:LLMClient` so tests drive
a `FakeLLM` (scripted turns, no network, deterministic).

---

## 3. Data model (v1)

```sql
incidents(id uuid pk, org_id, title, severity, status, started_at, detected_at, services text[], created_at)
runs(id uuid pk, incident_id fk, parent_run_id fk null, status, stop_reason,
     budget jsonb, usage jsonb, created_by, started_at, ended_at, error)
run_events(run_id fk, seq bigserial, type, payload jsonb, created_at, pk(run_id, seq))
run_messages(run_id fk, seq, role, content, tool_calls jsonb, tokens_in, tokens_out)  -- follow-up context
evidence(run_id fk, id uuid pk, kind, ref jsonb, excerpt, created_at)   -- citation registry
deployments(id uuid pk, org_id, service, env, version, commit_sha,
            deployed_at, source, external_id unique(org_id, source, external_id), payload jsonb)
ingest_jobs(id uuid pk, org_id, kind, object_key, external_id, status, attempts,
            stats jsonb, error, created_at, updated_at)
documents(id uuid pk, org_id, kind, title, service, source_key, metadata jsonb, created_at)
doc_chunks(id uuid pk, document_id fk, org_id, chunk_index, content,
           tsv tsvector generated always as (to_tsvector('english', content)) stored,
           embedding halfvec(1024))
  index on tsv gin; index on embedding hnsw (halfvec_cosine_ops); index (org_id, document_id)
```

Statuses are enums-as-text: run `queued→running→{answered|cancelled|failed|budget_exceeded}`,
job `queued→running→{done|failed|dead}`.

---

## 4. API surface (v1)

```
POST   /uploads                      -> {key, url}                    presigned PUT
POST   /uploads/{key}/complete       -> 202 {job_id}                 kind inferred or supplied
POST   /webhooks/deployments         -> 202                         idempotent on X-Delivery-Id
POST   /webhooks/alerts              -> 201 {incident_id}
POST   /incidents/{id}/runs          -> 202 {run_id}                body: question?, budget?
GET    /incidents/{id}/runs          -> list
GET    /runs/{id}                    -> status, usage, stop_reason
GET    /runs/{id}/events             -> SSE, Last-Event-ID aware
POST   /runs/{id}/steer              -> 202 {accepted}              body: {text, interrupt?}
POST   /runs/{id}/cancel             -> 202
GET    /incidents/{id}/evidence      -> registry (fetch cited artifact by id)
GET    /incidents/{id}/timeline      -> messages + tool steps, paginated
GET    /healthz                      -> vl, pg, redis, s3 checks
```

SSE frame types: `status`, `token`, `tool_start`, `tool_end`, `message`, `steer_ack`, `error`,
`done`. Heartbeat comment every 15s (proxies kill idle streams). `Cache-Control: no-cache`,
`X-Accel-Buffering: no`.

---

## 5. Agent loop (the core — get this right first)

```
run(run_id):
  ctx = load(run_id) or init(system_prompt, question)
  if no prior turns: ctx.brief = evidence_pack(incident)      # D6, deterministic, 3 queries
  for step in 0..budget.max_steps:
      if cancel_flag(run_id): return finish("cancelled")
      if budget_exceeded():    return finish("budget_exceeded")
      ctx.messages += drain_steer(run_id)                      # D4, always before the call
      async for chunk in llm.stream(ctx.messages, TOOL_SCHEMAS):  # D4 interrupt check per chunk
          stream("token", chunk)
          if interrupt_flag(run_id): break_generation = True; break
      for call in tool_calls:
          if cancel_flag: return finish("cancelled")
          result = await TOOLS[call.name](**validate(call.args))   # structured args only (D5)
          ev_ids = evidence.register(result)                        # capped: 5k chars output
          stream("tool_end", compact_summary(result, ev_ids))
          ctx.messages += tool_result(cap(result), ev_ids)
      persist(run_messages + checkpoint)                        # shield from CancelledError
              if not tool_calls: return finish("answered")
      stream("message", validate_citations(final_text))         # drop unknown ev ids
  return finish("budget_exceeded")
```

Rules:
- **Cancel**: set Redis flag (TTL 1h) → `task.cancel()`. Loop catches `CancelledError`, writes
  `cancelled`, keeps messages/evidence already persisted. Checkpoint write in `asyncio.shield`.
  One task registry `{run_id: Task}`; `/cancel` is idempotent.
- **Reconnect/follow-up**: a follow-up run reuses `run_messages` for the incident and starts a
  fresh budget. No run resumes mid-loop after a process crash (v1: mark `failed:worker_lost`).
- **Tool output caps are load-bearing**: every tool returns ≤ N rows and ≤ ~5k chars, with a
  `truncated: true` flag and histogram/counts instead of raw volume.
- **Compaction** (Phase 2): summarise tool turns older than the last 6 into one pinned block; pin
  evidence excerpts verbatim. Design the message type to allow this now (`role="tool_summary"`).

---

## 6. Tools (all read-only, all server-composed LogsQL)

| Tool | Backend | Returns |
|---|---|---|
| `search_logs(service?, severity?, text?, trace_id?, from, to, limit≤50)` | VL `/select/logsql/query` + `fields` pipe | rows + total matched + `truncated` |
| `get_error_spikes(services?, from, to, step)` | VL `/select/logsql/hits?field=service` + facets | per-service series, onset ts, baseline ratio, top signatures |
| `fetch_trace(trace_id, ±10m)` | VL query on `trace_id`, sorted | per-service ordered spans-as-log-lines |
| `list_deployments(service?, from, to)` | PG | deploys with Δt from onset |
| `search_runbooks(query, service?, k≤8)` | PG hybrid RRF | chunks + doc ids |
| `search_past_incidents(query, k≤5)` | PG hybrid RRF over postmortem docs | summaries + links |

Hybrid RRF in **one** SQL statement (no reranker, no second service):

```sql
WITH sem AS (SELECT id, row_number() OVER (ORDER BY embedding <=> $vec) rn
             FROM doc_chunks WHERE org_id=$org LIMIT 50),
     lex AS (SELECT id, row_number() OVER (ORDER BY ts_rank_cd(tsv, q) DESC) rn
             FROM doc_chunks, websearch_to_tsquery('english',$q) q
             WHERE org_id=$org AND tsv @@ q LIMIT 50)
SELECT c.*, sum(1.0/(60+rn)) score
FROM (SELECT * FROM sem UNION ALL SELECT * FROM lex) f JOIN doc_chunks c USING (id)
GROUP BY c.id ORDER BY score DESC LIMIT $k;
```

---

## 7. Ingestion pipeline

Upload → job row → `XADD jobs` → 202. Worker (`consumer group ingest`, batch 10):

1. `XAUTOCLAIM` idle>60s for crash recovery; attempts>3 → `jobs:dead` + `status=dead`.
2. `GET` object from MinIO, stream-decompress (`.gz`), sniff format
   (`json-lines` / `docker-json` / `otel-json` / plain), parse line-by-line.
3. Join multi-line continuations; normalize to `{_time (RFC3339Nano, UTC), service, env, severity,
   trace_id, request_id, message, ingest_job_id}`; keep `raw` only on parse failure.
4. Redact secrets.
5. Buffer 1000 lines → `POST /insert/jsonline?_stream_fields=service,env&_time_field=_time&_msg_field=message`
   (batch size is the throughput knob; 1k × 10 workers is plenty for v1).
6. Update `stats{lines_in, lines_ok, lines_bad, bytes_done}` every batch (UI progress), `XACK` at end.

Deploy webhooks bypass the queue (they are small): upsert on `external_id`, so a retried delivery
is a no-op.

---

## 8. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 | compose up, healthz green, Alembic baseline, CI (ruff+pytest) | `docker compose up` then `curl /healthz` → all deps ok |
| M1 | Presigned upload → job → worker → VL; deploy webhook → PG; runbook → chunks+FTS | upload 100MB `.gz` of 1M lines; `search_logs` finds a known trace_id; no line loss on injected worker kill |
| M2 | `VLLogsClient` + 6 tools as plain functions; `get_error_spikes` returns onset | seeded 24h corpus, onset within ±60s of the injected fault |
| M3 | Loop + SSE + `/cancel` with `FakeLLM` | scripted 3-tool diagnosis streams token-by-token; cancel mid-tool saves prior turns; reconnect with `Last-Event-ID` replays and does not duplicate |
| M4 | `/steer` (both tiers), follow-up runs on incident context, citations validated | steer changes the next step's tool call (asserted); unknown `[ev:id]` stripped |
| M5 | Hybrid RRF runbook/postmortem retrieval | exact error code query returns lexically-correct chunk; paraphrase returns semantically-correct one; both in top-3 |
| M6 | Evidence pack + real LLM + citation UI contract (`GET /evidence/{id}`) | end-to-end: new incident → run → answers with ≥1 log, ≥1 deploy, ≥1 runbook citation, all resolvable |
| M7 | Faults + benchmark + OTel | see §9 |

Rough effort, one developer: M0 0.5d · M1 4d · M2 3d · M3 4d · M4 2d · M5 2d · M6 3d · M7 4d
≈ 4–5 weeks to a demoable platform, plus whatever the LLM prompt work actually takes.

---

## 9. Tests & evaluation (this is what makes it credible)

- **Unit**: parsers (each format + truncated last line + multi-line java/python/go traces),
  redaction, LogsQL builder (injection attempts in `text_contains` are escaped, never concatenated),
  RRF query, budget accounting, citation validator.
- **Integration** (real pg/redis/vl/minio via compose): ingest→query round trip, spike onset,
  SSE replay, cancel race (cancel while a tool is awaiting → exactly one terminal event).
- **Fault injection** (pytest marks, not chaos infra yet): kill worker mid-batch, Redis drop mid-SSE,
  VL 503, LLM timeout, duplicate webhook, 10 concurrent runs.
- **Benchmark**: generate a synthetic 5-service outage (gateway/auth/order/payment/notification,
  24h of logs, a bad deploy at T0, a Postgres-pool-timeout signature, one red herring) with the
  answer key. Score: root-cause service hit rate, onset error, citation precision (cited evidence
  actually supports the claim), tool calls and tokens per diagnosis. Run it in CI on the PR that
  touches `agent/` or `search/`.
- **Observed vs hypothesised**: every diagnosis message carries a structured field
  `{claims: [{text, evidence_ids: [], kind: observed|hypothesis}]}`; the citation validator enforces
  that `observed` claims have ≥1 evidence id from the run's registry.

---

## 10. Risks

| Risk | Mitigation |
|---|---|
| Prompt injection via logs/runbooks | D5 (no model-authored queries, untrusted-data framing, structured citations) |
| Cost blowups on steering loops | hard budget per run, capped tool outputs, step limit, token accounting persisted |
| VL retention/reindex surprises | one 30-day policy, no per-field expectations, `_stream_fields` fixed at ingest |
| SSE through corp proxies | heartbeats, `no-buffering` headers, PG replay fallback |
| Duplicate log lines on retry | byte-offset checkpoint, bounded re-ship, documented at-least-once |
| pgvector recall on error codes | FTS branch is mandatory in the fusion, not an optional add-on |

## 11. Step 0 (today)

```bash
brew install --cask orbstack   # or Docker Desktop; no container runtime on this machine
mkdir -p ~/opslens && cd ~/opslens && uv init --package . && uv add fastapi 'uvicorn[standard]' \
  'sqlalchemy[asyncio]' asyncpg pgvector alembic redis httpx boto3 pydantic-settings orjson
uv add --dev pytest pytest-asyncio ruff
```

## 12. Deviations found while building (kept here so the doc stays true)

- `ghcr.io` hangs on this network → the Dockerfile installs `uv` via pip instead of
  `COPY --from=ghcr.io/astral-sh/uv`.
- `redis-py` 8 defaults `socket_timeout=5s`, which ties with `XREADGROUP block=5000` and kills the
  consumer on its first idle read. Set `socket_timeout=20` on the shared client.
- The worker's consumer loop wraps the body in `except Exception` + backoff; an unhandled Redis
  blip otherwise exits the process and leaves jobs stuck in `queued`.
- `docker build`/`pull` progress output is invisible when piped on this machine; redirect to a file
  and watch that instead of assuming a hang.
- Object storage: MinIO images are gone; SeaweedFS is the default. LocalStack 4.14.0 runs the flow
  but loses S3 state across a restart, and 2026.x community images require `LOCALSTACK_AUTH_TOKEN`
  (exit 55). Neither store rejects tampered presigned signatures in community mode, so signature
  enforcement can't be tested locally — do it against real S3 in CI.
- **SSE reads `run_events` from Postgres (250ms poll), not a Redis Stream.** One writer, many
  readers, and `seq` is the resume cursor, so Redis would only duplicate the source of truth.
  Revisit if a single API instance stops being enough: add `LISTEN/NOTIFY` first, a fanout layer
  only if that isn't enough.
- **Runs execute in the API process** (`asyncio.create_task` + registry), as the spec's "decoupled
  async task" allows. A restart kills in-flight runs; they stay `running` with no task. Move the
  loop into the worker process (claim `runs` from a stream) before calling this production-ready.
- **UI is a static HTML/JS page** served by FastAPI — no build step, no framework. Fine for the
  demo; a React app is a rewrite of `web/`, not a refactor.
- Embeddings: `bge-m3` (1024 dims, Ollama) matches the schema exactly. Chat: `qwen3:8b` via
  Ollama; tests use `FakeLLM`, so no network in CI.
