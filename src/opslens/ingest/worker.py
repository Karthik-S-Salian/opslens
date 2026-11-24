"""Ingest worker: claims jobs from a Redis Stream consumer group and ships lines to VictoriaLogs."""

from __future__ import annotations

import asyncio
import gzip
import logging
import re
import socket
from collections.abc import AsyncIterator
from typing import Any

import httpx
import orjson
from redis.exceptions import ResponseError
from sqlalchemy import text

from opslens.agent.llm import OllamaLLM
from opslens.config import Settings, get_settings
from opslens.ingest.docs import chunk
from opslens.ingest.parse import LineAssembler
from opslens.ingest.redact import redact
from opslens.ingest.vl import VictoriaLogs
from opslens.store.resources import Resources

log = logging.getLogger("opslens.worker")

STREAM = "jobs:ingest"
GROUP = "ingest"
CONSUMER = f"worker-{socket.gethostname()}"
BATCH = 1000
CHUNK = 1 << 20
MAX_ATTEMPTS = 3
CLAIM_IDLE_MS = 60_000
MAX_DOC_BYTES = 20 * 1024 * 1024
EMBED_BATCH = 16


class PrefixedReader:
    """Replays bytes already read while sniffing, then continues from the underlying stream."""

    def __init__(self, prefix: bytes, inner: Any) -> None:
        self._prefix = prefix
        self._inner = inner

    def read(self, size: int = -1) -> bytes:
        if not self._prefix:
            return self._inner.read(size)
        if size is None or size < 0:
            data, self._prefix = self._prefix, b""
            return data + self._inner.read()
        data, self._prefix = self._prefix[:size], self._prefix[size:]
        if len(data) < size:
            data += self._inner.read(size - len(data))
        return data


async def iter_lines(reader: Any) -> AsyncIterator[str]:
    buffer = b""
    while True:
        chunk = await asyncio.to_thread(reader.read, CHUNK)
        if not chunk:
            break
        buffer += chunk
        *complete, buffer = buffer.split(b"\n")
        for line in complete:
            yield line.decode("utf-8", "replace")
    if buffer:
        yield buffer.decode("utf-8", "replace")


def _open(object_body: Any, head: bytes) -> Any:
    reader = PrefixedReader(head, object_body)
    return gzip.GzipFile(fileobj=reader) if head[:2] == b"\x1f\x8b" else reader


async def process_job(
    resources: Resources, vl: VictoriaLogs, job: dict[str, Any]
) -> dict[str, int]:
    defaults = (job["stats"] or {}).get("defaults") or {}
    body = (
        await asyncio.to_thread(
            resources.s3.get_object, Bucket=resources.settings.s3_bucket, Key=job["object_key"]
        )
    )["Body"]
    head = await asyncio.to_thread(body.read, CHUNK)
    assembler = LineAssembler(defaults=defaults)

    stats = {"lines_in": 0, "lines_ok": 0, "lines_bad": 0}
    batch: list[dict[str, Any]] = []

    async def flush() -> None:
        if batch:
            await vl.insert(batch)
            batch.clear()

    async for line in iter_lines(_open(body, head)):
        stats["lines_in"] += 1
        for record in assembler.feed(line):
            stats["lines_ok"] += 1
            stats["lines_bad"] += 1 if record.get("parse_error") else 0
            record["message"] = redact(record["message"])
            record["ingest_job_id"] = str(job["id"])
            batch.append(record)
        if len(batch) >= BATCH:
            await flush()
            await _update_job(resources, job["id"], stats=stats)

    for record in assembler.flush():
        stats["lines_ok"] += 1
        record["message"] = redact(record["message"])
        record["ingest_job_id"] = str(job["id"])
        batch.append(record)
    await flush()
    return stats


def _doc_key(kind: str, service: str | None, title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:120] or "untitled"
    return f"{kind}:{service or '-'}:{slug}"


async def process_knowledge_job(
    resources: Resources, llm: OllamaLLM, job: dict[str, Any]
) -> dict[str, int]:
    defaults = (job["stats"] or {}).get("defaults") or {}
    title = defaults.get("title") or job["object_key"].rsplit("/", 1)[-1]
    service = defaults.get("service")
    size = int((job["stats"] or {}).get("bytes") or 0)
    if size > MAX_DOC_BYTES:
        raise ValueError(f"document is {size} bytes, limit is {MAX_DOC_BYTES}")

    body = (
        await asyncio.to_thread(
            resources.s3.get_object, Bucket=resources.settings.s3_bucket, Key=job["object_key"]
        )
    )["Body"]
    raw = await asyncio.to_thread(body.read)
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    text_value = raw.decode("utf-8", "replace")
    pieces = chunk(text_value)
    if not pieces:
        raise ValueError("document has no text")

    embeddings: list[list[float]] = []
    for start in range(0, len(pieces), EMBED_BATCH):
        embeddings.extend(await llm.embed(pieces[start : start + EMBED_BATCH]))

    source_key = _doc_key(job["kind"], service, title)
    async with resources.db.begin() as conn:
        document_id = (
            await conn.execute(
                text(
                    """
                    INSERT INTO documents (org_id, kind, title, service, source_key)
                    VALUES (:org, :kind, :title, :service, :key)
                    ON CONFLICT (org_id, source_key) DO UPDATE SET
                        title = EXCLUDED.title, service = EXCLUDED.service,
                        kind = EXCLUDED.kind, metadata = documents.metadata
                    RETURNING id
                    """
                ),
                {
                    "org": resources.settings.org_id,
                    "kind": job["kind"],
                    "title": title,
                    "service": service,
                    "key": source_key,
                },
            )
        ).scalar_one()
        await conn.execute(
            text("DELETE FROM doc_chunks WHERE document_id = :id"), {"id": document_id}
        )
        for index, (piece, embedding) in enumerate(zip(pieces, embeddings, strict=True)):
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
                    "org": resources.settings.org_id,
                    "idx": index,
                    "content": piece,
                    "vec": "[" + ",".join(f"{v:.6f}" for v in embedding) + "]",
                },
            )
    return {"chunks": len(pieces), "chars": len(text_value)}


async def _update_job(resources: Resources, job_id: Any, **fields: Any) -> None:
    params: dict[str, Any] = {}
    assignments: list[str] = []
    for name, value in fields.items():
        if isinstance(value, dict):
            assignments.append(f"{name} = CAST(:{name} AS jsonb)")
            params[name] = orjson.dumps(value).decode()
        else:
            assignments.append(f"{name} = :{name}")
            params[name] = value
    statement = (
        f"UPDATE ingest_jobs SET {', '.join(assignments)}, updated_at = now() WHERE id = :job_id"
    )
    async with resources.db.begin() as conn:
        await conn.execute(text(statement), {**params, "job_id": job_id})


async def _job(resources: Resources, job_id: str) -> dict[str, Any] | None:
    async with resources.db.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT id, status, attempts, stats, object_key, kind"
                        " FROM ingest_jobs WHERE id = :id"
                    ),
                    {"id": job_id},
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


async def handle(
    resources: Resources, vl: VictoriaLogs, llm: OllamaLLM, entry_id: str, fields: dict[str, str]
) -> None:
    job_id = fields.get("job_id")
    if not job_id:
        await resources.redis.xack(STREAM, GROUP, entry_id)
        return

    job = await _job(resources, job_id)
    if job is None or job["status"] in ("done", "dead"):
        await resources.redis.xack(STREAM, GROUP, entry_id)
        return

    attempts = job["attempts"] + 1
    if attempts > MAX_ATTEMPTS:
        await _update_job(resources, job_id, status="dead", error="max attempts exceeded")
        await resources.redis.xack(STREAM, GROUP, entry_id)
        log.error("job %s dead after %s attempts", job_id, attempts)
        return

    await _update_job(resources, job_id, status="running", attempts=attempts, error=None)
    try:
        if job["kind"] == "logs":
            stats = await process_job(resources, vl, job)
        else:
            stats = await process_knowledge_job(resources, llm, job)
    except Exception as exc:  # noqa: BLE001 - retried via the pending list, then dead
        await _update_job(
            resources, job_id, status="queued", error=f"{type(exc).__name__}: {exc}"[:500]
        )
        log.exception("job %s failed (attempt %s)", job_id, attempts)
        return
    await _update_job(resources, job_id, status="done", stats=stats)
    await resources.redis.xack(STREAM, GROUP, entry_id)
    log.info("job %s done: %s", job_id, stats)


async def run(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    resources = Resources.build(settings)
    vl = VictoriaLogs(resources.http)
    ollama = httpx.AsyncClient(base_url=settings.ollama_url, timeout=settings.llm_timeout_seconds)
    llm = OllamaLLM(ollama, settings.chat_model, settings.embed_model)
    try:
        try:
            await resources.redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        log.info("consumer %s waiting on %s", CONSUMER, STREAM)
        while True:
            try:
                _, claimed, _ = await resources.redis.xautoclaim(
                    STREAM, GROUP, CONSUMER, min_idle_time=CLAIM_IDLE_MS, start_id="0-0", count=10
                )
                for entry_id, fields in claimed:
                    await handle(resources, vl, llm, entry_id, fields)

                response = await resources.redis.xreadgroup(
                    GROUP, CONSUMER, {STREAM: ">"}, count=5, block=5000
                )
                for _, entries in response or []:
                    for entry_id, fields in entries:
                        await handle(resources, vl, llm, entry_id, fields)
            except Exception:  # noqa: BLE001 - a Redis blip must not kill the consumer
                log.exception("consumer loop error; retrying")
                await asyncio.sleep(2)
    finally:
        await resources.aclose()
        await ollama.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run())


if __name__ == "__main__":
    main()
