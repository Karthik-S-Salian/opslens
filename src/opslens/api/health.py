"""Dependency probes: one per backing service."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from opslens.store.resources import Resources

router = APIRouter()

Probe = Callable[[Resources], Awaitable[None]]


async def _postgres(r: Resources) -> None:
    async with r.db.connect() as conn:
        await conn.exec_driver_sql("select 1")


async def _redis(r: Resources) -> None:
    await r.redis.ping()


async def _victorialogs(r: Resources) -> None:
    resp = await r.http.get("/health", timeout=2.0)
    resp.raise_for_status()


async def _s3(r: Resources) -> None:
    await asyncio.to_thread(r.s3.head_bucket, Bucket=r.settings.s3_bucket)


PROBES: dict[str, Probe] = {
    "postgres": _postgres,
    "redis": _redis,
    "victorialogs": _victorialogs,
    "s3": _s3,
}


@router.get("/healthz")
async def healthz(request: Request) -> JSONResponse:
    resources: Resources = request.app.state.resources

    async def run(name: str, probe: Probe) -> tuple[str, dict]:
        started = time.perf_counter()
        try:
            await asyncio.wait_for(probe(resources), timeout=3.0)
            result: dict = {"ok": True}
        except Exception as exc:  # noqa: BLE001 - a probe failure is data, not a crash
            result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}
        result["ms"] = round((time.perf_counter() - started) * 1000, 1)
        return name, result

    checks = dict(await asyncio.gather(*(run(n, p) for n, p in PROBES.items())))
    ok = all(c["ok"] for c in checks.values())
    return JSONResponse(
        {"status": "ok" if ok else "degraded", "checks": checks},
        status_code=200 if ok else 503,
    )
