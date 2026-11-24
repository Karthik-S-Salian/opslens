import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from opslens.agent.llm import OllamaLLM
from opslens.api.health import router as health_router
from opslens.api.incidents import router as incidents_router
from opslens.api.ingest import router as ingest_router
from opslens.api.runs import router as runs_router
from opslens.config import get_settings
from opslens.store.resources import Resources

log = logging.getLogger("opslens")
WEB_DIR = Path(__file__).resolve().parent.parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    resources = Resources.build(settings)
    app.state.resources = resources
    app.state.llm = OllamaLLM(
        httpx.AsyncClient(
            base_url=settings.ollama_url,
            timeout=httpx.Timeout(
                connect=5.0, read=settings.llm_timeout_seconds, write=30.0, pool=5.0
            ),
        ),
        settings.chat_model,
        settings.embed_model,
    )
    await _bootstrap_bucket(resources)
    # Runs live in this process; after a restart nothing is executing, so don't leave zombies.
    async with resources.db.begin() as conn:
        await conn.execute(
            text(
                "UPDATE runs SET status = 'failed', stop_reason = 'worker_lost',"
                " ended_at = now(), error = 'api restarted'"
                " WHERE status IN ('queued', 'running')"
            )
        )
    try:
        yield
    finally:
        await resources.aclose()
        await app.state.llm.http.aclose()


async def _bootstrap_bucket(resources: Resources, attempts: int = 15) -> None:
    """Create the bucket once object storage answers; compose starts services in parallel."""
    for attempt in range(1, attempts + 1):
        try:
            await asyncio.to_thread(resources.ensure_bucket)
            return
        except Exception:  # noqa: BLE001 - keep trying, healthz reports the truth meanwhile
            if attempt == attempts:
                log.exception("bucket bootstrap failed; /healthz will report it")
            else:
                await asyncio.sleep(1)


app = FastAPI(title="OpsLens", version="0.1.0", lifespan=lifespan)
app.include_router(health_router)
app.include_router(ingest_router)
app.include_router(incidents_router)
app.include_router(runs_router)

# Mounted last: everything above is matched first.
if WEB_DIR.is_dir():
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="ui")
