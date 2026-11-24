from types import SimpleNamespace

import httpx
import pytest

from opslens.api import health
from opslens.api.main import app


async def _ok(_resources) -> None:
    return None


@pytest.fixture
async def client():
    app.state.resources = SimpleNamespace()  # probes are monkeypatched in these tests
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def test_healthz_all_ok(client, monkeypatch):
    monkeypatch.setattr(health, "PROBES", {"postgres": _ok, "redis": _ok})

    resp = await client.get("/healthz")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert set(body["checks"]) == {"postgres", "redis"}
    assert body["checks"]["postgres"]["ok"] is True
    assert "ms" in body["checks"]["postgres"]


async def test_healthz_degraded(client, monkeypatch):
    async def down(_resources) -> None:
        raise ConnectionError("connection refused")

    monkeypatch.setattr(health, "PROBES", {"postgres": _ok, "redis": down})

    resp = await client.get("/healthz")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"]["ok"] is True
    assert body["checks"]["redis"]["ok"] is False
    assert body["checks"]["redis"]["error"] == "ConnectionError: connection refused"
