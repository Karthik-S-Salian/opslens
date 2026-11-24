"""Worker control flow with fake storage: retries, dead-lettering, acks."""

from types import SimpleNamespace

import pytest

from opslens.ingest import worker


class FakeRedis:
    def __init__(self) -> None:
        self.acked: list[str] = []

    async def xack(self, stream, group, entry_id) -> None:
        self.acked.append(entry_id)


@pytest.fixture
def env(monkeypatch):
    updates: list[dict] = []
    job = {"id": "job-1", "status": "queued", "attempts": 0, "stats": {}, "object_key": "k"}

    async def fake_job(resources, job_id):
        return dict(job)

    async def fake_update(resources, job_id, **fields):
        updates.append(fields)

    monkeypatch.setattr(worker, "_job", fake_job)
    monkeypatch.setattr(worker, "_update_job", fake_update)
    return SimpleNamespace(job=job, updates=updates, redis=FakeRedis()), updates


async def test_success_marks_done_and_acks(monkeypatch, env):
    resources, updates = env

    async def ok(*_args, **_kwargs):
        return {"lines_in": 10, "lines_ok": 10, "lines_bad": 0}

    monkeypatch.setattr(worker, "process_job", ok)

    await worker.handle(resources, None, "1-0", {"job_id": "job-1"})

    assert resources.redis.acked == ["1-0"]
    assert updates[-1] == {
        "status": "done",
        "stats": {"lines_in": 10, "lines_ok": 10, "lines_bad": 0},
    }


async def test_failure_requeues_without_ack(monkeypatch, env):
    resources, updates = env

    async def boom(*_args, **_kwargs):
        raise RuntimeError("victorialogs down")

    monkeypatch.setattr(worker, "process_job", boom)

    await worker.handle(resources, None, "1-0", {"job_id": "job-1"})

    assert resources.redis.acked == []  # pending entry is retried by XAUTOCLAIM
    assert updates[-1]["status"] == "queued"
    assert updates[-1]["error"] == "RuntimeError: victorialogs down"
    assert {"status": "running", "attempts": 1, "error": None} in updates


async def test_dead_after_max_attempts(monkeypatch, env):
    resources, updates = env
    resources.job["attempts"] = worker.MAX_ATTEMPTS

    await worker.handle(resources, None, "1-0", {"job_id": "job-1"})

    assert resources.redis.acked == ["1-0"]
    assert updates[-1] == {"status": "dead", "error": "max attempts exceeded"}


async def test_finished_job_is_acked_without_work(monkeypatch, env):
    resources, updates = env
    resources.job["status"] = "done"

    async def must_not_run(*_args, **_kwargs):
        raise AssertionError("should not process a finished job")

    monkeypatch.setattr(worker, "process_job", must_not_run)

    await worker.handle(resources, None, "1-0", {"job_id": "job-1"})

    assert resources.redis.acked == ["1-0"]
    assert updates == []
