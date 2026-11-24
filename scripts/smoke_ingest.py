"""End-to-end ingest smoke test: presign -> upload -> complete -> worker -> VictoriaLogs.

Run against a live stack: uv run python scripts/smoke_ingest.py
"""

import gzip
import json
import time
from datetime import UTC, datetime, timedelta

import httpx

from opslens.config import get_settings

API = "http://localhost:8000"
VL = "http://localhost:9428"
LINES = 5000


def build_log() -> bytes:
    start = datetime.now(UTC) - timedelta(minutes=10)
    out: list[str] = []
    for i in range(LINES):
        ts = (start + timedelta(milliseconds=i)).isoformat().replace("+00:00", "Z")
        out.append(
            json.dumps(
                {
                    "timestamp": ts,
                    "level": "ERROR" if i % 50 == 0 else "INFO",
                    "service": "payment",
                    "env": "prod",
                    "trace_id": f"trace-{i:05d}",
                    "message": f"charge attempt {i} took {i % 97}ms",
                }
            )
        )
        if i == 100:
            out += [
                "2026-11-24T10:36:01Z ERROR [payment] pool timeout after password=hunter2",
                "Traceback (most recent call last):",
                '  File "/app/payment/db.py", line 42, in acquire',
                "TimeoutError: connection pool exhausted",
            ]
    return gzip.compress(("\n".join(out) + "\n").encode())


def main() -> None:
    body = build_log()
    headers = {"Authorization": f"Bearer {get_settings().api_token}"}
    with httpx.Client(timeout=60, headers=headers) as http:
        upload = http.post(
            f"{API}/uploads",
            json={"filename": "smoke.log.gz", "content_type": "application/gzip"},
        )
        upload.raise_for_status()
        target = upload.json()

        put = http.put(target["url"], content=body, headers={"Content-Type": "application/gzip"})
        assert put.status_code == 200, f"PUT failed: {put.status_code} {put.text}"

        complete = http.post(
            f"{API}/uploads/{target['key']}/complete",
            json={"kind": "logs", "service": "payment", "env": "prod"},
        )
        complete.raise_for_status()
        job_id = complete.json()["job_id"]
        print(f"job {job_id} queued ({len(body)} bytes)")

        for _ in range(120):
            status = http.get(f"{API}/jobs/{job_id}").json()
            if status["status"] in ("done", "dead"):
                break
            time.sleep(0.5)
        print("job status:", status["status"], status["stats"], status.get("error") or "")
        assert status["status"] == "done", status

        for _ in range(20):
            rows = http.get(
                f"{VL}/select/logsql/query",
                params={"query": f"ingest_job_id:{job_id}", "limit": LINES + 100},
            )
            found = [json.loads(line) for line in rows.text.splitlines() if line.strip()]
            if len(found) >= LINES:
                break
            time.sleep(1)

        print(f"log rows in VictoriaLogs: {len(found)}")
        traceback_rows = [r for r in found if "Traceback" in r.get("_msg", "")]
        assert len(found) == LINES + 1, f"expected {LINES + 1} records, got {len(found)}"
        assert len(traceback_rows) == 1, "traceback block was not joined into one record"
        assert "TimeoutError" in traceback_rows[0]["_msg"]
        assert traceback_rows[0]["severity"] == "ERROR"
        assert traceback_rows[0]["service"] == "payment"
        # secrets are scrubbed before storage
        assert "hunter2" not in traceback_rows[0]["_msg"]
        assert "password: [redacted]" in traceback_rows[0]["_msg"]
        print("OK: json + prefixed text + multi-line traceback all landed, secrets redacted")


if __name__ == "__main__":
    main()
