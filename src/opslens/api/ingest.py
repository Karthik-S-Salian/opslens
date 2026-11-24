"""Ingest control plane: presigned uploads, job dispatch, CI/CD webhooks."""

from __future__ import annotations

import asyncio
import hashlib
import re
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

import orjson
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import text

from opslens.api.auth import require_token
from opslens.ingest.worker import STREAM
from opslens.store.resources import Resources

router = APIRouter(dependencies=[Depends(require_token)])

UPLOAD_KEY = re.compile(r"^uploads/[0-9a-f-]{36}/[A-Za-z0-9._-]{1,200}$")
UPLOAD_URL_TTL = 3600


def _resources(request: Request) -> Resources:
    return request.app.state.resources


def safe_object_key(filename: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", filename.rsplit("/", 1)[-1]).lstrip(".") or "upload.log"
    return f"uploads/{uuid.uuid4()}/{name}"


class UploadRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    kind: Literal["logs"] = "logs"
    content_type: str = "application/octet-stream"


class UploadComplete(BaseModel):
    kind: Literal["logs"] = "logs"
    service: str | None = None
    env: str | None = None


class DeploymentIn(BaseModel):
    service: str
    env: str = "prod"
    version: str | None = None
    commit_sha: str | None = None
    deployed_at: datetime | None = None
    source: str = "webhook"
    external_id: str | None = None


@router.post("/uploads", status_code=201)
async def create_upload(body: UploadRequest, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    key = safe_object_key(body.filename)
    url = await asyncio.to_thread(
        resources.s3_presign.generate_presigned_url,
        "put_object",
        Params={
            "Bucket": resources.settings.s3_bucket,
            "Key": key,
            "ContentType": body.content_type,
        },
        ExpiresIn=UPLOAD_URL_TTL,
    )
    return {"key": key, "url": url, "expires_in": UPLOAD_URL_TTL}


@router.post("/uploads/{key:path}/complete", status_code=202)
async def complete_upload(key: str, body: UploadComplete, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    if not UPLOAD_KEY.match(key):
        raise HTTPException(status_code=400, detail="invalid upload key")

    try:
        head = await asyncio.to_thread(
            resources.s3.head_object, Bucket=resources.settings.s3_bucket, Key=key
        )
    except Exception as exc:  # noqa: BLE001 - any S3 error means "not uploaded"
        raise HTTPException(status_code=404, detail="object not found") from exc

    if head["ContentLength"] > resources.settings.max_upload_bytes:
        await asyncio.to_thread(
            resources.s3.delete_object, Bucket=resources.settings.s3_bucket, Key=key
        )
        raise HTTPException(status_code=413, detail="upload exceeds max_upload_bytes")

    defaults = {k: v for k, v in {"service": body.service, "env": body.env}.items() if v}
    async with resources.db.begin() as conn:
        job_id = (
            await conn.execute(
                text(
                    """
                    INSERT INTO ingest_jobs (org_id, kind, object_key, stats)
                    VALUES (:org_id, :kind, :object_key, CAST(:stats AS jsonb))
                    RETURNING id
                    """
                ),
                {
                    "org_id": resources.settings.org_id,
                    "kind": body.kind,
                    "object_key": key,
                    "stats": orjson.dumps(
                        {"defaults": defaults, "bytes": head["ContentLength"]}
                    ).decode(),
                },
            )
        ).scalar_one()

    await resources.redis.xadd(STREAM, {"job_id": str(job_id)})
    return {"job_id": str(job_id), "status": "queued"}


@router.get("/jobs/{job_id}")
async def get_job(job_id: uuid.UUID, request: Request) -> dict[str, Any]:
    resources = _resources(request)
    async with resources.db.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT id, kind, status, attempts, stats, error, created_at, updated_at"
                        " FROM ingest_jobs WHERE id = :id"
                    ),
                    {"id": job_id},
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise HTTPException(status_code=404, detail="job not found")
    return dict(row)


@router.post("/webhooks/deployments", status_code=202)
async def deployment_webhook(
    body: DeploymentIn,
    request: Request,
    x_delivery_id: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    resources = _resources(request)
    payload = body.model_dump(mode="json")
    external_id = (
        body.external_id
        or x_delivery_id
        or hashlib.sha256(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)).hexdigest()[:32]
    )
    deployed_at = body.deployed_at or datetime.now(UTC)

    async with resources.db.begin() as conn:
        row = (
            await conn.execute(
                text(
                    """
                    INSERT INTO deployments
                        (org_id, service, env, version, commit_sha, deployed_at, source,
                         external_id, payload)
                    VALUES
                        (:org_id, :service, :env, :version, :commit_sha, :deployed_at, :source,
                         :external_id, CAST(:payload AS jsonb))
                    ON CONFLICT (org_id, source, external_id) DO NOTHING
                    RETURNING id
                    """
                ),
                {
                    "org_id": resources.settings.org_id,
                    "service": body.service,
                    "env": body.env,
                    "version": body.version,
                    "commit_sha": body.commit_sha,
                    "deployed_at": deployed_at,
                    "source": body.source,
                    "external_id": external_id,
                    "payload": orjson.dumps(payload).decode(),
                },
            )
        ).scalar_one_or_none()

    return {"id": str(row) if row else None, "created": row is not None}
