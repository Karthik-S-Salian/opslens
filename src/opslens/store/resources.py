"""Shared clients, created once at app startup."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3
import httpx
from botocore.client import Config
from botocore.exceptions import ClientError
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from opslens.config import Settings


@dataclass(slots=True)
class Resources:
    settings: Settings
    db: AsyncEngine
    redis: Redis
    http: httpx.AsyncClient
    s3: Any
    s3_presign: Any

    @classmethod
    def build(cls, settings: Settings) -> Resources:
        def client(endpoint: str) -> Any:
            return boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                region_name="us-east-1",
                # Virtual-host addressing doesn't resolve for localhost/compose hosts.
                config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
            )

        return cls(
            settings=settings,
            db=create_async_engine(settings.database_url, pool_pre_ping=True),
            redis=Redis.from_url(
                settings.redis_url,
                decode_responses=True,
                # redis-py defaults socket_timeout=5s, tying with XREADGROUP(block=5000).
                socket_timeout=20,
                socket_connect_timeout=5,
            ),
            http=httpx.AsyncClient(base_url=settings.victorialogs_url, timeout=10.0),
            s3=client(settings.s3_endpoint_url),
            # SigV4 covers the Host header, so browser-facing URLs need a client on the public host.
            s3_presign=client(settings.s3_public_endpoint_url or settings.s3_endpoint_url),
        )

    def ensure_bucket(self) -> None:
        try:
            self.s3.head_bucket(Bucket=self.settings.s3_bucket)
        except ClientError:
            self.s3.create_bucket(Bucket=self.settings.s3_bucket)

    async def aclose(self) -> None:
        await self.db.dispose()
        await self.redis.aclose()
        await self.http.aclose()
        self.s3.close()
        self.s3_presign.close()
