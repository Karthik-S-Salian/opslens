from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://opslens:opslens@localhost:5432/opslens"
    redis_url: str = "redis://localhost:6379/0"
    victorialogs_url: str = "http://localhost:9428"
    s3_endpoint_url: str = "http://localhost:8333"
    # Host the *client* uses to reach S3; presigned URLs are signed against this one.
    s3_public_endpoint_url: str | None = None
    s3_access_key: str = "opslens"
    s3_secret_key: str = "opslens123"
    s3_bucket: str = "opslens"
    org_id: str = "default"
    max_upload_bytes: int = 5 * 1024**3
    log_level: str = "INFO"

    # Auth: static bearer token for v1 (empty string disables the check).
    api_token: str = "dev-token"

    # LLM (Ollama). bge-m3 is 1024-dim, matching doc_chunks.embedding halfvec(1024).
    ollama_url: str = "http://localhost:11434"
    chat_model: str = "qwen3:8b"
    embed_model: str = "bge-m3"
    embed_dim: int = 1024
    llm_timeout_seconds: float = 300.0

    # Per-run budget, enforced by the agent loop.
    max_steps: int = 8
    max_tool_calls: int = 14
    max_run_seconds: int = 300


@lru_cache
def get_settings() -> Settings:
    return Settings()
