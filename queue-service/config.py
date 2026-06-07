from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8005
    log_level: str = "info"

    # Postgres
    postgres_dsn: str = "postgresql+psycopg://nas:nas@postgres:5432/nas"
    job_schema: str = "ingest"

    # Valkey
    valkey_url: str = "redis://valkey:6379/0"
    stream_key: str = "bulk_ingest_events"
    stream_group: str = "bulk_ingest_workers"

    # Job config
    job_heartbeat_timeout_seconds: int = 120
    job_max_retries: int = 3
    retry_backoff_seconds: str = "60,300,900"  # comma-sep to avoid list parsing issues
    dlq_after_retries: int = 3

    # Internal service auth — callers must pass X-Service-Key header
    service_keys: str = "internal-dev-key"  # comma-separated

    class Config:
        env_prefix = "QS_"

    def get_retry_backoff(self) -> list[int]:
        return [int(x.strip()) for x in self.retry_backoff_seconds.split(",") if x.strip()]

    def get_service_keys(self) -> list[str]:
        return [k.strip() for k in self.service_keys.split(",") if k.strip()]


settings = Settings()
