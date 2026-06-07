from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8004
    kafka_brokers: str = "redpanda:9092"
    kafka_group_id: str = "event-streaming-service"
    kafka_topics: list[str] = [
        "nas.address.events",
        "nas.job.events",
        "nas.match.events",
    ]
    postgres_dsn: str = "postgresql+psycopg://nas:nas@postgres:5432/nas"
    postgres_schema: str = "nas"
    api_keys: str = ""  # comma-separated list of valid API keys
    webhook_timeout_seconds: int = 10
    webhook_max_attempts: int = 5
    log_level: str = "info"

    class Config:
        env_prefix = "ESS_"
        env_file = ".env"


settings = Settings()
