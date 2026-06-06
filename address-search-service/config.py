"""Address Search Service configuration — reads from SEARCH_* environment variables."""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Server
    host: str = "0.0.0.0"
    port: int = 8003
    log_level: str = "info"

    # OpenSearch
    opensearch_url: str = "http://opensearch:9200"
    opensearch_index: str = "nas_addresses"
    opensearch_timeout: int = 10

    # Postgres (read-only, for spatial/geocode queries)
    postgres_dsn: str = "postgresql://nas:nas@postgres:5432/nas"
    postgres_schema: str = "nas"
    postgres_lookup_schema: str = "nas_lookup"

    # Cache (Valkey/Redis)
    redis_url: str = "redis://valkey:6379/0"
    cache_ttl_seconds: int = 600

    # Auth
    auth_enabled: bool = True
    api_key_header: str = "X-API-Key"
    api_keys: list[str] = []

    # Search defaults
    default_limit: int = 10
    max_limit: int = 100
    max_batch_size: int = 100
    batch_timeout_seconds: int = 10

    # Spatial defaults
    default_radius_m: int = 500
    max_radius_m: int = 5000

    model_config = {"env_prefix": "SEARCH_"}

    @field_validator("api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, v):
        if isinstance(v, str):
            return [k.strip() for k in v.split(",") if k.strip()]
        return v or []


settings = Settings()
