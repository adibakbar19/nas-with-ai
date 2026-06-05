"""Read API configuration — reads from environment variables."""

from __future__ import annotations

import os
import re


_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _build_dsn() -> str:
    """Build PostgreSQL DSN from environment variables.

    NOTE: This is intentionally duplicated from shared/nas_config/db.py.
    nas-processor read API has a separate Docker build context and cannot install
    shared packages without restructuring the build. If the DSN logic
    ever changes, update both this function and shared/nas_config/db.py.

    Tracked in: docs/deferred/shared_package_docker_build.md
    """
    for key in ("POSTGRES_DSN", "DATABASE_URL"):
        raw = os.environ.get(key, "").strip()
        if raw:
            return raw
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    password = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    database = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class Settings:
    """Settings for nas-processor read API."""

    HOST: str = os.environ.get("HOST", "0.0.0.0")
    PORT: int = int(os.environ.get("PORT", "8001"))
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "info")

    # Keycloak
    KEYCLOAK_URL: str = os.environ.get("KEYCLOAK_URL", "").rstrip("/")
    KEYCLOAK_AUDIENCE: str = os.environ.get("KEYCLOAK_AUDIENCE", "nas-processor-api")

    # Redis (for auth routing lookup)
    REDIS_URL: str = os.environ.get("REDIS_URL", "redis://valkey:6379/0")

    # Postgres
    POSTGRES_DSN: str = _build_dsn()

    @staticmethod
    def postgres_schema() -> str:
        schema = (
            os.environ.get("PGSCHEMA") or "nas"
        ).strip() or "nas"
        if not _SCHEMA_NAME_RE.fullmatch(schema):
            raise ValueError(f"Invalid Postgres schema name: {schema!r}")
        return schema

    @staticmethod
    def lookup_schema() -> str:
        schema = (
            os.environ.get("LOOKUP_SCHEMA") or "nas_lookup"
        ).strip() or "nas_lookup"
        if not _SCHEMA_NAME_RE.fullmatch(schema):
            raise ValueError(f"Invalid Postgres schema name: {schema!r}")
        return schema

    # OpenSearch
    ES_URL: str = os.environ.get("ES_URL", "http://opensearch:9200").rstrip("/")
    ES_INDEX: str = os.environ.get("ES_INDEX", "nas_addresses")


settings = Settings()
