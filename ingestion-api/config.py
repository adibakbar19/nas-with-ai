"""Service configuration — reads from environment variables."""

from __future__ import annotations

import os
import re


_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _build_dsn() -> str:
    """Build PostgreSQL DSN from environment variables.

    NOTE: This is intentionally duplicated from shared/nas_config/db.py.
    ingestion-api has a separate Docker build context and cannot install
    shared packages without restructuring the build. If the DSN logic
    ever changes, update both this function and shared/nas_config/db.py.

    Tracked in: docs/deferred/shared_package_docker_build.md
    """
    for key in ("POSTGRES_DSN", "DATABASE_URL", "INGEST_JOB_STATE_DSN"):
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
    """Settings for ingestion-api. Extends as routes are added."""

    HOST: str = os.environ.get("HOST", "0.0.0.0")
    PORT: int = int(os.environ.get("PORT", "3000"))
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "info")

    # Keycloak
    KEYCLOAK_URL: str = os.environ.get("KEYCLOAK_URL", "").rstrip("/")
    KEYCLOAK_AUDIENCE: str = os.environ.get("KEYCLOAK_AUDIENCE", "ingestion-api")

    # Redis (for auth routing lookup)
    REDIS_URL: str = os.environ.get("REDIS_URL", "redis://valkey:6379/0")

    # Postgres (for job state, multipart sessions, idempotency)
    POSTGRES_DSN: str = _build_dsn()

    @staticmethod
    def postgres_schema() -> str:
        schema = (
            os.environ.get("INGEST_JOB_STATE_SCHEMA")
            or os.environ.get("PGSCHEMA")
            or "ingest"
        ).strip() or "ingest"
        if not _SCHEMA_NAME_RE.fullmatch(schema):
            raise ValueError(f"Invalid Postgres schema name: {schema!r}")
        return schema

    # Valkey stream (for queueing ingest jobs)
    VALKEY_URL: str = os.environ.get("VALKEY_URL", "") or os.environ.get("REDIS_URL", "redis://valkey:6379/0")
    VALKEY_STREAM_KEY: str = os.environ.get("VALKEY_STREAM_KEY", "bulk_ingest_events")
    VALKEY_STREAM_GROUP: str = os.environ.get("VALKEY_STREAM_GROUP", "bulk_ingest_workers")


settings = Settings()
