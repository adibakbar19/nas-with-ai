"""Alembic environment configuration for ingestion-api.

Reads DB connection from environment variables (same pattern as the service).
Targets the 'ingest' schema.
"""

from __future__ import annotations

import os
import re
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None

_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _build_dsn() -> str:
    """Build PostgreSQL DSN from environment variables."""
    for key in ("POSTGRES_DSN", "DATABASE_URL", "INGEST_JOB_STATE_DSN"):
        raw = os.environ.get(key, "").strip()
        if raw:
            if raw.startswith("postgresql+"):
                return raw
            if raw.startswith("postgres://"):
                return "postgresql+psycopg://" + raw[len("postgres://"):]
            if raw.startswith("postgresql://"):
                return "postgresql+psycopg://" + raw[len("postgresql://"):]
            return raw
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    password = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    database = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def _schema() -> str:
    """Return the target schema (default: ingest)."""
    schema = (
        os.environ.get("INGEST_JOB_STATE_SCHEMA")
        or "ingest"
    ).strip() or "ingest"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


database_url = _build_dsn()
schema_name = _schema()

config.set_main_option("sqlalchemy.url", database_url)
config.set_main_option("ingest_schema", schema_name)


def run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(database_url, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
