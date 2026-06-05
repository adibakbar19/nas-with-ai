"""Create ingest schema and tables.

Revision ID: 20260605_0001
Revises:
Create Date: 2026-06-05 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260605_0001"
down_revision = None
branch_labels = None
depends_on = None

_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _SCHEMA_NAME_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _schema() -> str:
    schema = op.get_context().config.get_main_option("ingest_schema") or "ingest"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def _qualified(schema: str, table_name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table_name)}"


def upgrade() -> None:
    schema = _schema()

    ingest_job = _qualified(schema, "ingest_job")
    multipart = _qualified(schema, "multipart_upload_session")

    op.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}")

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ingest_job} (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'unknown',
            created_at TIMESTAMPTZ NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            data JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )
        """
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS ingest_job_status_idx ON {ingest_job} (status)"
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS ingest_job_created_at_idx
        ON {ingest_job} (created_at DESC NULLS LAST, updated_at DESC)
        """
    )

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {multipart} (
            session_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'initiated',
            bucket TEXT NOT NULL,
            object_name TEXT NOT NULL,
            upload_id TEXT NOT NULL,
            file_name TEXT NOT NULL,
            content_type TEXT NULL,
            content_bytes BIGINT NOT NULL,
            part_size INTEGER NOT NULL,
            job_id TEXT NULL,
            data JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS multipart_upload_status_idx
        ON {multipart} (status, updated_at DESC)
        """
    )


def downgrade() -> None:
    schema = _schema()

    multipart = _qualified(schema, "multipart_upload_session")
    ingest_job = _qualified(schema, "ingest_job")

    op.execute(f"DROP INDEX IF EXISTS {_quote_ident(schema)}.multipart_upload_status_idx")
    op.execute(f"DROP TABLE IF EXISTS {multipart}")
    op.execute(f"DROP INDEX IF EXISTS {_quote_ident(schema)}.ingest_job_created_at_idx")
    op.execute(f"DROP INDEX IF EXISTS {_quote_ident(schema)}.ingest_job_status_idx")
    op.execute(f"DROP TABLE IF EXISTS {ingest_job}")
    op.execute(f"DROP SCHEMA IF EXISTS {_quote_ident(schema)}")
