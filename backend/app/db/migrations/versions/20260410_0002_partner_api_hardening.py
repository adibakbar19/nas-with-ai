"""Add agency scoping and idempotency persistence.

Revision ID: 20260410_0002
Revises: 20260407_0001
Create Date: 2026-04-10 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260410_0002"
down_revision = "20260407_0001"
branch_labels = None
depends_on = None

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _schema() -> str:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    if not _IDENT_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def _qualified_table(schema: str, table_name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table_name)}"


def upgrade() -> None:
    schema = _schema()
    ingest_job = _qualified_table(schema, "ingest_job")
    multipart_upload_session = _qualified_table(schema, "multipart_upload_session")
    api_idempotency_request = _qualified_table(schema, "api_idempotency_request")

    op.execute(f"ALTER TABLE {ingest_job} ADD COLUMN IF NOT EXISTS agency_id TEXT NULL")
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS "ingest_job_agency_created_at_idx"
        ON {ingest_job} (agency_id, created_at DESC NULLS LAST, updated_at DESC)
        """
    )

    op.execute(f"ALTER TABLE {multipart_upload_session} ADD COLUMN IF NOT EXISTS agency_id TEXT NULL")
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS "multipart_upload_agency_status_idx"
        ON {multipart_upload_session} (agency_id, status, updated_at DESC)
        """
    )

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {api_idempotency_request} (
          agency_id TEXT NOT NULL,
          operation TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          request_fingerprint TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          resource_type TEXT NULL,
          resource_id TEXT NULL,
          response JSONB NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (agency_id, operation, idempotency_key)
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS "api_idempotency_agency_updated_idx"
        ON {api_idempotency_request} (agency_id, updated_at DESC)
        """
    )


def downgrade() -> None:
    schema = _schema()
    ingest_job = _qualified_table(schema, "ingest_job")
    multipart_upload_session = _qualified_table(schema, "multipart_upload_session")
    api_idempotency_request = _qualified_table(schema, "api_idempotency_request")

    op.execute('DROP INDEX IF EXISTS "api_idempotency_agency_updated_idx"')
    op.execute(f"DROP TABLE IF EXISTS {api_idempotency_request}")

    op.execute('DROP INDEX IF EXISTS "multipart_upload_agency_status_idx"')
    op.execute(f"ALTER TABLE {multipart_upload_session} DROP COLUMN IF EXISTS agency_id")

    op.execute('DROP INDEX IF EXISTS "ingest_job_agency_created_at_idx"')
    op.execute(f"ALTER TABLE {ingest_job} DROP COLUMN IF EXISTS agency_id")
