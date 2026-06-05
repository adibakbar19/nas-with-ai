"""Drop ingest tables from nas schema.

These tables have been migrated to the dedicated 'ingest' schema
managed by ingestion-api's own Alembic setup.

Revision ID: 20260605_0002
Revises: 20260604_0009
Create Date: 2026-06-05 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260605_0002"
down_revision = "20260604_0009"
branch_labels = None
depends_on = None

_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _SCHEMA_NAME_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _schema() -> str:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def upgrade() -> None:
    schema = _schema()
    s = _quote_ident(schema)

    op.execute(f'DROP INDEX IF EXISTS {s}."multipart_upload_agency_status_idx"')
    op.execute(f'DROP INDEX IF EXISTS {s}."multipart_upload_status_idx"')
    op.execute(f"DROP TABLE IF EXISTS {s}.\"multipart_upload_session\"")
    op.execute(f'DROP INDEX IF EXISTS {s}."ingest_job_agency_created_at_idx"')
    op.execute(f'DROP INDEX IF EXISTS {s}."ingest_job_created_at_idx"')
    op.execute(f'DROP INDEX IF EXISTS {s}."ingest_job_status_idx"')
    op.execute(f"DROP TABLE IF EXISTS {s}.\"ingest_job\"")


def downgrade() -> None:
    # Recreating these tables is handled by migration 20260407_0001.
    # A downgrade here would conflict. No-op intentionally.
    pass
