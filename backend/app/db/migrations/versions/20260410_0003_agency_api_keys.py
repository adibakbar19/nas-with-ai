"""Create DB-backed agency API keys.

Revision ID: 20260410_0003
Revises: 20260410_0002
Create Date: 2026-04-10 00:30:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260410_0003"
down_revision = "20260410_0002"
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
    agency_api_key = _qualified_table(schema, "agency_api_key")

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {agency_api_key} (
          key_id TEXT PRIMARY KEY,
          agency_id TEXT NOT NULL,
          label TEXT NULL,
          secret_hash TEXT NOT NULL,
          secret_preview TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_by TEXT NULL,
          revoked_reason TEXT NULL,
          metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_used_at TIMESTAMPTZ NULL,
          expires_at TIMESTAMPTZ NULL,
          revoked_at TIMESTAMPTZ NULL
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS "agency_api_key_agency_status_created_idx"
        ON {agency_api_key} (agency_id, status, created_at DESC)
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS "agency_api_key_status_expires_idx"
        ON {agency_api_key} (status, expires_at)
        """
    )


def downgrade() -> None:
    schema = _schema()
    agency_api_key = _qualified_table(schema, "agency_api_key")

    op.execute('DROP INDEX IF EXISTS "agency_api_key_status_expires_idx"')
    op.execute('DROP INDEX IF EXISTS "agency_api_key_agency_status_created_idx"')
    op.execute(f"DROP TABLE IF EXISTS {agency_api_key}")
