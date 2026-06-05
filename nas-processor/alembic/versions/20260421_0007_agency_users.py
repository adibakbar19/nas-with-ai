"""Create DB-backed agency users.

Revision ID: 20260421_0007
Revises: 20260418_0006
Create Date: 2026-04-21 12:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260421_0007"
down_revision = "20260418_0006"
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
    agency_user = _qualified_table(schema, "agency_user")

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {agency_user} (
          user_id TEXT PRIMARY KEY,
          agency_id TEXT NOT NULL,
          username TEXT NOT NULL UNIQUE,
          display_name TEXT NULL,
          email TEXT NULL,
          password_hash TEXT NOT NULL,
          password_algo TEXT NOT NULL DEFAULT 'pbkdf2_sha256',
          role TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_by TEXT NULL,
          disabled_reason TEXT NULL,
          metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_login_at TIMESTAMPTZ NULL,
          disabled_at TIMESTAMPTZ NULL
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS "agency_user_agency_role_status_created_idx"
        ON {agency_user} (agency_id, role, status, created_at DESC)
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS "agency_user_status_username_idx"
        ON {agency_user} (status, username)
        """
    )


def downgrade() -> None:
    schema = _schema()
    agency_user = _qualified_table(schema, "agency_user")

    op.execute('DROP INDEX IF EXISTS "agency_user_status_username_idx"')
    op.execute('DROP INDEX IF EXISTS "agency_user_agency_role_status_created_idx"')
    op.execute(f"DROP TABLE IF EXISTS {agency_user}")
