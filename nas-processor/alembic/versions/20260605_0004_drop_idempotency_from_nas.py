"""Drop api_idempotency_request from nas schema.

This table has been migrated to the 'ingest' schema managed by
ingestion-api's own Alembic setup.

Revision ID: 20260605_0004
Revises: 20260605_0003
Create Date: 2026-06-05 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260605_0004"
down_revision = "20260605_0003"
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


def upgrade() -> None:
    schema = _schema()
    s = _quote_ident(schema)

    op.execute(f'DROP INDEX IF EXISTS {s}."api_idempotency_agency_updated_idx"')
    op.execute(f"DROP TABLE IF EXISTS {s}.\"api_idempotency_request\"")


def downgrade() -> None:
    # Table is now managed by ingestion-api. No-op on downgrade.
    pass
