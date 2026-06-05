"""Create api_idempotency_request table in ingest schema.

Revision ID: 20260605_0002
Revises: 20260605_0001
Create Date: 2026-06-05 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260605_0002"
down_revision = "20260605_0001"
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
    tbl = _qualified(schema, "api_idempotency_request")

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
            agency_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            request_fingerprint TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            resource_type TEXT,
            resource_id TEXT,
            response JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (agency_id, operation, idempotency_key)
        )
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS api_idempotency_agency_updated_idx
        ON {tbl} (agency_id, updated_at DESC)
    """)


def downgrade() -> None:
    schema = _schema()
    tbl = _qualified(schema, "api_idempotency_request")

    op.execute(f"DROP INDEX IF EXISTS {_quote_ident(schema)}.api_idempotency_agency_updated_idx")
    op.execute(f"DROP TABLE IF EXISTS {tbl}")
