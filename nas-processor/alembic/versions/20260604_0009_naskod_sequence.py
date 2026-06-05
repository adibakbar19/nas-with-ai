"""Add naskod_sequence table for DB-backed NASKOD generation.

Revision ID: 20260604_0009
Revises: 20260422_0008
Create Date: 2026-06-04 22:00:00

"""
from __future__ import annotations

import os
import re

from alembic import op


revision = "20260604_0009"
down_revision = "20260422_0008"
branch_labels = None
depends_on = None

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _app_schema() -> str:
    schema = (os.getenv("PGSCHEMA") or "nas").strip() or "nas"
    if not _IDENT_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def upgrade() -> None:
    schema = _app_schema()
    qualified = f"{_quote_ident(schema)}.{_quote_ident('naskod_sequence')}"

    op.execute(f'CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}')

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
            state_code    TEXT NOT NULL,
            district_code TEXT NOT NULL,
            address_type  TEXT NOT NULL,
            next_seq      BIGINT NOT NULL DEFAULT 1,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (state_code, district_code, address_type)
        )
    """)

    op.execute(f"COMMENT ON TABLE {qualified} IS 'DB-backed sequence counter for NASKOD generation. One row per (state, district, address_type) combination.'")


def downgrade() -> None:
    schema = _app_schema()
    qualified = f"{_quote_ident(schema)}.{_quote_ident('naskod_sequence')}"
    op.execute(f"DROP TABLE IF EXISTS {qualified}")
