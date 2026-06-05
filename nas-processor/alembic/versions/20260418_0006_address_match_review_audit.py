"""Add audit columns to address match review.

Revision ID: 20260418_0006
Revises: 20260418_0005
Create Date: 2026-04-18 01:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260418_0006"
down_revision = "20260418_0005"
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
    address_match_review = _qualified_table(schema, "address_match_review")

    op.execute(
        f"""
        ALTER TABLE IF EXISTS {address_match_review}
        ADD COLUMN IF NOT EXISTS reviewed_by TEXT NULL
        """
    )
    op.execute(
        f"""
        ALTER TABLE IF EXISTS {address_match_review}
        ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ NULL
        """
    )
    op.execute(
        f"""
        ALTER TABLE IF EXISTS {address_match_review}
        ADD COLUMN IF NOT EXISTS review_note TEXT NULL
        """
    )


def downgrade() -> None:
    schema = _schema()
    address_match_review = _qualified_table(schema, "address_match_review")

    op.execute(f"ALTER TABLE IF EXISTS {address_match_review} DROP COLUMN IF EXISTS review_note")
    op.execute(f"ALTER TABLE IF EXISTS {address_match_review} DROP COLUMN IF EXISTS reviewed_at")
    op.execute(f"ALTER TABLE IF EXISTS {address_match_review} DROP COLUMN IF EXISTS reviewed_by")
