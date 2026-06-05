"""Add error_reason and replaces_raw_id to raw_address.

error_reason: stores validation failure reason for failed rows.
replaces_raw_id: chains re-submitted rows to their predecessor.

Revision ID: 20260605_0005
Revises: 20260605_0004
Create Date: 2026-06-05 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260605_0005"
down_revision = "20260605_0004"
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
    tbl = f"{s}.\"raw_address\""

    op.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS error_reason TEXT")
    op.execute(
        f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS replaces_raw_id TEXT "
        f"REFERENCES {tbl}(raw_id) ON DELETE SET NULL"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS raw_address_replaces_idx "
        f"ON {tbl} (replaces_raw_id) WHERE replaces_raw_id IS NOT NULL"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS raw_address_error_reason_idx "
        f"ON {tbl} (error_reason) WHERE error_reason IS NOT NULL"
    )


def downgrade() -> None:
    schema = _schema()
    s = _quote_ident(schema)
    tbl = f"{s}.\"raw_address\""

    op.execute(f"DROP INDEX IF EXISTS {s}.raw_address_error_reason_idx")
    op.execute(f"DROP INDEX IF EXISTS {s}.raw_address_replaces_idx")
    op.execute(f"ALTER TABLE {tbl} DROP COLUMN IF EXISTS replaces_raw_id")
    op.execute(f"ALTER TABLE {tbl} DROP COLUMN IF EXISTS error_reason")
