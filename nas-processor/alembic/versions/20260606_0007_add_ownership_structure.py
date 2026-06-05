"""Move ownership_structure from address_type to standardized_address.

ownership_structure is a per-address property (the same building subtype
can be freehold or leasehold depending on the development), so it belongs
on the address record, not on the type reference table.

Revision ID: 20260606_0007
Revises: 20260606_0006
Create Date: 2026-06-06 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260606_0007"
down_revision = "20260606_0006"
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
    at_tbl = f"{_quote_ident(schema)}.\"address_type\""
    sa_tbl = f"{_quote_ident(schema)}.\"standardized_address\""

    # Idempotent guard — already dropped in 0006 but safe to repeat
    op.execute(f"ALTER TABLE {at_tbl} DROP COLUMN IF EXISTS ownership_structure")

    op.execute(f"ALTER TABLE {sa_tbl} ADD COLUMN IF NOT EXISTS ownership_structure TEXT")
    op.execute(f"""
        COMMENT ON COLUMN {sa_tbl}.ownership_structure IS
        'Land title type for this specific address. Same address subtype
can be freehold or leasehold depending on the development.
Per Malaysian land law (NLC 1965): freehold=geran, leasehold=pajakan.'
    """)


def downgrade() -> None:
    schema = _schema()
    sa_tbl = f"{_quote_ident(schema)}.\"standardized_address\""
    at_tbl = f"{_quote_ident(schema)}.\"address_type\""

    op.execute(f"ALTER TABLE {sa_tbl} DROP COLUMN IF EXISTS ownership_structure")
    op.execute(f"ALTER TABLE {at_tbl} ADD COLUMN IF NOT EXISTS ownership_structure TEXT")
