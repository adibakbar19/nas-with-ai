"""Add coordinate_level to nas.standardized_address.

Records the precision level of the lat/lon values so consumers know
whether a coordinate is exact (rooftop GPS) or an administrative centroid.

Revision ID: 20260606_0009
Revises: 20260606_0008
Create Date: 2026-06-06 00:00:00
"""
from __future__ import annotations

import re

from alembic import op

revision = "20260606_0009"
down_revision = "20260606_0008"
branch_labels = None
depends_on = None

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _schema() -> str:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    if not _IDENT_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def upgrade() -> None:
    schema = _schema()
    sa_tbl = f'"{schema}"."standardized_address"'

    op.execute(f"ALTER TABLE {sa_tbl} ADD COLUMN IF NOT EXISTS coordinate_level TEXT")
    op.execute(f"""
        COMMENT ON COLUMN {sa_tbl}.coordinate_level IS
        'Coordinate precision level:
         rooftop           -- exact GPS from source data
         postcode_centroid -- centroid of postcode zone boundary
         mukim_centroid    -- centroid of mukim boundary
         district_centroid -- centroid of district boundary
         state_centroid    -- centroid of state boundary
         NULL              -- no coordinates available'
    """)


def downgrade() -> None:
    schema = _schema()
    op.execute(f'ALTER TABLE "{schema}"."standardized_address" DROP COLUMN IF EXISTS coordinate_level')
