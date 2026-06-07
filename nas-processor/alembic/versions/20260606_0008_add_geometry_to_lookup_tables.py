"""Add geom columns to main nas_lookup tables; add state columns to pbt.

Boundary geometry lives in separate *_boundary tables (loaded in a prior
session). This migration adds geom columns to the authoritative reference
tables so centroid queries can run directly against mukim/district/state/
postcode without joining to the boundary tables each time.

Also adds state_id + state_name to pbt (the GeoJSON loader that populated
pbt only had pbt_name + boundary_geom; the CSV brings the state linkage).

Revision ID: 20260606_0008
Revises: 20260606_0007
Create Date: 2026-06-06 00:00:00
"""
from __future__ import annotations

from alembic import op

revision = "20260606_0008"
down_revision = "20260606_0007"
branch_labels = None
depends_on = None

_LOOKUP = "nas_lookup"


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    # mukim
    op.execute(
        f"ALTER TABLE {_LOOKUP}.mukim "
        "ADD COLUMN IF NOT EXISTS geom GEOMETRY(MultiPolygon, 4326)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS mukim_geom_idx "
        f"ON {_LOOKUP}.mukim USING GIST (geom)"
    )

    # district
    op.execute(
        f"ALTER TABLE {_LOOKUP}.district "
        "ADD COLUMN IF NOT EXISTS geom GEOMETRY(MultiPolygon, 4326)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS district_geom_idx "
        f"ON {_LOOKUP}.district USING GIST (geom)"
    )

    # state
    op.execute(
        f"ALTER TABLE {_LOOKUP}.state "
        "ADD COLUMN IF NOT EXISTS geom GEOMETRY(MultiPolygon, 4326)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS state_geom_idx "
        f"ON {_LOOKUP}.state USING GIST (geom)"
    )

    # postcode
    op.execute(
        f"ALTER TABLE {_LOOKUP}.postcode "
        "ADD COLUMN IF NOT EXISTS geom GEOMETRY(MultiPolygon, 4326)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS postcode_geom_idx "
        f"ON {_LOOKUP}.postcode USING GIST (geom)"
    )

    # pbt: add state linkage columns (boundary was loaded from GeoJSON which had no state info)
    op.execute(
        f"ALTER TABLE {_LOOKUP}.pbt "
        "ADD COLUMN IF NOT EXISTS state_id TEXT"
    )
    op.execute(
        f"ALTER TABLE {_LOOKUP}.pbt "
        "ADD COLUMN IF NOT EXISTS state_name TEXT"
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE {_LOOKUP}.pbt DROP COLUMN IF EXISTS state_name")
    op.execute(f"ALTER TABLE {_LOOKUP}.pbt DROP COLUMN IF EXISTS state_id")
    op.execute(f"DROP INDEX IF EXISTS {_LOOKUP}.postcode_geom_idx")
    op.execute(f"ALTER TABLE {_LOOKUP}.postcode DROP COLUMN IF EXISTS geom")
    op.execute(f"DROP INDEX IF EXISTS {_LOOKUP}.state_geom_idx")
    op.execute(f"ALTER TABLE {_LOOKUP}.state DROP COLUMN IF EXISTS geom")
    op.execute(f"DROP INDEX IF EXISTS {_LOOKUP}.district_geom_idx")
    op.execute(f"ALTER TABLE {_LOOKUP}.district DROP COLUMN IF EXISTS geom")
    op.execute(f"DROP INDEX IF EXISTS {_LOOKUP}.mukim_geom_idx")
    op.execute(f"ALTER TABLE {_LOOKUP}.mukim DROP COLUMN IF EXISTS geom")
