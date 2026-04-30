"""Add canonical ID linkage columns to lookup boundary tables.

Revision ID: 20260422_0008
Revises: 20260421_0007
Create Date: 2026-04-22 11:00:00

"""
from __future__ import annotations

import os
import re

from alembic import op


revision = "20260422_0008"
down_revision = "20260421_0007"
branch_labels = None
depends_on = None

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _lookup_schema() -> str:
    schema = (os.getenv("LOOKUP_SCHEMA") or "nas_lookup").strip() or "nas_lookup"
    if not _IDENT_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def _qualified_table(schema: str, table_name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table_name)}"


def _table_exists(schema: str, table_name: str) -> bool:
    bind = op.get_bind()
    row = bind.exec_driver_sql(
        """
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = %s
            AND table_name = %s
        )
        """,
        (schema, table_name),
    ).scalar()
    return bool(row)


def _schema_exists(schema: str) -> bool:
    bind = op.get_bind()
    return bool(
        bind.exec_driver_sql(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)",
            (schema,),
        ).scalar()
    )


def upgrade() -> None:
    schema = _lookup_schema()

    # nas_lookup is created and populated by db-bootstrap which runs after db-migrate.
    # On a fresh DB this schema does not exist yet, so skip — bootstrap handles it.
    if not _schema_exists(schema):
        return

    state = _qualified_table(schema, "state")
    district = _qualified_table(schema, "district")
    mukim = _qualified_table(schema, "mukim")
    postcode = _qualified_table(schema, "postcode")
    state_boundary = _qualified_table(schema, "state_boundary")
    district_boundary = _qualified_table(schema, "district_boundary")
    mukim_boundary = _qualified_table(schema, "mukim_boundary")
    postcode_boundary = _qualified_table(schema, "postcode_boundary")

    op.execute(f"ALTER TABLE IF EXISTS {state_boundary} ADD COLUMN IF NOT EXISTS state_id INTEGER NULL")
    op.execute(f"ALTER TABLE IF EXISTS {district_boundary} ADD COLUMN IF NOT EXISTS state_id INTEGER NULL")
    op.execute(f"ALTER TABLE IF EXISTS {district_boundary} ADD COLUMN IF NOT EXISTS district_id INTEGER NULL")
    op.execute(f"ALTER TABLE IF EXISTS {mukim_boundary} ADD COLUMN IF NOT EXISTS state_id INTEGER NULL")
    op.execute(f"ALTER TABLE IF EXISTS {mukim_boundary} ADD COLUMN IF NOT EXISTS district_id INTEGER NULL")
    op.execute(f"ALTER TABLE IF EXISTS {postcode_boundary} ADD COLUMN IF NOT EXISTS postcode_id INTEGER NULL")

    required_tables = (
        "state",
        "district",
        "mukim",
        "postcode",
        "state_boundary",
        "district_boundary",
        "mukim_boundary",
        "postcode_boundary",
    )
    if not all(_table_exists(schema, table_name) for table_name in required_tables):
        return

    op.execute(
        f"""
        UPDATE {state_boundary} sb
        SET state_id = st.state_id
        FROM {state} st
        WHERE sb.state_id IS NULL
          AND (
            (
              NULLIF(TRIM(COALESCE(sb.state_code::text, '')), '') IS NOT NULL
              AND UPPER(TRIM(COALESCE(st.state_code::text, ''))) = UPPER(TRIM(COALESCE(sb.state_code::text, '')))
            )
            OR (
              NULLIF(TRIM(COALESCE(sb.state_name::text, '')), '') IS NOT NULL
              AND UPPER(TRIM(COALESCE(st.state_name::text, ''))) = UPPER(TRIM(COALESCE(sb.state_name::text, '')))
            )
          )
        """
    )
    op.execute(
        f"""
        UPDATE {district_boundary} db
        SET district_id = d.district_id,
            state_id = COALESCE(db.state_id, d.state_id)
        FROM {district} d
        JOIN {state} st ON st.state_id = d.state_id
        WHERE (db.district_id IS NULL OR db.state_id IS NULL)
          AND (
            (
              NULLIF(TRIM(COALESCE(db.district_code::text, '')), '') IS NOT NULL
              AND UPPER(TRIM(COALESCE(d.district_code::text, ''))) = UPPER(TRIM(COALESCE(db.district_code::text, '')))
            )
            OR (
              NULLIF(TRIM(COALESCE(db.district_name::text, '')), '') IS NOT NULL
              AND UPPER(TRIM(COALESCE(d.district_name::text, ''))) = UPPER(TRIM(COALESCE(db.district_name::text, '')))
            )
          )
          AND (
            NULLIF(TRIM(COALESCE(db.state_code::text, '')), '') IS NULL
            OR UPPER(TRIM(COALESCE(st.state_code::text, ''))) = UPPER(TRIM(COALESCE(db.state_code::text, '')))
          )
        """
    )
    op.execute(
        f"""
        UPDATE {mukim_boundary} mb
        SET mukim_id = COALESCE(mb.mukim_id, m.mukim_id),
            district_id = COALESCE(mb.district_id, m.district_id),
            state_id = COALESCE(mb.state_id, d.state_id)
        FROM {mukim} m
        JOIN {district} d ON d.district_id = m.district_id
        JOIN {state} st ON st.state_id = d.state_id
        WHERE (
            mb.district_id IS NULL
            OR mb.state_id IS NULL
            OR mb.mukim_id IS NULL
          )
          AND (
            (
              mb.mukim_id IS NOT NULL
              AND m.mukim_id = mb.mukim_id
            )
            OR (
              mb.mukim_id IS NULL
              AND NULLIF(TRIM(COALESCE(mb.mukim_code::text, '')), '') IS NOT NULL
              AND UPPER(TRIM(COALESCE(m.mukim_code::text, ''))) = UPPER(TRIM(COALESCE(mb.mukim_code::text, '')))
            )
            OR (
              mb.mukim_id IS NULL
              AND NULLIF(TRIM(COALESCE(mb.mukim_code::text, '')), '') IS NULL
              AND NULLIF(TRIM(COALESCE(mb.mukim_name::text, '')), '') IS NOT NULL
              AND UPPER(TRIM(COALESCE(m.mukim_name::text, ''))) = UPPER(TRIM(COALESCE(mb.mukim_name::text, '')))
            )
          )
          AND (
            NULLIF(TRIM(COALESCE(mb.district_code::text, '')), '') IS NULL
            OR UPPER(TRIM(COALESCE(d.district_code::text, ''))) = UPPER(TRIM(COALESCE(mb.district_code::text, '')))
          )
          AND (
            NULLIF(TRIM(COALESCE(mb.state_code::text, '')), '') IS NULL
            OR UPPER(TRIM(COALESCE(st.state_code::text, ''))) = UPPER(TRIM(COALESCE(mb.state_code::text, '')))
          )
        """
    )
    op.execute(
        f"""
        UPDATE {postcode_boundary} pb
        SET postcode_id = p.postcode_id
        FROM {postcode} p
        WHERE pb.postcode_id IS NULL
          AND TRIM(COALESCE(p.postcode::text, '')) = TRIM(COALESCE(pb.postcode::text, ''))
        """
    )

    op.execute(f'CREATE INDEX IF NOT EXISTS "state_boundary_state_id_idx" ON {state_boundary} (state_id)')
    op.execute(f'CREATE INDEX IF NOT EXISTS "district_boundary_state_id_idx" ON {district_boundary} (state_id)')
    op.execute(f'CREATE INDEX IF NOT EXISTS "district_boundary_district_id_idx" ON {district_boundary} (district_id)')
    op.execute(f'CREATE INDEX IF NOT EXISTS "mukim_boundary_state_id_idx" ON {mukim_boundary} (state_id)')
    op.execute(f'CREATE INDEX IF NOT EXISTS "mukim_boundary_district_id_idx" ON {mukim_boundary} (district_id)')
    op.execute(f'CREATE INDEX IF NOT EXISTS "mukim_boundary_mukim_id_idx" ON {mukim_boundary} (mukim_id)')
    op.execute(f'CREATE INDEX IF NOT EXISTS "postcode_boundary_postcode_id_idx" ON {postcode_boundary} (postcode_id)')


def downgrade() -> None:
    schema = _lookup_schema()
    state_boundary = _qualified_table(schema, "state_boundary")
    district_boundary = _qualified_table(schema, "district_boundary")
    mukim_boundary = _qualified_table(schema, "mukim_boundary")
    postcode_boundary = _qualified_table(schema, "postcode_boundary")

    op.execute('DROP INDEX IF EXISTS "postcode_boundary_postcode_id_idx"')
    op.execute('DROP INDEX IF EXISTS "mukim_boundary_mukim_id_idx"')
    op.execute('DROP INDEX IF EXISTS "mukim_boundary_district_id_idx"')
    op.execute('DROP INDEX IF EXISTS "mukim_boundary_state_id_idx"')
    op.execute('DROP INDEX IF EXISTS "district_boundary_district_id_idx"')
    op.execute('DROP INDEX IF EXISTS "district_boundary_state_id_idx"')
    op.execute('DROP INDEX IF EXISTS "state_boundary_state_id_idx"')

    op.execute(f"ALTER TABLE IF EXISTS {postcode_boundary} DROP COLUMN IF EXISTS postcode_id")
    op.execute(f"ALTER TABLE IF EXISTS {mukim_boundary} DROP COLUMN IF EXISTS district_id")
    op.execute(f"ALTER TABLE IF EXISTS {mukim_boundary} DROP COLUMN IF EXISTS state_id")
    op.execute(f"ALTER TABLE IF EXISTS {district_boundary} DROP COLUMN IF EXISTS district_id")
    op.execute(f"ALTER TABLE IF EXISTS {district_boundary} DROP COLUMN IF EXISTS state_id")
    op.execute(f"ALTER TABLE IF EXISTS {state_boundary} DROP COLUMN IF EXISTS state_id")
