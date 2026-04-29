"""Add canonical address key and alias table.

Revision ID: 20260418_0004
Revises: 20260410_0003
Create Date: 2026-04-18 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260418_0004"
down_revision = "20260410_0003"
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
    standardized_address = _qualified_table(schema, "standardized_address")
    address_alias = _qualified_table(schema, "address_alias")

    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute(
        f"""
        ALTER TABLE IF EXISTS {standardized_address}
        ADD COLUMN IF NOT EXISTS canonical_address_key TEXT NULL
        """
    )
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {address_alias} (
          alias_id INTEGER PRIMARY KEY,
          address_id INTEGER NOT NULL,
          raw_address_variant TEXT NULL,
          normalized_address_variant TEXT NOT NULL,
          alias_checksum TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = 'standardized_address'
          ) AND NOT EXISTS (
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE c.conname = 'address_alias_address_fk'
              AND n.nspname = '{schema}'
          ) THEN
            ALTER TABLE {address_alias}
            ADD CONSTRAINT address_alias_address_fk
            FOREIGN KEY (address_id) REFERENCES {standardized_address} (address_id)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY IMMEDIATE;
          END IF;
        END$$;
        """
    )
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = 'standardized_address'
          ) THEN
            WITH keyed AS (
              SELECT
                sa.address_id,
                encode(
                  digest(
                    concat_ws(
                      '|',
                      upper(trim(coalesce(sa.premise_no, ''))),
                      upper(trim(coalesce(sa.building_name, ''))),
                      upper(trim(coalesce(sa.floor_level, ''))),
                      upper(trim(coalesce(sa.unit_no, ''))),
                      upper(trim(coalesce(sa.lot_no, ''))),
                      coalesce(sa.street_id::text, ''),
                      coalesce(sa.locality_id::text, ''),
                      coalesce(sa.mukim_id::text, ''),
                      coalesce(sa.district_id::text, ''),
                      coalesce(sa.state_id::text, ''),
                      coalesce(sa.postcode_id::text, ''),
                      coalesce(sa.pbt_id::text, ''),
                      upper(trim(coalesce(sa.country, '')))
                    ),
                    'sha256'
                  ),
                  'hex'
                ) AS canonical_address_key
              FROM {standardized_address} sa
            ),
            ranked AS (
              SELECT
                address_id,
                canonical_address_key,
                row_number() OVER (PARTITION BY canonical_address_key ORDER BY address_id ASC) AS rn
              FROM keyed
            )
            UPDATE {standardized_address} sa
            SET canonical_address_key = CASE WHEN ranked.rn = 1 THEN ranked.canonical_address_key ELSE NULL END
            FROM ranked
            WHERE ranked.address_id = sa.address_id;
          END IF;
        END$$;
        """
    )
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = 'standardized_address'
          ) THEN
            INSERT INTO {address_alias} (
              alias_id,
              address_id,
              raw_address_variant,
              normalized_address_variant,
              alias_checksum,
              created_at,
              updated_at
            )
            SELECT
              row_number() OVER (ORDER BY sa.address_id ASC),
              sa.address_id,
              trim(
                concat_ws(
                  ', ',
                  sa.premise_no,
                  sa.building_name,
                  sa.floor_level,
                  sa.unit_no,
                  sa.lot_no,
                  s.street_name_prefix,
                  s.street_name,
                  l.locality_name,
                  p.postcode,
                  d.district_name,
                  st.state_name
                )
              ) AS raw_address_variant,
              upper(
                trim(
                  regexp_replace(
                    trim(
                      concat_ws(
                        ', ',
                        sa.premise_no,
                        sa.building_name,
                        sa.floor_level,
                        sa.unit_no,
                        sa.lot_no,
                        s.street_name_prefix,
                        s.street_name,
                        l.locality_name,
                        p.postcode,
                        d.district_name,
                        st.state_name
                      )
                    ),
                    '\\s+',
                    ' ',
                    'g'
                  )
                )
              ) AS normalized_address_variant,
              encode(
                digest(
                  upper(
                    trim(
                      regexp_replace(
                        trim(
                          concat_ws(
                            ', ',
                            sa.premise_no,
                            sa.building_name,
                            sa.floor_level,
                            sa.unit_no,
                            sa.lot_no,
                            s.street_name_prefix,
                            s.street_name,
                            l.locality_name,
                            p.postcode,
                            d.district_name,
                            st.state_name
                          )
                        ),
                        '\\s+',
                        ' ',
                        'g'
                      )
                    )
                  ),
                  'sha256'
                ),
                'hex'
              ) AS alias_checksum,
              NOW(),
              NOW()
            FROM {standardized_address} sa
            LEFT JOIN {_qualified_table(schema, "street")} s ON s.street_id = sa.street_id
            LEFT JOIN {_qualified_table(schema, "locality")} l ON l.locality_id = sa.locality_id
            LEFT JOIN {_qualified_table(schema, "postcode")} p ON p.postcode_id = sa.postcode_id
            LEFT JOIN {_qualified_table(schema, "district")} d ON d.district_id = sa.district_id
            LEFT JOIN {_qualified_table(schema, "state")} st ON st.state_id = sa.state_id
            ON CONFLICT DO NOTHING;
          END IF;
        END$$;
        """
    )
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = 'standardized_address'
          ) THEN
            CREATE UNIQUE INDEX IF NOT EXISTS standardized_address_canonical_key_uniq
            ON {standardized_address} (canonical_address_key)
            WHERE canonical_address_key IS NOT NULL;
          END IF;
        END$$;
        """
    )
    op.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS address_alias_address_checksum_uniq
        ON {address_alias} (address_id, alias_checksum)
        """
    )


def downgrade() -> None:
    schema = _schema()
    standardized_address = _qualified_table(schema, "standardized_address")
    address_alias = _qualified_table(schema, "address_alias")

    op.execute(f'DROP INDEX IF EXISTS "{schema}"."address_alias_address_checksum_uniq"')
    op.execute(f'DROP INDEX IF EXISTS "{schema}"."standardized_address_canonical_key_uniq"')
    op.execute(f"DROP TABLE IF EXISTS {address_alias}")
    op.execute(f"ALTER TABLE IF EXISTS {standardized_address} DROP COLUMN IF EXISTS canonical_address_key")
