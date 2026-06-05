"""Add address match review queue.

Revision ID: 20260418_0005
Revises: 20260418_0004
Create Date: 2026-04-18 00:30:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260418_0005"
down_revision = "20260418_0004"
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
    address_match_review = _qualified_table(schema, "address_match_review")

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {address_match_review} (
          review_id INTEGER PRIMARY KEY,
          candidate_canonical_address_key TEXT NOT NULL,
          candidate_checksum TEXT NULL,
          candidate_raw_address_variant TEXT NULL,
          candidate_normalized_address_variant TEXT NOT NULL,
          candidate_state_id INTEGER NULL,
          candidate_district_id INTEGER NULL,
          candidate_postcode_id INTEGER NULL,
          candidate_street_id INTEGER NULL,
          candidate_locality_id INTEGER NULL,
          matched_address_id INTEGER NOT NULL,
          matched_canonical_address_key TEXT NULL,
          match_score INTEGER NOT NULL,
          match_reasons TEXT NOT NULL,
          review_status TEXT NOT NULL DEFAULT 'open',
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
            WHERE c.conname = 'address_match_review_address_fk'
              AND n.nspname = '{schema}'
          ) THEN
            ALTER TABLE {address_match_review}
            ADD CONSTRAINT address_match_review_address_fk
            FOREIGN KEY (matched_address_id) REFERENCES {standardized_address} (address_id)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY IMMEDIATE;
          END IF;
        END$$;
        """
    )
    op.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS address_match_review_candidate_match_uniq
        ON {address_match_review} (candidate_canonical_address_key, matched_address_id)
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS address_match_review_status_idx
        ON {address_match_review} (review_status, updated_at DESC)
        """
    )


def downgrade() -> None:
    schema = _schema()
    address_match_review = _qualified_table(schema, "address_match_review")

    op.execute(f'DROP INDEX IF EXISTS "{schema}"."address_match_review_status_idx"')
    op.execute(f'DROP INDEX IF EXISTS "{schema}"."address_match_review_candidate_match_uniq"')
    op.execute(f"DROP TABLE IF EXISTS {address_match_review}")
