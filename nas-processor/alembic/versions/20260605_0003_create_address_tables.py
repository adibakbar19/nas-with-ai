"""Create NAS address tables.

Drops the existing standardized_address (auto-created by pandas loader)
and recreates it with the full production schema, plus new supporting tables.

Revision ID: 20260605_0003
Revises: 20260605_0002
Create Date: 2026-06-05 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260605_0003"
down_revision = "20260605_0002"
branch_labels = None
depends_on = None

_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _SCHEMA_NAME_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _schema() -> str:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def _q(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def upgrade() -> None:
    schema = _schema()
    s = _quote_ident(schema)

    op.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")

    # Drop old standardized_address (auto-created by pandas to_sql, no constraints)
    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'standardized_address')} CASCADE")
    # Drop old address_alias and address_match_review (will be recreated with new schema)
    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'address_alias')} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'address_match_review')} CASCADE")

    # ── address_type (reference table) ────────────────────────────────────────
    op.execute(f"""
        CREATE TABLE {_q(schema, 'address_type')} (
            address_type_id SERIAL PRIMARY KEY,
            address_type TEXT NOT NULL,
            address_subtype TEXT,
            property_code TEXT,
            ownership_structure TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # ── standardized_address ──────────────────────────────────────────────────
    op.execute(f"""
        CREATE TABLE {_q(schema, 'standardized_address')} (
            record_id TEXT PRIMARY KEY,
            naskod TEXT UNIQUE,
            canonical_address_key TEXT NOT NULL,

            -- Address components DMS 2039 Table 10
            premise_no TEXT,
            lot_no TEXT,
            unit_no TEXT,
            floor_no TEXT,
            floor_level TEXT,
            building_name TEXT,
            street_name_prefix TEXT,
            street_name TEXT,
            sub_locality_1 TEXT,
            sub_locality_2 TEXT,
            sub_locality_3 TEXT,

            -- Rural/landmark (structured)
            rural_relative_direction TEXT,
            rural_landmark_name TEXT,
            rural_estate_name TEXT,
            rural_descriptor TEXT,

            -- Locality
            locality_name TEXT,
            postcode TEXT,
            postcode_name TEXT,
            state_code TEXT,
            state_name TEXT,

            -- Administrative boundary
            district_code TEXT,
            district_name TEXT,
            mukim_code TEXT,
            mukim_name TEXT,
            mukim_id TEXT,
            pbt_id TEXT,
            pbt_name TEXT,

            -- Geospatial (PostGIS)
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            geom GEOMETRY(Point, 4326),

            -- Classification
            address_type_id INTEGER REFERENCES {_q(schema, 'address_type')}(address_type_id),
            address_clean TEXT,

            -- Validation
            confidence_score DOUBLE PRECISION,
            confidence_band TEXT,
            validation_status TEXT,
            validation_date TIMESTAMPTZ,
            validation_by TEXT,

            -- Lifecycle
            lifecycle_status TEXT NOT NULL DEFAULT 'validated',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # Indexes for standardized_address
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} (naskod)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} (canonical_address_key)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} (postcode)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} (mukim_id)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} (pbt_id)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} (lifecycle_status)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} (state_code)")
    op.execute(f"CREATE UNIQUE INDEX ON {_q(schema, 'standardized_address')} (naskod) WHERE naskod IS NOT NULL")
    op.execute(f"CREATE INDEX ON {_q(schema, 'standardized_address')} USING GIST (geom)")

    # ── raw_address ───────────────────────────────────────────────────────────
    op.execute(f"""
        CREATE TABLE {_q(schema, 'raw_address')} (
            raw_id TEXT PRIMARY KEY,
            source_system TEXT NOT NULL,
            source_system_id TEXT,
            agency_id TEXT,
            raw_text TEXT NOT NULL,
            source_ref_id TEXT,
            standardized_id TEXT REFERENCES {_q(schema, 'standardized_address')}(record_id),
            match_status TEXT NOT NULL DEFAULT 'unmatched',
            match_confidence DOUBLE PRECISION,
            ingest_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            job_id TEXT
        )
    """)

    # Indexes for raw_address
    op.execute(f"CREATE INDEX ON {_q(schema, 'raw_address')} (source_system)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'raw_address')} (agency_id)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'raw_address')} (standardized_id)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'raw_address')} (match_status)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'raw_address')} (job_id)")

    # ── address_alias ─────────────────────────────────────────────────────────
    op.execute(f"""
        CREATE TABLE {_q(schema, 'address_alias')} (
            alias_id TEXT PRIMARY KEY,
            standardized_id TEXT NOT NULL REFERENCES {_q(schema, 'standardized_address')}(record_id),
            alias_text TEXT NOT NULL,
            alias_type TEXT NOT NULL,
            source_system TEXT,
            valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            valid_until TIMESTAMPTZ,
            superseded_by TEXT REFERENCES {_q(schema, 'standardized_address')}(record_id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # Indexes for address_alias
    op.execute(f"CREATE INDEX ON {_q(schema, 'address_alias')} (standardized_id)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'address_alias')} (alias_type)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'address_alias')} (valid_until)")

    # ── match_review_queue ────────────────────────────────────────────────────
    op.execute(f"""
        CREATE TABLE {_q(schema, 'match_review_queue')} (
            review_id TEXT PRIMARY KEY,
            raw_id TEXT NOT NULL REFERENCES {_q(schema, 'raw_address')}(raw_id),
            candidate_ids TEXT[],
            candidate_scores DOUBLE PRECISION[],
            reason TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            assigned_to TEXT,
            assigned_at TIMESTAMPTZ,
            resolved_at TIMESTAMPTZ,
            decision TEXT,
            resolved_by TEXT,
            resolution_notes TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # Indexes for match_review_queue
    op.execute(f"CREATE INDEX ON {_q(schema, 'match_review_queue')} (status)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'match_review_queue')} (assigned_to)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'match_review_queue')} (raw_id)")
    op.execute(f"CREATE INDEX ON {_q(schema, 'match_review_queue')} (created_at DESC)")


def downgrade() -> None:
    schema = _schema()

    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'match_review_queue')} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'address_alias')} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'raw_address')} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'standardized_address')} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {_q(schema, 'address_type')} CASCADE")
