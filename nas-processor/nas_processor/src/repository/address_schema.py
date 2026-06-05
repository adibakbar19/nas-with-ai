"""Canonical schema definitions for nas.standardized_address and nas.raw_address.

Single source of truth for column names, types, and validation rules.
Matches the Alembic migrations through 20260606_0007.
"""

from __future__ import annotations

import polars as pl


# ── standardized_address columns ───────────────────────────────────────────────

STANDARDIZED_ADDRESS_COLUMNS: list[str] = [
    "record_id",
    "naskod",
    "canonical_address_key",
    # Address components DMS 2039 Table 10
    "premise_no",
    "lot_no",
    "unit_no",
    "floor_no",
    "floor_level",
    "building_name",
    "street_name_prefix",
    "street_name",
    "sub_locality_1",
    "sub_locality_2",
    "sub_locality_3",
    # Rural/landmark
    "rural_relative_direction",
    "rural_landmark_name",
    "rural_estate_name",
    "rural_descriptor",
    # Locality
    "locality_name",
    "postcode",
    "postcode_name",
    "state_code",
    "state_name",
    # Administrative boundary
    "district_code",
    "district_name",
    "mukim_code",
    "mukim_name",
    "mukim_id",
    "pbt_id",
    "pbt_name",
    # Geospatial
    "latitude",
    "longitude",
    "geom",
    # Classification
    "address_type_id",
    "address_clean",
    "ownership_structure",
    # Validation
    "confidence_score",
    "confidence_band",
    "validation_status",
    "validation_date",
    "validation_by",
    # Lifecycle
    "lifecycle_status",
    "created_at",
    "updated_at",
]

# Type groups
FLOAT_COLUMNS: frozenset[str] = frozenset({"latitude", "longitude", "confidence_score"})
INTEGER_COLUMNS: frozenset[str] = frozenset({"address_type_id"})
TIMESTAMP_COLUMNS: frozenset[str] = frozenset({"validation_date", "created_at", "updated_at"})

# Primary key
PRIMARY_KEY: str = "record_id"


# ── raw_address columns ────────────────────────────────────────────────────────

RAW_ADDRESS_COLUMNS: list[str] = [
    "raw_id",
    "source_system",
    "source_system_id",
    "agency_id",
    "raw_text",
    "source_ref_id",
    "standardized_id",
    "match_status",
    "match_confidence",
    "ingest_timestamp",
    "job_id",
    "error_reason",
    "replaces_raw_id",
]

RAW_ADDRESS_FLOAT_COLUMNS: frozenset[str] = frozenset({"match_confidence"})
RAW_ADDRESS_TIMESTAMP_COLUMNS: frozenset[str] = frozenset({"ingest_timestamp"})

# address_type base types (matches nas.address_type.address_type column)
ADDRESS_BASE_TYPES: frozenset[str] = frozenset({
    "residential", "commercial", "industrial",
    "government", "poi", "rural", "po_box",
})


# ── Validation ─────────────────────────────────────────────────────────────────

def validate_dataframe(df: pl.DataFrame) -> None:
    """Validate a Polars DataFrame against the standardized_address schema.

    Raises ValueError if:
      - Any expected column is missing
      - Any unexpected extra column is present
      - Float columns are not numeric
      - Integer columns are not numeric
    """
    actual_cols = set(df.columns)
    expected_cols = set(STANDARDIZED_ADDRESS_COLUMNS)

    missing = sorted(expected_cols - actual_cols)
    extra = sorted(actual_cols - expected_cols)

    errors: list[str] = []

    if missing:
        errors.append(f"Missing columns: {missing}")
    if extra:
        errors.append(f"Unexpected columns: {extra}")

    # Type checks for float columns
    for col in FLOAT_COLUMNS:
        if col in actual_cols:
            actual_dtype = df.schema[col]
            if actual_dtype not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Null):
                errors.append(f"Column '{col}' must be numeric, got {actual_dtype}")

    # Type checks for integer columns
    for col in INTEGER_COLUMNS:
        if col in actual_cols:
            actual_dtype = df.schema[col]
            if actual_dtype not in (pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.UInt32, pl.Null):
                errors.append(f"Column '{col}' must be integer, got {actual_dtype}")

    if errors:
        raise ValueError(
            "DataFrame does not match standardized_address schema:\n  "
            + "\n  ".join(errors)
        )
