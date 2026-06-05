"""Validate stage — confidence scoring and three-way split.

All scoring uses vectorized pl.Expr arithmetic — no loops, no map_elements.

Thresholds:
    SUCCESS:  confidence_score >= 85
    WARNING:  50 <= confidence_score < 85
    FAILED:   confidence_score < 50
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)


# ── Scoring constants ─────────────────────────────────────────────────────────

_SCORE_STATE = 15
_SCORE_DISTRICT = 15
_SCORE_MUKIM = 20
_SCORE_POSTCODE = 15
_SCORE_STREET = 10
_SCORE_PREMISE = 10
_SCORE_LOCALITY = 10
_SCORE_SPATIAL = 5
_PENALTY_UNGAZETTED = -5

_THRESHOLD_SUCCESS = 85
_THRESHOLD_WARNING = 50


# ── Helper: safe column reference ────────────────────────────────────────────


def _col_not_null(df: pl.DataFrame, col: str) -> pl.Expr:
    """Return an expression that is True where col is not null, or False if col doesn't exist."""
    if col in df.columns:
        return pl.col(col).is_not_null()
    return pl.lit(False)


def _col_eq(df: pl.DataFrame, col: str, value) -> pl.Expr:
    """Return an expression that is True where col == value, or False if col doesn't exist."""
    if col in df.columns:
        return pl.col(col) == value
    return pl.lit(False)


# ── Scoring ───────────────────────────────────────────────────────────────────
# VECTORIZED: All operations are pl.Expr arithmetic.


def score_addresses(df: pl.DataFrame) -> pl.DataFrame:
    """Add confidence_score and confidence_band columns.

    VECTORIZED: Uses pl.Expr arithmetic only.
    No loops, no map_elements.

    confidence_band values:
        'HIGH'    for score >= 85
        'MEDIUM'  for 50 <= score < 85
        'LOW'     for score < 50
    """
    # Build score expression additively
    score = pl.lit(0)

    # +15 state resolved
    score = score + pl.when(_col_not_null(df, "state_code")).then(pl.lit(_SCORE_STATE)).otherwise(pl.lit(0))

    # +15 district resolved
    score = score + pl.when(_col_not_null(df, "district_code")).then(pl.lit(_SCORE_DISTRICT)).otherwise(pl.lit(0))

    # +20 mukim resolved
    score = score + pl.when(_col_not_null(df, "mukim_code")).then(pl.lit(_SCORE_MUKIM)).otherwise(pl.lit(0))

    # +15 postcode resolved (not null AND not ungazetted)
    if "postcode_raw" in df.columns and "postcode_ungazetted" in df.columns:
        postcode_resolved = (
            pl.col("postcode_raw").is_not_null()
            & (pl.col("postcode_ungazetted") != True)  # noqa: E712
        )
    elif "postcode_raw" in df.columns:
        postcode_resolved = pl.col("postcode_raw").is_not_null()
    else:
        postcode_resolved = pl.lit(False)
    score = score + pl.when(postcode_resolved).then(pl.lit(_SCORE_POSTCODE)).otherwise(pl.lit(0))

    # +10 street_name extracted
    score = score + pl.when(_col_not_null(df, "street_name")).then(pl.lit(_SCORE_STREET)).otherwise(pl.lit(0))

    # +10 premise_no extracted
    score = score + pl.when(_col_not_null(df, "premise_no")).then(pl.lit(_SCORE_PREMISE)).otherwise(pl.lit(0))

    # +10 locality_name resolved
    score = score + pl.when(_col_not_null(df, "locality_name")).then(pl.lit(_SCORE_LOCALITY)).otherwise(pl.lit(0))

    # +5 spatial confirmed (has lat/lng with non-null values)
    if "latitude" in df.columns and "longitude" in df.columns:
        spatial_confirmed = pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null()
    else:
        spatial_confirmed = pl.lit(False)
    score = score + pl.when(spatial_confirmed).then(pl.lit(_SCORE_SPATIAL)).otherwise(pl.lit(0))

    # -5 penalty for ungazetted postcode
    if "postcode_ungazetted" in df.columns:
        penalty = pl.when(pl.col("postcode_ungazetted") == True).then(pl.lit(_PENALTY_UNGAZETTED)).otherwise(pl.lit(0))  # noqa: E712
    else:
        penalty = pl.lit(0)
    score = score + penalty

    # Clamp to 0-100
    score = score.clip(0, 100)

    # Confidence band
    band = (
        pl.when(score >= _THRESHOLD_SUCCESS).then(pl.lit("HIGH"))
          .when(score >= _THRESHOLD_WARNING).then(pl.lit("MEDIUM"))
          .otherwise(pl.lit("LOW"))
    )

    return df.with_columns([
        score.cast(pl.Int32).alias("confidence_score"),
        band.alias("confidence_band"),
    ])


# ── Error reasons ─────────────────────────────────────────────────────────────
# VECTORIZED: Uses pl.Expr when/then chains + string concatenation.


def build_error_reasons(df: pl.DataFrame) -> pl.DataFrame:
    """Add error_reason and suggestion columns.

    VECTORIZED: Uses pl.Expr when/then chains and pl.concat_str.

    error_reason: pipe-separated list of what failed.
    suggestion: human-readable hint for correction.
    """
    # Build individual reason flags as nullable strings
    # Each is either the reason text or null
    reasons: list[pl.Expr] = []

    if "state_code" in df.columns:
        reasons.append(
            pl.when(pl.col("state_code").is_null())
              .then(pl.lit("state_unresolved"))
              .otherwise(pl.lit(None).cast(pl.Utf8))
        )

    if "district_code" in df.columns:
        reasons.append(
            pl.when(pl.col("district_code").is_null())
              .then(pl.lit("district_unresolved"))
              .otherwise(pl.lit(None).cast(pl.Utf8))
        )

    if "mukim_code" in df.columns:
        reasons.append(
            pl.when(pl.col("mukim_code").is_null())
              .then(pl.lit("mukim_unresolved"))
              .otherwise(pl.lit(None).cast(pl.Utf8))
        )

    if "postcode_ungazetted" in df.columns:
        reasons.append(
            pl.when(pl.col("postcode_ungazetted") == True)  # noqa: E712
              .then(pl.lit("postcode_ungazetted"))
              .otherwise(pl.lit(None).cast(pl.Utf8))
        )

    if "postcode_raw" in df.columns:
        reasons.append(
            pl.when(pl.col("postcode_raw").is_null())
              .then(pl.lit("postcode_missing"))
              .otherwise(pl.lit(None).cast(pl.Utf8))
        )

    if "street_name" in df.columns:
        reasons.append(
            pl.when(pl.col("street_name").is_null())
              .then(pl.lit("street_unresolved"))
              .otherwise(pl.lit(None).cast(pl.Utf8))
        )

    # Concatenate non-null reasons with pipe separator
    if reasons:
        # Add each reason as a temporary column, then concat
        reason_cols = [f"_reason_{i}" for i in range(len(reasons))]
        df = df.with_columns([
            expr.alias(col_name) for expr, col_name in zip(reasons, reason_cols)
        ])
        # Concatenate non-null values with pipe separator
        df = df.with_columns(
            pl.concat_str(reason_cols, separator="|", ignore_nulls=True).alias("error_reason"),
        ).drop(reason_cols)
        # Replace empty string with null
        df = df.with_columns(
            pl.when(pl.col("error_reason") == "")
              .then(pl.lit(None).cast(pl.Utf8))
              .otherwise(pl.col("error_reason"))
              .alias("error_reason"),
        )
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("error_reason"))

    # Build suggestion based on what's missing
    suggestion = (
        pl.when(pl.col("error_reason").is_null())
          .then(pl.lit(None).cast(pl.Utf8))
          .when(pl.col("error_reason").str.contains("state_unresolved"))
          .then(pl.lit("Check state name spelling or provide a valid postcode"))
          .when(pl.col("error_reason").str.contains("postcode_ungazetted"))
          .then(pl.lit("Postcode not yet gazetted — provide district name"))
          .when(pl.col("error_reason").str.contains("mukim_unresolved"))
          .then(pl.lit("Mukim could not be determined — specify mukim name"))
          .when(pl.col("error_reason").str.contains("district_unresolved"))
          .then(pl.lit("District could not be resolved — check address or provide district"))
          .otherwise(pl.lit("Review address components for completeness"))
    )

    df = df.with_columns(suggestion.alias("suggestion"))

    return df


# ── Split ─────────────────────────────────────────────────────────────────────
# VECTORIZED: Uses pl.DataFrame.filter().


def split_by_confidence(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Split df into (success, warning, failed) by confidence_score.

    success: confidence_score >= 85
    warning: 50 <= confidence_score < 85
    failed:  confidence_score < 50

    Each may be empty — handles gracefully.
    """
    success = df.filter(pl.col("confidence_score") >= _THRESHOLD_SUCCESS)
    warning = df.filter(
        (pl.col("confidence_score") >= _THRESHOLD_WARNING)
        & (pl.col("confidence_score") < _THRESHOLD_SUCCESS)
    )
    failed = df.filter(pl.col("confidence_score") < _THRESHOLD_WARNING)

    return success, warning, failed


# ── Main entry point ──────────────────────────────────────────────────────────


def validate_chunk(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Main entry point — score and split a chunk.

    Steps:
    1. score_addresses(df)
    2. build_error_reasons(df)
    3. split_by_confidence(df)
    4. Drop error columns from success_df

    Returns (success_df, warning_df, failed_df).
    """
    logger.info("validate_start rows=%d", len(df))

    # 1. Score
    df = score_addresses(df)

    # 2. Build error reasons (for all rows — will drop from success later)
    df = build_error_reasons(df)

    # 3. Split
    success, warning, failed = split_by_confidence(df)

    # 4. Drop error columns from success (they passed — don't need them)
    drop_cols = [c for c in ["error_reason", "suggestion"] if c in success.columns]
    if drop_cols:
        success = success.drop(drop_cols)

    logger.info(
        "validate_complete success=%d warning=%d failed=%d",
        len(success), len(warning), len(failed),
    )

    return success, warning, failed
