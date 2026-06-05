"""Structured CSV correction output for warning and failed rows.

Produces human-reviewable CSVs that can be opened in Excel,
corrected by users, and resubmitted for re-processing.

UTF-8-BOM encoding ensures Excel opens the file without
prompting for encoding selection.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


# System-parsed columns to include in correction output (if present in df)
_PARSED_COLUMNS: list[str] = [
    "postcode_raw",
    "state_code",
    "state_name",
    "district_code",
    "district_name",
    "mukim_code",
    "mukim_name",
    "locality_name",
    "street_name",
    "premise_no",
    "postcode_ungazetted",
]

# UTF-8 BOM bytes for Excel compatibility
_UTF8_BOM = b"\xef\xbb\xbf"


def build_correction_csv(
    df: pl.DataFrame,
    *,
    original_columns: list[str],
    status: str,
) -> pl.DataFrame:
    """Build a correction-ready DataFrame.

    Column order in output:
    1. _status           — "warning" or "failed"
    2. _confidence_score — the score that caused this outcome
    3. _confidence_band  — HIGH/MEDIUM/LOW
    4. _error_reason     — pipe-separated failure reasons
    5. _suggestion       — human-readable correction hint
    6. record_id         — for resubmission matching
    7. [original_columns] — columns from the source file (original order)
    8. [parsed_columns]  — what the system managed to parse

    System columns prefixed with underscore so users know
    not to edit them (except to fix the address fields).

    Returns pl.DataFrame ready to write as CSV.
    """
    # Build system metadata columns
    system_cols: list[pl.Expr] = [
        pl.lit(status).alias("_status"),
    ]

    # confidence_score — cast to string for uniform CSV output
    if "confidence_score" in df.columns:
        system_cols.append(pl.col("confidence_score").cast(pl.Utf8).alias("_confidence_score"))
    else:
        system_cols.append(pl.lit(None).cast(pl.Utf8).alias("_confidence_score"))

    if "confidence_band" in df.columns:
        system_cols.append(pl.col("confidence_band").alias("_confidence_band"))
    else:
        system_cols.append(pl.lit(None).cast(pl.Utf8).alias("_confidence_band"))

    if "error_reason" in df.columns:
        system_cols.append(pl.col("error_reason").alias("_error_reason"))
    else:
        system_cols.append(pl.lit(None).cast(pl.Utf8).alias("_error_reason"))

    if "suggestion" in df.columns:
        system_cols.append(pl.col("suggestion").alias("_suggestion"))
    else:
        system_cols.append(pl.lit(None).cast(pl.Utf8).alias("_suggestion"))

    # record_id for resubmission matching
    if "record_id" in df.columns:
        system_cols.append(pl.col("record_id"))
    else:
        system_cols.append(pl.lit(None).cast(pl.Utf8).alias("record_id"))

    # Original source columns (preserve order, only include if present)
    orig_cols: list[pl.Expr] = []
    for col in original_columns:
        if col in df.columns and col != "record_id":  # record_id already added above
            orig_cols.append(pl.col(col).cast(pl.Utf8))

    # Parsed columns (only include if present in df)
    parsed_cols: list[pl.Expr] = []
    for col in _PARSED_COLUMNS:
        if col in df.columns:
            parsed_cols.append(pl.col(col).cast(pl.Utf8).alias(f"_parsed_{col}"))

    # Select in order: system → original → parsed
    all_exprs = system_cols + orig_cols + parsed_cols
    return df.select(all_exprs)


def write_correction_csv(
    df: pl.DataFrame,
    *,
    output_path: Path,
    original_columns: list[str],
    status: str,
) -> int:
    """Write correction CSV to output_path.

    Returns number of rows written.
    Raises ValueError if df is empty — caller should check before calling.
    Creates parent directories if needed.
    Uses UTF-8-BOM encoding so Excel opens correctly.
    """
    if df.is_empty():
        raise ValueError("Cannot write empty DataFrame to correction CSV")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build the correction DataFrame
    correction_df = build_correction_csv(
        df,
        original_columns=original_columns,
        status=status,
    )

    # Write CSV content to bytes, then prepend BOM
    csv_bytes = correction_df.write_csv().encode("utf-8")

    with open(output_path, "wb") as f:
        f.write(_UTF8_BOM)
        f.write(csv_bytes)

    row_count = len(correction_df)
    logger.info(
        "correction_csv_written path=%s status=%s rows=%d",
        output_path, status, row_count,
    )
    return row_count


def write_correction_csvs(
    warning_df: pl.DataFrame,
    failed_df: pl.DataFrame,
    *,
    output_dir: Path,
    job_id: str,
    original_columns: list[str],
) -> dict[str, int | str | None]:
    """Write both warning and failed CSVs.

    Output files:
    - {output_dir}/{job_id}_warning.csv  (if warning_df not empty)
    - {output_dir}/{job_id}_failed.csv   (if failed_df not empty)

    Returns dict with counts:
    {
        'warning_rows': N,
        'failed_rows': N,
        'warning_path': str | None,
        'failed_path': str | None,
    }
    """
    output_dir = Path(output_dir)
    result: dict[str, int | str | None] = {
        "warning_rows": 0,
        "failed_rows": 0,
        "warning_path": None,
        "failed_path": None,
    }

    if not warning_df.is_empty():
        warning_path = output_dir / f"{job_id}_warning.csv"
        result["warning_rows"] = write_correction_csv(
            warning_df,
            output_path=warning_path,
            original_columns=original_columns,
            status="warning",
        )
        result["warning_path"] = str(warning_path)

    if not failed_df.is_empty():
        failed_path = output_dir / f"{job_id}_failed.csv"
        result["failed_rows"] = write_correction_csv(
            failed_df,
            output_path=failed_path,
            original_columns=original_columns,
            status="failed",
        )
        result["failed_path"] = str(failed_path)

    logger.info(
        "correction_csvs_complete job_id=%s warning_rows=%d failed_rows=%d",
        job_id, result["warning_rows"], result["failed_rows"],
    )

    return result
