"""Retry failed rows using record_id based corrections — Polars version."""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

import polars as pl

from nas_config.config_loader import load_config
from nas_config.db import build_dsn

from ..audit.audit_log import audit_event, start_audit_run
from ..repository.lookup_repository import load_lookup_frames
from ..repository.naskod_repository import NaskodRepository
from ._common import DERIVED_COLUMNS, run_retry_pipeline

logger = logging.getLogger(__name__)


def _detect_address_col(columns: list[str]) -> str | None:
    """Detect address column from column names."""
    candidates = [
        "address",
        "source_address_new",
        "source_address_old",
        "source_address",
        "full_address",
        "alamat",
        "raw_address",
        "address_clean",
    ]
    lower_map = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def _apply_corrections(
    df_failed: pl.DataFrame,
    corrections: pl.DataFrame,
) -> pl.DataFrame:
    """Apply corrections to failed rows by record_id join.

    For each correction field: use correction value if non-null,
    otherwise keep original value. Fully vectorized with pl.coalesce().
    """
    if "record_id" not in df_failed.columns:
        raise ValueError("Failed parquet must include 'record_id'.")
    if "record_id" not in corrections.columns:
        raise ValueError("Corrections CSV must include 'record_id'.")

    correction_cols = [c for c in corrections.columns if c != "record_id"]
    if not correction_cols:
        raise ValueError("Corrections CSV has no correction columns.")

    # Deduplicate corrections by record_id
    corr = corrections.unique(subset=["record_id"], keep="first")

    # Rename correction columns to avoid collision
    rename_map = {c: f"_corr_{c}" for c in correction_cols}
    corr = corr.rename(rename_map)

    # Left join on record_id
    out = df_failed.join(corr, on="record_id", how="left")

    # Detect address column for corrected_address overlay
    target_address_col = _detect_address_col(df_failed.columns)

    # Apply corrected_address if present
    if "_corr_corrected_address" in out.columns:
        if target_address_col:
            out = out.with_columns(
                pl.coalesce([
                    pl.col("_corr_corrected_address"),
                    pl.col(target_address_col),
                ]).alias(target_address_col)
            )
        else:
            out = out.with_columns(
                pl.col("_corr_corrected_address").alias("address")
            )

    # Apply other correction columns (overlay non-null corrections)
    for source_col in correction_cols:
        if source_col == "corrected_address":
            continue
        corr_col = f"_corr_{source_col}"
        if corr_col in out.columns:
            if source_col in out.columns:
                out = out.with_columns(
                    pl.coalesce([
                        pl.col(corr_col),
                        pl.col(source_col),
                    ]).alias(source_col)
                )
            else:
                out = out.with_columns(
                    pl.col(corr_col).alias(source_col)
                )

    # Drop _corr_ working columns
    drop_cols = [c for c in out.columns if c.startswith("_corr_")]
    out = out.drop(drop_cols)

    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry failed rows using record_id based corrections."
    )
    parser.add_argument("--failed-path", required=True, help="Failed parquet path")
    parser.add_argument("--corrections-csv", required=True, help="Corrections CSV path")
    parser.add_argument("--success-out", required=True, help="Retried success output parquet path")
    parser.add_argument("--warning-out", required=True, help="Retried warning output parquet path")
    parser.add_argument("--failed-out", required=True, help="Retried failed output parquet path")
    parser.add_argument("--require-mukim", action="store_true", help="Fail rows missing mukim")
    parser.add_argument("--config", default=None, help="Optional pipeline config path")
    parser.add_argument("--job-id", default="retry", help="Job identifier")
    parser.add_argument("--schema", default="nas", help="DB schema (default: nas)")
    parser.add_argument("--no-spatial", action="store_true", help="Skip spatial enrichment")
    parser.add_argument("--audit-log", default=os.getenv("NAS_AUDIT_LOG", "logs/audit.jsonl"))
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "retry_failed_rows", vars(args))
    status = "ok"
    try:
        config = load_config(args.config)
        lookups = load_lookup_frames(config=config)
        naskod_repo = NaskodRepository()
        dsn = build_dsn(driver="psycopg")

        # Read inputs
        df_failed = pl.read_parquet(args.failed_path)
        corrections = pl.read_csv(args.corrections_csv, infer_schema_length=0)

        # Apply corrections
        retry = _apply_corrections(df_failed, corrections)

        # Drop derived columns (force re-derivation)
        drop_cols = [c for c in retry.columns if c in DERIVED_COLUMNS]
        retry = retry.drop(drop_cols)

        # Determine original columns for correction CSV output
        original_columns = [c for c in retry.columns if c != "record_id"]

        # Run retry pipeline
        output_dir = Path(args.success_out).parent
        counts = run_retry_pipeline(
            retry,
            lookups=lookups,
            naskod_repo=naskod_repo,
            dsn=dsn,
            config=config,
            output_dir=output_dir,
            job_id=args.job_id,
            original_columns=original_columns,
            skip_spatial=args.no_spatial,
            schema=args.schema,
        )

        naskod_repo.close()

        audit_event(
            args.audit_log,
            "retry_failed_rows",
            run_id,
            "retry_complete",
            input_failed_count=len(df_failed),
            corrections_count=len(corrections),
            success_count=counts["success"],
            warning_count=counts["warning"],
            failed_count=counts["failed"],
        )
    except Exception as exc:
        status = "error"
        audit_event(args.audit_log, "retry_failed_rows", run_id, "run_error",
                    error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        audit_event(
            args.audit_log, "retry_failed_rows", run_id, "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
