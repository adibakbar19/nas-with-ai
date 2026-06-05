"""Shared constants and helpers for retry job modules — Polars version."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from ..repository.lookup_repository import LookupFrames
from ..repository.naskod_repository import NaskodRepository
from ..transform.address.normalise import normalise_chunk
from ..transform.address.lookup import enrich_lookup
from ..transform.correction import write_correction_csvs
from ..pipeline.stages.validate import validate_chunk
from ..pipeline.stages.naskod import assign_naskod
from ..pipeline.stages.load import load_success_and_warning, lookup_raw_ids_by_record_ids, mark_raw_addresses_superseded
from ..pipeline.stages.spatial import enrich_spatial

logger = logging.getLogger(__name__)


# Columns that are dropped before re-processing (forces re-derivation).
DERIVED_COLUMNS: set[str] = {
    "address_clean",
    "premise_no",
    "street_name",
    "locality_name",
    "postcode",
    "state_code",
    "state_name",
    "district_code",
    "district_name",
    "mukim_code",
    "mukim_name",
    "mukim_id",
    "pbt_id",
    "pbt_name",
    "confidence_score",
    "confidence_band",
    "validation_status",
    "error_reason",
    "error_reasons",
    "warning_reason",
    "warning_reasons",
    "reason_codes",
    "suggestion",
    "naskod",
    "canonical_address_key",
    "address_norm",
    "source_address_new",
    "address_for_lookup",
    "postcode_raw",
    "postcode_ungazetted",
    "_spatial_confirmed",
}


def run_retry_pipeline(
    df: pl.DataFrame,
    *,
    lookups: LookupFrames,
    naskod_repo: NaskodRepository,
    dsn: str,
    config: dict,
    output_dir: Path,
    job_id: str,
    original_columns: list[str],
    skip_spatial: bool = False,
    schema: str = "nas",
    agency_id: str | None = None,
) -> dict[str, int]:
    """Run the retry pipeline on corrected rows.

    Takes a Polars DataFrame (corrections already applied),
    processes through normalise → lookup → spatial → validate →
    naskod → load → correction CSVs.

    Chains new raw_address rows to original failed rows via replaces_raw_id.

    Returns counts: {'success': N, 'warning': N, 'failed': N}.
    """
    logger.info("retry_pipeline_start rows=%d job_id=%s", len(df), job_id)

    # Look up original raw_ids for submission chaining
    record_ids = df["record_id"].to_list() if "record_id" in df.columns else []
    replaces_map = lookup_raw_ids_by_record_ids(record_ids, dsn=dsn, schema=schema) if record_ids else {}
    if replaces_map:
        logger.info("retry_chain_found originals=%d", len(replaces_map))

    # 1. Normalise
    df = normalise_chunk(df, config=config)

    # 2. Lookup enrichment (three-tier)
    df = enrich_lookup(df, lookups)

    # 3. Spatial enrichment (optional)
    if not skip_spatial:
        df = enrich_spatial(df, dsn=dsn, lookup_schema="nas_lookup")

    # 4. Validate → split
    success, warning, failed = validate_chunk(df)

    # 5. Assign NASKOD (success + warning only)
    if not success.is_empty():
        success = assign_naskod(success, naskod_repo=naskod_repo)
    if not warning.is_empty():
        warning = assign_naskod(warning, naskod_repo=naskod_repo)

    # 6. Load to DB (success + warning + failed → raw_address)
    if not success.is_empty() or not warning.is_empty() or not failed.is_empty():
        try:
            load_success_and_warning(
                success, warning, failed, dsn=dsn, schema=schema,
                job_id=job_id, agency_id=agency_id,
                replaces_map=replaces_map if replaces_map else None,
            )
        except Exception as exc:
            logger.error("retry_load_failed job_id=%s error=%s", job_id, exc)

    # 7. Mark original failed rows as superseded
    if replaces_map:
        try:
            mark_raw_addresses_superseded(list(replaces_map.values()), dsn=dsn, schema=schema)
        except Exception as exc:
            logger.error("retry_mark_superseded_failed job_id=%s error=%s", job_id, exc)

    # 8. Write correction CSVs (warning + failed)
    write_correction_csvs(
        warning, failed,
        output_dir=output_dir,
        job_id=job_id,
        original_columns=original_columns,
    )

    # 9. Write parquet outputs for downstream consumers
    output_dir.mkdir(parents=True, exist_ok=True)
    if not success.is_empty():
        success.write_parquet(output_dir / f"{job_id}_success.parquet")
    if not warning.is_empty():
        warning.write_parquet(output_dir / f"{job_id}_warning.parquet")
    if not failed.is_empty():
        failed.write_parquet(output_dir / f"{job_id}_failed.parquet")

    counts = {
        "success": len(success),
        "warning": len(warning),
        "failed": len(failed),
    }
    logger.info(
        "retry_pipeline_complete job_id=%s success=%d warning=%d failed=%d",
        job_id, counts["success"], counts["warning"], counts["failed"],
    )
    return counts
