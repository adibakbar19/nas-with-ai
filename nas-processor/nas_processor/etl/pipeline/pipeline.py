"""NAS ETL pipeline v2 — chunked streaming orchestrator.

Streams source files in 500k-row chunks through:
  extract → normalise → lookup → enrich → validate →
  naskod → load → correction CSV → checkpoint

Fully Polars end-to-end. No pandas anywhere.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

import polars as pl

from nas_config.config_loader import load_config
from nas_config.db import build_dsn

from ..repository.lookup_repository import LookupFrames, load_lookup_frames
from ..repository.naskod_repository import NaskodRepository
from ..transform.address.normalise import normalise_chunk
from ..transform.address.lookup import enrich_lookup
from ..transform.correction import write_correction_csvs
from .enrichers import AddressEnricher, NoOpEnricher
from .stages.classify import classify_dataframe, load_type_id_map, resolve_type_ids
from .stages.extract import extract_chunks
from .stages.validate import validate_chunk
from .stages.naskod import assign_naskod
from .stages.load import load_success_and_warning
from .stages.spatial import enrich_spatial

logger = logging.getLogger(__name__)


# ── Progress callback (inline fallback) ───────────────────────────────────────

try:
    from nas_contracts.progress import ProgressCallback, StdoutProgressCallback
except ImportError:
    from typing import Protocol

    class ProgressCallback(Protocol):  # type: ignore[no-redef]
        def on_stage_start(self, stage: str) -> None: ...
        def on_stage_complete(self, stage: str, *, rows_processed: int, rows_failed: int, duration_ms: int, **extra: Any) -> None: ...

    class StdoutProgressCallback:  # type: ignore[no-redef]
        def on_stage_start(self, stage: str) -> None:
            print(f"PIPELINE_STAGE:{stage}", flush=True)

        def on_stage_complete(self, stage: str, *, rows_processed: int, rows_failed: int, duration_ms: int, **extra: Any) -> None:
            parts = f"rows_processed={rows_processed} rows_failed={rows_failed} stage_duration_ms={duration_ms}"
            if extra:
                parts += " " + " ".join(f"{k}={v}" for k, v in extra.items())
            print(f"STAGE_METRICS stage={stage} {parts}", flush=True)


# ── Reference data loading ────────────────────────────────────────────────────


def load_reference_data(config: dict) -> tuple[LookupFrames, NaskodRepository]:
    """Load all reference data needed by the pipeline.

    Loads lookup frames from DB (Polars DataFrames).
    Creates NaskodRepository instance.
    Called once before chunk processing begins.
    """
    lookups = load_lookup_frames(config=config)
    naskod_repo = NaskodRepository()
    return lookups, naskod_repo


# ── Checkpoint helpers ────────────────────────────────────────────────────────


def _checkpoint_path(
    checkpoint_root: Path,
    chunk_num: int,
    split: str,
) -> Path:
    """Return path for a chunk checkpoint file.

    Format: {checkpoint_root}/chunk_{chunk_num:04d}_{split}.parquet
    """
    return checkpoint_root / f"chunk_{chunk_num:04d}_{split}.parquet"


def _chunk_is_checkpointed(checkpoint_root: Path, chunk_num: int) -> bool:
    """Return True if all three split files exist for this chunk."""
    return all(
        _checkpoint_path(checkpoint_root, chunk_num, split).exists()
        for split in ("success", "warning", "failed")
    )


def _write_checkpoint(
    checkpoint_root: Path,
    chunk_num: int,
    success: pl.DataFrame,
    warning: pl.DataFrame,
    failed: pl.DataFrame,
) -> None:
    """Write all three split checkpoints for a chunk."""
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    for split, df in [("success", success), ("warning", warning), ("failed", failed)]:
        path = _checkpoint_path(checkpoint_root, chunk_num, split)
        df.write_parquet(path)


def _read_checkpoint(
    checkpoint_root: Path,
    chunk_num: int,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Read all three split checkpoints for a chunk."""
    success = pl.read_parquet(_checkpoint_path(checkpoint_root, chunk_num, "success"))
    warning = pl.read_parquet(_checkpoint_path(checkpoint_root, chunk_num, "warning"))
    failed = pl.read_parquet(_checkpoint_path(checkpoint_root, chunk_num, "failed"))
    return success, warning, failed


# ── Pipeline execution ────────────────────────────────────────────────────────


def run_pipeline(
    args: argparse.Namespace,
    *,
    progress: ProgressCallback | None = None,
    enricher: AddressEnricher | None = None,
) -> dict[str, int]:
    """Execute the full ETL pipeline.

    Pipeline per chunk:
    1. extract chunk (streaming)
    2. normalise_chunk
    3. enrich_lookup (three-tier)
    4. enricher.enrich (NoOp or Bedrock)
    5. validate_chunk → (success, warning, failed)
    6. assign_naskod (success + warning only)
    7. load_success_and_warning (DB upsert)
    8. write_correction_csvs (warning + failed → CSV)
    9. checkpoint (parquet for resume)
    """
    if progress is None:
        progress = StdoutProgressCallback()
    if enricher is None:
        enricher = NoOpEnricher()

    started = time.time()
    progress.on_stage_start("pipeline")

    # Load config
    config = load_config(args.config if hasattr(args, "config") and args.config else None)

    # Load reference data (once)
    logger.info("pipeline_loading_reference_data")
    lookups, naskod_repo = load_reference_data(config)

    # Resolve paths
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_root = Path(
        args.checkpoint_root
        if hasattr(args, "checkpoint_root") and args.checkpoint_root
        else str(output_dir / "checkpoints")
    )
    checkpoint_root.mkdir(parents=True, exist_ok=True)

    resume = getattr(args, "resume", False)
    chunk_size = getattr(args, "chunk_size", 500_000)
    schema = getattr(args, "schema", "nas")
    job_id = getattr(args, "job_id", "unknown")
    skip_spatial = getattr(args, "no_spatial", False)

    # DSN from env
    dsn = build_dsn(driver="psycopg")

    # Load address type lookup map (once) — used in classify stage per chunk
    type_id_map = load_type_id_map(dsn=dsn, schema=schema)
    logger.info("address_type_map_loaded types=%d", len(type_id_map))

    # Track totals
    totals = {
        "total_rows": 0,
        "success_rows": 0,
        "warning_rows": 0,
        "failed_rows": 0,
        "chunks_processed": 0,
        "chunks_skipped": 0,
    }

    # Track original columns from source file (for correction CSVs)
    original_columns: list[str] = []

    # Stream chunks
    chunk_num = 0
    for chunk in extract_chunks(input_path, chunk_size=chunk_size, config=config, progress=progress):
        chunk_num += 1
        chunk_rows = len(chunk)
        totals["total_rows"] += chunk_rows

        # Capture original columns from first chunk
        if not original_columns:
            original_columns = [c for c in chunk.columns if c != "record_id"]

        progress.on_stage_start(f"chunk_{chunk_num}")
        logger.info("chunk_start chunk=%d rows=%d", chunk_num, chunk_rows)

        # Resume: skip if checkpointed
        if resume and _chunk_is_checkpointed(checkpoint_root, chunk_num):
            success, warning, failed = _read_checkpoint(checkpoint_root, chunk_num)
            totals["success_rows"] += len(success)
            totals["warning_rows"] += len(warning)
            totals["failed_rows"] += len(failed)
            totals["chunks_skipped"] += 1
            logger.info(
                "chunk_resumed chunk=%d success=%d warning=%d failed=%d",
                chunk_num, len(success), len(warning), len(failed),
            )
            continue

        t0 = time.perf_counter()

        # 2. Normalise
        chunk = normalise_chunk(chunk, config=config)

        # 3. Lookup enrichment (three-tier)
        chunk = enrich_lookup(chunk, lookups)

        # 4. AI enrichment (NoOp by default)
        chunk = enricher.enrich(chunk)

        # 5. Spatial enrichment (PostGIS boundary joins — coordinates only)
        if not skip_spatial:
            chunk = enrich_spatial(chunk, dsn=dsn, lookup_schema="nas_lookup")
        else:
            logger.debug("spatial_skipped reason=--no-spatial flag")

        # 6. Validate → split
        success, warning, failed = validate_chunk(chunk)

        # 7. Assign NASKOD (success + warning only)
        if not success.is_empty():
            success = assign_naskod(success, naskod_repo=naskod_repo)
        if not warning.is_empty():
            warning = assign_naskod(warning, naskod_repo=naskod_repo)

        # 7b. Classify address type (success + warning only)
        if not success.is_empty():
            success = classify_dataframe(success)
            success = resolve_type_ids(success, type_id_map)
        if not warning.is_empty():
            warning = classify_dataframe(warning)
            warning = resolve_type_ids(warning, type_id_map)
        logger.info(
            "classify_complete chunk=%d success_typed=%d warning_typed=%d",
            chunk_num,
            0 if success.is_empty() else success.filter(pl.col("address_type_id").is_not_null()).height,
            0 if warning.is_empty() else warning.filter(pl.col("address_type_id").is_not_null()).height,
        )

        # 8. Load to DB (success + warning + failed→raw_address)
        if not success.is_empty() or not warning.is_empty() or not failed.is_empty():
            try:
                load_success_and_warning(
                    success, warning, failed, dsn=dsn, schema=schema,
                    job_id=job_id, agency_id=getattr(args, "agency_id", None),
                )
            except Exception as exc:
                logger.error("load_failed chunk=%d error=%s", chunk_num, exc)
                # Continue — data is still in checkpoint

        # 9. Correction CSVs
        write_correction_csvs(
            warning, failed,
            output_dir=output_dir,
            job_id=f"{job_id}_chunk{chunk_num:04d}",
            original_columns=original_columns,
        )

        # 10. Checkpoint
        _write_checkpoint(checkpoint_root, chunk_num, success, warning, failed)

        # Update totals
        totals["success_rows"] += len(success)
        totals["warning_rows"] += len(warning)
        totals["failed_rows"] += len(failed)
        totals["chunks_processed"] += 1

        duration_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            "chunk_complete chunk=%d success=%d warning=%d failed=%d duration_ms=%d",
            chunk_num, len(success), len(warning), len(failed), duration_ms,
        )
        progress.on_stage_complete(
            f"chunk_{chunk_num}",
            rows_processed=chunk_rows,
            rows_failed=len(failed),
            duration_ms=duration_ms,
        )

    # Cleanup
    naskod_repo.close()

    total_duration_ms = int((time.time() - started) * 1000)
    logger.info(
        "pipeline_complete total_rows=%d success=%d warning=%d failed=%d "
        "chunks_processed=%d chunks_skipped=%d duration_ms=%d",
        totals["total_rows"], totals["success_rows"], totals["warning_rows"],
        totals["failed_rows"], totals["chunks_processed"], totals["chunks_skipped"],
        total_duration_ms,
    )
    progress.on_stage_complete(
        "pipeline",
        rows_processed=totals["total_rows"],
        rows_failed=totals["failed_rows"],
        duration_ms=total_duration_ms,
    )

    return totals


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="NAS ETL pipeline v2 — chunked streaming address normalization."
    )
    parser.add_argument("--input", required=True, help="Source file path (CSV/JSON/Excel)")
    parser.add_argument("--output-dir", required=True, help="Output directory for correction CSVs")
    parser.add_argument("--job-id", required=True, help="Job identifier for output filenames")
    parser.add_argument("--config", default=None, help="Config JSON path (default: config/config.json)")
    parser.add_argument("--checkpoint-root", default=None, help="Checkpoint directory")
    parser.add_argument("--resume", action="store_true", help="Resume from existing checkpoints")
    parser.add_argument("--chunk-size", type=int, default=500_000, help="Rows per chunk (default: 500000)")
    parser.add_argument("--schema", default="nas", help="DB schema (default: nas)")
    parser.add_argument("--no-spatial", action="store_true", help="Skip spatial enrichment")
    parser.add_argument("--agency-id", default=None, help="Agency identifier for raw_address tracking")
    parser.add_argument("--enricher", default="noop", choices=["noop", "bedrock"], help="Enricher: noop|bedrock")
    return parser.parse_args(argv)


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = parse_args()

    # Resolve enricher (lazy import for Bedrock)
    enricher: AddressEnricher
    if args.enricher == "bedrock":
        from .enrichers.bedrock import BedrockEnricher
        enricher = BedrockEnricher()
    else:
        enricher = NoOpEnricher()

    run_pipeline(args, enricher=enricher)


if __name__ == "__main__":
    main()
