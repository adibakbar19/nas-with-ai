"""Progress reporting protocol for the NAS ETL pipeline.

Consumers of the pipeline (CLI, queue worker, tests) implement ProgressCallback
to receive structured progress events instead of parsing stdout markers.

The StdoutProgressCallback produces output byte-for-byte identical to the
previous _emit_stage / _stage_log / _emit_metrics functions so that
ingest_runner.py log parsing (backend/app/runtime/logs.py) continues to work
without changes during the migration.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ProgressCallback(Protocol):
    """Structured progress reporting interface for pipeline stages."""

    def on_stage_start(self, stage: str) -> None:
        """Called when a pipeline stage begins execution."""
        ...

    def on_stage_complete(
        self,
        stage: str,
        *,
        rows_processed: int,
        rows_failed: int,
        duration_ms: int,
        **extra: object,
    ) -> None:
        """Called when a pipeline stage finishes successfully.

        Extra keyword arguments (e.g. rows_sent_to_ai, rows_deduped) are
        passed through to the output for stages that emit additional metrics.
        """
        ...

    def on_checkpoint_skip(self, stage: str, path: str, row_count: int) -> None:
        """Called when a stage is skipped because its checkpoint already exists."""
        ...

    def on_checkpoint_written(self, stage: str, path: str, row_count: int) -> None:
        """Called after a stage checkpoint parquet file is written."""
        ...

    def on_counts(self, *, success: int, warning: int, failed: int) -> None:
        """Called with final row counts after validation split."""
        ...


class StdoutProgressCallback:
    """Default callback that prints markers to stdout.

    Output format is identical to the previous pipeline behavior so that
    ingest_runner.py log parsing (backend/app/runtime/logs.py) continues
    to work without changes during the migration.
    """

    def on_stage_start(self, stage: str) -> None:
        print(f"PIPELINE_STAGE:{stage}", flush=True)

    def on_stage_complete(
        self,
        stage: str,
        *,
        rows_processed: int,
        rows_failed: int,
        duration_ms: int,
        **extra: object,
    ) -> None:
        parts = f"rows_processed={rows_processed} rows_failed={rows_failed} stage_duration_ms={duration_ms}"
        if extra:
            extra_parts = " ".join(f"{k}={v}" for k, v in extra.items())
            parts = f"{parts} {extra_parts}"
        print(f"STAGE_METRICS stage={stage} {parts}", flush=True)

    def on_checkpoint_skip(self, stage: str, path: str, row_count: int) -> None:
        print(
            f"CHECKPOINT stage={stage} event=skip_resume row_count={row_count} stage_path={path}",
            flush=True,
        )

    def on_checkpoint_written(self, stage: str, path: str, row_count: int) -> None:
        print(
            f"CHECKPOINT stage={stage} event=write_done row_count={row_count} stage_path={path}",
            flush=True,
        )

    def on_counts(self, *, success: int, warning: int, failed: int) -> None:
        print(
            f"CHECKPOINT stage=load event=done success_count={success} warning_count={warning} failed_count={failed}",
            flush=True,
        )
