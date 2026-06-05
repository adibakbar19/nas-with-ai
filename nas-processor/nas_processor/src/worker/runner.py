"""Job runner with explicit dependency injection.

Replaces the implicit IngestService().run_job() → ingest_runner.run_ingest_job()
call chain. All dependencies (S3 client, job state, config) are injected via
the constructor. The ETL pipeline is called in-process using run_pipeline()
with a DbProgressCallback that writes progress directly to job state.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from nas_contracts.progress import ProgressCallback

logger = logging.getLogger(__name__)


# ── Protocols ──────────────────────────────────────────────────────────────────

class JobStateRepository(Protocol):
    """Protocol for job state persistence."""

    def get_job(self, job_id: str) -> dict[str, Any] | None: ...
    def set_job(self, job_id: str, *, persist: bool = True, **changes: Any) -> None: ...
    def queue_search_sync_job(self, job_id: str) -> None: ...


class S3Client(Protocol):
    """Protocol for S3 operations."""

    def download_file(self, bucket: str, key: str, filename: str) -> None: ...
    def upload_file(self, filename: str, bucket: str, key: str) -> None: ...


# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RunnerConfig:
    """Configuration values injected at construction time."""

    project_root: Path
    log_dir: Path
    upload_staging_dir: Path
    output_uploads_dir: Path
    default_bucket: str
    db_schema: str
    persist_live_updates: bool = True
    persist_live_interval_seconds: float = 2.0


# ── Progress callback ──────────────────────────────────────────────────────────

class DbProgressCallback:
    """Implements ProgressCallback by writing directly to job state DB.

    Used when calling run_pipeline() in-process — replaces stdout parsing.
    """

    STAGE_PCTS: dict[str, int] = {
        "extract": 10,
        "clean_text": 25,
        "matcher": 40,
        "spatial_validate": 55,
        "validate": 70,
        "final": 85,
        "load": 90,
    }

    def __init__(
        self,
        *,
        job_id: str,
        job_state: JobStateRepository,
        persist_interval_seconds: float = 2.0,
    ) -> None:
        self._job_id = job_id
        self._jobs = job_state
        self._interval = persist_interval_seconds
        self._last_persist = 0.0

    def on_stage_start(self, stage: str) -> None:
        pct = self.STAGE_PCTS.get(stage, 50)
        self._write(progress_pct=pct, progress_stage=stage)

    def on_stage_complete(
        self,
        stage: str,
        *,
        rows_processed: int,
        rows_failed: int,
        duration_ms: int,
        **extra: object,
    ) -> None:
        self._write(last_log_line=f"{stage} done: {rows_processed} rows, {rows_failed} failed")

    def on_checkpoint_skip(self, stage: str, path: str, row_count: int) -> None:
        self._write(last_log_line=f"resuming {stage} ({row_count} rows)")

    def on_checkpoint_written(self, stage: str, path: str, row_count: int) -> None:
        self._write(last_log_line=f"checkpoint {stage} ({row_count} rows)")

    def on_counts(self, *, success: int, warning: int, failed: int) -> None:
        self._write(
            progress_pct=95,
            progress_stage="writing_output",
            last_log_line=f"success={success} warning={warning} failed={failed}",
        )

    def _write(self, **fields: Any) -> None:
        now = time.time()
        persist = now - self._last_persist >= self._interval
        if persist:
            self._last_persist = now
        self._jobs.set_job(self._job_id, persist=persist, **fields)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_log_line(raw: str) -> str:
    import re
    return re.sub(r"\x1b\[[0-9;]*m", "", raw).rstrip()


def _parse_counts_from_log(log_path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not log_path.exists():
        return counts
    try:
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if "success_count=" in line and "warning_count=" in line and "failed_count=" in line:
                for part in line.split():
                    if "=" in part:
                        key, _, val = part.partition("=")
                        if key in ("success_count", "warning_count", "failed_count"):
                            try:
                                counts[key] = int(val)
                            except ValueError:
                                pass
    except OSError:
        pass
    return counts


def _extract_error_summary(log_path: Path) -> str | None:
    if not log_path.exists():
        return None
    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()
        for line in reversed(lines):
            stripped = line.strip()
            if stripped and ("error" in stripped.lower() or "exception" in stripped.lower()):
                return stripped[:500]
    except OSError:
        pass
    return None


# ── JobRunner ──────────────────────────────────────────────────────────────────

class JobRunner:
    """Executes ingest jobs with explicit dependencies.

    The ETL pipeline is called in-process via run_pipeline() with a
    DbProgressCallback. The DB load step remains a subprocess call.
    """

    def __init__(
        self,
        *,
        s3_client: S3Client,
        job_state: JobStateRepository,
        config: RunnerConfig,
    ) -> None:
        self._s3 = s3_client
        self._jobs = job_state
        self._config = config

    def run_job(self, job_id: str) -> None:
        """Execute a job by ID. Dispatches to bulk_ingest or retry_failed_rows."""
        job = self._jobs.get_job(job_id)
        if not job:
            logger.warning("job_not_found job_id=%s", job_id)
            return

        job_type = str(job.get("job_type") or "bulk_ingest").strip().lower()
        if job_type == "retry_failed_rows":
            self._run_retry(job_id, job)
        else:
            self._run_bulk_ingest(job_id, job)

    def _run_bulk_ingest(self, job_id: str, job: dict[str, Any]) -> None:
        cfg = self._config
        cfg.log_dir.mkdir(parents=True, exist_ok=True)
        cfg.upload_staging_dir.mkdir(parents=True, exist_ok=True)
        cfg.output_uploads_dir.mkdir(parents=True, exist_ok=True)
        log_path = cfg.log_dir / f"{job_id}.log"

        self._jobs.set_job(
            job_id, status="running", started_at=_now_iso(),
            ended_at=None, error=None, log_path=str(log_path),
            progress_pct=1, progress_stage="starting",
        )

        success_path = Path(job.get("success_path") or (cfg.output_uploads_dir / job_id / "cleaned"))
        warning_path = Path(job.get("warning_path") or (cfg.output_uploads_dir / job_id / "warnings"))
        failed_path = Path(job.get("failed_path") or (cfg.output_uploads_dir / job_id / "failed"))
        checkpoint_root = Path(job.get("checkpoint_root") or (cfg.output_uploads_dir / job_id / "checkpoints"))

        try:
            source_type = job.get("source_type", "csv")
            object_name = job["object_name"]
            file_name = job["file_name"]
            config_path = job.get("config_path") or "config/config.json"
            bucket = str(job.get("bucket") or cfg.default_bucket)

            local_input = cfg.upload_staging_dir / job_id / file_name
            local_input.parent.mkdir(parents=True, exist_ok=True)

            resume_from_checkpoint = bool(job.get("resume_from_checkpoint", True))
            resume_failed_only = bool(job.get("resume_failed_only", True))
            has_checkpoints = checkpoint_root.exists() and any(checkpoint_root.iterdir())
            should_resume = resume_from_checkpoint and has_checkpoints

            # Download source from S3
            self._s3.download_file(bucket, object_name, str(local_input))

            # Build pipeline args and run in-process
            from nas_processor.etl.pipeline.pipeline import run_pipeline, parse_args as pipeline_parse_args

            # output_dir is the parent directory for all pipeline output
            output_dir = success_path.parent

            pipeline_argv = [
                "--input", str(local_input),
                "--output-dir", str(output_dir),
                "--job-id", job_id,
                "--config", config_path,
                "--checkpoint-root", str(checkpoint_root),
            ]
            agency_id = str(job.get("agency_id") or "")
            if agency_id:
                pipeline_argv.extend(["--agency-id", agency_id])
            if should_resume:
                pipeline_argv.append("--resume")

            args = pipeline_parse_args(pipeline_argv)
            progress = DbProgressCallback(
                job_id=job_id,
                job_state=self._jobs,
                persist_interval_seconds=cfg.persist_live_interval_seconds,
            )

            # Run ETL pipeline in-process with structured progress
            run_pipeline(args, progress=progress)

            # Pipeline succeeded — run post-success processing
            if not self._post_success(job_id, success_path, warning_path, failed_path, checkpoint_root, log_path):
                return

            output_prefix = self._upload_output(job_id, job, success_path, warning_path, failed_path, log_path)
            self._jobs.set_job(
                job_id, status="completed", ended_at=_now_iso(),
                success_path=str(success_path), warning_path=str(warning_path),
                failed_path=str(failed_path), checkpoint_root=str(checkpoint_root),
                log_path=str(log_path), output_object_prefix=output_prefix,
                progress_pct=100, progress_stage="completed", load_status="completed",
            )

        except Exception as exc:
            self._jobs.set_job(
                job_id, status="failed", ended_at=_now_iso(),
                error=f"pipeline error: {exc}",
                log_path=str(log_path), progress_pct=100,
                progress_stage="failed", load_status="failed",
            )
            with log_path.open("a", encoding="utf-8") as f:
                f.write(f"[{_now_iso()}] pipeline error job={job_id}: {exc}\n")
                f.write(traceback.format_exc() + "\n")

    def _run_retry(self, job_id: str, job: dict[str, Any]) -> None:
        """Run retry-failed-rows job (still subprocess — different CLI tool)."""
        cfg = self._config
        cfg.log_dir.mkdir(parents=True, exist_ok=True)
        cfg.upload_staging_dir.mkdir(parents=True, exist_ok=True)
        cfg.output_uploads_dir.mkdir(parents=True, exist_ok=True)
        log_path = cfg.log_dir / f"{job_id}.log"

        self._jobs.set_job(
            job_id, status="running", started_at=_now_iso(),
            ended_at=None, error=None, log_path=str(log_path),
            progress_pct=1, progress_stage="starting_retry",
        )

        success_path = Path(job.get("success_path") or (cfg.output_uploads_dir / job_id / "cleaned"))
        warning_path = Path(job.get("warning_path") or (cfg.output_uploads_dir / job_id / "warnings"))
        failed_path = Path(job.get("failed_path") or (cfg.output_uploads_dir / job_id / "failed"))

        try:
            object_name = str(job.get("object_name") or "").strip()
            if not object_name:
                raise RuntimeError("retry job is missing corrections object_name")
            file_name = str(job.get("file_name") or "corrections.csv")
            bucket = str(job.get("bucket") or cfg.default_bucket)
            source_failed_path_raw = str(job.get("source_failed_path") or "").strip()
            if not source_failed_path_raw:
                raise RuntimeError("retry job is missing source_failed_path")
            source_failed_path = Path(source_failed_path_raw)
            if not source_failed_path.is_absolute():
                source_failed_path = cfg.project_root / source_failed_path
            if not source_failed_path.exists():
                raise RuntimeError(f"source failed output not found: {source_failed_path}")

            config_path = str(job.get("config_path") or "config/config.json").strip()
            require_mukim = bool(job.get("require_mukim", False))
            local_input = cfg.upload_staging_dir / job_id / file_name
            local_input.parent.mkdir(parents=True, exist_ok=True)

            self._s3.download_file(bucket, object_name, str(local_input))

            cmd = [
                sys.executable, "-m", "nas_processor.etl.jobs.retry_failed_rows",
                "--failed-path", str(source_failed_path),
                "--corrections-csv", str(local_input),
                "--success-out", str(success_path),
                "--warning-out", str(warning_path),
                "--failed-out", str(failed_path),
            ]
            if config_path:
                cmd.extend(["--config", config_path])
            if require_mukim:
                cmd.append("--require-mukim")

            code = self._run_subprocess(job_id, cmd, log_path)
            counts = _parse_counts_from_log(log_path)

            if code == 0:
                if not self._post_success(job_id, success_path, warning_path, failed_path, None, log_path):
                    return
                output_prefix = self._upload_output(job_id, job, success_path, warning_path, failed_path, log_path)
                self._jobs.set_job(
                    job_id, status="completed", ended_at=_now_iso(),
                    success_path=str(success_path), warning_path=str(warning_path),
                    failed_path=str(failed_path), log_path=str(log_path),
                    output_object_prefix=output_prefix,
                    progress_pct=100, progress_stage="completed", load_status="completed",
                    **counts,
                )
                return

            self._jobs.set_job(
                job_id, status="failed", ended_at=_now_iso(),
                success_path=str(success_path), warning_path=str(warning_path),
                failed_path=str(failed_path),
                error=_extract_error_summary(log_path) or f"retry exit code {code}",
                log_path=str(log_path), progress_pct=100,
                progress_stage="failed", load_status="failed", **counts,
            )
        except Exception as exc:
            self._jobs.set_job(
                job_id, status="failed", ended_at=_now_iso(),
                error=f"backend retry error: {exc}",
                log_path=str(log_path), progress_pct=100,
                progress_stage="failed", load_status="failed",
            )

    def _run_subprocess(self, job_id: str, cmd: list[str], log_path: Path) -> int:
        """Run a subprocess (DB load, retry). Streams stdout to log + updates progress."""
        cfg = self._config
        last_emit = 0.0
        last_persist = 0.0

        with log_path.open("a", encoding="utf-8") as logfile:
            logfile.write(f"[{_now_iso()}] start subprocess job={job_id}\n")
            logfile.flush()
            proc = subprocess.Popen(
                cmd, cwd=str(cfg.project_root),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                env=os.environ.copy(), text=True, bufsize=1,
            )
            if proc.stdout is not None:
                for raw_line in proc.stdout:
                    logfile.write(raw_line)
                    logfile.flush()
                    cleaned = _sanitize_log_line(raw_line)
                    if cleaned:
                        now = time.time()
                        if now - last_emit >= 0.5:
                            persist_now = cfg.persist_live_updates and now - last_persist >= cfg.persist_live_interval_seconds
                            if persist_now:
                                last_persist = now
                            self._jobs.set_job(
                                job_id, persist=persist_now,
                                last_log_line=cleaned,
                            )
                            last_emit = now
            code = proc.wait()
            logfile.write(f"[{_now_iso()}] end subprocess job={job_id} exit_code={code}\n")
        return code

    def _post_success(
        self,
        job_id: str,
        success_path: Path,
        warning_path: Path | None,
        failed_path: Path,
        checkpoint_root: Path | None,
        log_path: Path,
    ) -> bool:
        """Queue search sync after pipeline success. Returns True always.

        DB load is now handled in-process by the pipeline's load stage.
        This method only queues the async search sync job.
        """
        try:
            self._jobs.queue_search_sync_job(job_id)
        except Exception as exc:
            self._jobs.set_job(
                job_id, search_sync_status="failed",
                search_sync_error=f"failed to queue search sync: {exc}",
                last_log_line=f"Failed to queue search sync: {exc}",
            )
        return True

    def _upload_output(
        self,
        job_id: str,
        job: dict[str, Any],
        success_path: Path,
        warning_path: Path | None,
        failed_path: Path,
        log_path: Path,
    ) -> str | None:
        """Upload output to S3. Returns prefix on success, None on failure."""
        agency_id = str(job.get("agency_id") or "")
        bucket = str(job.get("bucket") or self._config.default_bucket)
        prefix = f"agency/{agency_id}/jobs/{job_id}/output/"

        try:
            def _upload_dir(local_dir: Path, s3_subdir: str) -> None:
                if not local_dir.exists():
                    return
                for fp in local_dir.rglob("*"):
                    if fp.is_file():
                        key = f"{prefix}{s3_subdir}/{fp.relative_to(local_dir)}"
                        self._s3.upload_file(str(fp), bucket, key)

            _upload_dir(success_path, "cleaned")
            if warning_path:
                _upload_dir(warning_path, "warnings")
            _upload_dir(failed_path, "failed")
            if log_path.exists():
                self._s3.upload_file(str(log_path), bucket, f"{prefix}logs/{log_path.name}")
            return prefix
        except Exception:
            logger.warning("s3_output_upload_failed job_id=%s", job_id, exc_info=True)
            self._jobs.set_job(job_id, s3_output_upload_failed=True)
            return None
