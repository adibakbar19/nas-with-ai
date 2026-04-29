"""Executes bulk ingest and retry-failed-rows jobs dispatched from the queue worker."""

from __future__ import annotations

import os
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any

from backend.app.object_store import build_s3_client
from backend.app.runtime import jobs as ingest_jobs
from backend.app.runtime import settings as runtime_settings
from backend.app.runtime.logs import extract_error_summary, progress_from_log_line, sanitize_log_line


def _run_post_success_processing(
    *,
    job_id: str,
    success_path: Path,
    warning_path: Path | None,
    failed_path: Path,
    checkpoint_root: Path | None,
    log_path: Path,
) -> bool:
    checkpoint_root_value = str(checkpoint_root) if checkpoint_root else None

    ingest_jobs.set_job(
        job_id,
        progress_pct=97,
        progress_stage="loading_db",
        load_status="running",
        last_log_line="Starting Postgres load",
    )
    load_cmd = [
        sys.executable,
        "-m",
        "etl.load.postgres",
        "--input",
        str(success_path),
        "--table",
        "standardized_address",
    ]
    if warning_path is not None:
        load_cmd.append(str(warning_path))
    load_cmd.extend(["--mode", "append"])
    load_emit = 0.0
    load_persist = 0.0
    with log_path.open("a", encoding="utf-8") as logfile:
        logfile.write(f"[{ingest_jobs.now_iso()}] start db_load job={job_id} cmd={' '.join(load_cmd)}\n")
        logfile.flush()
        load_proc = subprocess.Popen(
            load_cmd,
            cwd=str(runtime_settings.PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
            text=True,
            bufsize=1,
        )
        if load_proc.stdout is not None:
            for raw_line in load_proc.stdout:
                logfile.write(raw_line)
                logfile.flush()
                cleaned = sanitize_log_line(raw_line)
                if cleaned:
                    now = time.time()
                    if now - load_emit >= 0.5:
                        persist_now = False
                        if runtime_settings.INGEST_PERSIST_LIVE_UPDATES and now - load_persist >= runtime_settings.INGEST_PERSIST_LIVE_INTERVAL_SECONDS:
                            persist_now = True
                            load_persist = now
                        ingest_jobs.set_job(
                            job_id,
                            persist=persist_now,
                            progress_pct=98,
                            progress_stage="loading_db",
                            load_status="running",
                            last_log_line=cleaned,
                        )
                        load_emit = now
        load_code = load_proc.wait()
        logfile.write(f"[{ingest_jobs.now_iso()}] end db_load job={job_id} exit_code={load_code}\n")
        logfile.flush()

    if load_code != 0:
        ingest_jobs.set_job(
            job_id,
            status="failed",
            ended_at=ingest_jobs.now_iso(),
            success_path=str(success_path),
            warning_path=str(warning_path) if warning_path is not None else None,
            failed_path=str(failed_path),
            checkpoint_root=checkpoint_root_value,
            error=extract_error_summary(log_path) or f"db load exit code {load_code}",
            log_path=str(log_path),
            progress_pct=100,
            progress_stage="failed",
            load_status="failed",
        )
        return False

    try:
        ingest_jobs.queue_search_sync_job(job_id)
    except Exception as exc:
        ingest_jobs.set_job(
            job_id,
            search_sync_status="failed",
            search_sync_error=f"failed to queue Elasticsearch sync: {exc}",
            last_log_line=f"Failed to queue Elasticsearch sync: {exc}",
        )

    return True


def _run_retry_failed_rows_job(job_id: str, job: dict[str, Any]) -> None:
    runtime_settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
    runtime_settings.UPLOAD_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    runtime_settings.OUTPUT_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = runtime_settings.LOG_DIR / f"{job_id}.log"

    ingest_jobs.set_job(
        job_id,
        status="running",
        started_at=ingest_jobs.now_iso(),
        ended_at=None,
        error=None,
        log_path=str(log_path),
        progress_pct=1,
        progress_stage="starting_retry",
    )

    success_path = Path(job.get("success_path") or (runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "cleaned"))
    warning_path = Path(job.get("warning_path") or (runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "warnings"))
    failed_path = Path(job.get("failed_path") or (runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "failed"))

    try:
        object_name = str(job.get("object_name") or "").strip()
        if not object_name:
            raise RuntimeError("retry job is missing corrections object_name")
        file_name = str(job.get("file_name") or "corrections.csv")
        bucket = str(job.get("bucket") or runtime_settings.OBJECT_STORE_BUCKET)
        source_failed_path_raw = str(job.get("source_failed_path") or "").strip()
        if not source_failed_path_raw:
            raise RuntimeError("retry job is missing source_failed_path")
        source_failed_path = runtime_settings.resolve_project_path(source_failed_path_raw)
        if not source_failed_path.exists():
            raise RuntimeError(f"source failed output not found: {source_failed_path}")

        config_path = str(job.get("config_path") or "config/config.json").strip()
        require_mukim = bool(job.get("require_mukim", False))
        local_input = runtime_settings.UPLOAD_STAGING_DIR / job_id / file_name
        local_input.parent.mkdir(parents=True, exist_ok=True)

        build_s3_client(runtime_settings.OBJECT_STORE).download_file(bucket, object_name, str(local_input))

        cmd = [
            sys.executable,
            "-m",
            "etl.jobs.retry_failed_rows",
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

        progress_pct = 5
        progress_stage = "retrying_failed_rows"
        last_emit = 0.0
        last_persist = 0.0

        with log_path.open("w", encoding="utf-8") as logfile:
            logfile.write(f"[{ingest_jobs.now_iso()}] start retry job={job_id} cmd={' '.join(cmd)}\n")
            logfile.flush()
            proc = subprocess.Popen(
                cmd,
                cwd=str(runtime_settings.PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=os.environ.copy(),
                text=True,
                bufsize=1,
            )
            ingest_jobs.set_job(
                job_id,
                persist=False,
                progress_pct=progress_pct,
                progress_stage=progress_stage,
                last_log_line="Retry failed rows process started",
            )
            if proc.stdout is not None:
                for raw_line in proc.stdout:
                    logfile.write(raw_line)
                    logfile.flush()
                    cleaned = sanitize_log_line(raw_line)
                    if cleaned:
                        progress_pct = max(progress_pct, 50)
                        now = time.time()
                        if now - last_emit >= 0.5:
                            persist_now = False
                            if runtime_settings.INGEST_PERSIST_LIVE_UPDATES and now - last_persist >= runtime_settings.INGEST_PERSIST_LIVE_INTERVAL_SECONDS:
                                persist_now = True
                                last_persist = now
                            ingest_jobs.set_job(
                                job_id,
                                persist=persist_now,
                                progress_pct=progress_pct,
                                progress_stage=progress_stage,
                                last_log_line=cleaned,
                            )
                            last_emit = now
            code = proc.wait()
            logfile.write(f"[{ingest_jobs.now_iso()}] end retry job={job_id} exit_code={code}\n")

        if code == 0:
            if not _run_post_success_processing(
                job_id=job_id,
                success_path=success_path,
                warning_path=warning_path,
                failed_path=failed_path,
                checkpoint_root=None,
                log_path=log_path,
            ):
                return
            ingest_jobs.set_job(
                job_id,
                status="completed",
                ended_at=ingest_jobs.now_iso(),
                success_path=str(success_path),
                warning_path=str(warning_path),
                failed_path=str(failed_path),
                log_path=str(log_path),
                progress_pct=100,
                progress_stage="completed",
                load_status="completed",
            )
            return

        ingest_jobs.set_job(
            job_id,
            status="failed",
            ended_at=ingest_jobs.now_iso(),
            success_path=str(success_path),
            warning_path=str(warning_path),
            failed_path=str(failed_path),
            error=extract_error_summary(log_path) or f"retry failed rows exit code {code}",
            log_path=str(log_path),
            progress_pct=100,
            progress_stage="failed",
            load_status="failed",
        )
    except Exception as exc:
        ingest_jobs.set_job(
            job_id,
            status="failed",
            ended_at=ingest_jobs.now_iso(),
            success_path=str(success_path),
            warning_path=str(warning_path),
            failed_path=str(failed_path),
            error=f"backend retry error: {exc}",
            log_path=str(log_path),
            progress_pct=100,
            progress_stage="failed",
            load_status="failed",
        )


def run_ingest_job(job_id: str) -> None:
    job = ingest_jobs.get_job(job_id)
    if not job:
        return

    job_type = str(job.get("job_type") or "bulk_ingest").strip().lower()
    if job_type == "retry_failed_rows":
        _run_retry_failed_rows_job(job_id, job)
        return

    runtime_settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
    runtime_settings.UPLOAD_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    runtime_settings.OUTPUT_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = runtime_settings.LOG_DIR / f"{job_id}.log"

    ingest_jobs.set_job(
        job_id,
        status="running",
        started_at=ingest_jobs.now_iso(),
        ended_at=None,
        error=None,
        log_path=str(log_path),
        progress_pct=1,
        progress_stage="starting",
    )

    success_path = Path(job.get("success_path") or (runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "cleaned"))
    warning_path = Path(job.get("warning_path") or (runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "warnings"))
    failed_path = Path(job.get("failed_path") or (runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "failed"))
    checkpoint_root = Path(job.get("checkpoint_root") or (runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "checkpoints"))

    try:
        source_type = job.get("source_type", "csv")
        object_name = job["object_name"]
        file_name = job["file_name"]
        config_path = job.get("config_path") or "config/config.json"

        local_input = runtime_settings.UPLOAD_STAGING_DIR / job_id / file_name
        local_input.parent.mkdir(parents=True, exist_ok=True)

        resume_from_checkpoint = bool(job.get("resume_from_checkpoint", True))
        resume_failed_only = bool(job.get("resume_failed_only", True))
        has_checkpoints = checkpoint_root.exists() and any(checkpoint_root.iterdir())
        should_resume = resume_from_checkpoint and has_checkpoints

        build_s3_client(runtime_settings.OBJECT_STORE).download_file(
            str(job.get("bucket") or runtime_settings.OBJECT_STORE_BUCKET),
            object_name,
            str(local_input),
        )

        cmd = [
            sys.executable,
            "-m",
            "etl.pipeline",
            "--input", str(local_input),
            "--source-type", source_type,
            "--config", config_path,
            "--success", str(success_path),
            "--warning", str(warning_path),
            "--failed", str(failed_path),
            "--checkpoint-root", str(checkpoint_root),
        ]
        if should_resume:
            cmd.append("--resume")
            if resume_failed_only:
                cmd.append("--resume-failed-only")

        progress_pct = 5
        progress_stage = "resuming" if should_resume else "starting_pipeline"
        last_emit = 0.0
        last_persist = 0.0

        with log_path.open("w", encoding="utf-8") as logfile:
            logfile.write(f"[{ingest_jobs.now_iso()}] start job={job_id} cmd={' '.join(cmd)}\n")
            logfile.flush()
            proc = subprocess.Popen(
                cmd,
                cwd=str(runtime_settings.PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=os.environ.copy(),
                text=True,
                bufsize=1,
            )
            ingest_jobs.set_job(
                job_id,
                persist=False,
                progress_pct=progress_pct,
                progress_stage=progress_stage,
                last_log_line="Resuming from checkpoints" if should_resume else "Pipeline process started",
            )
            if proc.stdout is not None:
                for raw_line in proc.stdout:
                    logfile.write(raw_line)
                    logfile.flush()
                    cleaned = sanitize_log_line(raw_line)
                    if cleaned:
                        progress_pct, progress_stage = progress_from_log_line(
                            cleaned, progress_pct, progress_stage
                        )
                        now = time.time()
                        if now - last_emit >= 0.5:
                            persist_now = False
                            if runtime_settings.INGEST_PERSIST_LIVE_UPDATES and now - last_persist >= runtime_settings.INGEST_PERSIST_LIVE_INTERVAL_SECONDS:
                                persist_now = True
                                last_persist = now
                            ingest_jobs.set_job(
                                job_id,
                                persist=persist_now,
                                progress_pct=progress_pct,
                                progress_stage=progress_stage,
                                last_log_line=cleaned,
                            )
                            last_emit = now
            code = proc.wait()
            logfile.write(f"[{ingest_jobs.now_iso()}] end job={job_id} exit_code={code}\n")

        if code == 0:
            if not _run_post_success_processing(
                job_id=job_id,
                success_path=success_path,
                warning_path=warning_path,
                failed_path=failed_path,
                checkpoint_root=checkpoint_root,
                log_path=log_path,
            ):
                return
            ingest_jobs.set_job(
                job_id,
                status="completed",
                ended_at=ingest_jobs.now_iso(),
                success_path=str(success_path),
                warning_path=str(warning_path),
                failed_path=str(failed_path),
                checkpoint_root=str(checkpoint_root),
                log_path=str(log_path),
                progress_pct=100,
                progress_stage="completed",
                load_status="completed",
            )
            return

        ingest_jobs.set_job(
            job_id,
            status="failed",
            ended_at=ingest_jobs.now_iso(),
            success_path=str(success_path),
            warning_path=str(warning_path),
            failed_path=str(failed_path),
            error=extract_error_summary(log_path) or f"pipeline exit code {code}",
            log_path=str(log_path),
            progress_pct=100,
            progress_stage="failed",
            load_status="failed",
        )
    except Exception as exc:
        ingest_jobs.set_job(
            job_id,
            status="failed",
            ended_at=ingest_jobs.now_iso(),
            success_path=str(success_path),
            warning_path=str(warning_path),
            failed_path=str(failed_path),
            error=f"backend ingest error: {exc}",
            log_path=str(log_path),
            progress_pct=100,
            progress_stage="failed",
            load_status="failed",
        )
        with log_path.open("a", encoding="utf-8") as logfile:
            logfile.write(f"[{ingest_jobs.now_iso()}] backend ingest error job={job_id}: {exc}\n")
            logfile.write(traceback.format_exc())
            logfile.write("\n")
