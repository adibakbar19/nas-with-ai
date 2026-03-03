import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from etl.env_check import validate_backend_env
from backend.app.dependencies import get_queue_producer
from backend.app.schemas.ingest import BulkIngestEvent


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parents[1]
LOG_DIR = PROJECT_ROOT / "logs" / "jobs"
JOB_STATE_FILE = PROJECT_ROOT / "logs" / "ingest_jobs_state.json"
UPLOAD_STAGING_DIR = PROJECT_ROOT / "data" / "uploads"
OUTPUT_UPLOADS_DIR = PROJECT_ROOT / "output" / "uploads"
ENV_FILE = PROJECT_ROOT / ".env"


def _load_env_file(env_file: Path) -> None:
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ.setdefault(key, value)


def _parse_cors_origins(raw: str) -> list[str]:
    origins = [item.strip() for item in raw.split(",")]
    return [item for item in origins if item]


_load_env_file(ENV_FILE)
RUNNING_RECOVERY_WINDOW_SECONDS = max(
    60,
    int(os.getenv("INGEST_RUNNING_RECOVERY_WINDOW_SECONDS", "600")),
)
STRICT_ENV_CHECK = os.getenv("STRICT_ENV_CHECK", "1").lower() in {"1", "true", "yes", "y"}
if STRICT_ENV_CHECK:
    missing_backend_env = validate_backend_env()
    if missing_backend_env:
        raise RuntimeError(f"Missing required backend env vars: {', '.join(missing_backend_env)}")

ES_URL = os.getenv("ES_URL", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "nas_addresses")
INGEST_AUTO_SYNC_ES = os.getenv("INGEST_AUTO_SYNC_ES", "1").lower() in {"1", "true", "yes", "y"}
INGEST_PERSIST_LIVE_UPDATES = os.getenv("INGEST_PERSIST_LIVE_UPDATES", "1").lower() in {"1", "true", "yes", "y"}
INGEST_PERSIST_LIVE_INTERVAL_SECONDS = max(
    0.5,
    float(os.getenv("INGEST_PERSIST_LIVE_INTERVAL_SECONDS", "2.0")),
)
AUDIT_LOG_PATH = Path(os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in {"1", "true", "yes", "y"}
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "nas-uploads")
INGEST_EXECUTION_MODE = os.getenv("INGEST_EXECUTION_MODE", "local_thread").strip().lower()
CORS_ALLOW_ORIGINS = _parse_cors_origins(
    os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:3000,http://localhost:5173,https://admin.alamat.gov.my",
    )
)


app = FastAPI(title="NAS API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

INGEST_JOBS: dict[str, dict[str, Any]] = {}
INGEST_JOBS_LOCK = threading.Lock()
INGEST_PROCS: dict[str, dict[str, Any]] = {}
INGEST_PROCS_LOCK = threading.Lock()
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_minio_client() -> Minio:
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in .env")
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    return client


def _infer_source_type(filename: str) -> str:
    lower_name = filename.lower()
    if lower_name.endswith(".json") or lower_name.endswith(".jsonl") or lower_name.endswith(".ndjson"):
        return "json"
    if lower_name.endswith(".xlsx") or lower_name.endswith(".xls"):
        return "excel"
    return "csv"


def _persist_jobs_state_unlocked() -> None:
    JOB_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    snapshot = list(INGEST_JOBS.values())
    JOB_STATE_FILE.write_text(json.dumps(snapshot, ensure_ascii=True, indent=2), encoding="utf-8")


def _set_job(job_id: str, *, persist: bool = True, **changes: Any) -> None:
    with INGEST_JOBS_LOCK:
        if job_id not in INGEST_JOBS:
            INGEST_JOBS[job_id] = {"job_id": job_id}
        INGEST_JOBS[job_id].update(changes)
        if persist:
            _persist_jobs_state_unlocked()


def _load_jobs_state() -> None:
    if not JOB_STATE_FILE.exists():
        return
    try:
        rows = json.loads(JOB_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(rows, list):
        return
    restored: dict[str, dict[str, Any]] = {}
    now = _now_iso()
    now_ts = datetime.now(timezone.utc).timestamp()
    for row in rows:
        if not isinstance(row, dict):
            continue
        job_id = str(row.get("job_id") or "").strip()
        if not job_id:
            continue
        status = str(row.get("status") or "").lower()
        if status in {"running", "queued", "pausing"}:
            keep_running = False
            if status == "running":
                log_path_value = row.get("log_path")
                if log_path_value:
                    log_path = Path(str(log_path_value))
                    if log_path.exists():
                        try:
                            modified_ago = now_ts - log_path.stat().st_mtime
                        except OSError:
                            modified_ago = RUNNING_RECOVERY_WINDOW_SECONDS + 1
                        if modified_ago <= RUNNING_RECOVERY_WINDOW_SECONDS:
                            keep_running = True
            if keep_running:
                row["status"] = "running"
                row.setdefault("progress_stage", "starting")
                row.setdefault("ended_at", None)
                row.setdefault("error", None)
            else:
                row["status"] = "interrupted"
                row.setdefault("ended_at", now)
                row.setdefault("error", "Backend restarted before job completion")
                row["progress_stage"] = "interrupted"
                if row.get("load_to_db"):
                    row["load_status"] = "pending"
        restored[job_id] = row
    with INGEST_JOBS_LOCK:
        INGEST_JOBS.clear()
        INGEST_JOBS.update(restored)


def _get_job(job_id: str) -> dict[str, Any] | None:
    with INGEST_JOBS_LOCK:
        row = INGEST_JOBS.get(job_id)
        return dict(row) if row else None


def _list_jobs() -> list[dict[str, Any]]:
    with INGEST_JOBS_LOCK:
        rows = [dict(v) for v in INGEST_JOBS.values()]
    rows.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return rows


def _read_jobs_state_snapshot() -> list[dict[str, Any]]:
    if not JOB_STATE_FILE.exists():
        return []
    try:
        rows = json.loads(JOB_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(rows, list):
        return []
    parsed = [row for row in rows if isinstance(row, dict)]
    parsed.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return parsed


def _get_job_for_api(job_id: str) -> dict[str, Any] | None:
    if INGEST_EXECUTION_MODE == "queue_worker":
        rows = _read_jobs_state_snapshot()
        for row in rows:
            if str(row.get("job_id") or "") == job_id:
                return row
        return None
    return _get_job(job_id)


def _queue_ingest_job(job_id: str) -> None:
    job = _get_job(job_id)
    if not job or not job.get("object_name"):
        for row in _read_jobs_state_snapshot():
            if str(row.get("job_id") or "") == job_id:
                job = row
                _set_job(job_id, persist=False, **{k: v for k, v in row.items() if k != "job_id"})
                break
    if not job:
        raise RuntimeError(f"job_id not found: {job_id}")
    event = BulkIngestEvent(
        job_id=job_id,
        object_name=str(job.get("object_name") or ""),
        bucket=str(job.get("bucket") or MINIO_BUCKET),
        file_name=job.get("file_name"),
        source_type=str(job.get("source_type") or "csv"),
        config_path=str(job.get("config_path") or "config/config.json"),
        load_to_db=bool(job.get("load_to_db", True)),
        success_path=job.get("success_path"),
        failed_path=job.get("failed_path"),
        checkpoint_root=job.get("checkpoint_root"),
        resume_from_checkpoint=bool(job.get("resume_from_checkpoint", True)),
        resume_failed_only=bool(job.get("resume_failed_only", True)),
    )
    producer = get_queue_producer()
    producer.publish_bulk_ingest(event)


def _register_job_proc(job_id: str, proc: subprocess.Popen[str], *, phase: str) -> None:
    with INGEST_PROCS_LOCK:
        INGEST_PROCS[job_id] = {"proc": proc, "phase": phase, "pause_requested": False}


def _unregister_job_proc(job_id: str, proc: subprocess.Popen[str]) -> None:
    with INGEST_PROCS_LOCK:
        current = INGEST_PROCS.get(job_id)
        if current and current.get("proc") is proc:
            INGEST_PROCS.pop(job_id, None)


def _job_pause_requested(job_id: str) -> bool:
    with INGEST_PROCS_LOCK:
        current = INGEST_PROCS.get(job_id)
        return bool(current and current.get("pause_requested"))


def _request_pause(job_id: str) -> tuple[bool, str | None]:
    with INGEST_PROCS_LOCK:
        current = INGEST_PROCS.get(job_id)
        if not current:
            return False, None
        phase = str(current.get("phase") or "pipeline")
        if phase != "pipeline":
            return False, phase
        current["pause_requested"] = True
        proc = current.get("proc")
        try:
            if proc and proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        except Exception:
            return False, phase
        return True, phase


def _state_pause_requested(job_id: str) -> bool:
    for row in _read_jobs_state_snapshot():
        if str(row.get("job_id") or "") != job_id:
            continue
        status = str(row.get("status") or "").lower()
        if status == "pausing":
            return True
        if bool(row.get("pause_requested")):
            return True
        return False
    return False


_load_jobs_state()


def _extract_error_summary(log_path: Path, *, max_lines: int = 400, max_chars: int = 500) -> str | None:
    if not log_path.exists():
        return None
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    tail = lines[-max_lines:]

    exc_re = re.compile(r"([A-Za-z_][A-Za-z0-9_.]*(?:Exception|Error)):\s*(.+)")

    for raw in reversed(tail):
        line = raw.strip()
        if not line or " end job=" in line:
            continue
        match = exc_re.search(line)
        if match:
            return f"{match.group(1)}: {match.group(2)}"[:max_chars]

    for raw in reversed(tail):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Traceback"):
            continue
        if line.startswith("File "):
            continue
        if line.startswith("at "):
            continue
        if line.startswith("[") and (" start job=" in line or " end job=" in line):
            continue
        return line[:max_chars]
    return None


def _sanitize_log_line(raw_line: str, *, max_chars: int = 400) -> str:
    line = raw_line.replace("\r", "").rstrip("\n")
    line = _ANSI_ESCAPE_RE.sub("", line)
    if len(line) > max_chars:
        line = f"{line[:max_chars-3]}..."
    return line


def _progress_from_log_line(line: str, current_pct: int, current_stage: str) -> tuple[int, str]:
    text = line.strip()
    if not text:
        return current_pct, current_stage

    if text.startswith("PIPELINE_STAGE:"):
        stage = text.split(":", 1)[1].strip().lower()
        stage_aliases = {
            "clean": "transform",
            "validated": "validate",
        }
        stage = stage_aliases.get(stage, stage)
        stage_progress = {
            "extract": 20,
            "transform": 45,
            "validate": 70,
            "final": 88,
        }
        if stage in stage_progress:
            return max(current_pct, stage_progress[stage]), stage

    lowered = text.lower()
    if "traceback (most recent call last)" in lowered:
        return max(current_pct, 95), "failing"

    if "using spark's default log4j profile" in lowered:
        return max(current_pct, 10), "initializing_spark"
    if "setting default log level" in lowered:
        return max(current_pct, 12), "initializing_spark"
    if "native-hadoop library" in lowered:
        return max(current_pct, 15), "spark_ready"
    if "validation_complete" in lowered:
        return max(current_pct, 90), "validate"
    if "write_complete" in lowered:
        return max(current_pct, 96), "final"

    # Spark progress line example:
    # [Stage 0:>                                                          (0 + 1) / 1]
    match = re.search(r"\((\d+)\s*\+\s*(\d+)\)\s*/\s*(\d+)\]", text)
    if match:
        done = int(match.group(1))
        active = int(match.group(2))
        total = max(1, int(match.group(3)))
        ratio = min(1.0, (done + active) / total)
        pct = max(current_pct, min(94, int(20 + ratio * 70)))
        return pct, current_stage

    return current_pct, current_stage


def _run_ingest_job(job_id: str) -> None:
    job = _get_job(job_id)
    if not job:
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{job_id}.log"
    _set_job(
        job_id,
        status="running",
        started_at=_now_iso(),
        ended_at=None,
        error=None,
        log_path=str(log_path),
        progress_pct=1,
        progress_stage="starting",
    )
    load_to_db = bool(job.get("load_to_db", True))
    persist_live_updates = INGEST_PERSIST_LIVE_UPDATES
    persist_interval_seconds = INGEST_PERSIST_LIVE_INTERVAL_SECONDS

    try:
        source_type = job.get("source_type", "csv")
        object_name = job["object_name"]
        file_name = job["file_name"]
        config_path = job.get("config_path") or "config/config.json"

        local_input = UPLOAD_STAGING_DIR / job_id / file_name
        local_input.parent.mkdir(parents=True, exist_ok=True)

        success_path = Path(job.get("success_path") or (OUTPUT_UPLOADS_DIR / job_id / "cleaned"))
        failed_path = Path(job.get("failed_path") or (OUTPUT_UPLOADS_DIR / job_id / "failed"))
        checkpoint_root = Path(job.get("checkpoint_root") or (OUTPUT_UPLOADS_DIR / job_id / "checkpoints"))
        resume_from_checkpoint = bool(job.get("resume_from_checkpoint", True))
        resume_failed_only = bool(job.get("resume_failed_only", True))
        has_checkpoints = checkpoint_root.exists() and any(checkpoint_root.iterdir())
        should_resume = resume_from_checkpoint and has_checkpoints

        client = _get_minio_client()
        client.fget_object(MINIO_BUCKET, object_name, str(local_input))

        cmd = [
            sys.executable,
            "pipeline.py",
            "--input",
            str(local_input),
            "--source-type",
            source_type,
            "--config",
            config_path,
            "--success",
            str(success_path),
            "--failed",
            str(failed_path),
            "--checkpoint-root",
            str(checkpoint_root),
        ]
        if should_resume:
            cmd.append("--resume")
            if resume_failed_only:
                cmd.append("--resume-failed-only")
        env = os.environ.copy()
        env.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        progress_pct = 5
        progress_stage = "resuming" if should_resume else "starting_pipeline"
        last_emit = 0.0
        last_persist = 0.0

        with log_path.open("w", encoding="utf-8") as logfile:
            logfile.write(f"[{_now_iso()}] start job={job_id} cmd={' '.join(cmd)}\n")
            logfile.flush()
            proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                text=True,
                bufsize=1,
            )
            _register_job_proc(job_id, proc, phase="pipeline")
            pause_requested = False
            _set_job(
                job_id,
                persist=False,
                progress_pct=progress_pct,
                progress_stage=progress_stage,
                last_log_line="Resuming from checkpoints" if should_resume else "Pipeline process started",
            )
            try:
                if proc.stdout is not None:
                    for raw_line in proc.stdout:
                        if (
                            INGEST_EXECUTION_MODE == "queue_worker"
                            and not _job_pause_requested(job_id)
                            and _state_pause_requested(job_id)
                        ):
                            _request_pause(job_id)
                        logfile.write(raw_line)
                        logfile.flush()
                        cleaned = _sanitize_log_line(raw_line)
                        if cleaned:
                            progress_pct, progress_stage = _progress_from_log_line(
                                cleaned, progress_pct, progress_stage
                            )
                            now = time.time()
                            if now - last_emit >= 0.5:
                                persist_now = False
                                if persist_live_updates and now - last_persist >= persist_interval_seconds:
                                    persist_now = True
                                    last_persist = now
                                _set_job(
                                    job_id,
                                    persist=persist_now,
                                    progress_pct=progress_pct,
                                    progress_stage=progress_stage,
                                    last_log_line=cleaned,
                                )
                                last_emit = now
                code = proc.wait()
                pause_requested = _job_pause_requested(job_id)
            finally:
                _unregister_job_proc(job_id, proc)
            logfile.write(f"[{_now_iso()}] end job={job_id} exit_code={code}\n")

        if pause_requested:
            _set_job(
                job_id,
                status="paused",
                ended_at=_now_iso(),
                success_path=str(success_path),
                failed_path=str(failed_path),
                checkpoint_root=str(checkpoint_root),
                log_path=str(log_path),
                error=None,
                progress_stage="paused",
                last_log_line="Paused by user. Resume continues from checkpoints.",
                load_status="paused" if load_to_db else "skipped",
            )
            return

        if code == 0:
            if load_to_db:
                _set_job(
                    job_id,
                    progress_pct=97,
                    progress_stage="loading_db",
                    load_status="running",
                    last_log_line="Starting Postgres load",
                )
                load_cmd = [
                    sys.executable,
                    "load_postgres.py",
                    "--input",
                    str(success_path),
                    "--normalized",
                    "--mode",
                    "append",
                    "--pbt-dir",
                    "data/boundary/Sempadan Kawalan PBT",
                ]
                load_env = os.environ.copy()
                load_env.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
                load_emit = 0.0
                load_persist = 0.0
                with log_path.open("a", encoding="utf-8") as logfile:
                    logfile.write(f"[{_now_iso()}] start db_load job={job_id} cmd={' '.join(load_cmd)}\n")
                    logfile.flush()
                    load_proc = subprocess.Popen(
                        load_cmd,
                        cwd=str(PROJECT_ROOT),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        env=load_env,
                        text=True,
                        bufsize=1,
                    )
                    _register_job_proc(job_id, load_proc, phase="db_load")
                    try:
                        if load_proc.stdout is not None:
                            for raw_line in load_proc.stdout:
                                if (
                                    INGEST_EXECUTION_MODE == "queue_worker"
                                    and not _job_pause_requested(job_id)
                                    and _state_pause_requested(job_id)
                                ):
                                    _request_pause(job_id)
                                logfile.write(raw_line)
                                logfile.flush()
                                cleaned = _sanitize_log_line(raw_line)
                                if cleaned:
                                    now = time.time()
                                    if now - load_emit >= 0.5:
                                        persist_now = False
                                        if persist_live_updates and now - load_persist >= persist_interval_seconds:
                                            persist_now = True
                                            load_persist = now
                                        _set_job(
                                            job_id,
                                            persist=persist_now,
                                            progress_pct=98,
                                            progress_stage="loading_db",
                                            load_status="running",
                                            last_log_line=cleaned,
                                        )
                                        load_emit = now
                        load_code = load_proc.wait()
                    finally:
                        _unregister_job_proc(job_id, load_proc)
                    logfile.write(f"[{_now_iso()}] end db_load job={job_id} exit_code={load_code}\n")
                    logfile.flush()
                if load_code != 0:
                    _set_job(
                        job_id,
                        status="failed",
                        ended_at=_now_iso(),
                        success_path=str(success_path),
                        failed_path=str(failed_path),
                        checkpoint_root=str(checkpoint_root),
                        error=_extract_error_summary(log_path) or f"db load exit code {load_code}",
                        log_path=str(log_path),
                        progress_pct=100,
                        progress_stage="failed",
                        load_status="failed",
                    )
                    return
                if INGEST_AUTO_SYNC_ES:
                    _set_job(
                        job_id,
                        progress_pct=99,
                        progress_stage="syncing_search",
                        load_status="running",
                        last_log_line="Starting Elasticsearch sync",
                    )
                    es_cmd = [
                        sys.executable,
                        "load_elasticsearch.py",
                        "--input",
                        str(success_path),
                        "--es-url",
                        ES_URL,
                        "--index",
                        ES_INDEX,
                    ]
                    es_env = os.environ.copy()
                    es_emit = 0.0
                    es_persist = 0.0
                    with log_path.open("a", encoding="utf-8") as logfile:
                        logfile.write(f"[{_now_iso()}] start es_sync job={job_id} cmd={' '.join(es_cmd)}\n")
                        logfile.flush()
                        es_proc = subprocess.Popen(
                            es_cmd,
                            cwd=str(PROJECT_ROOT),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            env=es_env,
                            text=True,
                            bufsize=1,
                        )
                        _register_job_proc(job_id, es_proc, phase="es_sync")
                        try:
                            if es_proc.stdout is not None:
                                for raw_line in es_proc.stdout:
                                    logfile.write(raw_line)
                                    logfile.flush()
                                    cleaned = _sanitize_log_line(raw_line)
                                    if cleaned:
                                        now = time.time()
                                        if now - es_emit >= 0.5:
                                            persist_now = False
                                            if persist_live_updates and now - es_persist >= persist_interval_seconds:
                                                persist_now = True
                                                es_persist = now
                                            _set_job(
                                                job_id,
                                                persist=persist_now,
                                                progress_pct=99,
                                                progress_stage="syncing_search",
                                                load_status="running",
                                                last_log_line=cleaned,
                                            )
                                            es_emit = now
                            es_code = es_proc.wait()
                        finally:
                            _unregister_job_proc(job_id, es_proc)
                        logfile.write(f"[{_now_iso()}] end es_sync job={job_id} exit_code={es_code}\n")
                        logfile.flush()
                    if es_code != 0:
                        _set_job(
                            job_id,
                            status="failed",
                            ended_at=_now_iso(),
                            success_path=str(success_path),
                            failed_path=str(failed_path),
                            checkpoint_root=str(checkpoint_root),
                            error=_extract_error_summary(log_path) or f"elasticsearch sync exit code {es_code}",
                            log_path=str(log_path),
                            progress_pct=100,
                            progress_stage="failed",
                            load_status="failed",
                        )
                        return
            _set_job(
                job_id,
                status="completed",
                ended_at=_now_iso(),
                success_path=str(success_path),
                failed_path=str(failed_path),
                checkpoint_root=str(checkpoint_root),
                log_path=str(log_path),
                progress_pct=100,
                progress_stage="completed",
                load_status="completed" if load_to_db else "skipped",
            )
            return
        _set_job(
            job_id,
            status="failed",
            ended_at=_now_iso(),
            error=_extract_error_summary(log_path) or f"pipeline exit code {code}",
            log_path=str(log_path),
            progress_pct=100,
            progress_stage="failed",
            load_status="failed" if load_to_db else "skipped",
        )
    except Exception as exc:
        _set_job(
            job_id,
            status="failed",
            ended_at=_now_iso(),
            error=f"backend ingest error: {exc}",
            log_path=str(log_path),
            progress_pct=100,
            progress_stage="failed",
            load_status="failed" if load_to_db else "skipped",
        )
        with log_path.open("a", encoding="utf-8") as logfile:
            logfile.write(f"[{_now_iso()}] backend ingest error job={job_id}: {exc}\n")
            logfile.write(traceback.format_exc())
            logfile.write("\n")


def _request_json(method: str, url: str, *, timeout: int = 15, **kwargs) -> dict[str, Any]:
    response = requests.request(method, url, timeout=timeout, **kwargs)
    if response.status_code >= 300:
        raise HTTPException(status_code=502, detail=f"Upstream error {response.status_code}: {response.text}")
    return response.json()


def _autocomplete(query: str, size: int) -> list[dict[str, Any]]:
    payload = {
        "size": size,
        "_source": [
            "record_id",
            "naskod",
            "address_clean",
            "postcode",
            "postcode_code",
            "postcode_name",
            "locality_name",
            "district_name",
            "mukim_name",
            "state_name",
            "confidence_score",
        ],
        "query": {
            "multi_match": {
                "query": query,
                "type": "bool_prefix",
                "fields": ["autocomplete", "autocomplete._2gram", "autocomplete._3gram"],
            }
        },
        "sort": [
            {"_score": {"order": "desc"}},
            {"confidence_score": {"order": "desc", "missing": "_last"}},
        ],
    }
    body = _request_json("POST", f"{ES_URL}/{ES_INDEX}/_search", json=payload, timeout=20)
    hits = []
    for item in body.get("hits", {}).get("hits", []):
        source = item.get("_source", {})
        source["_score"] = item.get("_score")
        hits.append(source)
    return hits


def _load_audit_runs(limit: int) -> list[dict[str, Any]]:
    if not AUDIT_LOG_PATH.exists():
        return []

    runs: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "run_id": None,
            "status": "running",
            "started_at": None,
            "ended_at": None,
            "last_event": None,
            "last_stage": None,
            "records_in": None,
            "records_success": None,
            "records_failed": None,
            "message": None,
        }
    )
    with AUDIT_LOG_PATH.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            run_id = event.get("run_id")
            if not run_id:
                continue

            row = runs[run_id]
            row["run_id"] = run_id
            row["last_event"] = event.get("event")
            row["message"] = event.get("message")
            row["last_stage"] = event.get("stage", row["last_stage"])
            if event.get("event") == "run_start":
                row["started_at"] = event.get("timestamp", row["started_at"])
            if event.get("event") == "run_end":
                row["ended_at"] = event.get("timestamp", row["ended_at"])
                row["status"] = event.get("status", "unknown")
                row["records_in"] = event.get("records_in")
                row["records_success"] = event.get("records_success")
                row["records_failed"] = event.get("records_failed")
            if row["started_at"] is None:
                row["started_at"] = event.get("timestamp")

    run_items = list(runs.values())
    run_items.sort(key=lambda x: (x.get("started_at") or ""), reverse=True)
    return run_items[:limit]


from backend.app.api.v1.ingest import router as ingest_router
from backend.app.api.v1.address_db import router as address_db_router
from backend.app.api.v1.ops import router as ops_router
from backend.app.api.v1.search import router as search_router

app.include_router(ops_router)
app.include_router(search_router)
app.include_router(address_db_router)
app.include_router(ingest_router)
