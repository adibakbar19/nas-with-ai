from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.app.monitoring import record_job_transition
from backend.app.repositories.ingest_job_state_repository import IngestJobStateRepository


_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_repository_lock = threading.Lock()
_repository: IngestJobStateRepository | None = None
_dsn: str | None = None
_schema: str | None = None
_running_recovery_window_seconds = 600


def configure(*, dsn: str, schema: str, running_recovery_window_seconds: int = 600) -> None:
    global _dsn, _schema, _running_recovery_window_seconds
    _dsn = dsn
    _schema = schema
    _running_recovery_window_seconds = max(60, int(running_recovery_window_seconds))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo() -> IngestJobStateRepository:
    global _repository
    if _repository is not None:
        return _repository
    if not _dsn or not _schema:
        raise RuntimeError("job state runtime is not configured")
    with _repository_lock:
        if _repository is None:
            _repository = IngestJobStateRepository(dsn=_dsn, schema=_schema)
    return _repository


def _recover_jobs_state_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    restored: dict[str, dict[str, Any]] = {}
    now = now_iso()
    now_ts = datetime.now(timezone.utc).timestamp()
    for row in rows:
        job_id = str(row.get("job_id") or "").strip()
        if not job_id:
            continue
        recovered = dict(row)
        status = str(recovered.get("status") or "").lower()
        if status in {"running", "queued"}:
            keep_running = False
            if status == "running":
                log_path_value = recovered.get("log_path")
                if log_path_value:
                    log_path = Path(str(log_path_value))
                    if log_path.exists():
                        try:
                            modified_ago = now_ts - log_path.stat().st_mtime
                        except OSError:
                            modified_ago = _running_recovery_window_seconds + 1
                        if modified_ago <= _running_recovery_window_seconds:
                            keep_running = True
            if keep_running:
                recovered["status"] = "running"
                recovered.setdefault("progress_stage", "starting")
                recovered.setdefault("ended_at", None)
                recovered.setdefault("error", None)
            else:
                recovered["status"] = "interrupted"
                recovered.setdefault("ended_at", now)
                recovered.setdefault("error", "Backend restarted before job completion")
                recovered["progress_stage"] = "interrupted"
                recovered["load_status"] = "pending"
        restored[job_id] = recovered
    return restored


def _cache_job_unlocked(job_id: str, state: dict[str, Any]) -> None:
    cached = dict(state)
    cached["job_id"] = job_id
    _jobs[job_id] = cached


def persist_job_state(job_id: str, state: dict[str, Any]) -> None:
    _repo().save_job(job_id=job_id, state=state)


def set_job(job_id: str, *, persist: bool = True, **changes: Any) -> None:
    with _jobs_lock:
        current = dict(_jobs.get(job_id) or {})
        previous = dict(current) if current else None
        current["job_id"] = job_id
        current.update(changes)
        _cache_job_unlocked(job_id, current)
        snapshot = dict(current)
    record_job_transition(previous, snapshot)
    if persist:
        persist_job_state(job_id, snapshot)


def load_jobs_state() -> None:
    rows = _repo().list_jobs()
    restored = _recover_jobs_state_rows(rows)
    with _jobs_lock:
        _jobs.clear()
        _jobs.update(restored)
    for job_id, row in restored.items():
        persist_job_state(job_id, row)


def get_job(job_id: str, *, agency_id: str | None = None) -> dict[str, Any] | None:
    with _jobs_lock:
        row = _jobs.get(job_id)
    if row and (not agency_id or str(row.get("agency_id") or "").strip() == agency_id):
        return dict(row)
    persisted = _repo().get_job(job_id=job_id, agency_id=agency_id)
    if persisted:
        with _jobs_lock:
            _cache_job_unlocked(job_id, persisted)
        return dict(persisted)
    return None


def claim_job_for_worker(
    job_id: str,
    *,
    worker_id: str,
    queue_event_id: str | None = None,
    queue_message_id: str | None = None,
) -> dict[str, Any] | None:
    claimed = _repo().claim_job(
        job_id=job_id,
        worker_id=worker_id,
        expected_statuses=("queued",),
        queue_event_id=queue_event_id,
        queue_message_id=queue_message_id,
    )
    if claimed:
        with _jobs_lock:
            _cache_job_unlocked(job_id, claimed)
        return dict(claimed)

    latest = _repo().get_job(job_id=job_id)
    if latest:
        with _jobs_lock:
            _cache_job_unlocked(job_id, latest)
    return None


def requeue_stale_claims(
    *,
    stale_after_seconds: int,
    limit: int,
    on_requeue: Callable[[str], None] | None = None,
) -> list[dict[str, Any]]:
    recovered = _repo().requeue_stale_claims(
        stale_after_seconds=stale_after_seconds,
        statuses=("running",),
        limit=limit,
    )
    if not recovered:
        return []
    for row in recovered:
        job_id = str(row.get("job_id") or "").strip()
        if not job_id:
            continue
        with _jobs_lock:
            _cache_job_unlocked(job_id, row)
        if on_requeue is not None:
            on_requeue(job_id)
    return recovered


def list_jobs(agency_id: str | None = None) -> list[dict[str, Any]]:
    rows = _repo().list_jobs(agency_id=agency_id)
    with _jobs_lock:
        for row in rows:
            job_id = str(row.get("job_id") or "").strip()
            if job_id:
                _cache_job_unlocked(job_id, row)
    return rows


def read_jobs_state_snapshot(agency_id: str | None = None) -> list[dict[str, Any]]:
    return list_jobs(agency_id=agency_id)


def get_persisted_job(job_id: str, *, agency_id: str | None = None) -> dict[str, Any] | None:
    row = _repo().get_job(job_id=job_id, agency_id=agency_id)
    if row:
        with _jobs_lock:
            _cache_job_unlocked(job_id, row)
        return row
    return None


def get_job_for_api(job_id: str, agency_id: str | None = None) -> dict[str, Any] | None:
    row = get_persisted_job(job_id, agency_id=agency_id)
    if row:
        return row
    return get_job(job_id, agency_id=agency_id)
