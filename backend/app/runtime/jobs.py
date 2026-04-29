from __future__ import annotations

import traceback
from typing import Any

from backend.app.core.settings import get_settings
from backend.app.dependencies import get_queue_producer
from backend.app.runtime import job_state
from backend.app.runtime import settings
from backend.app.schemas.ingest import BulkIngestEvent, SearchSyncEvent
from backend.app.services.search_sync_service import SearchSyncService


job_state.configure(
    dsn=settings.INGEST_JOB_STATE_DSN,
    schema=settings.INGEST_JOB_STATE_SCHEMA,
    running_recovery_window_seconds=settings.RUNNING_RECOVERY_WINDOW_SECONDS,
)


def now_iso() -> str:
    return job_state.now_iso()


def infer_source_type(filename: str) -> str:
    lower_name = filename.lower()
    if lower_name.endswith(".json") or lower_name.endswith(".jsonl") or lower_name.endswith(".ndjson"):
        return "json"
    if lower_name.endswith(".xlsx") or lower_name.endswith(".xls"):
        return "excel"
    return "csv"


def persist_job_state(job_id: str, state: dict[str, Any]) -> None:
    job_state.persist_job_state(job_id, state)


def set_job(job_id: str, *, persist: bool = True, **changes: Any) -> None:
    job_state.set_job(job_id, persist=persist, **changes)


def load_jobs_state() -> None:
    job_state.load_jobs_state()


def get_job(job_id: str, *, agency_id: str | None = None) -> dict[str, Any] | None:
    return job_state.get_job(job_id, agency_id=agency_id)


def claim_job_for_worker(
    job_id: str,
    *,
    worker_id: str,
    queue_event_id: str | None = None,
    queue_message_id: str | None = None,
) -> dict[str, Any] | None:
    return job_state.claim_job_for_worker(
        job_id=job_id,
        worker_id=worker_id,
        queue_event_id=queue_event_id,
        queue_message_id=queue_message_id,
    )


def recover_stale_claims_and_requeue(*, limit: int = 20) -> list[dict[str, Any]]:
    if not settings.INGEST_PERSIST_LIVE_UPDATES:
        return []

    def requeue(job_id: str) -> None:
        try:
            queue_ingest_job(job_id)
        except Exception as exc:
            set_job(
                job_id,
                status="failed",
                ended_at=now_iso(),
                error=f"stale-claim recovery requeue failed: {exc}",
                progress_stage="failed",
                load_status="failed",
            )

    return job_state.requeue_stale_claims(
        stale_after_seconds=settings.INGEST_STALE_CLAIM_SECONDS,
        limit=limit,
        on_requeue=requeue,
    )


def list_jobs(agency_id: str | None = None) -> list[dict[str, Any]]:
    return job_state.list_jobs(agency_id=agency_id)


def read_jobs_state_snapshot(agency_id: str | None = None) -> list[dict[str, Any]]:
    return job_state.read_jobs_state_snapshot(agency_id=agency_id)


def get_persisted_job(job_id: str, *, agency_id: str | None = None) -> dict[str, Any] | None:
    return job_state.get_persisted_job(job_id, agency_id=agency_id)


def get_job_for_api(job_id: str, agency_id: str | None = None) -> dict[str, Any] | None:
    return job_state.get_job_for_api(job_id, agency_id=agency_id)


def queue_ingest_job(job_id: str) -> None:
    job = get_persisted_job(job_id) or get_job(job_id)
    if not job:
        raise RuntimeError(f"job_id not found: {job_id}")
    event = BulkIngestEvent(
        job_id=job_id,
        object_name=str(job.get("object_name") or ""),
        bucket=str(job.get("bucket") or settings.OBJECT_STORE_BUCKET),
        file_name=job.get("file_name"),
        source_type=str(job.get("source_type") or "csv"),
        config_path=str(job.get("config_path") or "config/config.json"),
        load_to_db=True,
        success_path=job.get("success_path"),
        failed_path=job.get("failed_path"),
        checkpoint_root=job.get("checkpoint_root"),
        resume_from_checkpoint=bool(job.get("resume_from_checkpoint", True)),
        resume_failed_only=bool(job.get("resume_failed_only", True)),
    )
    get_queue_producer().publish_bulk_ingest(event)


def queue_search_sync_job(job_id: str, *, recreate_index: bool = False) -> None:
    event = SearchSyncEvent(
        job_id=job_id,
        schema_name=settings.INGEST_JOB_STATE_SCHEMA,
        es_index=settings.ES_INDEX,
        recreate_index=recreate_index,
    )
    get_queue_producer().publish_search_sync(event)
    set_job(
        job_id,
        search_sync_status="queued",
        search_sync_requested_at=event.requested_at,
        search_sync_event_id=event.event_id,
        last_log_line="Queued Elasticsearch sync from Postgres",
    )


def run_ingest_job(job_id: str) -> None:
    from backend.app.workers.ingest_runner import run_ingest_job as _run

    _run(job_id)


def run_search_sync_job(job_id: str, event: dict[str, Any] | None = None) -> None:
    job = get_job_for_api(job_id)
    if not job:
        raise RuntimeError(f"job_id not found for search sync: {job_id}")

    schema_name = str((event or {}).get("schema_name") or settings.INGEST_JOB_STATE_SCHEMA).strip()
    schema_name = schema_name or settings.INGEST_JOB_STATE_SCHEMA
    es_index = str((event or {}).get("es_index") or settings.ES_INDEX).strip() or settings.ES_INDEX
    recreate_index = bool((event or {}).get("recreate_index", False))
    log_path = settings.resolve_project_path(str(job.get("log_path") or (settings.LOG_DIR / f"{job_id}.log")))

    set_job(
        job_id,
        search_sync_status="running",
        search_sync_started_at=now_iso(),
        search_sync_error=None,
        last_log_line="Starting Elasticsearch sync from Postgres",
    )

    app_settings = get_settings()
    service = SearchSyncService(
        postgres_dsn=app_settings.postgres_dsn,
        postgres_schema=app_settings.postgres_schema,
        es_url=settings.ES_URL,
        es_index=settings.ES_INDEX,
    )
    settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as logfile:
        logfile.write(
            f"[{now_iso()}] start search_sync job={job_id} schema={schema_name} "
            f"index={es_index} recreate_index={recreate_index}\n"
        )
        logfile.flush()
        try:
            result = service.sync_from_postgres(
                schema_name=schema_name,
                es_index=es_index,
                recreate_index=recreate_index,
            )
            logfile.write(
                f"[{now_iso()}] end search_sync job={job_id} indexed={result['indexed']} "
                f"failed={result['failed']} index={result['index']}\n"
            )
            logfile.flush()
            if int(result["failed"]) > 0:
                raise RuntimeError(f"Elasticsearch sync failed for {result['failed']} documents")
        except Exception as exc:
            logfile.write(f"[{now_iso()}] search_sync error job={job_id}: {exc}\n")
            logfile.write(traceback.format_exc())
            logfile.write("\n")
            logfile.flush()
            set_job(
                job_id,
                search_sync_status="failed",
                search_sync_ended_at=now_iso(),
                search_sync_error=str(exc),
                last_log_line=f"Elasticsearch sync failed: {exc}",
            )
            return

    set_job(
        job_id,
        search_sync_status="completed",
        search_sync_ended_at=now_iso(),
        search_sync_error=None,
        last_log_line=f"Elasticsearch sync completed from Postgres: {result['indexed']} indexed",
    )
