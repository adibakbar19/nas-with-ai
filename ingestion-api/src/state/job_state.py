"""Job state management — thin wrapper over IngestJobStateRepository + queue producer.

Phase 2 migration: queue_ingest_job() now prefers the queue-service HTTP API.
Falls back to direct Valkey push if queue-service is unavailable, so the
ingest pipeline keeps working during any queue-service outage.

No module-level singletons. All dependencies injected via constructor.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from src.queue.producer import ValkeyStreamQueueProducer
from src.schemas.events import BulkIngestEvent
from src.state.job_repository import IngestJobStateRepository

logger = logging.getLogger(__name__)


class IngestJobState:
    """Manages ingest job lifecycle: create, update, query, enqueue."""

    def __init__(
        self,
        *,
        job_repo: IngestJobStateRepository,
        producer: ValkeyStreamQueueProducer,
        object_store_bucket: str,
        queue_client=None,  # QueueServiceClient | None
    ) -> None:
        self._repo = job_repo
        self._producer = producer
        self._bucket = object_store_bucket
        self._queue_client = queue_client

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def persist_job_state(self, job_id: str, state: dict[str, Any]) -> None:
        self._repo.save_job(job_id=job_id, state=state)

    def set_job(self, job_id: str, **changes: Any) -> None:
        """Update job state in the database."""
        current = self._repo.get_job(job_id=job_id) or {}
        current["job_id"] = job_id
        current.update(changes)
        snapshot = dict(current)
        logger.debug("job_state_update job_id=%s status=%s", job_id, snapshot.get("status"))
        self._repo.save_job(job_id=job_id, state=snapshot)

    def get_job(self, job_id: str, *, agency_id: str | None = None) -> dict[str, Any] | None:
        # Try queue-service first (has typed columns + full data blob)
        if self._queue_client is not None:
            qs_job = self._queue_client.get_job(job_id, agency_id=agency_id)
            if qs_job is not None:
                # Merge data blob fields to top level for backward compatibility
                merged = dict(qs_job.get("data") or {})
                merged.update({k: v for k, v in qs_job.items() if k != "data" and v is not None})
                return merged

        return self._repo.get_job(job_id=job_id, agency_id=agency_id)

    def list_jobs(self, *, agency_id: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        if self._queue_client is not None:
            result = self._queue_client.list_jobs(agency_id=agency_id, limit=limit or 20)
            if result is not None:
                jobs = []
                for j in result:
                    merged = dict(j.get("data") or {})
                    merged.update({k: v for k, v in j.items() if k != "data" and v is not None})
                    jobs.append(merged)
                return jobs
        return self._repo.list_jobs(agency_id=agency_id, limit=limit)

    def queue_ingest_job(self, job_id: str) -> None:
        """Enqueue job via queue-service (preferred) or direct Valkey (fallback)."""
        job = self._repo.get_job(job_id=job_id)
        if not job:
            raise RuntimeError(f"job_id not found: {job_id}")

        # Phase 2: try queue-service first
        if self._queue_client is not None:
            result = self._queue_client.enqueue_job(
                job_id=job_id,
                job_type=str(job.get("job_type") or "bulk_ingest"),
                data=job,
            )
            if result is not None:
                logger.info("queue_ingest_job_via_qs job_id=%s status=%s",
                            job_id, result.get("status"))
                return

        # Fallback: direct Valkey stream push (legacy path)
        event = BulkIngestEvent(
            job_id=job_id,
            object_name=str(job.get("object_name") or ""),
            bucket=str(job.get("bucket") or self._bucket),
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
        self._producer.publish(
            event_type=event.event_type,
            event_id=event.event_id,
            payload=event.model_dump_json(),
            job_id=job_id,
        )
        logger.info("queue_ingest_job_direct job_id=%s event_id=%s", job_id, event.event_id)
