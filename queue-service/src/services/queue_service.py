"""Core queue service — enqueue, progress, complete, fail, recovery."""
from __future__ import annotations

import logging
import uuid
from typing import Any

import sqlalchemy as sa

from src.clients.valkey import ValkeyQueueClient
from src.repository.dlq_repository import DlqRepository
from src.repository.job_repository import JobRepository

logger = logging.getLogger(__name__)


class QueueService:
    def __init__(
        self,
        engine: sa.Engine,
        valkey: ValkeyQueueClient,
        settings,
    ) -> None:
        self._repo = JobRepository(engine, settings.job_schema)
        self._dlq = DlqRepository(engine, settings.job_schema)
        self._valkey = valkey
        self._settings = settings

    def enqueue(
        self,
        job_id: str,
        job_type: str,
        source_system: str,
        data: dict,
        priority: int = 5,
    ) -> dict[str, Any]:
        """Create job in DB then push to Valkey stream.

        If Valkey push fails, job stays in 'pending' state and the retry
        scheduler will push it when the stream recovers.
        """
        agency_id = str(data.get("agency_id") or "").strip() or None
        queue_name = self._settings.stream_key

        job = self._repo.create_job(
            job_id=job_id,
            job_type=job_type,
            source_system=source_system,
            data=data,
            priority=priority,
            queue_name=queue_name,
            agency_id=agency_id,
        )

        try:
            stream_msg_id = self._valkey.push_job(job_id, job_type, data)
            job = self._repo.mark_queued(job_id, stream_msg_id) or job
            logger.info("job_enqueued job_id=%s type=%s msg_id=%s",
                        job_id, job_type, stream_msg_id)
        except Exception as exc:
            logger.warning("job_pending_valkey_failed job_id=%s error=%s",
                           job_id, exc)
            # Job stays 'pending'; retry scheduler will push it

        return job

    def update_progress(
        self,
        job_id: str,
        worker_id: str,
        **fields: Any,
    ) -> dict[str, Any] | None:
        """Update job state and heartbeat. Never raises."""
        try:
            from datetime import datetime, timezone
            updates = dict(fields)
            updates["worker_id"] = worker_id
            # Always refresh heartbeat on any progress update
            if "heartbeat_at" not in updates:
                updates["heartbeat_at"] = datetime.now(timezone.utc).isoformat()
            return self._repo.update_job(job_id, **updates)
        except Exception as exc:
            logger.warning("update_progress_failed job_id=%s error=%s", job_id, exc)
            return None

    def handle_completion(
        self,
        job_id: str,
        worker_id: str,
        result_data: dict,
    ) -> dict[str, Any] | None:
        job = self._repo.complete_job(job_id, worker_id, result_data)
        if job:
            logger.info("job_completed job_id=%s worker=%s", job_id, worker_id)
            self._emit_event("job.completed", job_id, job)
        return job

    def handle_failure(
        self,
        job_id: str,
        worker_id: str,
        error: str,
    ) -> dict[str, Any] | None:
        backoff = self._settings.get_retry_backoff()
        job = self._repo.fail_job(job_id, worker_id, error, retry=True, backoff_seconds=backoff)
        if not job:
            return None

        if job.get("status") == "failed":
            # Permanently failed — move to DLQ
            try:
                self._dlq.move_to_dlq(
                    job_id=job_id,
                    job_type=str(job.get("job_type") or "bulk_ingest"),
                    source_system=str(job.get("source_system") or ""),
                    original_data=job,
                    failure_reason=error,
                    retry_count=int(job.get("retry_count") or 0),
                )
                logger.warning("job_moved_to_dlq job_id=%s retries=%d",
                               job_id, job.get("retry_count"))
            except Exception as exc:
                logger.error("dlq_move_failed job_id=%s error=%s", job_id, exc)
            self._emit_event("job.failed", job_id, job)
        else:
            logger.info("job_scheduled_retry job_id=%s retry=%d",
                        job_id, job.get("retry_count"))

        return job

    def recover_stale_jobs(self) -> int:
        """Find running jobs with expired heartbeat and requeue or DLQ them."""
        timeout = self._settings.job_heartbeat_timeout_seconds
        stale = self._repo.find_stale_jobs(timeout)
        recovered = 0

        for job in stale:
            job_id = job.get("job_id", "")
            retry_count = int(job.get("retry_count") or 0)
            max_retries = int(job.get("max_retries") or 3)

            if retry_count < max_retries:
                self._repo.update_job(
                    job_id,
                    status="queued",
                    claimed_by=None,
                    claimed_at=None,
                    heartbeat_at=None,
                    next_retry_at=None,
                    retry_count=retry_count + 1,
                )
                # Re-push to stream
                try:
                    self._valkey.push_job(job_id, str(job.get("job_type") or "bulk_ingest"), job)
                except Exception as exc:
                    logger.warning("stale_requeue_stream_failed job_id=%s error=%s", job_id, exc)
                logger.warning("stale_job_recovered job_id=%s retry=%d", job_id, retry_count + 1)
                recovered += 1
            else:
                self._repo.update_job(job_id, status="failed",
                                      error_detail="stale worker — exceeded max retries")
                try:
                    self._dlq.move_to_dlq(
                        job_id=job_id,
                        job_type=str(job.get("job_type") or "bulk_ingest"),
                        source_system=str(job.get("source_system") or ""),
                        original_data=job,
                        failure_reason="stale worker — exceeded max retries",
                        retry_count=retry_count,
                    )
                except Exception as exc:
                    logger.error("stale_dlq_failed job_id=%s error=%s", job_id, exc)
                logger.warning("stale_job_dlq job_id=%s", job_id)

        return recovered

    def recover_stuck_queued_jobs(self, stuck_after_seconds: int = 300) -> int:
        """Re-push queued jobs whose stream message was consumed but never claimed.

        This handles the case where a worker acked the stream message but crashed
        before claiming the job, leaving it stranded in 'queued' status forever.
        """
        stuck = self._repo.find_stuck_queued_jobs(stuck_after_seconds)
        pushed = 0
        for job in stuck:
            job_id = job.get("job_id", "")
            job_type = str(job.get("job_type") or "bulk_ingest")
            try:
                self._valkey.push_job(job_id, job_type, job)
                # Touch updated_at so we don't re-push on next cycle
                self._repo.update_job(job_id, status="queued")
                pushed += 1
                logger.warning("stuck_queued_job_repushed job_id=%s", job_id)
            except Exception as exc:
                logger.warning("stuck_requeue_failed job_id=%s error=%s", job_id, exc)
        return pushed

    def process_retry_queue(self) -> int:
        """Push retry-due jobs back to Valkey stream, set status=queued."""
        try:
            with self._repo._engine.begin() as conn:
                from sqlalchemy import text
                rows = conn.execute(text(f"""
                    UPDATE {self._repo._tbl}
                    SET status = 'queued',
                        next_retry_at = NULL,
                        updated_at = NOW()
                    WHERE status = 'retry'
                      AND next_retry_at <= NOW()
                    RETURNING job_id, job_type, data
                """)).fetchall()
        except Exception as exc:
            logger.warning("retry_queue_failed error=%s", exc)
            return 0

        pushed = 0
        for row in rows:
            job_id = row._mapping["job_id"]
            job_type = row._mapping["job_type"] or "bulk_ingest"
            data = row._mapping["data"] or {}
            try:
                self._valkey.push_job(job_id, job_type, data if isinstance(data, dict) else {})
                pushed += 1
                logger.info("retry_pushed job_id=%s", job_id)
            except Exception as exc:
                logger.warning("retry_push_failed job_id=%s error=%s", job_id, exc)
                # Revert to retry status
                try:
                    with self._repo._engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE {self._repo._tbl}
                            SET status='retry', next_retry_at=NOW() + INTERVAL '60 seconds'
                            WHERE job_id=:id
                        """), {"id": job_id})
                except Exception:
                    pass

        return pushed

    def requeue_dlq_job(self, dlq_id: str) -> dict[str, Any] | None:
        """Requeue a dead letter job as a new job."""
        entry = self._dlq.get_dlq_entry(dlq_id)
        if not entry:
            return None

        new_job_id = uuid.uuid4().hex
        new_job = self.enqueue(
            job_id=new_job_id,
            job_type=str(entry.get("job_type") or "bulk_ingest"),
            source_system=str(entry.get("source_system") or "queue-service"),
            data=entry.get("original_data") or {},
        )
        self._dlq.mark_requeued(dlq_id, new_job_id)
        logger.info("dlq_requeued dlq_id=%s new_job_id=%s", dlq_id, new_job_id)
        return new_job

    def _emit_event(self, event_type: str, job_id: str, job: dict) -> None:
        try:
            from nas_processor.src.events.publisher import publish as _pub  # type: ignore
            _pub(event_type, entity_id=job_id, entity_type="job",
                 payload={"job_id": job_id,
                          "agency_id": str(job.get("agency_id") or ""),
                          "file_name": str(job.get("file_name") or "")})
        except Exception:
            pass  # Event publishing is optional; never blocks queue ops
