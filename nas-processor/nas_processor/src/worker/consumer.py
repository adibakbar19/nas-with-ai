"""Valkey stream consumer — reads ingest job events and dispatches to JobRunner.

Zero backend/ imports. All dependencies wired via constructor or env vars.
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import redis

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _build_dsn() -> str:
    """Build PostgreSQL DSN from environment variables."""
    for key in ("POSTGRES_DSN", "DATABASE_URL"):
        raw = os.environ.get(key, "").strip()
        if raw:
            return raw
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    password = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    database = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class IngestWorker:
    """Valkey stream consumer that dispatches ingest jobs to a JobRunner.

    All dependencies are injected via the constructor.
    """

    def __init__(
        self,
        *,
        job_runner: Any,
        job_repo: Any,
        valkey_url: str,
        valkey_stream_key: str,
        valkey_stream_group: str,
        valkey_stream_consumer: str,
        valkey_stream_block_ms: int = 5000,
        valkey_stream_claim_idle_ms: int = 60000,
        valkey_stream_claim_count: int = 10,
    ) -> None:
        self._runner = job_runner
        self._job_repo = job_repo
        self._valkey_url = valkey_url
        self._valkey_stream_key = valkey_stream_key
        self._valkey_stream_group = valkey_stream_group
        self._valkey_stream_consumer = valkey_stream_consumer
        self._valkey_stream_block_ms = max(1000, valkey_stream_block_ms)
        self._valkey_stream_claim_idle_ms = max(1000, valkey_stream_claim_idle_ms)
        self._valkey_stream_claim_count = max(1, valkey_stream_claim_count)
        self._valkey_connect_retry_seconds = 2.0
        self._worker_id = valkey_stream_consumer or f"{socket.gethostname() or 'worker'}-{os.getpid()}"
        self._stale_recovery_interval_seconds = max(
            5.0, float(os.getenv("INGEST_STALE_CLAIM_RECOVERY_INTERVAL_SECONDS", "30")),
        )
        self._last_stale_recovery_at = 0.0
        self._valkey_stream = None

    def run_forever(self) -> None:
        logger.info("worker_started consumer=%s stream=%s", self._worker_id, self._valkey_stream_key)
        self._ensure_valkey_stream_ready()
        while True:
            try:
                self._recover_stale_claims_if_due()
                self._poll_valkey_stream_once()
            except Exception:
                logger.exception("worker_loop_error")
                self._valkey_stream = None
                self._ensure_valkey_stream_ready()
                time.sleep(2)

    def _build_valkey_stream_client(self):
        return redis.Redis.from_url(self._valkey_url, decode_responses=True)

    def _ensure_valkey_stream_ready(self) -> None:
        while True:
            try:
                if self._valkey_stream is None:
                    self._valkey_stream = self._build_valkey_stream_client()
                self._ensure_stream_group()
                logger.info(
                    "valkey_stream_ready stream=%s group=%s consumer=%s",
                    self._valkey_stream_key, self._valkey_stream_group, self._valkey_stream_consumer,
                )
                return
            except Exception:
                logger.exception("valkey_stream_not_ready retry_in=%ss", self._valkey_connect_retry_seconds)
                self._valkey_stream = None
                time.sleep(self._valkey_connect_retry_seconds)

    def _ensure_stream_group(self) -> None:
        assert self._valkey_stream is not None
        try:
            self._valkey_stream.ping()
            self._valkey_stream.xgroup_create(
                self._valkey_stream_key, self._valkey_stream_group, id="0", mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    @staticmethod
    def _extract_stream_payload(fields: dict[str, Any]) -> dict[str, Any]:
        payload = fields.get("payload")
        if payload is None:
            raise ValueError("missing payload field in Valkey stream message")
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json.loads(str(payload))

    def _poll_valkey_stream_once(self) -> None:
        assert self._valkey_stream is not None
        claimed = self._claim_stale_pending_messages()
        if claimed:
            self._process_stream_messages(claimed)
            return
        entries = self._valkey_stream.xreadgroup(
            groupname=self._valkey_stream_group,
            consumername=self._valkey_stream_consumer,
            streams={self._valkey_stream_key: ">"},
            count=1,
            block=self._valkey_stream_block_ms,
        )
        if not entries:
            return
        self._process_stream_messages(entries)

    def _claim_stale_pending_messages(self) -> list:
        assert self._valkey_stream is not None
        try:
            result = self._valkey_stream.xautoclaim(
                self._valkey_stream_key, self._valkey_stream_group, self._valkey_stream_consumer,
                min_idle_time=self._valkey_stream_claim_idle_ms, start_id="0-0",
                count=self._valkey_stream_claim_count,
            )
        except Exception:
            logger.exception("valkey_stream_claim_failed")
            return []
        messages = []
        if isinstance(result, (list, tuple)) and len(result) >= 2:
            raw_messages = result[1]
            if isinstance(raw_messages, list):
                messages = [(str(mid), fields) for mid, fields in raw_messages]
        if messages:
            logger.info("valkey_stream_claimed_pending count=%s", len(messages))
            return [(self._valkey_stream_key, messages)]
        return []

    def _process_stream_messages(self, entries: list) -> None:
        assert self._valkey_stream is not None
        for _stream_name, messages in entries:
            for message_id, fields in messages:
                try:
                    payload = self._extract_stream_payload(fields)
                    claimed_event = self._prepare_event(payload, delivery_id=message_id)
                except Exception:
                    logger.exception("event_processing_failed message_id=%s", message_id)
                    try:
                        self._valkey_stream.xack(self._valkey_stream_key, self._valkey_stream_group, message_id)
                    except Exception:
                        pass
                    continue
                try:
                    self._valkey_stream.xack(self._valkey_stream_key, self._valkey_stream_group, message_id)
                except Exception:
                    logger.exception("ack_failed message_id=%s", message_id)
                if claimed_event:
                    self._run_claimed_job(claimed_event)

    def _recover_stale_claims_if_due(self) -> None:
        now = time.time()
        if now - self._last_stale_recovery_at < self._stale_recovery_interval_seconds:
            return
        self._last_stale_recovery_at = now
        try:
            recovered = self._job_repo.requeue_stale_claims(
                stale_after_seconds=int(os.getenv("INGEST_STALE_CLAIM_SECONDS", "300")),
                statuses=("running",),
                limit=20,
            )
            if recovered:
                logger.warning("recovered_stale_claims count=%s", len(recovered))
        except Exception:
            logger.exception("stale_claim_recovery_failed")

    def _prepare_event(self, event: dict[str, Any], *, delivery_id: str | None = None) -> tuple[str, str, dict[str, Any]] | None:
        event_type = event.get("event_type")
        job_id = str(event.get("job_id") or "").strip()
        if not job_id:
            logger.warning("missing_job_id event=%s", event)
            return None

        if event_type == "search_sync_requested":
            logger.warning(
                "search_sync_requested: not yet implemented in nas-processor-worker, skipping job_id=%s",
                job_id,
            )
            return None

        if event_type != "bulk_ingest_requested":
            logger.warning("unknown_event_type event_type=%s", event_type)
            return None

        # Phase 3: get job via queue-service (falls back to direct DB in _job_repo)
        job = None
        if hasattr(self._runner, "_jobs") and hasattr(self._runner._jobs, "_qc"):
            job = self._runner._jobs._qc.get_job(job_id)
        if job is None:
            job = self._job_repo.get_job(job_id=job_id)
        if not job:
            logger.warning("job_not_found job_id=%s", job_id)
            return None

        # Claim the job — mark it running via queue-service first, then fall back
        claimed = False
        if hasattr(self._runner, "_jobs") and hasattr(self._runner._jobs, "_qc"):
            # Optimistic claim via progress update
            self._runner._jobs._qc.update_progress(
                job_id, self._worker_id,
                status="running",
                claimed_by=self._worker_id,
                claimed_at=datetime.now(timezone.utc).isoformat(),
                heartbeat_at=datetime.now(timezone.utc).isoformat(),
            )
            claimed = True
        else:
            claimed_row = self._job_repo.claim_job(
                job_id=job_id,
                worker_id=self._worker_id,
                expected_statuses=("queued",),
                queue_event_id=str(event.get("event_id") or "").strip() or None,
                queue_message_id=delivery_id,
            )
            claimed = claimed_row is not None

        if not claimed:
            logger.info("skip_unclaimed job_id=%s delivery_id=%s", job_id, delivery_id)
            return None

        logger.info("claimed_ingest job_id=%s worker_id=%s", job_id, self._worker_id)
        return ("bulk_ingest_requested", job_id, event)

    def _run_claimed_job(self, claimed: tuple[str, str, dict[str, Any]]) -> None:
        event_type, job_id, event = claimed
        if event_type == "bulk_ingest_requested":
            self._runner.run_job(job_id)
            return
        logger.warning("unknown_claimed_event event_type=%s job_id=%s", event_type, job_id)


def main() -> None:
    """Worker entry point — wires all dependencies from env vars."""
    from nas_processor.src.worker.runner import JobRunner, RunnerConfig
    from nas_processor.src.worker.object_store import ObjectStoreSettings, build_s3_client
    from nas_processor.src.api.repositories.job_repository import IngestJobStateRepository

    dsn = _build_dsn()
    ingest_schema = (
        os.environ.get("INGEST_JOB_STATE_SCHEMA")
        or os.environ.get("PGSCHEMA")
        or "nas"
    ).strip() or "nas"
    db_schema = os.environ.get("PGSCHEMA", "nas").strip() or "nas"

    # Job state repository (direct DB access)
    job_repo = IngestJobStateRepository(dsn=dsn, schema=ingest_schema)

    # S3 client
    s3_settings = ObjectStoreSettings.from_env()
    s3_client = build_s3_client(s3_settings)

    # Runner config
    runner_config = RunnerConfig(
        project_root=Path("."),
        log_dir=Path(os.getenv("LOG_DIR", "logs/jobs")),
        upload_staging_dir=Path(os.getenv("UPLOAD_STAGING_DIR", "data/uploads")),
        output_uploads_dir=Path(os.getenv("OUTPUT_UPLOADS_DIR", "output/uploads")),
        default_bucket=s3_settings.bucket,
        db_schema=db_schema,
        persist_live_updates=os.getenv("INGEST_PERSIST_LIVE_UPDATES", "1").lower() in {"1", "true", "yes"},
        persist_live_interval_seconds=float(os.getenv("INGEST_PERSIST_LIVE_INTERVAL_SECONDS", "2.0")),
    )

    # Build and start worker — determine worker ID first
    hostname = socket.gethostname() or "worker"
    default_consumer = f"{hostname}-{os.getpid()}"
    worker_id = os.getenv("VALKEY_STREAM_CONSUMER", default_consumer)

    # Phase 3: use QueueClientJobState to report via queue-service HTTP API.
    # Falls back to direct DB via _JobStateAdapter for get_job() and
    # queue_search_sync_job() which are separate concerns.
    from nas_processor.src.worker.queue_client import QueueClient
    from nas_processor.src.worker.runner import QueueClientJobState

    queue_client = QueueClient(
        base_url=os.environ.get("QUEUE_SERVICE_URL", "http://queue-service:8005"),
        service_key=os.environ.get("QUEUE_SERVICE_KEY", "internal-dev-key"),
    )

    class _SearchSyncAdapter:
        """Provides only queue_search_sync_job() — keeps search sync in-process."""

        def queue_search_sync_job(self, job_id: str) -> None:
            from nas_config.db import build_dsn
            from nas_processor.src.search.config import SearchConfig
            from nas_processor.src.search.sync import sync_job_to_search

            config = SearchConfig(
                opensearch_url=os.environ.get("OPENSEARCH_URL", "http://opensearch:9200"),
                index_name=os.environ.get("OPENSEARCH_INDEX", "nas_addresses"),
            )
            result = sync_job_to_search(
                job_id,
                dsn=build_dsn(driver="psycopg"),
                search_config=config,
            )
            logger.info(
                "search_sync_complete job_id=%s indexed=%d errors=%d skipped=%d",
                job_id,
                result.get("indexed", 0),
                result.get("errors", 0),
                result.get("skipped", 0),
            )

    job_state = QueueClientJobState(
        queue_client=queue_client,
        worker_id=worker_id,
        fallback_repo=_SearchSyncAdapter(),
    )

    runner = JobRunner(
        s3_client=s3_client,
        job_state=job_state,
        config=runner_config,
    )

    worker = IngestWorker(
        job_runner=runner,
        job_repo=job_repo,
        valkey_url=os.getenv("VALKEY_URL", "redis://valkey:6379/0"),
        valkey_stream_key=os.getenv("VALKEY_STREAM_KEY", "bulk_ingest_events"),
        valkey_stream_group=os.getenv("VALKEY_STREAM_GROUP", "bulk_ingest_workers"),
        valkey_stream_consumer=worker_id,
        valkey_stream_block_ms=int(os.getenv("VALKEY_STREAM_BLOCK_MS", "5000")),
        valkey_stream_claim_idle_ms=int(os.getenv("VALKEY_STREAM_CLAIM_IDLE_MS", "60000")),
        valkey_stream_claim_count=int(os.getenv("VALKEY_STREAM_CLAIM_COUNT", "10")),
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
