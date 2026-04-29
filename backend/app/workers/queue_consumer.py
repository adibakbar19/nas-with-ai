"""Async worker that consumes queue events and executes ingest jobs outside API request cycle."""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from typing import Any

from backend.app.monitoring import (
    ensure_worker_metrics_server,
    record_queue_claim,
    record_worker_loop_error,
    record_worker_started,
)
from backend.app.runtime import jobs as ingest_jobs
from backend.app.services.ingest_service import IngestService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class IngestWorker:
    def __init__(
        self,
        *,
        valkey_url: str,
        valkey_stream_key: str,
        valkey_stream_group: str,
        valkey_stream_consumer: str,
        valkey_stream_block_ms: int,
        valkey_stream_claim_idle_ms: int,
        valkey_stream_claim_count: int,
    ) -> None:
        self._queue_backend = "valkey_stream"
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
            5.0,
            float(os.getenv("INGEST_STALE_CLAIM_RECOVERY_INTERVAL_SECONDS", "30")),
        )
        self._last_stale_recovery_at = 0.0
        self._valkey_stream = None

        if not valkey_url:
            raise RuntimeError("VALKEY_URL is required for Valkey stream worker")
        if not valkey_stream_key:
            raise RuntimeError("VALKEY_STREAM_KEY is required for Valkey stream worker")
        if not valkey_stream_group:
            raise RuntimeError("VALKEY_STREAM_GROUP is required for Valkey stream worker")
        try:
            import redis
        except Exception as exc:  # pragma: no cover - dependency provided in runtime image
            raise RuntimeError("redis package is required for Valkey stream worker") from exc
        self._valkey_factory = redis.Redis

    def run_forever(self) -> None:
        record_worker_started(self._queue_backend)
        logger.info("worker_started backend=%s", self._queue_backend)
        self._ensure_valkey_stream_ready()
        while True:
            try:
                self._recover_stale_claims_if_due()
                self._poll_valkey_stream_once()
            except Exception:
                record_worker_loop_error(self._queue_backend)
                logger.exception("worker_loop_error")
                self._valkey_stream = None
                self._ensure_valkey_stream_ready()
                time.sleep(2)

    def _build_valkey_stream_client(self):
        return self._valkey_factory.from_url(self._valkey_url, decode_responses=True)

    def _ensure_valkey_stream_ready(self) -> None:
        while True:
            try:
                if self._valkey_stream is None:
                    self._valkey_stream = self._build_valkey_stream_client()
                self._ensure_stream_group()
                logger.info(
                    "valkey_stream_ready stream=%s group=%s consumer=%s",
                    self._valkey_stream_key,
                    self._valkey_stream_group,
                    self._valkey_stream_consumer,
                )
                return
            except Exception:
                logger.exception(
                    "valkey_stream_not_ready valkey_url=%s stream=%s group=%s retry_in_seconds=%s",
                    self._valkey_url,
                    self._valkey_stream_key,
                    self._valkey_stream_group,
                    self._valkey_connect_retry_seconds,
                )
                self._valkey_stream = None
                time.sleep(self._valkey_connect_retry_seconds)

    def _ensure_stream_group(self) -> None:
        assert self._valkey_stream is not None
        try:
            self._valkey_stream.ping()
            self._valkey_stream.xgroup_create(
                self._valkey_stream_key,
                self._valkey_stream_group,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            message = str(exc)
            if "BUSYGROUP" not in message:
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

    def _claim_stale_pending_messages(self) -> list[tuple[str, list[tuple[str, dict[str, Any]]]]]:
        assert self._valkey_stream is not None
        try:
            result = self._valkey_stream.xautoclaim(
                self._valkey_stream_key,
                self._valkey_stream_group,
                self._valkey_stream_consumer,
                min_idle_time=self._valkey_stream_claim_idle_ms,
                start_id="0-0",
                count=self._valkey_stream_claim_count,
            )
        except Exception:
            logger.exception(
                "valkey_stream_claim_failed stream=%s group=%s consumer=%s",
                self._valkey_stream_key,
                self._valkey_stream_group,
                self._valkey_stream_consumer,
            )
            return []

        messages: list[tuple[str, dict[str, Any]]] = []
        if isinstance(result, (list, tuple)) and len(result) >= 2:
            raw_messages = result[1]
            if isinstance(raw_messages, list):
                messages = [(str(message_id), fields) for message_id, fields in raw_messages]

        if messages:
            logger.info(
                "valkey_stream_claimed_pending stream=%s group=%s consumer=%s count=%s idle_ms=%s",
                self._valkey_stream_key,
                self._valkey_stream_group,
                self._valkey_stream_consumer,
                len(messages),
                self._valkey_stream_claim_idle_ms,
            )
            return [(self._valkey_stream_key, messages)]
        return []

    def _process_stream_messages(self, entries: list[tuple[str, list[tuple[str, dict[str, Any]]]]]) -> None:
        assert self._valkey_stream is not None
        for _stream_name, messages in entries:
            for message_id, fields in messages:
                try:
                    payload = self._extract_stream_payload(fields)
                    claimed_event = self._prepare_event(payload, delivery_id=message_id)
                except Exception:
                    logger.exception("valkey_stream_event_processing_failed message_id=%s", message_id)
                    try:
                        self._valkey_stream.xack(
                            self._valkey_stream_key,
                            self._valkey_stream_group,
                            message_id,
                        )
                    except Exception:
                        logger.exception("valkey_stream_ack_failed message_id=%s", message_id)
                    continue

                try:
                    self._valkey_stream.xack(
                        self._valkey_stream_key,
                        self._valkey_stream_group,
                        message_id,
                    )
                except Exception:
                    logger.exception("valkey_stream_ack_failed message_id=%s", message_id)

                if claimed_event:
                    self._run_claimed_job(claimed_event)

    def _recover_stale_claims_if_due(self) -> None:
        now = time.time()
        if now - self._last_stale_recovery_at < self._stale_recovery_interval_seconds:
            return
        self._last_stale_recovery_at = now
        recovered = ingest_jobs.recover_stale_claims_and_requeue(limit=20)
        if recovered:
            logger.warning(
                "recovered_stale_claims count=%s job_ids=%s",
                len(recovered),
                ",".join(str(row.get("job_id") or "") for row in recovered if row.get("job_id")),
            )

    @staticmethod
    def _read_job_from_state(job_id: str) -> dict[str, Any] | None:
        return ingest_jobs.get_job_for_api(job_id)

    def _prepare_event(self, event: dict[str, Any], *, delivery_id: str | None = None) -> tuple[str, str, dict[str, Any]] | None:
        event_type = event.get("event_type")
        job_id = str(event.get("job_id") or "").strip()
        if not job_id:
            logger.warning("missing_job_id event=%s", event)
            return None

        if event_type == "search_sync_requested":
            return self._prepare_search_sync_event(event, job_id=job_id, delivery_id=delivery_id)

        if event_type != "bulk_ingest_requested":
            logger.warning("unknown_event_type event_type=%s", event_type)
            return None

        job = self._read_job_from_state(job_id)
        if not job:
            logger.warning("job_not_found_in_state job_id=%s", job_id)
            return None

        claimed = ingest_jobs.claim_job_for_worker(
            job_id,
            worker_id=self._worker_id,
            queue_event_id=str(event.get("event_id") or "").strip() or None,
            queue_message_id=delivery_id,
        )
        if not claimed:
            current = self._read_job_from_state(job_id)
            status = str((current or {}).get("status") or "").lower() or "unknown"
            logger.info("skip_event_unclaimed job_id=%s status=%s delivery_id=%s", job_id, status, delivery_id)
            return None

        logger.info(
            "claimed_ingest job_id=%s bucket=%s object_name=%s source_type=%s worker_id=%s delivery_id=%s",
            job_id,
            claimed.get("bucket"),
            claimed.get("object_name"),
            claimed.get("source_type"),
            self._worker_id,
            delivery_id,
        )
        record_queue_claim(self._queue_backend)
        return ("bulk_ingest_requested", job_id, event)

    def _prepare_search_sync_event(
        self,
        event: dict[str, Any],
        *,
        job_id: str,
        delivery_id: str | None,
    ) -> tuple[str, str, dict[str, Any]] | None:
        job = self._read_job_from_state(job_id)
        if not job:
            logger.warning("search_sync_job_not_found job_id=%s delivery_id=%s", job_id, delivery_id)
            return None
        logger.info(
            "claimed_search_sync job_id=%s index=%s schema=%s worker_id=%s delivery_id=%s",
            job_id,
            event.get("es_index"),
            event.get("schema_name"),
            self._worker_id,
            delivery_id,
        )
        record_queue_claim(self._queue_backend)
        return ("search_sync_requested", job_id, event)

    @staticmethod
    def _run_claimed_job(claimed: tuple[str, str, dict[str, Any]]) -> None:
        event_type, job_id, event = claimed
        if event_type == "bulk_ingest_requested":
            IngestService().run_job(job_id)
            return
        if event_type == "search_sync_requested":
            ingest_jobs.run_search_sync_job(job_id, event=event)
            return
        logger.warning("unknown_claimed_event_type event_type=%s job_id=%s", event_type, job_id)


def main() -> None:
    metrics_port = max(0, int(os.getenv("WORKER_METRICS_PORT", "9101")))
    if ensure_worker_metrics_server(metrics_port):
        logger.info("worker_metrics_server_started port=%s", metrics_port)

    hostname = socket.gethostname() or "worker"
    default_consumer = f"{hostname}-{os.getpid()}"
    worker = IngestWorker(
        valkey_url=os.getenv("VALKEY_URL", "redis://localhost:6379/0"),
        valkey_stream_key=os.getenv("VALKEY_STREAM_KEY", "bulk_ingest_events"),
        valkey_stream_group=os.getenv("VALKEY_STREAM_GROUP", "bulk_ingest_workers"),
        valkey_stream_consumer=os.getenv("VALKEY_STREAM_CONSUMER", default_consumer),
        valkey_stream_block_ms=max(1000, int(os.getenv("VALKEY_STREAM_BLOCK_MS", "5000"))),
        valkey_stream_claim_idle_ms=max(1000, int(os.getenv("VALKEY_STREAM_CLAIM_IDLE_MS", "60000"))),
        valkey_stream_claim_count=max(1, int(os.getenv("VALKEY_STREAM_CLAIM_COUNT", "10"))),
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
