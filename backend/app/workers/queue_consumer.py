"""Async worker that consumes queue events and executes ingest jobs outside API request cycle."""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Any

from backend.app.services.ingest_service import IngestService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class IngestWorker:
    def __init__(
        self,
        *,
        queue_backend: str,
        queue_url: str,
        aws_region: str,
        event_log_path: str,
        offset_path: str,
        redis_url: str,
        redis_stream_key: str,
        redis_stream_group: str,
        redis_stream_consumer: str,
        redis_stream_block_ms: int,
        redis_stream_claim_idle_ms: int,
        redis_stream_claim_count: int,
    ) -> None:
        self._queue_backend = queue_backend
        self._queue_url = queue_url
        self._aws_region = aws_region
        self._event_log_path = Path(event_log_path)
        self._offset_path = Path(offset_path)
        self._redis_url = redis_url
        self._redis_stream_key = redis_stream_key
        self._redis_stream_group = redis_stream_group
        self._redis_stream_consumer = redis_stream_consumer
        self._redis_stream_block_ms = max(1000, redis_stream_block_ms)
        self._redis_stream_claim_idle_ms = max(1000, redis_stream_claim_idle_ms)
        self._redis_stream_claim_count = max(1, redis_stream_claim_count)
        self._redis_connect_retry_seconds = 2.0
        self._worker_id = redis_stream_consumer or f"{socket.gethostname() or 'worker'}-{os.getpid()}"
        self._stale_recovery_interval_seconds = max(
            5.0,
            float(os.getenv("INGEST_STALE_CLAIM_RECOVERY_INTERVAL_SECONDS", "30")),
        )
        self._last_stale_recovery_at = 0.0
        self._sqs_client = None
        self._redis_stream = None
        self._redis_factory = None

        if queue_backend == "sqs":
            try:
                import boto3
            except Exception as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("boto3 is required for SQS worker backend") from exc
            self._sqs_client = boto3.client("sqs", region_name=aws_region)
        elif queue_backend == "redis_stream":
            if not redis_url:
                raise RuntimeError("REDIS_URL is required for redis_stream worker backend")
            if not redis_stream_key:
                raise RuntimeError("REDIS_STREAM_KEY is required for redis_stream worker backend")
            if not redis_stream_group:
                raise RuntimeError("REDIS_STREAM_GROUP is required for redis_stream worker backend")
            try:
                import redis
            except Exception as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("redis is required for redis_stream worker backend") from exc
            self._redis_factory = redis.Redis

    def run_forever(self) -> None:
        logger.info("worker_started backend=%s", self._queue_backend)
        if self._queue_backend == "redis_stream":
            self._ensure_redis_stream_ready()
        while True:
            try:
                self._recover_stale_claims_if_due()
                if self._queue_backend == "sqs":
                    self._poll_sqs_once()
                elif self._queue_backend == "redis_stream":
                    self._poll_redis_stream_once()
                else:
                    self._poll_log_once()
            except Exception:
                logger.exception("worker_loop_error")
                if self._queue_backend == "redis_stream":
                    self._redis_stream = None
                    self._ensure_redis_stream_ready()
                time.sleep(2)

    def _poll_sqs_once(self) -> None:
        assert self._sqs_client is not None
        response = self._sqs_client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=20,
            VisibilityTimeout=120,
        )
        messages = response.get("Messages", [])
        if not messages:
            return

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            body = msg.get("Body", "{}")
            payload = json.loads(body)
            job_id = self._prepare_event(payload, delivery_id=msg.get("MessageId"))
            if job_id:
                self._run_claimed_job(job_id)
            self._sqs_client.delete_message(QueueUrl=self._queue_url, ReceiptHandle=receipt_handle)

    def _poll_log_once(self) -> None:
        if not self._event_log_path.exists():
            time.sleep(1)
            return

        offset = self._read_offset()
        with self._event_log_path.open("r", encoding="utf-8") as handle:
            handle.seek(offset)
            while True:
                raw = handle.readline()
                if not raw:
                    break
                next_offset = handle.tell()
                line = raw.strip()
                if not line:
                    self._write_offset(next_offset)
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("skip_malformed_event line=%s", line[:200])
                    self._write_offset(next_offset)
                    continue
                try:
                    job_id = self._prepare_event(payload)
                    if job_id:
                        self._run_claimed_job(job_id)
                except Exception:
                    logger.exception("event_processing_failed payload=%s", payload)
                self._write_offset(next_offset)

        time.sleep(0.5)

    def _build_redis_stream_client(self):
        if self._redis_factory is None:
            raise RuntimeError("redis client factory is not initialized")
        return self._redis_factory.from_url(self._redis_url, decode_responses=True)

    def _ensure_redis_stream_ready(self) -> None:
        while True:
            try:
                if self._redis_stream is None:
                    self._redis_stream = self._build_redis_stream_client()
                self._ensure_stream_group()
                logger.info(
                    "redis_stream_ready stream=%s group=%s consumer=%s",
                    self._redis_stream_key,
                    self._redis_stream_group,
                    self._redis_stream_consumer,
                )
                return
            except Exception:
                logger.exception(
                    "redis_stream_not_ready redis_url=%s stream=%s group=%s retry_in_seconds=%s",
                    self._redis_url,
                    self._redis_stream_key,
                    self._redis_stream_group,
                    self._redis_connect_retry_seconds,
                )
                self._redis_stream = None
                time.sleep(self._redis_connect_retry_seconds)

    def _ensure_stream_group(self) -> None:
        assert self._redis_stream is not None
        try:
            self._redis_stream.ping()
            self._redis_stream.xgroup_create(
                self._redis_stream_key,
                self._redis_stream_group,
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
            raise ValueError("missing payload field in redis stream message")
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json.loads(str(payload))

    def _poll_redis_stream_once(self) -> None:
        assert self._redis_stream is not None
        claimed = self._claim_stale_pending_messages()
        if claimed:
            self._process_stream_messages(claimed)
            return

        entries = self._redis_stream.xreadgroup(
            groupname=self._redis_stream_group,
            consumername=self._redis_stream_consumer,
            streams={self._redis_stream_key: ">"},
            count=1,
            block=self._redis_stream_block_ms,
        )
        if not entries:
            return

        self._process_stream_messages(entries)

    def _claim_stale_pending_messages(self) -> list[tuple[str, list[tuple[str, dict[str, Any]]]]]:
        assert self._redis_stream is not None
        try:
            result = self._redis_stream.xautoclaim(
                self._redis_stream_key,
                self._redis_stream_group,
                self._redis_stream_consumer,
                min_idle_time=self._redis_stream_claim_idle_ms,
                start_id="0-0",
                count=self._redis_stream_claim_count,
            )
        except Exception:
            logger.exception(
                "redis_stream_claim_failed stream=%s group=%s consumer=%s",
                self._redis_stream_key,
                self._redis_stream_group,
                self._redis_stream_consumer,
            )
            return []

        messages: list[tuple[str, dict[str, Any]]] = []
        if isinstance(result, (list, tuple)) and len(result) >= 2:
            raw_messages = result[1]
            if isinstance(raw_messages, list):
                messages = [(str(message_id), fields) for message_id, fields in raw_messages]

        if messages:
            logger.info(
                "redis_stream_claimed_pending stream=%s group=%s consumer=%s count=%s idle_ms=%s",
                self._redis_stream_key,
                self._redis_stream_group,
                self._redis_stream_consumer,
                len(messages),
                self._redis_stream_claim_idle_ms,
            )
            return [(self._redis_stream_key, messages)]
        return []

    def _process_stream_messages(self, entries: list[tuple[str, list[tuple[str, dict[str, Any]]]]]) -> None:
        assert self._redis_stream is not None
        for _stream_name, messages in entries:
            for message_id, fields in messages:
                try:
                    payload = self._extract_stream_payload(fields)
                    job_id = self._prepare_event(payload, delivery_id=message_id)
                except Exception:
                    logger.exception("redis_stream_event_processing_failed message_id=%s", message_id)
                    continue

                try:
                    self._redis_stream.xack(
                        self._redis_stream_key,
                        self._redis_stream_group,
                        message_id,
                    )
                except Exception:
                    logger.exception("redis_stream_ack_failed message_id=%s", message_id)

                if job_id:
                    self._run_claimed_job(job_id)

    def _recover_stale_claims_if_due(self) -> None:
        now = time.time()
        if now - self._last_stale_recovery_at < self._stale_recovery_interval_seconds:
            return
        self._last_stale_recovery_at = now
        from backend.app import main as ingest_runtime

        recovered = ingest_runtime._recover_stale_claims_and_requeue(limit=20)
        if recovered:
            logger.warning(
                "recovered_stale_claims count=%s job_ids=%s",
                len(recovered),
                ",".join(str(row.get("job_id") or "") for row in recovered if row.get("job_id")),
            )

    def _read_offset(self) -> int:
        if not self._offset_path.exists():
            return 0
        try:
            raw = self._offset_path.read_text(encoding="utf-8").strip()
            return int(raw or "0")
        except Exception:
            return 0

    def _write_offset(self, offset: int) -> None:
        self._offset_path.parent.mkdir(parents=True, exist_ok=True)
        self._offset_path.write_text(str(offset), encoding="utf-8")

    @staticmethod
    def _read_job_from_state(job_id: str) -> dict[str, Any] | None:
        from backend.app import main as ingest_runtime

        return ingest_runtime._get_job_for_api(job_id)

    def _prepare_event(self, event: dict[str, Any], *, delivery_id: str | None = None) -> str | None:
        event_type = event.get("event_type")
        if event_type != "bulk_ingest_requested":
            logger.warning("unknown_event_type event_type=%s", event_type)
            return None

        job_id = str(event.get("job_id") or "").strip()
        if not job_id:
            logger.warning("missing_job_id event=%s", event)
            return None

        from backend.app import main as ingest_runtime

        job = self._read_job_from_state(job_id)
        if not job:
            logger.warning("job_not_found_in_state job_id=%s", job_id)
            return None

        claimed = ingest_runtime._claim_job_for_worker(
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
        return job_id

    @staticmethod
    def _run_claimed_job(job_id: str) -> None:
        IngestService().run_job(job_id)


def main() -> None:
    # Import backend runtime once up front so shared startup guards fail fast.
    from backend.app import main as _ingest_runtime  # noqa: F401

    hostname = socket.gethostname() or "worker"
    default_consumer = f"{hostname}-{os.getpid()}"
    worker = IngestWorker(
        queue_backend=os.getenv("QUEUE_BACKEND", "redis_stream").lower(),
        queue_url=os.getenv("SQS_QUEUE_URL", ""),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-5"),
        event_log_path=os.getenv("QUEUE_EVENT_LOG", "logs/queue/bulk_ingest_events.jsonl"),
        offset_path=os.getenv("QUEUE_OFFSET_FILE", "logs/queue/bulk_ingest_events.offset"),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        redis_stream_key=os.getenv("REDIS_STREAM_KEY", "bulk_ingest_events"),
        redis_stream_group=os.getenv("REDIS_STREAM_GROUP", "bulk_ingest_workers"),
        redis_stream_consumer=os.getenv("REDIS_STREAM_CONSUMER", default_consumer),
        redis_stream_block_ms=max(1000, int(os.getenv("REDIS_STREAM_BLOCK_MS", "5000"))),
        redis_stream_claim_idle_ms=max(1000, int(os.getenv("REDIS_STREAM_CLAIM_IDLE_MS", "60000"))),
        redis_stream_claim_count=max(1, int(os.getenv("REDIS_STREAM_CLAIM_COUNT", "10"))),
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
