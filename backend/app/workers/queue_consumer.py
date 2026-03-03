"""Async worker that consumes queue events and executes ingest jobs outside API request cycle."""

from __future__ import annotations

import json
import logging
import os
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
    ) -> None:
        self._queue_backend = queue_backend
        self._queue_url = queue_url
        self._aws_region = aws_region
        self._event_log_path = Path(event_log_path)
        self._offset_path = Path(offset_path)
        self._sqs_client = None

        if queue_backend == "sqs":
            try:
                import boto3
            except Exception as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("boto3 is required for SQS worker backend") from exc
            self._sqs_client = boto3.client("sqs", region_name=aws_region)

    def run_forever(self) -> None:
        logger.info("worker_started backend=%s", self._queue_backend)
        while True:
            try:
                if self._queue_backend == "sqs":
                    self._poll_sqs_once()
                else:
                    self._poll_log_once()
            except Exception:
                logger.exception("worker_loop_error")
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
            self._handle_event(payload)
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
                    self._handle_event(payload)
                except Exception:
                    logger.exception("event_processing_failed payload=%s", payload)
                self._write_offset(next_offset)

        time.sleep(0.5)

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

        path = ingest_runtime.JOB_STATE_FILE
        if not path.exists():
            return None
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(rows, list):
            return None
        for row in rows:
            if isinstance(row, dict) and str(row.get("job_id") or "") == job_id:
                return row
        return None

    @classmethod
    def _handle_event(cls, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        if event_type != "bulk_ingest_requested":
            logger.warning("unknown_event_type event_type=%s", event_type)
            return

        job_id = str(event.get("job_id") or "").strip()
        if not job_id:
            logger.warning("missing_job_id event=%s", event)
            return

        from backend.app import main as ingest_runtime

        job = cls._read_job_from_state(job_id)
        if not job:
            logger.warning("job_not_found_in_state job_id=%s", job_id)
            return

        status = str(job.get("status") or "").lower()
        if status in {"running", "completed", "paused", "pausing"}:
            logger.info("skip_event_existing_status job_id=%s status=%s", job_id, status)
            return

        changes = {k: v for k, v in job.items() if k != "job_id"}
        ingest_runtime._set_job(job_id, persist=False, **changes)

        logger.info(
            "processing_ingest job_id=%s bucket=%s object_name=%s source_type=%s",
            job_id,
            job.get("bucket"),
            job.get("object_name"),
            job.get("source_type"),
        )
        IngestService().run_job(job_id)


def main() -> None:
    worker = IngestWorker(
        queue_backend=os.getenv("QUEUE_BACKEND", "log").lower(),
        queue_url=os.getenv("SQS_QUEUE_URL", ""),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-5"),
        event_log_path=os.getenv("QUEUE_EVENT_LOG", "logs/queue/bulk_ingest_events.jsonl"),
        offset_path=os.getenv("QUEUE_OFFSET_FILE", "logs/queue/bulk_ingest_events.offset"),
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
