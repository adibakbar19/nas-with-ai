import json
import logging
from pathlib import Path
from typing import Protocol

from backend.app.schemas.ingest import BulkIngestEvent


logger = logging.getLogger(__name__)


class QueueProducer(Protocol):
    def publish_bulk_ingest(self, event: BulkIngestEvent) -> str:
        ...


class LoggingQueueProducer:
    """File-backed local queue producer for development and simple deployments."""

    def __init__(self, *, event_log_path: str) -> None:
        self._event_log_path = Path(event_log_path)

    def publish_bulk_ingest(self, event: BulkIngestEvent) -> str:
        self._event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._event_log_path.open("a", encoding="utf-8") as handle:
            handle.write(event.model_dump_json())
            handle.write("\n")
        logger.info("queue_publish backend=log event_id=%s job_id=%s", event.event_id, event.job_id)
        return event.event_id


class SQSQueueProducer:
    def __init__(self, *, queue_url: str, region: str) -> None:
        if not queue_url:
            raise ValueError("SQS queue_url must be configured")
        try:
            import boto3
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("boto3 is required for SQSQueueProducer") from exc
        self._client = boto3.client("sqs", region_name=region)
        self._queue_url = queue_url

    def publish_bulk_ingest(self, event: BulkIngestEvent) -> str:
        body = event.model_dump()
        kwargs = {
            "QueueUrl": self._queue_url,
            "MessageBody": json.dumps(body, ensure_ascii=True),
        }
        if self._queue_url.endswith(".fifo"):
            kwargs["MessageDeduplicationId"] = event.event_id
            kwargs["MessageGroupId"] = "nas-bulk-ingest"
        response = self._client.send_message(**kwargs)
        return str(response.get("MessageId") or event.event_id)
