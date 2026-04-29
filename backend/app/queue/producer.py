import logging
from typing import Protocol

from backend.app.monitoring import record_queue_publish
from backend.app.schemas.ingest import BulkIngestEvent, SearchSyncEvent


logger = logging.getLogger(__name__)


class QueueProducer(Protocol):
    def publish_bulk_ingest(self, event: BulkIngestEvent) -> str:
        ...

    def publish_search_sync(self, event: SearchSyncEvent) -> str:
        ...


class ValkeyStreamQueueProducer:
    """Valkey Stream producer for bulk ingest jobs."""

    def __init__(self, *, valkey_url: str, stream_key: str) -> None:
        if not valkey_url:
            raise ValueError("VALKEY_URL must be configured")
        if not stream_key:
            raise ValueError("VALKEY_STREAM_KEY must be configured")
        try:
            import redis
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("redis package is required for Valkey Streams") from exc
        self._client = redis.Redis.from_url(valkey_url, decode_responses=True)
        self._stream_key = stream_key

    def _publish_event(self, *, event_type: str, event_id: str, payload: str, job_id: str) -> str:
        message_id = self._client.xadd(
            self._stream_key,
            {"event_type": event_type, "event_id": event_id, "payload": payload},
        )
        record_queue_publish("valkey_stream")
        logger.info(
            "queue_publish backend=valkey_stream event_type=%s message_id=%s event_id=%s job_id=%s",
            event_type,
            message_id,
            event_id,
            job_id,
        )
        return str(message_id)

    def publish_bulk_ingest(self, event: BulkIngestEvent) -> str:
        payload = event.model_dump_json()
        return self._publish_event(
            event_type=event.event_type,
            event_id=event.event_id,
            payload=payload,
            job_id=event.job_id,
        )

    def publish_search_sync(self, event: SearchSyncEvent) -> str:
        payload = event.model_dump_json()
        return self._publish_event(
            event_type=event.event_type,
            event_id=event.event_id,
            payload=payload,
            job_id=event.job_id,
        )
