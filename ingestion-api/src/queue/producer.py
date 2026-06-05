"""Valkey Stream producer for publishing ingest job events."""

import logging

import redis

logger = logging.getLogger(__name__)


class ValkeyStreamQueueProducer:
    """Publishes events to a Valkey Stream for worker consumption."""

    def __init__(self, *, valkey_url: str, stream_key: str) -> None:
        if not valkey_url:
            raise ValueError("VALKEY_URL must be configured")
        if not stream_key:
            raise ValueError("VALKEY_STREAM_KEY must be configured")
        self._client = redis.Redis.from_url(valkey_url, decode_responses=True)
        self._stream_key = stream_key

    def publish(self, *, event_type: str, event_id: str, payload: str, job_id: str) -> str:
        """Publish an event to the Valkey stream.

        Args:
            event_type: Event type identifier (e.g. "bulk_ingest_requested")
            event_id: Unique event ID
            payload: JSON-serialized event payload
            job_id: Associated job ID (for logging)

        Returns:
            The Valkey stream message ID.
        """
        message_id = self._client.xadd(
            self._stream_key,
            {"event_type": event_type, "event_id": event_id, "payload": payload},
        )
        logger.info(
            "queue_publish stream=%s event_type=%s message_id=%s event_id=%s job_id=%s",
            self._stream_key,
            event_type,
            message_id,
            event_id,
            job_id,
        )
        return str(message_id)
