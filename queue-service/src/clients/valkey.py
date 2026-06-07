"""Valkey stream client for queue-service."""
from __future__ import annotations

import json
import logging

import redis

logger = logging.getLogger(__name__)


class ValkeyQueueClient:
    def __init__(self, url: str, stream_key: str, stream_group: str) -> None:
        self._client = redis.Redis.from_url(url, decode_responses=True)
        self._stream_key = stream_key
        self._stream_group = stream_group

    def ensure_stream_group(self) -> None:
        try:
            self._client.xgroup_create(
                self._stream_key, self._stream_group, id="$", mkstream=True
            )
            logger.info("stream_group_created key=%s group=%s",
                        self._stream_key, self._stream_group)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                logger.warning("stream_group_ensure_error error=%s", exc)

    def push_job(self, job_id: str, job_type: str, data: dict) -> str:
        """XADD a job event to the stream. Returns stream message ID."""
        import uuid
        event_id = uuid.uuid4().hex
        payload = json.dumps({
            "event_type": "bulk_ingest_requested",
            "event_id": event_id,
            "job_id": job_id,
            **data,
        })
        msg_id = self._client.xadd(
            self._stream_key,
            {
                "event_type": "bulk_ingest_requested",
                "event_id": event_id,
                "payload": payload,
            },
        )
        logger.info("stream_push job_id=%s msg_id=%s", job_id, msg_id)
        return str(msg_id)

    def get_stream_info(self) -> dict:
        try:
            info = self._client.xinfo_stream(self._stream_key)
            return {
                "length": info.get("length", 0),
                "first_entry": str(info.get("first-entry", "")),
                "last_entry": str(info.get("last-entry", "")),
            }
        except Exception as exc:
            logger.warning("stream_info_failed error=%s", exc)
            return {"length": 0}

    def get_group_info(self) -> list[dict]:
        try:
            groups = self._client.xinfo_groups(self._stream_key)
            return [
                {
                    "name": g.get("name"),
                    "consumers": g.get("consumers", 0),
                    "pending": g.get("pending", 0),
                    "last_delivered_id": g.get("last-delivered-id"),
                    "lag": g.get("lag", 0),
                }
                for g in groups
            ]
        except Exception as exc:
            logger.warning("group_info_failed error=%s", exc)
            return []

    def cleanup_dead_consumers(
        self,
        idle_threshold_ms: int = 300_000,
    ) -> int:
        """Remove consumers with no pending messages idle > threshold.

        Called on startup and periodically to avoid the consumer list
        growing unboundedly when workers restart with new IDs.
        """
        try:
            consumers = self._client.xinfo_consumers(
                self._stream_key, self._stream_group
            )
        except Exception as exc:
            logger.warning("consumer_list_failed error=%s", exc)
            return 0

        cleaned = 0
        for c in consumers:
            pending = int(c.get("pending", 0))
            idle = int(c.get("idle", 0))
            if pending == 0 and idle > idle_threshold_ms:
                try:
                    self._client.xgroup_delconsumer(
                        self._stream_key, self._stream_group, c["name"]
                    )
                    logger.info("dead_consumer_removed name=%s idle_ms=%d",
                                c["name"], idle)
                    cleaned += 1
                except Exception as exc:
                    logger.warning("consumer_delete_failed name=%s error=%s",
                                   c["name"], exc)
        if cleaned:
            logger.info("dead_consumers_cleaned count=%d", cleaned)
        return cleaned

    def check_health(self) -> bool:
        try:
            self._client.ping()
            return True
        except Exception:
            return False
