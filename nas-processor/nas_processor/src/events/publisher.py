"""NAS event publisher — Kafka (Redpanda) + Postgres event_log.

Never raises. A failed publish logs a warning and returns None so the
ETL pipeline is never blocked by event infrastructure.

Usage:
    from nas_processor.src.events.publisher import publish as publish_event

    publish_event("address.created", entity_id=record_id, entity_type="address",
                  payload={"record_id": record_id, ...})
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

TOPIC_MAP: dict[str, str] = {
    "address.created":           "nas.address.events",
    "address.updated":           "nas.address.events",
    "address.lifecycle_changed": "nas.address.events",
    "address.naskod_assigned":   "nas.address.events",
    "job.completed":             "nas.job.events",
    "job.failed":                "nas.job.events",
    "job.review_required":       "nas.job.events",
    "match.auto_matched":        "nas.match.events",
    "match.needs_review":        "nas.match.events",
    "match.review_resolved":     "nas.match.events",
}


class EventPublisher:
    """Publishes events to Kafka and logs them to Postgres.

    Constructed once at startup. All methods are fire-and-forget — they
    never raise; failures are logged as warnings.
    """

    def __init__(
        self,
        kafka_brokers: str,
        dsn: str,
        schema: str = "nas",
        enabled: bool = True,
    ) -> None:
        self._enabled = enabled
        self._schema = schema
        self._engine = None
        self._producer = None

        if not enabled:
            logger.info("event_publisher_disabled NAS_EVENTS_ENABLED=false")
            return

        try:
            from confluent_kafka import Producer
            self._producer = Producer({
                "bootstrap.servers": kafka_brokers,
                "client.id": "nas-event-publisher",
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 500,
            })
        except Exception as exc:
            logger.warning(
                "event_publisher_kafka_init_failed error=%s events_will_not_be_published=true", exc
            )
            self._enabled = False
            return

        try:
            import sqlalchemy as sa
            self._engine = sa.create_engine(dsn, pool_pre_ping=True)
        except Exception as exc:
            logger.warning("event_publisher_db_init_failed error=%s db_logging_disabled=true", exc)

        logger.info("event_publisher_ready brokers=%s", kafka_brokers)

    def publish(
        self,
        event_type: str,
        entity_id: str | None = None,
        entity_type: str | None = None,
        payload: dict[str, Any] | None = None,
        source: str = "nas-processor",
    ) -> str | None:
        """Publish an event. Never raises — logs and returns None on any error."""
        if not self._enabled:
            return None

        topic = TOPIC_MAP.get(event_type)
        if not topic:
            logger.warning("unknown_event_type type=%s", event_type)
            return None

        event_id = uuid.uuid4().hex
        event: dict[str, Any] = {
            "event_id": event_id,
            "event_type": event_type,
            "event_source": source,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "payload": payload or {},
            "published_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0",
        }

        try:
            self._producer.produce(
                topic=topic,
                key=(entity_id or event_id).encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except Exception as exc:
            logger.warning("event_kafka_produce_failed type=%s error=%s", event_type, exc)
            return None

        self._log_to_db(event, topic)
        return event_id

    def _on_delivery(self, err, msg) -> None:
        if err:
            logger.warning("kafka_delivery_failed topic=%s error=%s", msg.topic(), err)
        else:
            logger.debug("kafka_delivery_ok topic=%s offset=%s", msg.topic(), msg.offset())

    def _log_to_db(self, event: dict[str, Any], topic: str) -> None:
        if self._engine is None:
            return
        try:
            import sqlalchemy as sa
            with self._engine.connect() as conn:
                conn.execute(sa.text(f"""
                    INSERT INTO "{self._schema}".event_log
                      (event_id, event_type, event_source, entity_id,
                       entity_type, payload, kafka_topic, published_at, schema_version)
                    VALUES
                      (:event_id, :event_type, :event_source, :entity_id,
                       :entity_type, CAST(:payload AS jsonb), :kafka_topic,
                       :published_at, :schema_version)
                    ON CONFLICT (event_id) DO NOTHING
                """), {
                    "event_id":       event["event_id"],
                    "event_type":     event["event_type"],
                    "event_source":   event["event_source"],
                    "entity_id":      event.get("entity_id"),
                    "entity_type":    event.get("entity_type"),
                    "payload":        json.dumps(event.get("payload", {})),
                    "kafka_topic":    topic,
                    "published_at":   event["published_at"],
                    "schema_version": event.get("schema_version", "1.0"),
                })
                conn.commit()
        except Exception as exc:
            logger.warning("event_log_db_failed event_id=%s error=%s", event["event_id"], exc)

    def flush(self, timeout: float = 5.0) -> None:
        """Flush pending Kafka messages. Call before shutdown."""
        if self._producer:
            try:
                self._producer.flush(timeout)
            except Exception as exc:
                logger.warning("event_publisher_flush_failed error=%s", exc)


# ── Global singleton ──────────────────────────────────────────────────────────

_publisher: EventPublisher | None = None


def get_publisher() -> EventPublisher:
    global _publisher
    if _publisher is None:
        _publisher = EventPublisher(
            kafka_brokers=os.environ.get("KAFKA_BROKERS", "redpanda:9092"),
            dsn=os.environ.get("POSTGRES_DSN") or _build_dsn(),
            schema=os.environ.get("PGSCHEMA", "nas").strip() or "nas",
            enabled=os.environ.get("NAS_EVENTS_ENABLED", "true").lower() == "true",
        )
    return _publisher


def publish(event_type: str, **kwargs) -> str | None:
    """Module-level convenience function. Never raises."""
    try:
        return get_publisher().publish(event_type, **kwargs)
    except Exception as exc:
        logger.warning("publish_failed event_type=%s error=%s", event_type, exc)
        return None


def _build_dsn() -> str:
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    pwd  = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    db   = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"
