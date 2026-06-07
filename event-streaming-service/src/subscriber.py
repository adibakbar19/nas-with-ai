"""Kafka consumer — reads NAS events and queues webhook deliveries."""
from __future__ import annotations

import json
import logging
import uuid

import sqlalchemy as sa
from sqlalchemy import text

logger = logging.getLogger(__name__)


class EventSubscriber:
    def __init__(self, settings, engine: sa.Engine) -> None:
        self._settings = settings
        self._engine = engine

    def run_forever(self) -> None:
        try:
            from confluent_kafka import Consumer, KafkaError
        except ImportError:
            logger.error("confluent-kafka not installed — subscriber disabled")
            return

        consumer = Consumer({
            "bootstrap.servers": self._settings.kafka_brokers,
            "group.id": self._settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(self._settings.kafka_topics)
        logger.info("event_subscriber_started topics=%s", self._settings.kafka_topics)

        while True:
            try:
                from confluent_kafka import KafkaError
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("kafka_error %s", msg.error())
                    continue

                self._process_message(msg)
                consumer.commit(message=msg, asynchronous=False)

            except Exception:
                logger.exception("subscriber_loop_error")

    def _process_message(self, msg) -> None:
        try:
            event = json.loads(msg.value().decode("utf-8"))
            event_type = event.get("event_type")
            event_id = event.get("event_id")
            schema = self._settings.postgres_schema

            if not event_id:
                logger.warning("message_missing_event_id topic=%s", msg.topic())
                return

            # Ensure event is in event_log (it may already be there from the publisher)
            with self._engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO "{schema}".event_log
                      (event_id, event_type, event_source, entity_id,
                       entity_type, payload, kafka_topic, published_at, schema_version)
                    VALUES
                      (:event_id, :event_type, :event_source, :entity_id,
                       :entity_type, CAST(:payload AS jsonb), :kafka_topic,
                       :published_at, :schema_version)
                    ON CONFLICT (event_id) DO NOTHING
                """), {
                    "event_id":       event_id,
                    "event_type":     event_type or "",
                    "event_source":   event.get("event_source") or "kafka",
                    "entity_id":      event.get("entity_id"),
                    "entity_type":    event.get("entity_type"),
                    "payload":        json.dumps(event.get("payload", {})),
                    "kafka_topic":    msg.topic(),
                    "published_at":   event.get("published_at"),
                    "schema_version": event.get("schema_version", "1.0"),
                })

            # Find active subscriptions matching this event_type
            with self._engine.connect() as conn:
                subs = conn.execute(text(f"""
                    SELECT subscription_id, url, secret
                    FROM "{schema}".webhook_subscription
                    WHERE is_active = true
                      AND event_types @> ARRAY[:event_type]::text[]
                """), {"event_type": event_type}).fetchall()

            if not subs:
                logger.debug("event_no_subscribers type=%s", event_type)
                return

            with self._engine.begin() as conn:
                for sub in subs:
                    delivery_id = uuid.uuid4().hex
                    conn.execute(text(f"""
                        INSERT INTO "{schema}".webhook_delivery
                          (delivery_id, subscription_id, event_id,
                           status, next_retry_at)
                        VALUES
                          (:delivery_id, :sub_id, :event_id,
                           'pending', NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "delivery_id": delivery_id,
                        "sub_id":      sub.subscription_id,
                        "event_id":    event_id,
                    })

            logger.info("event_processed type=%s subs=%d event_id=%s",
                        event_type, len(subs), event_id)

        except Exception:
            logger.exception("message_process_error topic=%s", msg.topic())
