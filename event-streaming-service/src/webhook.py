"""Webhook deliverer — processes pending deliveries with retry and HMAC signing."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time

import requests
import sqlalchemy as sa
from sqlalchemy import text

logger = logging.getLogger(__name__)

BACKOFF_SECONDS = [60, 300, 900, 3600, 14400]


class WebhookDeliverer:
    def __init__(self, settings, engine: sa.Engine) -> None:
        self._settings = settings
        self._engine = engine

    def run_forever(self) -> None:
        while True:
            try:
                self._process_pending()
                time.sleep(5)
            except Exception:
                logger.exception("deliverer_loop_error")
                time.sleep(5)

    def _process_pending(self) -> None:
        schema = self._settings.postgres_schema
        with self._engine.begin() as conn:
            rows = conn.execute(text(f"""
                SELECT d.delivery_id, d.subscription_id,
                       d.event_id, d.attempt_count,
                       s.url, s.secret,
                       e.event_type, e.payload,
                       e.published_at, e.event_source
                FROM "{schema}".webhook_delivery d
                JOIN "{schema}".webhook_subscription s
                  ON s.subscription_id = d.subscription_id
                JOIN "{schema}".event_log e
                  ON e.event_id = d.event_id
                WHERE d.status IN ('pending', 'retry')
                  AND d.next_retry_at <= NOW()
                  AND s.is_active = true
                LIMIT 50
                FOR UPDATE OF d SKIP LOCKED
            """)).fetchall()

            for row in rows:
                self._attempt_delivery(conn, row)

    def _attempt_delivery(self, conn, row) -> None:
        payload_dict = {
            "event_id":     row.event_id,
            "event_type":   row.event_type,
            "event_source": row.event_source,
            "published_at": str(row.published_at),
            "payload":      row.payload if isinstance(row.payload, dict) else {},
        }
        body = json.dumps(payload_dict)
        signature = hmac.new(
            (row.secret or "").encode("utf-8"),
            body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        success = False
        resp = None
        error_msg = None

        try:
            resp = requests.post(
                row.url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "X-NAS-Event-Type":    row.event_type,
                    "X-NAS-Event-ID":      row.event_id,
                    "X-NAS-Delivery-ID":   row.delivery_id,
                    "X-NAS-Signature":     f"sha256={signature}",
                },
                timeout=self._settings.webhook_timeout_seconds,
            )
            success = 200 <= resp.status_code < 300
            if not success:
                error_msg = f"HTTP {resp.status_code}"
        except Exception as exc:
            error_msg = str(exc)

        attempt = row.attempt_count + 1
        schema = self._settings.postgres_schema

        if success:
            conn.execute(text(f"""
                UPDATE "{schema}".webhook_delivery
                SET status='delivered', attempt_count=:attempt,
                    last_attempt_at=NOW(), response_status=:status,
                    response_body=:body
                WHERE delivery_id=:id
            """), {
                "attempt": attempt,
                "status":  resp.status_code,
                "body":    resp.text[:500],
                "id":      row.delivery_id,
            })
            conn.execute(text(f"""
                UPDATE "{schema}".webhook_subscription
                SET last_delivery_at=NOW(), failure_count=0
                WHERE subscription_id=:id
            """), {"id": row.subscription_id})
            logger.info("webhook_delivered delivery_id=%s url=%s", row.delivery_id, row.url)

        else:
            if attempt >= self._settings.webhook_max_attempts:
                new_status = "failed"
                retry_sql = "NULL"
            else:
                new_status = "retry"
                backoff = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
                retry_sql = f"NOW() + interval '{backoff} seconds'"

            conn.execute(text(f"""
                UPDATE "{schema}".webhook_delivery
                SET status=:status, attempt_count=:attempt,
                    last_attempt_at=NOW(),
                    response_status=:resp_status,
                    error_message=:err,
                    next_retry_at={retry_sql}
                WHERE delivery_id=:id
            """), {
                "status":      new_status,
                "attempt":     attempt,
                "resp_status": resp.status_code if resp else None,
                "err":         error_msg,
                "id":          row.delivery_id,
            })

            if attempt >= self._settings.webhook_max_attempts:
                conn.execute(text(f"""
                    UPDATE "{schema}".webhook_subscription
                    SET failure_count = failure_count + 1,
                        is_active = CASE
                          WHEN failure_count + 1 >= 10 THEN false
                          ELSE is_active END
                    WHERE subscription_id=:id
                """), {"id": row.subscription_id})

            logger.warning("webhook_delivery_failed delivery_id=%s attempt=%d/%d error=%s",
                           row.delivery_id, attempt, self._settings.webhook_max_attempts, error_msg)
