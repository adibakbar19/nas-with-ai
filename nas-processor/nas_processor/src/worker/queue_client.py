"""HTTP client for queue-service — used by the worker to report job state.

Replaces direct DB writes via IngestJobStateRepository.set_job() with HTTP
calls to queue-service. All methods are fire-and-forget: they log a warning
on failure and return — they NEVER raise. A failed progress update must not
stop ETL processing.
"""
from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SHORT = 5.0
_DEFAULT_TIMEOUT_LONG = 10.0


def _build_url() -> str:
    return os.environ.get("QUEUE_SERVICE_URL", "http://queue-service:8005").rstrip("/")


def _build_key() -> str:
    return os.environ.get("QUEUE_SERVICE_KEY", "internal-dev-key")


class QueueClient:
    """HTTP client for queue-service.

    Workers use this to report job lifecycle without needing DB access.
    """

    def __init__(
        self,
        base_url: str | None = None,
        service_key: str | None = None,
    ) -> None:
        self._base = (base_url or _build_url()).rstrip("/")
        self._headers = {
            "X-Service-Key": service_key or _build_key(),
            "Content-Type": "application/json",
        }

    def get_job(self, job_id: str) -> dict | None:
        try:
            resp = httpx.get(
                f"{self._base}/jobs/{job_id}",
                headers=self._headers,
                timeout=_DEFAULT_TIMEOUT_SHORT,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            job = resp.json()
            # Flatten data blob for callers that expect top-level fields
            merged = dict(job.get("data") or {})
            merged.update({k: v for k, v in job.items() if k != "data" and v is not None})
            return merged
        except Exception as exc:
            logger.error("queue_get_job_failed job_id=%s error=%s", job_id, exc)
            return None

    def update_progress(self, job_id: str, worker_id: str, **fields) -> None:
        """Update job progress. Never raises — logs warning on failure."""
        try:
            httpx.patch(
                f"{self._base}/jobs/{job_id}/progress",
                json={"worker_id": worker_id, **fields},
                headers=self._headers,
                timeout=_DEFAULT_TIMEOUT_SHORT,
            )
        except Exception as exc:
            logger.warning("queue_update_progress_failed job_id=%s error=%s", job_id, exc)

    def heartbeat(self, job_id: str, worker_id: str) -> None:
        """Send heartbeat to keep job alive. Never raises."""
        try:
            httpx.post(
                f"{self._base}/jobs/{job_id}/heartbeat",
                json={"worker_id": worker_id},
                headers=self._headers,
                timeout=3.0,
            )
        except Exception as exc:
            logger.warning("queue_heartbeat_failed job_id=%s error=%s", job_id, exc)

    def complete_job(self, job_id: str, worker_id: str, result_data: dict) -> None:
        """Mark job completed. Never raises."""
        try:
            httpx.post(
                f"{self._base}/jobs/{job_id}/complete",
                json={"worker_id": worker_id, "result_data": result_data},
                headers=self._headers,
                timeout=_DEFAULT_TIMEOUT_LONG,
            )
        except Exception as exc:
            logger.error("queue_complete_job_failed job_id=%s error=%s", job_id, exc)

    def fail_job(self, job_id: str, worker_id: str, error: str) -> None:
        """Mark job failed. Never raises."""
        try:
            httpx.post(
                f"{self._base}/jobs/{job_id}/fail",
                json={"worker_id": worker_id, "error": error},
                headers=self._headers,
                timeout=_DEFAULT_TIMEOUT_LONG,
            )
        except Exception as exc:
            logger.error("queue_fail_job_failed job_id=%s error=%s", job_id, exc)
