"""HTTP client for queue-service.

Falls back silently to None on any error — callers must handle None
to use the legacy direct-Valkey path. This ensures ingestion-api keeps
working if queue-service is temporarily unavailable.
"""
from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


class QueueServiceClient:
    def __init__(self, base_url: str, service_key: str) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {
            "X-Service-Key": service_key,
            "Content-Type": "application/json",
        }

    def enqueue_job(
        self,
        job_id: str,
        job_type: str,
        data: dict,
        priority: int = 5,
    ) -> dict | None:
        """Enqueue a job via queue-service. Returns job dict or None on failure."""
        try:
            resp = httpx.post(
                f"{self._base}/jobs",
                json={
                    "job_id":        job_id,
                    "job_type":      job_type,
                    "source_system": "ingestion-api",
                    "data":          data,
                    "priority":      priority,
                },
                headers=self._headers,
                timeout=10.0,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("queue_service_enqueue_failed job_id=%s error=%s", job_id, exc)
            return None

    def get_job(self, job_id: str, agency_id: str | None = None) -> dict | None:
        """Get job state from queue-service. Returns None if not found or on error."""
        try:
            resp = httpx.get(
                f"{self._base}/jobs/{job_id}",
                headers=self._headers,
                timeout=5.0,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            job = resp.json()
            # Re-attach agency_id filtering: if caller filters by agency and job
            # belongs to a different agency, treat as not found
            if agency_id and job.get("agency_id") and job["agency_id"] != agency_id:
                return None
            return job
        except Exception as exc:
            logger.warning("queue_service_get_failed job_id=%s error=%s", job_id, exc)
            return None

    def list_jobs(
        self,
        agency_id: str | None = None,
        limit: int = 20,
    ) -> list[dict] | None:
        """List jobs filtered by agency. Returns None on error."""
        try:
            params: dict = {"limit": limit}
            if agency_id:
                params["agency_id"] = agency_id
            resp = httpx.get(
                f"{self._base}/jobs",
                params=params,
                headers=self._headers,
                timeout=5.0,
            )
            resp.raise_for_status()
            return resp.json().get("items", [])
        except Exception as exc:
            logger.warning("queue_service_list_failed error=%s", exc)
            return None
