from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest, start_http_server


HTTP_REQUESTS_TOTAL = Counter(
    "nas_http_requests_total",
    "Total HTTP requests handled by the NAS API.",
    ["method", "route", "status_code"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "nas_http_request_duration_seconds",
    "HTTP request latency for the NAS API.",
    ["method", "route"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)
INGEST_JOB_STATUS_TRANSITIONS_TOTAL = Counter(
    "nas_ingest_job_status_transitions_total",
    "Job status transitions emitted by the NAS runtime.",
    ["job_type", "status"],
)
INGEST_JOB_TERMINAL_DURATION_SECONDS = Histogram(
    "nas_ingest_job_terminal_duration_seconds",
    "Duration from job start to terminal state.",
    ["job_type", "status"],
    buckets=(1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0),
)
UPLOAD_BYTES_TOTAL = Counter(
    "nas_upload_bytes_total",
    "Bytes uploaded into NAS-managed object storage flows.",
    ["upload_type"],
)
QUEUE_PUBLISH_TOTAL = Counter(
    "nas_queue_publish_total",
    "Queue publish calls by backend.",
    ["backend"],
)
QUEUE_CLAIM_TOTAL = Counter(
    "nas_queue_claim_total",
    "Queue claims successfully acquired by a worker.",
    ["backend"],
)
WORKER_STARTED_TOTAL = Counter(
    "nas_worker_started_total",
    "Worker start events by queue backend.",
    ["backend"],
)
WORKER_LOOP_ERRORS_TOTAL = Counter(
    "nas_worker_loop_errors_total",
    "Unhandled worker loop errors by queue backend.",
    ["backend"],
)

_worker_metrics_lock = threading.Lock()
_worker_metrics_started = False


def metrics_payload() -> bytes:
    return generate_latest()


def record_http_request(method: str, route: str, status_code: int, duration_seconds: float) -> None:
    normalized_route = route or "unknown"
    status_label = str(int(status_code))
    HTTP_REQUESTS_TOTAL.labels(method=method.upper(), route=normalized_route, status_code=status_label).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method=method.upper(), route=normalized_route).observe(
        max(0.0, float(duration_seconds))
    )


def record_job_transition(previous_state: dict[str, Any] | None, current_state: dict[str, Any]) -> None:
    prev_status = str((previous_state or {}).get("status") or "").strip().lower()
    new_status = str(current_state.get("status") or "").strip().lower()
    if not new_status or prev_status == new_status:
        return

    job_type = str(current_state.get("job_type") or "bulk_ingest").strip().lower() or "bulk_ingest"
    INGEST_JOB_STATUS_TRANSITIONS_TOTAL.labels(job_type=job_type, status=new_status).inc()

    if new_status not in {"completed", "failed", "interrupted"}:
        return
    started_at = _parse_iso_timestamp(current_state.get("started_at"))
    ended_at = _parse_iso_timestamp(current_state.get("ended_at"))
    if started_at is None or ended_at is None:
        return
    duration_seconds = (ended_at - started_at).total_seconds()
    if duration_seconds < 0:
        return
    INGEST_JOB_TERMINAL_DURATION_SECONDS.labels(job_type=job_type, status=new_status).observe(duration_seconds)


def record_upload_bytes(upload_type: str, content_bytes: int) -> None:
    if content_bytes <= 0:
        return
    normalized_type = upload_type.strip().lower() or "unknown"
    UPLOAD_BYTES_TOTAL.labels(upload_type=normalized_type).inc(float(content_bytes))


def record_queue_publish(backend: str) -> None:
    QUEUE_PUBLISH_TOTAL.labels(backend=backend.strip().lower() or "unknown").inc()


def record_queue_claim(backend: str) -> None:
    QUEUE_CLAIM_TOTAL.labels(backend=backend.strip().lower() or "unknown").inc()


def record_worker_started(backend: str) -> None:
    WORKER_STARTED_TOTAL.labels(backend=backend.strip().lower() or "unknown").inc()


def record_worker_loop_error(backend: str) -> None:
    WORKER_LOOP_ERRORS_TOTAL.labels(backend=backend.strip().lower() or "unknown").inc()


def ensure_worker_metrics_server(port: int) -> bool:
    global _worker_metrics_started
    if port <= 0:
        return False
    with _worker_metrics_lock:
        if _worker_metrics_started:
            return False
        start_http_server(port)
        _worker_metrics_started = True
        return True


def _parse_iso_timestamp(raw_value: Any) -> datetime | None:
    if not raw_value:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
