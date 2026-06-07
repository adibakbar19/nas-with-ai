"""Request/response schemas for queue-service."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EnqueueJobRequest(BaseModel):
    job_id: str
    job_type: str = "bulk_ingest"
    source_system: str = "ingestion-api"
    data: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10)


class UpdateProgressRequest(BaseModel):
    worker_id: str
    # Any extra fields are merged into the job state
    model_config = {"extra": "allow"}


class CompleteJobRequest(BaseModel):
    worker_id: str
    result_data: dict[str, Any] = Field(default_factory=dict)


class FailJobRequest(BaseModel):
    worker_id: str
    error: str


class HeartbeatRequest(BaseModel):
    worker_id: str


class JobResponse(BaseModel):
    job_id: str
    status: str
    job_type: str | None = None
    source_system: str | None = None
    agency_id: str | None = None
    priority: int | None = None
    retry_count: int | None = None
    max_retries: int | None = None
    claimed_by: str | None = None
    worker_id: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "allow"}


class JobListResponse(BaseModel):
    items: list[JobResponse]
    total: int
    limit: int
    offset: int


class QueueStatsResponse(BaseModel):
    queued: int
    pending: int
    running: int
    completed: int
    failed: int
    retry: int
    dead_letter: int
    stream_length: int
    pending_messages: int
    oldest_queued_age_seconds: float | None = None
    avg_processing_seconds: float | None = None
