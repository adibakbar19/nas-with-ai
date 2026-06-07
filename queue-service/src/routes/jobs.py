"""Job lifecycle routes — create, read, progress, complete, fail, heartbeat."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from config import settings
from src.schemas.job import (
    CompleteJobRequest,
    EnqueueJobRequest,
    FailJobRequest,
    HeartbeatRequest,
    JobListResponse,
    JobResponse,
    UpdateProgressRequest,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _require_service_key(x_service_key: str = Header(default="")):
    keys = settings.get_service_keys()
    if keys and x_service_key not in keys:
        raise HTTPException(status_code=401, detail="Invalid service key")


def _qs(request: Request):
    return request.app.state.queue_service


def _job_to_response(job: dict[str, Any]) -> JobResponse:
    return JobResponse(
        job_id=job.get("job_id", ""),
        status=job.get("status", "unknown"),
        job_type=job.get("job_type"),
        source_system=job.get("source_system"),
        agency_id=job.get("agency_id"),
        priority=job.get("priority"),
        retry_count=job.get("retry_count"),
        max_retries=job.get("max_retries"),
        claimed_by=job.get("claimed_by"),
        worker_id=job.get("worker_id"),
        created_at=str(job["created_at"]) if job.get("created_at") else None,
        updated_at=str(job["updated_at"]) if job.get("updated_at") else None,
        completed_at=str(job["completed_at"]) if job.get("completed_at") else None,
        data={k: v for k, v in job.items()
              if k not in {"job_id", "status", "job_type", "source_system",
                           "agency_id", "priority", "retry_count", "max_retries",
                           "claimed_by", "worker_id", "created_at", "updated_at",
                           "completed_at", "heartbeat_at", "claimed_at"}},
    )


@router.post("", response_model=JobResponse, status_code=201,
             dependencies=[Depends(_require_service_key)])
async def enqueue_job(body: EnqueueJobRequest, request: Request):
    qs = _qs(request)
    existing = qs._repo.get_job(body.job_id)
    # Skip only if the job is actively being processed or already done.
    # "queued", "pending", "uploaded" all need the stream push to happen.
    # ingestion-api sets status="queued" in DB before calling us, so we
    # must NOT skip on "queued" — we still need to push to the stream.
    if existing and existing.get("status") in ("claimed", "running", "completed"):
        return _job_to_response(existing)
    job = qs.enqueue(
        job_id=body.job_id,
        job_type=body.job_type,
        source_system=body.source_system,
        data=body.data,
        priority=body.priority,
    )
    return _job_to_response(job)


@router.get("", response_model=JobListResponse,
            dependencies=[Depends(_require_service_key)])
async def list_jobs(
    request: Request,
    status: str | None = None,
    source_system: str | None = None,
    job_type: str | None = None,
    agency_id: str | None = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = 0,
):
    qs = _qs(request)
    jobs, total = qs._repo.list_jobs(
        status=status, source_system=source_system,
        job_type=job_type, agency_id=agency_id,
        limit=limit, offset=offset,
    )
    return JobListResponse(
        items=[_job_to_response(j) for j in jobs],
        total=total, limit=limit, offset=offset,
    )


@router.get("/{job_id}", response_model=JobResponse,
            dependencies=[Depends(_require_service_key)])
async def get_job(job_id: str, request: Request):
    qs = _qs(request)
    job = qs._repo.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return _job_to_response(job)


@router.patch("/{job_id}/progress", response_model=JobResponse,
              dependencies=[Depends(_require_service_key)])
async def update_progress(job_id: str, body: UpdateProgressRequest, request: Request):
    qs = _qs(request)
    extra = body.model_extra or {}
    job = qs.update_progress(
        job_id=job_id,
        worker_id=body.worker_id,
        **extra,
    )
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return _job_to_response(job)


@router.post("/{job_id}/complete", response_model=JobResponse,
             dependencies=[Depends(_require_service_key)])
async def complete_job(job_id: str, body: CompleteJobRequest, request: Request):
    qs = _qs(request)
    job = qs.handle_completion(
        job_id=job_id,
        worker_id=body.worker_id,
        result_data=body.result_data,
    )
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return _job_to_response(job)


@router.post("/{job_id}/fail", response_model=JobResponse,
             dependencies=[Depends(_require_service_key)])
async def fail_job(job_id: str, body: FailJobRequest, request: Request):
    qs = _qs(request)
    job = qs.handle_failure(
        job_id=job_id,
        worker_id=body.worker_id,
        error=body.error,
    )
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return _job_to_response(job)


@router.post("/{job_id}/heartbeat", status_code=200,
             dependencies=[Depends(_require_service_key)])
async def heartbeat(job_id: str, body: HeartbeatRequest, request: Request):
    qs = _qs(request)
    ok = qs._repo.heartbeat(job_id, body.worker_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found or wrong worker")
    return {"ok": True}
