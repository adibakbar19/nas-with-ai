"""Queue operations — stats, dead letter, recovery."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from config import settings
from src.schemas.job import QueueStatsResponse

router = APIRouter(prefix="/queue", tags=["queue"])


def _require_service_key(x_service_key: str = Header(default="")):
    keys = settings.get_service_keys()
    if keys and x_service_key not in keys:
        raise HTTPException(status_code=401, detail="Invalid service key")


def _qs(request: Request):
    return request.app.state.queue_service


def _vc(request: Request):
    return request.app.state.valkey_client


@router.get("/stats", response_model=QueueStatsResponse,
            dependencies=[Depends(_require_service_key)])
async def get_stats(request: Request):
    qs = _qs(request)
    vc = _vc(request)
    db_stats = qs._repo.get_stats()
    stream_info = vc.get_stream_info()
    group_info = vc.get_group_info()
    pending = sum(g.get("pending", 0) for g in group_info)
    return QueueStatsResponse(
        queued=db_stats["queued"],
        pending=db_stats["pending"],
        running=db_stats["running"],
        completed=db_stats["completed"],
        failed=db_stats["failed"],
        retry=db_stats["retry"],
        dead_letter=db_stats["dead_letter"],
        stream_length=stream_info.get("length", 0),
        pending_messages=pending,
        oldest_queued_age_seconds=db_stats.get("oldest_queued_age_seconds"),
        avg_processing_seconds=db_stats.get("avg_processing_seconds"),
    )


@router.get("/dead-letter", dependencies=[Depends(_require_service_key)])
async def list_dead_letter(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    offset: int = 0,
):
    qs = _qs(request)
    items, total = qs._dlq.list_dlq(limit=limit, offset=offset)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.post("/dead-letter/{dlq_id}/requeue",
             dependencies=[Depends(_require_service_key)])
async def requeue_dlq(dlq_id: str, request: Request):
    qs = _qs(request)
    job = qs.requeue_dlq_job(dlq_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id!r} not found")
    return {"requeued_job_id": job.get("job_id"), "status": job.get("status")}


@router.post("/recover-stale", dependencies=[Depends(_require_service_key)])
async def recover_stale(request: Request):
    qs = _qs(request)
    count = qs.recover_stale_jobs()
    return {"recovered": count}


@router.post("/cleanup-consumers", dependencies=[Depends(_require_service_key)])
async def cleanup_consumers(request: Request):
    vc = _vc(request)
    cleaned = vc.cleanup_dead_consumers()
    return {"cleaned": cleaned}
