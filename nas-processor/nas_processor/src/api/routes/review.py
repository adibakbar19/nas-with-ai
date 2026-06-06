"""Match review queue routes — public endpoints, no auth required."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from nas_processor.src.api.errors import ServiceError
from nas_processor.src.api.repositories.review_repository import (
    ReviewError,
    assign_review,
    fetch_pending_reviews,
    get_review_by_id,
    get_review_stats,
    resolve_review,
)
from nas_processor.src.api.schemas.review import (
    AssignReviewRequest,
    ResolveReviewRequest,
    ResolveReviewResponse,
    ReviewListResponse,
    ReviewQueueItem,
)

router = APIRouter(tags=["review"])


def _dsn(request: Request) -> str:
    return request.app.state.review_dsn


def _schema(request: Request) -> str:
    return request.app.state.review_schema


def _review_error_response(exc: ReviewError) -> JSONResponse:
    code_to_status = {
        "not_found": 404,
        "conflict": 409,
        "already_resolved": 409,
        "invalid_decision": 400,
        "invalid_candidate": 400,
    }
    status_code = code_to_status.get(exc.code, 400)
    return JSONResponse(status_code=status_code, content={"detail": exc.message})


# ── Routes ─────────────────────────────────────────────────────────────────────
# NOTE: /review/stats must be registered BEFORE /review/{review_id}
# to prevent FastAPI capturing "stats" as a path parameter.


@router.get("/review/stats")
def review_stats(request: Request) -> dict[str, Any]:
    try:
        return get_review_stats(dsn=_dsn(request), schema=_schema(request))
    except Exception as exc:
        raise ServiceError(status_code=503, detail=f"stats query failed: {exc}") from exc


@router.get("/review", response_model=ReviewListResponse)
def list_reviews(
    request: Request,
    status: str | None = Query(default=None, description="pending / assigned / resolved"),
    assigned_to: str | None = Query(default=None),
    agency_id: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    try:
        rows, total = fetch_pending_reviews(
            assigned_to=assigned_to,
            status=status,
            agency_id=agency_id,
            limit=limit,
            offset=offset,
            dsn=_dsn(request),
            schema=_schema(request),
        )
    except Exception as exc:
        raise ServiceError(status_code=503, detail=f"review list query failed: {exc}") from exc
    return {"results": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/review/{review_id}", response_model=ReviewQueueItem)
def get_review(review_id: str, request: Request) -> dict[str, Any]:
    try:
        row = get_review_by_id(review_id, dsn=_dsn(request), schema=_schema(request))
    except Exception as exc:
        raise ServiceError(status_code=503, detail=f"review query failed: {exc}") from exc
    if row is None:
        raise ServiceError(status_code=404, detail=f"review {review_id!r} not found")
    return row


@router.post("/review/{review_id}/assign")
def assign_review_route(
    review_id: str,
    body: AssignReviewRequest,
    request: Request,
) -> Any:
    try:
        updated = assign_review(
            review_id,
            assigned_to=body.assigned_to,
            dsn=_dsn(request),
            schema=_schema(request),
        )
    except ReviewError as exc:
        return _review_error_response(exc)
    except Exception as exc:
        raise ServiceError(status_code=503, detail=f"assign failed: {exc}") from exc
    return updated


@router.post("/review/{review_id}/resolve", response_model=ResolveReviewResponse)
def resolve_review_route(
    review_id: str,
    body: ResolveReviewRequest,
    request: Request,
) -> Any:
    try:
        result = resolve_review(
            review_id,
            decision=body.decision,
            resolved_by="api",
            chosen_candidate_id=body.chosen_candidate_id,
            resolution_notes=body.resolution_notes,
            dsn=_dsn(request),
            schema=_schema(request),
        )
    except ReviewError as exc:
        return _review_error_response(exc)
    except Exception as exc:
        raise ServiceError(status_code=503, detail=f"resolve failed: {exc}") from exc
    return result
