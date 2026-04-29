from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.dependencies import get_address_match_review_service
from backend.app.schemas.address_match_review import (
    AddressMatchReviewDecisionRequest,
    AddressMatchReviewListResponse,
    AddressMatchReviewResponse,
)
from backend.app.security import Admin, get_current_admin
from backend.app.services.address_match_review_service import AddressMatchReviewService
from backend.app.services.errors import ServiceError


router = APIRouter(tags=["admin-address-match-review"])


@router.get(
    "/api/v1/admin/address-match-reviews",
    response_model=AddressMatchReviewListResponse,
    response_model_exclude_none=True,
)
def list_address_match_reviews(
    status: str | None = Query(default=None, description="Filter by open, approved_merge, rejected_new_address"),
    limit: int = Query(default=50, ge=1, le=200),
    _admin: Admin = Depends(get_current_admin),
    service: AddressMatchReviewService = Depends(get_address_match_review_service),
) -> dict[str, Any]:
    try:
        return service.list_reviews(status=status, limit=limit)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get(
    "/api/v1/admin/address-match-reviews/{review_id}",
    response_model=AddressMatchReviewResponse,
    response_model_exclude_none=True,
)
def get_address_match_review(
    review_id: int,
    _admin: Admin = Depends(get_current_admin),
    service: AddressMatchReviewService = Depends(get_address_match_review_service),
) -> dict[str, Any]:
    try:
        return service.get_review(review_id=review_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/admin/address-match-reviews/{review_id}/approve-merge",
    response_model=AddressMatchReviewResponse,
    response_model_exclude_none=True,
)
def approve_address_match_review(
    review_id: int,
    request: AddressMatchReviewDecisionRequest,
    admin: Admin = Depends(get_current_admin),
    service: AddressMatchReviewService = Depends(get_address_match_review_service),
) -> dict[str, Any]:
    try:
        return service.approve_merge(review_id=review_id, reviewed_by=admin.actor, review_note=request.review_note)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/admin/address-match-reviews/{review_id}/reject",
    response_model=AddressMatchReviewResponse,
    response_model_exclude_none=True,
)
def reject_address_match_review(
    review_id: int,
    request: AddressMatchReviewDecisionRequest,
    admin: Admin = Depends(get_current_admin),
    service: AddressMatchReviewService = Depends(get_address_match_review_service),
) -> dict[str, Any]:
    try:
        return service.reject_review(review_id=review_id, reviewed_by=admin.actor, review_note=request.review_note)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
