from typing import Any

from backend.app.repositories.address_match_review_repository import AddressMatchReviewRepository
from backend.app.services.errors import ServiceError


class AddressMatchReviewService:
    def __init__(self, *, repository: AddressMatchReviewRepository) -> None:
        self._repository = repository

    def list_reviews(self, *, status: str | None, limit: int) -> dict[str, Any]:
        status_norm = (status or "").strip().lower() or None
        if status_norm not in {None, "open", "approved_merge", "rejected_new_address"}:
            raise ServiceError(status_code=400, detail="invalid review status")
        try:
            rows = self._repository.list_reviews(status=status_norm, limit=limit)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"review query failed: {exc}") from exc
        return {"status": status_norm, "count": len(rows), "items": rows}

    def get_review(self, *, review_id: int) -> dict[str, Any]:
        try:
            row = self._repository.get_review(review_id=review_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"review query failed: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="review_id not found")
        return row

    def approve_merge(self, *, review_id: int, reviewed_by: str, review_note: str | None) -> dict[str, Any]:
        note = (review_note or "").strip() or None
        try:
            row = self._repository.approve_merge(review_id=review_id, reviewed_by=reviewed_by, review_note=note)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"failed to approve review: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="review_id not found")
        return row

    def reject_review(self, *, review_id: int, reviewed_by: str, review_note: str | None) -> dict[str, Any]:
        note = (review_note or "").strip() or None
        try:
            row = self._repository.reject_review(review_id=review_id, reviewed_by=reviewed_by, review_note=note)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"failed to reject review: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="review_id not found")
        return row
