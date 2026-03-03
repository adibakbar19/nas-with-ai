from typing import Any

from backend.app.repositories.address_read_repository import AddressReadRepository
from backend.app.services.errors import ServiceError


class AddressReadService:
    def __init__(self, *, repository: AddressReadRepository) -> None:
        self._repository = repository

    def search(self, *, query: str | None, limit: int) -> dict[str, Any]:
        query_norm = (query or "").strip() or None
        try:
            rows = self._repository.search_addresses(query=query_norm, limit=limit)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        return {"query": query_norm, "count": len(rows), "items": rows}

    def get_address(self, *, address_id: int) -> dict[str, Any]:
        try:
            row = self._repository.get_address(address_id=address_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="address_id not found")
        return row

    def get_by_naskod(self, *, naskod: str) -> dict[str, Any]:
        code = naskod.strip().upper()
        if not code:
            raise ServiceError(status_code=400, detail="naskod is required")
        try:
            row = self._repository.get_by_naskod(code=code)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="naskod not found")
        return row
