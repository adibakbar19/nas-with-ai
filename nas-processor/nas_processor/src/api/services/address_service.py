from __future__ import annotations

from typing import Any

from nas_processor.src.api.errors import ServiceError
from nas_processor.src.api.repositories.address_read_repository import AddressReadRepository


class AddressReadService:
    def __init__(self, *, repository: AddressReadRepository) -> None:
        self._repository = repository

    # ── Legacy methods (used by /api/v1/db/... routes) ────────────────────────

    def search(self, *, query: str | None, limit: int) -> dict[str, Any]:
        query_norm = (query or "").strip() or None
        try:
            rows = self._repository.search_addresses_legacy(query=query_norm, limit=limit)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        return {"query": query_norm, "count": len(rows), "items": rows}

    def get_address(self, *, record_id: str) -> dict[str, Any]:
        try:
            row = self._repository.get_address(record_id=record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="record_id not found")
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

    # ── New methods (used by /addresses/... routes) ────────────────────────────

    def get_address_detail(self, record_id: str) -> dict[str, Any]:
        try:
            address = self._repository.get_by_record_id(record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        if address is None:
            raise ServiceError(status_code=404, detail="record_id not found")
        try:
            raw_history = self._repository.get_raw_history(record_id)
            aliases = self._repository.get_aliases(record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"sub-resource query failed: {exc}") from exc
        return {"address": address, "raw_history": raw_history, "aliases": aliases}

    def get_naskod_detail(self, naskod: str) -> dict[str, Any]:
        code = naskod.strip().upper()
        if not code:
            raise ServiceError(status_code=400, detail="naskod is required")
        try:
            address = self._repository.get_by_naskod(code=code)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        if address is None:
            raise ServiceError(status_code=404, detail="naskod not found")
        record_id = address["record_id"]
        try:
            raw_history = self._repository.get_raw_history(record_id)
            aliases = self._repository.get_aliases(record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"sub-resource query failed: {exc}") from exc
        return {"address": address, "raw_history": raw_history, "aliases": aliases}

    def search_addresses(
        self,
        *,
        q: str | None,
        postcode: str | None,
        state_code: str | None,
        mukim_id: str | None,
        pbt_id: str | None,
        locality_name: str | None,
        confidence_band: str | None,
        lifecycle_status: str | None,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        try:
            rows, total = self._repository.search_addresses(
                q=q or None,
                postcode=postcode or None,
                state_code=state_code or None,
                mukim_id=mukim_id or None,
                pbt_id=pbt_id or None,
                locality_name=locality_name or None,
                confidence_band=confidence_band or None,
                lifecycle_status=lifecycle_status or None,
                limit=limit,
                offset=offset,
            )
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        return {"results": rows, "total": total, "limit": limit, "offset": offset}

    def get_raw_history(self, record_id: str) -> list[dict[str, Any]]:
        try:
            addr = self._repository.get_by_record_id(record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        if addr is None:
            raise ServiceError(status_code=404, detail="record_id not found")
        try:
            return self._repository.get_raw_history(record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"raw history query failed: {exc}") from exc

    def get_aliases(self, record_id: str) -> list[dict[str, Any]]:
        try:
            addr = self._repository.get_by_record_id(record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"database query failed: {exc}") from exc
        if addr is None:
            raise ServiceError(status_code=404, detail="record_id not found")
        try:
            return self._repository.get_aliases(record_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"alias query failed: {exc}") from exc
