from __future__ import annotations

import json
from typing import Any

from backend.app.repositories.boundary_admin_repository import BoundaryAdminRepository
from backend.app.services.errors import ServiceError


_SUPPORTED_BOUNDARY_TYPES = {
    "state_boundary": {"state_code", "state_name"},
    "district_boundary": {"state_code", "district_code", "district_name"},
    "mukim_boundary": {"state_code", "district_code", "district_name", "mukim_code", "mukim_name"},
    "postcode_boundary": {"postcode", "city", "state"},
    "pbt": {"pbt_id", "pbt_name"},
}


class BoundaryAdminService:
    def __init__(self, *, repository: BoundaryAdminRepository, lookup_schema: str) -> None:
        self._repository = repository
        self._lookup_schema = lookup_schema

    def _meta(self) -> dict[str, Any]:
        return {"lookup_schema": self._lookup_schema}

    def _wrap(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            **row,
            **self._meta(),
            "active_in_lookup": str(row.get("status") or "").strip().lower() == "active",
        }

    def list_versions(self, *, boundary_type: str | None, status: str | None, limit: int) -> dict[str, Any]:
        type_norm = (boundary_type or "").strip().lower() or None
        status_norm = (status or "").strip().lower() or None
        if type_norm is not None and type_norm not in _SUPPORTED_BOUNDARY_TYPES:
            raise ServiceError(status_code=400, detail="invalid boundary_type")
        if status_norm not in {None, "draft", "active", "superseded"}:
            raise ServiceError(status_code=400, detail="invalid boundary status")
        try:
            rows = self._repository.list_versions(boundary_type=type_norm, status=status_norm, limit=limit)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"boundary version query failed: {exc}") from exc
        return {
            **self._meta(),
            "boundary_type": type_norm,
            "status": status_norm,
            "count": len(rows),
            "items": [self._wrap(row) for row in rows],
        }

    def get_version(self, *, version_id: int) -> dict[str, Any]:
        try:
            row = self._repository.get_version(version_id=version_id)
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"boundary version query failed: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="version_id not found")
        return self._wrap(row)

    def _parse_upload_payload(self, *, boundary_type: str, raw_bytes: bytes) -> list[dict[str, Any]]:
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ServiceError(status_code=400, detail=f"invalid JSON payload: {exc}") from exc
        if isinstance(payload, dict):
            raw_features = payload.get("features")
        else:
            raw_features = payload
        if not isinstance(raw_features, list) or not raw_features:
            raise ServiceError(status_code=400, detail="boundary upload must contain a non-empty JSON array or {\"features\": [...]}")

        required_keys = _SUPPORTED_BOUNDARY_TYPES[boundary_type]
        normalized: list[dict[str, Any]] = []
        for idx, item in enumerate(raw_features, start=1):
            if not isinstance(item, dict):
                raise ServiceError(status_code=400, detail=f"feature {idx} must be a JSON object")
            boundary_wkt = str(item.get("boundary_wkt") or item.get("boundary_geom") or "").strip()
            if not boundary_wkt:
                raise ServiceError(status_code=400, detail=f"feature {idx} is missing boundary_wkt")
            missing_keys = [key for key in required_keys if str(item.get(key) or "").strip() == ""]
            if missing_keys:
                raise ServiceError(status_code=400, detail=f"feature {idx} is missing required keys: {', '.join(missing_keys)}")
            normalized_item = {key: item.get(key) for key in item.keys() if key not in {"boundary_wkt", "boundary_geom"}}
            normalized_item["boundary_wkt"] = boundary_wkt
            normalized.append(normalized_item)
        return normalized

    def create_upload(
        self,
        *,
        boundary_type: str,
        version_label: str,
        uploaded_by: str,
        source_note: str | None,
        upload_filename: str | None,
        raw_bytes: bytes,
    ) -> dict[str, Any]:
        type_norm = boundary_type.strip().lower()
        label = version_label.strip()
        note = (source_note or "").strip() or None
        if type_norm not in _SUPPORTED_BOUNDARY_TYPES:
            raise ServiceError(status_code=400, detail="invalid boundary_type")
        if not label:
            raise ServiceError(status_code=400, detail="version_label is required")
        features = self._parse_upload_payload(boundary_type=type_norm, raw_bytes=raw_bytes)
        try:
            row = self._repository.create_version(
                boundary_type=type_norm,
                version_label=label,
                uploaded_by=uploaded_by,
                source_note=note,
                features=features,
            )
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"failed to create boundary upload: {exc}") from exc
        return {**self._wrap(row), "upload_filename": upload_filename}

    def activate_version(self, *, version_id: int, activated_by: str, activation_note: str | None) -> dict[str, Any]:
        note = (activation_note or "").strip() or None
        try:
            row = self._repository.activate_version(version_id=version_id, activated_by=activated_by, activation_note=note)
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise ServiceError(status_code=503, detail=f"failed to activate boundary version: {exc}") from exc
        if row is None:
            raise ServiceError(status_code=404, detail="version_id not found")
        return self._wrap(row)
