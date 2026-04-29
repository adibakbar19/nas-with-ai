from __future__ import annotations

import hashlib
import secrets
from datetime import datetime
from typing import Any

from backend.app.db.migration_settings import get_runtime_db_dsn, get_runtime_db_schema
from backend.app.repositories.agency_api_key_repository import AgencyApiKeyRepository
from backend.app.services.errors import ServiceError


class AgencyApiKeyService:
    @staticmethod
    def _repository() -> AgencyApiKeyRepository:
        return AgencyApiKeyRepository(dsn=get_runtime_db_dsn(), schema=get_runtime_db_schema())

    @staticmethod
    def _build_secret_hash(key_id: str, secret: str) -> str:
        return hashlib.sha256(f"{key_id}.{secret}".encode("utf-8")).hexdigest()

    @staticmethod
    def _generate_key_id() -> str:
        return f"ak_{secrets.token_hex(8)}"

    @staticmethod
    def _generate_secret() -> str:
        return secrets.token_urlsafe(32)

    @classmethod
    def create_key(
        cls,
        *,
        agency_id: str,
        label: str | None = None,
        expires_at: str | None = None,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        agency = str(agency_id or "").strip()
        if not agency:
            raise ServiceError(status_code=400, detail="agency_id is required")
        key_id = cls._generate_key_id()
        secret = cls._generate_secret()
        record = cls._repository().create_key(
            record={
                "key_id": key_id,
                "agency_id": agency,
                "label": str(label).strip() if label else None,
                "secret_hash": cls._build_secret_hash(key_id, secret),
                "secret_preview": f"...{secret[-4:]}",
                "status": "active",
                "created_by": created_by,
                "expires_at": expires_at,
                "metadata": {},
            }
        )
        record.pop("_secret_hash", None)
        record["api_key"] = f"{key_id}.{secret}"
        return record

    @classmethod
    def list_keys(cls, *, agency_id: str) -> dict[str, Any]:
        agency = str(agency_id or "").strip()
        if not agency:
            raise ServiceError(status_code=400, detail="agency_id is required")
        items = cls._repository().list_keys(agency_id=agency)
        for item in items:
            item.pop("_secret_hash", None)
        return {"count": len(items), "items": items}

    @classmethod
    def revoke_key(cls, *, key_id: str, revoked_reason: str | None = None) -> dict[str, Any]:
        key = cls._repository().get_key(key_id=key_id)
        if not key:
            raise ServiceError(status_code=404, detail="key_id not found")
        if str(key.get("status") or "").lower() == "revoked":
            return {
                "key_id": key_id,
                "agency_id": key.get("agency_id"),
                "status": "revoked",
                "revoked_at": key.get("revoked_at"),
            }
        updated = cls._repository().revoke_key(key_id=key_id, revoked_reason=revoked_reason)
        if not updated:
            raise ServiceError(status_code=409, detail="failed to revoke key")
        return {
            "key_id": key_id,
            "agency_id": updated.get("agency_id"),
            "status": "revoked",
            "revoked_at": updated.get("revoked_at"),
        }

    @classmethod
    def rotate_key(
        cls,
        *,
        key_id: str,
        label: str | None = None,
        expires_at: str | None = None,
        revoked_reason: str | None = None,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        current = cls._repository().get_key(key_id=key_id)
        if not current:
            raise ServiceError(status_code=404, detail="key_id not found")
        agency_id = str(current.get("agency_id") or "").strip()
        if not agency_id:
            raise ServiceError(status_code=409, detail="key record is missing agency_id")

        cls._repository().revoke_key(
            key_id=key_id,
            revoked_reason=revoked_reason or f"rotated from {key_id}",
        )
        replacement = cls.create_key(
            agency_id=agency_id,
            label=label if label is not None else current.get("label"),
            expires_at=expires_at if expires_at is not None else current.get("expires_at"),
            created_by=created_by,
        )
        replacement["rotated_from_key_id"] = key_id
        return replacement
