from __future__ import annotations

import uuid
from typing import Any

from backend.app.db.migration_settings import get_runtime_db_dsn, get_runtime_db_schema
from backend.app.repositories.agency_user_repository import AgencyUserRepository
from backend.app.security import ROLE_PERMISSIONS, hash_password
from backend.app.services.errors import ServiceError


class AgencyUserService:
    @staticmethod
    def _repository() -> AgencyUserRepository:
        return AgencyUserRepository(dsn=get_runtime_db_dsn(), schema=get_runtime_db_schema())

    @staticmethod
    def _validate_role(role: str | None) -> str:
        normalized = str(role or "").strip() or "agency_operator"
        if normalized not in ROLE_PERMISSIONS:
            raise ServiceError(status_code=400, detail=f"unsupported role: {normalized}")
        if normalized == "platform_admin":
            raise ServiceError(status_code=400, detail="platform_admin cannot be assigned to agency users")
        return normalized

    @classmethod
    def create_user(
        cls,
        *,
        agency_id: str,
        username: str,
        password: str,
        role: str,
        display_name: str | None = None,
        email: str | None = None,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        agency = str(agency_id or "").strip()
        if not agency:
            raise ServiceError(status_code=400, detail="agency_id is required")
        username_value = str(username or "").strip()
        if not username_value:
            raise ServiceError(status_code=400, detail="username is required")
        if len(str(password or "")) < 8:
            raise ServiceError(status_code=400, detail="password must be at least 8 characters")
        existing = cls._repository().get_user_by_username(username=username_value)
        if existing is not None:
            raise ServiceError(status_code=409, detail="username already exists")
        record = cls._repository().create_user(
            record={
                "user_id": f"usr_{uuid.uuid4().hex}",
                "agency_id": agency,
                "username": username_value,
                "display_name": str(display_name).strip() if display_name else None,
                "email": str(email).strip() if email else None,
                "password_hash": hash_password(password),
                "password_algo": "pbkdf2_sha256",
                "role": cls._validate_role(role),
                "status": "active",
                "created_by": created_by,
                "metadata": {},
            }
        )
        record.pop("_password_hash", None)
        return record

    @classmethod
    def list_users(cls, *, agency_id: str) -> dict[str, Any]:
        agency = str(agency_id or "").strip()
        if not agency:
            raise ServiceError(status_code=400, detail="agency_id is required")
        items = cls._repository().list_users(agency_id=agency)
        for item in items:
            item.pop("_password_hash", None)
        return {"count": len(items), "items": items}
