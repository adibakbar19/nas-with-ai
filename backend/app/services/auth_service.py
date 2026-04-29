from __future__ import annotations

from backend.app.db.migration_settings import get_runtime_db_dsn, get_runtime_db_schema
from backend.app.repositories.agency_user_repository import AgencyUserRepository
from backend.app.schemas.auth import AgencyTokenRequest
from backend.app.security import authenticate_agency_api_key_value, create_agency_jwt, verify_password
from backend.app.services.errors import ServiceError


class AuthService:
    @staticmethod
    def _user_repository() -> AgencyUserRepository:
        return AgencyUserRepository(dsn=get_runtime_db_dsn(), schema=get_runtime_db_schema())

    def issue_agency_token(
        self,
        *,
        request: AgencyTokenRequest | None,
        api_key: str | None,
    ) -> dict[str, object]:
        presented_api_key = str(api_key or "").strip() or None
        if request and request.client_id and request.client_secret:
            if request.grant_type and request.grant_type != "client_credentials":
                raise ServiceError(status_code=400, detail="unsupported grant_type")
            presented_api_key = f"{request.client_id.strip()}.{request.client_secret.strip()}"

        if not presented_api_key:
            raise ServiceError(
                status_code=400,
                detail="provide X-API-Key or client_id/client_secret to mint a token",
            )

        agency = authenticate_agency_api_key_value(presented_api_key)
        if agency is None:
            raise ServiceError(status_code=401, detail="invalid agency credentials")

        try:
            token = create_agency_jwt(
                agency_id=agency.agency_id,
                user_id=agency.user_id,
                username=agency.username,
                key_id=agency.key_id,
                auth_scheme=agency.auth_scheme,
                role=agency.role,
                permissions=agency.permissions,
                principal_type=agency.principal_type,
                expires_in=request.expires_in if request else None,
            )
        except RuntimeError as exc:
            raise ServiceError(status_code=503, detail=str(exc)) from exc
        return token

    def login_user(
        self,
        *,
        username: str,
        password: str,
        expires_in: int | None = None,
    ) -> dict[str, object]:
        username_value = str(username or "").strip()
        if not username_value:
            raise ServiceError(status_code=400, detail="username is required")
        record = self._user_repository().get_user_by_username(username=username_value)
        if record is None:
            raise ServiceError(status_code=401, detail="invalid username or password")
        if str(record.get("status") or "").lower() != "active":
            raise ServiceError(status_code=403, detail="user account is not active")
        if not verify_password(password, str(record.get("_password_hash") or "")):
            raise ServiceError(status_code=401, detail="invalid username or password")
        try:
            token = create_agency_jwt(
                agency_id=str(record.get("agency_id") or "").strip(),
                user_id=str(record.get("user_id") or "").strip() or None,
                username=str(record.get("username") or "").strip() or None,
                key_id=None,
                auth_scheme="user_password",
                role=str(record.get("role") or "").strip() or None,
                principal_type="agency_user",
                expires_in=expires_in,
            )
        except RuntimeError as exc:
            raise ServiceError(status_code=503, detail=str(exc)) from exc
        self._user_repository().touch_last_login(user_id=str(record.get("user_id") or "").strip())
        return token
