from __future__ import annotations

import base64
import json
import logging
import os
import secrets
from functools import lru_cache
import hashlib
import hmac
import time

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from backend.app.db.migration_settings import get_runtime_db_dsn, get_runtime_db_schema
from backend.app.repositories.agency_api_key_repository import AgencyApiKeyRepository


logger = logging.getLogger(__name__)
DEFAULT_AGENCY_ROLE = "agency_operator"
ROLE_PERMISSIONS: dict[str, list[str]] = {
    "platform_admin": [
        "address.parse",
        "agency_keys.manage",
        "ingest.read",
        "ingest.retry",
        "ingest.start",
        "ingest.upload",
        "multipart.read",
        "multipart.write",
        "platform.admin",
    ],
    "agency_admin": [
        "address.parse",
        "agency_keys.manage",
        "ingest.read",
        "ingest.retry",
        "ingest.start",
        "ingest.upload",
        "multipart.read",
        "multipart.write",
    ],
    "agency_operator": [
        "address.parse",
        "ingest.read",
        "ingest.retry",
        "ingest.start",
        "ingest.upload",
        "multipart.read",
        "multipart.write",
    ],
    "agency_viewer": [
        "ingest.read",
        "multipart.read",
    ],
}


def _normalize_role(role: str | None, *, default: str = DEFAULT_AGENCY_ROLE) -> str:
    candidate = str(role or "").strip() or default
    if candidate not in ROLE_PERMISSIONS:
        return default
    return candidate


def _normalize_permissions(raw: object, *, default_role: str) -> list[str]:
    if isinstance(raw, list):
        normalized = sorted({str(item).strip() for item in raw if str(item).strip()})
        if normalized:
            return normalized
    return list(ROLE_PERMISSIONS.get(default_role, []))


class Agency(BaseModel):
    agency_id: str
    user_id: str | None = None
    username: str | None = None
    principal_type: str = "agency"
    role: str = DEFAULT_AGENCY_ROLE
    permissions: list[str] = Field(default_factory=lambda: list(ROLE_PERMISSIONS[DEFAULT_AGENCY_ROLE]))
    auth_type: str = "api_key"
    auth_scheme: str = "api_key"
    key_id: str | None = None


class Admin(BaseModel):
    actor: str = "admin"
    principal_type: str = "platform"
    role: str = "platform_admin"
    permissions: list[str] = Field(default_factory=lambda: list(ROLE_PERMISSIONS["platform_admin"]))
    auth_type: str = "admin_token"
    auth_scheme: str = "admin_token"


agency_api_key_header_scheme = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="Agency API key used directly or to mint JWT access tokens.",
)
agency_bearer_scheme = HTTPBearer(
    auto_error=False,
    description="Agency JWT access token. Raw API keys must use X-API-Key.",
)
admin_token_header_scheme = APIKeyHeader(
    name="X-Admin-Token",
    auto_error=False,
    description="Admin token for agency API key lifecycle operations.",
)


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        return None
    return value.strip()


@lru_cache(maxsize=1)
def _agency_api_key_repository() -> AgencyApiKeyRepository:
    return AgencyApiKeyRepository(dsn=get_runtime_db_dsn(), schema=get_runtime_db_schema())


def _build_secret_hash(key_id: str, secret: str) -> str:
    return hashlib.sha256(f"{key_id}.{secret}".encode("utf-8")).hexdigest()


@lru_cache(maxsize=1)
def _jwt_signing_key() -> str:
    return str(
        os.getenv("NAS_JWT_SIGNING_KEY")
        or os.getenv("JWT_SIGNING_KEY")
        or ""
    ).strip()


def _jwt_issuer() -> str:
    return str(os.getenv("NAS_JWT_ISSUER") or "nas-api").strip() or "nas-api"


def _jwt_audience() -> str:
    return str(os.getenv("NAS_JWT_AUDIENCE") or "nas-agency-api").strip() or "nas-agency-api"


def _jwt_access_ttl_seconds() -> int:
    return max(60, int(os.getenv("NAS_JWT_ACCESS_TTL_SECONDS", "3600")))


def _auth_type_from_scheme(auth_scheme: str | None) -> str:
    value = str(auth_scheme or "").strip().lower()
    if value.startswith("jwt:"):
        value = value.split(":", 1)[1]
    if value in {"api_key", "db_api_key"}:
        return "api_key"
    if value == "user_password":
        return "user_password"
    if value == "admin_token":
        return "admin_token"
    return value or "unknown"


def _principal_subject(
    *,
    agency_id: str,
    user_id: str | None,
    key_id: str | None,
    principal_type: str,
) -> str:
    if str(principal_type or "").strip() == "agency_user" and user_id:
        return str(user_id)
    if key_id:
        return str(key_id)
    return str(agency_id)


def _log_auth_success(principal: Agency | Admin) -> None:
    logger.info(
        "auth_success principal_type=%s agency_id=%s user_id=%s role=%s auth_type=%s auth_scheme=%s key_id=%s",
        getattr(principal, "principal_type", ""),
        getattr(principal, "agency_id", ""),
        getattr(principal, "user_id", ""),
        getattr(principal, "role", ""),
        getattr(principal, "auth_type", ""),
        getattr(principal, "auth_scheme", ""),
        getattr(principal, "key_id", ""),
    )


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("ascii"))


def _jwt_is_configured() -> bool:
    return bool(_jwt_signing_key())


def _password_iterations() -> int:
    return max(100000, int(os.getenv("NAS_PASSWORD_PBKDF2_ITERATIONS", "390000")))


def hash_password(password: str) -> str:
    secret = str(password or "")
    if not secret:
        raise ValueError("password is required")
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), salt, _password_iterations())
    return f"pbkdf2_sha256${_password_iterations()}${salt.hex()}${derived.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algo, iteration_text, salt_hex, digest_hex = str(password_hash or "").split("$", 3)
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    try:
        iterations = max(1, int(iteration_text))
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
    except (TypeError, ValueError):
        return False
    derived = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, iterations)
    return secrets.compare_digest(derived, expected)


def create_agency_jwt(
    *,
    agency_id: str,
    user_id: str | None = None,
    username: str | None = None,
    key_id: str | None,
    auth_scheme: str,
    role: str | None = None,
    permissions: list[str] | None = None,
    principal_type: str = "agency",
    expires_in: int | None = None,
) -> dict[str, object]:
    signing_key = _jwt_signing_key()
    if not signing_key:
        raise RuntimeError("JWT signing key is not configured")
    issued_at = int(time.time())
    ttl = max(60, int(expires_in or _jwt_access_ttl_seconds()))
    normalized_role = _normalize_role(role)
    normalized_permissions = _normalize_permissions(permissions, default_role=normalized_role)
    principal_type_value = str(principal_type or "agency").strip() or "agency"
    auth_type = _auth_type_from_scheme(auth_scheme)
    payload = {
        "sub": _principal_subject(
            agency_id=str(agency_id),
            user_id=user_id,
            key_id=key_id,
            principal_type=principal_type_value,
        ),
        "agency_id": str(agency_id),
        "user_id": str(user_id or ""),
        "username": str(username or ""),
        "principal_type": principal_type_value,
        "role": normalized_role,
        "permissions": normalized_permissions,
        "key_id": str(key_id or ""),
        "auth_type": auth_type,
        "auth_scheme": str(auth_scheme or "api_key"),
        "iss": _jwt_issuer(),
        "aud": _jwt_audience(),
        "iat": issued_at,
        "nbf": issued_at,
        "exp": issued_at + ttl,
    }
    header = {"alg": "HS256", "typ": "JWT"}
    header_segment = _b64url_encode(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    payload_segment = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    signature = hmac.new(signing_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    token = f"{header_segment}.{payload_segment}.{_b64url_encode(signature)}"
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": ttl,
        "agency_id": str(agency_id),
        "role": normalized_role,
        "auth_type": auth_type,
        "user_id": str(user_id or "") or None,
        "username": str(username or "") or None,
    }


def _authenticate_jwt(token: str) -> Agency | None:
    signing_key = _jwt_signing_key()
    if not signing_key:
        return None
    parts = token.split(".")
    if len(parts) != 3:
        return None
    header_segment, payload_segment, signature_segment = parts
    try:
        header = json.loads(_b64url_decode(header_segment).decode("utf-8"))
        payload = json.loads(_b64url_decode(payload_segment).decode("utf-8"))
        signature = _b64url_decode(signature_segment)
    except Exception:
        return None
    if not isinstance(header, dict) or header.get("alg") != "HS256":
        return None
    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    expected_signature = hmac.new(signing_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    if not secrets.compare_digest(signature, expected_signature):
        return None
    if not isinstance(payload, dict):
        return None
    now = int(time.time())
    if str(payload.get("iss") or "") != _jwt_issuer():
        return None
    if str(payload.get("aud") or "") != _jwt_audience():
        return None
    try:
        if int(payload.get("nbf") or 0) > now:
            return None
        if int(payload.get("exp") or 0) <= now:
            return None
    except (TypeError, ValueError):
        return None
    agency_id = str(payload.get("agency_id") or payload.get("sub") or "").strip()
    if not agency_id:
        return None
    user_id = str(payload.get("user_id") or "").strip() or None
    username = str(payload.get("username") or "").strip() or None
    key_id = str(payload.get("key_id") or "").strip() or None
    auth_scheme = str(payload.get("auth_scheme") or "jwt").strip() or "jwt"
    auth_type = str(payload.get("auth_type") or "").strip() or _auth_type_from_scheme(auth_scheme)
    role = _normalize_role(str(payload.get("role") or "").strip() or None)
    permissions = _normalize_permissions(payload.get("permissions"), default_role=role)
    principal_type = str(payload.get("principal_type") or "agency").strip() or "agency"
    return Agency(
        agency_id=agency_id,
        user_id=user_id,
        username=username,
        principal_type=principal_type,
        role=role,
        permissions=permissions,
        auth_type=auth_type,
        auth_scheme=f"jwt:{auth_scheme}",
        key_id=key_id,
    )


def _authenticate_db_api_key(presented: str) -> Agency | None:
    key_id, dot, secret = presented.partition(".")
    if not dot or not key_id.strip() or not secret.strip():
        return None
    try:
        record = _agency_api_key_repository().get_key(key_id=key_id.strip())
    except Exception:
        return None
    if not record:
        return None
    if str(record.get("status") or "").lower() != "active":
        return None
    expires_at = record.get("expires_at")
    if expires_at:
        try:
            from datetime import datetime, timezone

            expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
            if expiry <= datetime.now(timezone.utc):
                return None
        except ValueError:
            return None
    expected_hash = str(record.get("_secret_hash") or "")
    if not expected_hash:
        return None
    if not secrets.compare_digest(_build_secret_hash(key_id.strip(), secret.strip()), expected_hash):
        return None
    try:
        _agency_api_key_repository().touch_last_used(key_id=key_id.strip())
    except Exception:
        pass
    role = _normalize_role(str(record.get("role") or "").strip() or None)
    permissions = _normalize_permissions(record.get("permissions"), default_role=role)
    return Agency(
        agency_id=str(record.get("agency_id") or "").strip(),
        user_id=None,
        username=None,
        role=role,
        permissions=permissions,
        auth_type="api_key",
        auth_scheme="db_api_key",
        key_id=key_id.strip(),
    )


def authenticate_agency_api_key_value(presented: str) -> Agency | None:
    return _authenticate_db_api_key(presented)


def get_current_agency(
    x_api_key: str | None = Security(agency_api_key_header_scheme),
    bearer_credentials: HTTPAuthorizationCredentials | None = Security(agency_bearer_scheme),
) -> Agency:
    authorization = None
    if bearer_credentials is not None:
        authorization = f"{bearer_credentials.scheme} {bearer_credentials.credentials}"
    has_api_key = bool(x_api_key and x_api_key.strip())
    bearer = _extract_bearer_token(authorization)
    has_bearer = bool(bearer)
    if has_api_key and has_bearer:
        raise HTTPException(status_code=400, detail="provide either X-API-Key or Authorization bearer token, not both")

    if has_api_key:
        agency = authenticate_agency_api_key_value(x_api_key.strip())
        if agency is not None:
            _log_auth_success(agency)
            return agency
        raise HTTPException(status_code=401, detail="invalid API key")

    if not has_bearer:
        raise HTTPException(status_code=401, detail="missing authentication credentials")

    jwt_agency = _authenticate_jwt(bearer)
    if jwt_agency is not None:
        _log_auth_success(jwt_agency)
        return jwt_agency

    raise HTTPException(status_code=401, detail="invalid JWT bearer token")


def get_current_admin(x_admin_token: str | None = Security(admin_token_header_scheme)) -> Admin:
    expected = str(os.getenv("NAS_ADMIN_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="admin authentication is not configured")
    provided = str(x_admin_token or "").strip()
    if not provided:
        raise HTTPException(status_code=401, detail="missing admin token")
    if not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="invalid admin token")
    admin = Admin()
    _log_auth_success(admin)
    return admin


def has_permission(principal: Agency | Admin, permission: str) -> bool:
    normalized = str(permission or "").strip()
    if not normalized:
        return False
    return normalized in set(getattr(principal, "permissions", []) or [])


def require_permission(principal: Agency | Admin, permission: str) -> None:
    if has_permission(principal, permission):
        return
    raise HTTPException(status_code=403, detail=f"missing permission: {permission}")


def require_role(principal: Agency | Admin, role: str) -> None:
    expected_role = str(role or "").strip()
    if expected_role and str(getattr(principal, "role", "")).strip() == expected_role:
        return
    raise HTTPException(status_code=403, detail=f"missing required role: {expected_role}")


def require_agency_access(principal: Agency | Admin, target_agency_id: str) -> None:
    if str(getattr(principal, "role", "")).strip() == "platform_admin":
        return
    if str(getattr(principal, "agency_id", "")).strip() == str(target_agency_id or "").strip():
        return
    raise HTTPException(status_code=403, detail="forbidden for target agency")
