"""FastAPI auth dependencies — Keycloak + Redis routing.

Entry point: get_current_agency (used as Depends() in route handlers).
Adds realm_roles extraction and require_platform_admin for admin routes.
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from .keycloak import validate_token, KEYCLOAK_AUDIENCE
from .keystore import get_routing

bearer_scheme = HTTPBearer(auto_error=False)


class Agency(BaseModel):
    """Authenticated principal resolved from Keycloak token + Redis routing."""

    agency_id: str          # from Redis routing ("agency" field)
    source: str             # from Redis routing ("source" field)
    topic: str              # from Redis routing ("topic" field)
    client_id: str          # from Keycloak token (azp / client_id)
    permissions: list[str]  # from token resource_access.{audience}.roles
    sub: str                # from token sub claim
    realm_roles: list[str] = []  # from token realm_access.roles


async def get_current_agency(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> Agency:
    """Resolve the current agency from a Keycloak Bearer token + Redis routing.

    Flow:
    1. Extract Bearer token from Authorization header
    2. Validate token via Keycloak JWKS (RS256)
    3. Extract client_id from token (try "client_id" then "azp")
    4. Look up routing in Redis: apikey:{client_id} -> {source, agency, topic}
    5. Extract permissions from token: resource_access.{audience}.roles
    6. Extract realm_roles from token: realm_access.roles
    7. Return Agency model
    """
    if credentials is None:
        raise HTTPException(status_code=401, detail="missing authorization header")

    token = credentials.credentials
    payload = await validate_token(token, request)

    # Extract client_id
    client_id = payload.get("client_id") or payload.get("azp") or ""
    if not client_id:
        raise HTTPException(
            status_code=401, detail="token missing client_id or azp claim"
        )

    # Redis routing lookup
    routing = await get_routing(client_id, request)

    # Extract permissions from resource_access.{audience}.roles
    resource_access = payload.get("resource_access", {})
    client_roles = resource_access.get(KEYCLOAK_AUDIENCE, {}).get("roles", [])
    permissions = [str(r) for r in client_roles if r]

    # Extract realm roles
    realm_access = payload.get("realm_access", {})
    realm_roles = [str(r) for r in realm_access.get("roles", []) if r]

    return Agency(
        agency_id=routing["agency"],
        source=routing["source"],
        topic=routing["topic"],
        client_id=client_id,
        permissions=permissions,
        sub=str(payload.get("sub", "")),
        realm_roles=realm_roles,
    )


def require_permission(agency: Agency, permission: str) -> None:
    """Check that agency has the required permission. Raises 403 if missing."""
    normalized = (permission or "").strip()
    if not normalized:
        return
    if normalized not in set(agency.permissions):
        raise HTTPException(status_code=403, detail=f"missing permission: {normalized}")


def require_platform_admin(agency: Agency) -> None:
    """Check that agency has the platform_admin realm role. Raises 403 if missing."""
    if "platform_admin" not in agency.realm_roles:
        raise HTTPException(status_code=403, detail="platform_admin role required")
