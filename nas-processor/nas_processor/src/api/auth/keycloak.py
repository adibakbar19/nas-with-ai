"""Keycloak RS256 JWT validation using JWKS discovery.

Receives JWKSCache instance via FastAPI Request.app.state (lifespan-managed).
No module-level singletons — fully testable with fakes.
"""

from __future__ import annotations

import os

from fastapi import HTTPException, Request
from jose import jwt, JWTError

from .jwks_cache import JWKSCache

KEYCLOAK_AUDIENCE = os.environ.get("KEYCLOAK_AUDIENCE", "nas-processor-api")


async def validate_token(token: str, request: Request) -> dict:
    """Validate a Keycloak RS256 JWT and return the decoded payload.

    Raises HTTPException(401) on any failure: expired, bad signature,
    wrong audience, missing kid, or malformed token.
    """
    cache: JWKSCache = request.app.state.jwks_cache

    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError:
        raise HTTPException(status_code=401, detail="malformed token header")

    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="token missing kid header")

    alg = unverified_header.get("alg", "")
    if alg != "RS256":
        raise HTTPException(status_code=401, detail=f"unsupported algorithm: {alg}")

    try:
        key = await cache.get_key(kid)
    except KeyError:
        raise HTTPException(status_code=401, detail="unknown signing key")
    except Exception:
        raise HTTPException(status_code=401, detail="failed to fetch signing keys")

    try:
        payload = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=KEYCLOAK_AUDIENCE,
            options={"verify_at_hash": False},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="token expired")
    except jwt.JWTClaimsError as exc:
        raise HTTPException(status_code=401, detail=f"invalid claims: {exc}")
    except JWTError:
        raise HTTPException(status_code=401, detail="invalid token signature")

    return payload
