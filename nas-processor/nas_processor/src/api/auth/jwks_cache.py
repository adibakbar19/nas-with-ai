"""In-process JWKS cache with async key rotation support."""

from __future__ import annotations

import asyncio

import httpx


class JWKSCache:
    """Async JWKS cache. Thread-safe via asyncio.Lock."""

    def __init__(self, jwks_url: str) -> None:
        self._jwks_url = jwks_url
        self._keys: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def get_key(self, kid: str) -> dict:
        """Return the JWK dict for the given kid. Refetch if not cached."""
        if kid in self._keys:
            return self._keys[kid]

        async with self._lock:
            if kid in self._keys:
                return self._keys[kid]
            await self._refresh()

        if kid not in self._keys:
            raise KeyError(f"kid '{kid}' not found in Keycloak JWKS")
        return self._keys[kid]

    async def _refresh(self) -> None:
        """Fetch JWKS from Keycloak and rebuild the cache."""
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(self._jwks_url)
            response.raise_for_status()
            jwks = response.json()

        self._keys = {}
        for key in jwks.get("keys", []):
            key_kid = key.get("kid")
            if key_kid and key.get("kty") == "RSA":
                self._keys[key_kid] = key
