# Deferred: Migrate backend read API auth to Keycloak

## Status

Deferred — backend still uses hand-rolled HS256 JWT auth

## Problem

backend/app/api/v1/ read routes (address search, admin, jobs list, job detail)
still use the old auth stack:
- X-API-Key → AgencyApiKeyRepository (Postgres)
- Bearer → HS256 JWT signed with NAS_JWT_SIGNING_KEY

Keycloak RS256 tokens are rejected by the backend.

This means the frontend cannot use a single auth flow for both
ingestion-api (port 3000) and backend (port 8000).

## What needs to happen

When the backend read API routes move to nas-processor,
they get the new Keycloak auth layer (same as ingestion-api).
At that point the old auth stack in backend/ is retired entirely.

## Impact

- Frontend must maintain two auth flows until read API migrates
- backend/app/security.py and AgencyApiKeyRepository stay alive
- NAS_JWT_SIGNING_KEY env var still needed

## Blocked by

nas-processor read API extraction (next major step)
