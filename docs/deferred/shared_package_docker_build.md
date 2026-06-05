# Deferred: Install shared packages in ingestion-api Docker build

## Status

Deferred — build context isolation

## Problem

ingestion-api/Dockerfile uses context: ./ingestion-api which does not have access
to shared/nas_config, shared/nas_auth, or shared/nas_contracts. This means
ingestion-api cannot pip install -e shared/nas_config like backend/ does.

## Current workaround

_build_dsn() in ingestion-api/config.py duplicates the logic from
shared/nas_config/db.py (15 lines).

## Proper fix options

1. Change docker-compose build context to project root and use a --file flag to
   point at ingestion-api/Dockerfile
2. Publish shared packages to a private PyPI registry
3. Use a multi-stage Docker build that copies shared/ in

## Why deferred

Not blocking — the duplication is small and documented.
Revisit when adding a third service that needs shared packages.
