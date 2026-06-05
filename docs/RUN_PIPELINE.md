# Pipeline Run Guide

## Current Architecture

ETL pipeline is now triggered via the ingest API. Upload a file via `POST /api/v1/ingest/upload` and the worker processes it automatically.

The worker performs:
1. Run `etl.pipeline` (extract → transform → validate)
2. Write cleaned, warning, and failed parquet outputs
3. Load cleaned output into Postgres with `etl.load.postgres`
4. Reindex OpenSearch from Postgres with `backend.app.maintenance.reindex_search`
5. Upload output to S3 (best-effort)

## Prerequisites

- Docker Compose stack is running (`docker compose up -d`)
- DB-backed lookup and boundary tables exist in `LOOKUP_SCHEMA`
- `.env` contains Postgres, OpenSearch, Valkey, and object storage settings

## Useful Env Vars

- `PIPELINE_SOURCE_TYPE`: `auto`, `csv`, `json`, `excel`, or `xlsx`
- `PIPELINE_CONFIG`: default `config/config.json`
- `ES_URL`, `ES_INDEX`, `ES_SCHEMA`, `ES_BATCH_SIZE`

## Lookup Data

Lookup and boundary data is maintained in Postgres through backend/admin workflows.
