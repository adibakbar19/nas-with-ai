# Deferred: download_failed_rows endpoint

## Status

Deferred — adds pandas dependency

## Problem

GET /api/v1/ingest/jobs/{job_id}/failed-rows.csv has inline logic that:
1. Reads parquet files from S3 (preferred) or local filesystem (fallback)
2. Concatenates DataFrames
3. Converts to CSV and returns as a Response

This requires pandas and adds ~50MB to the container image.

## Current workaround

Endpoint remains available in backend/app/api/v1/ingest/ingest.py only.
Clients calling the old API on port 8000 can still use it.

## When to migrate

After end-to-end upload flow is verified working on ingestion-api (port 3000).
Add pandas to ingestion-api/requirements.txt and move the route + helper.

## Blocked by

Nothing — can be done at any time after STEP 9 is complete.
