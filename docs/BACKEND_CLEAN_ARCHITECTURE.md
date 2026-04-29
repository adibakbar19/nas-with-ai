# Backend Runtime Architecture (NAS)

Runtime API is unified under `backend/app/main.py` to avoid duplicate surfaces in Swagger.

Core modules still follow layered separation:

- Ingest orchestration service: `backend/app/services/ingest_service.py`
- Ops/Search API services: `backend/app/services/ops_service.py`, `backend/app/services/search_api_service.py`
- DB read service + repository: `backend/app/services/address_read_service.py`, `backend/app/repositories/address_read_repository.py`
- Queue producer: `backend/app/queue/producer.py`
- Worker consumer: `backend/app/workers/queue_consumer.py`
- Dependency wiring: `backend/app/dependencies.py`
- Runtime settings: `backend/app/core/settings.py`
- Shared config/env helpers: `nas_core/config/`

## Environment Variables

- `POSTGRES_DSN` (optional override)
- `INGEST_JOB_STATE_DSN` (optional override for runtime migration tables)
- `INGEST_JOB_STATE_SCHEMA` (optional override for runtime migration tables)
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSCHEMA`
- `POSTCODE_BOUNDARY_TABLE` (default `postcode_boundary`)
- `ES_URL`, `ES_INDEX`
- `OBJECT_STORE_BUCKET`
- `OBJECT_STORE_ENDPOINT` (optional; leave empty for AWS S3)
- `OBJECT_STORE_PUBLIC_ENDPOINT` (optional; used for browser multipart uploads)
- `OBJECT_STORE_USE_PATH_STYLE` (typically `false` for AWS S3)
- `OBJECT_STORE_MANAGE_CORS` (typically `false`; bucket CORS should be managed by infrastructure)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (optional if using IAM role)
- `INGEST_EXECUTION_MODE=queue_worker`
- `VALKEY_URL` (required)
- `VALKEY_STREAM_KEY` (default `bulk_ingest_events`)
- `VALKEY_STREAM_GROUP` (default `bulk_ingest_workers`)
- `VALKEY_STREAM_BLOCK_MS` (default `5000`)
- `VALKEY_STREAM_CLAIM_IDLE_MS` (default `60000`, reclaim stale pending messages after 60s idle)
- `VALKEY_STREAM_CLAIM_COUNT` (default `10`, max stale pending messages to reclaim per poll)
- `WORKER_METRICS_PORT` (default `9101`)
- `AWS_REGION` (default `ap-southeast-5`)
- `NAS_ADMIN_TOKEN` (required to create, revoke, and rotate DB-backed agency API keys)
- `NAS_JWT_SIGNING_KEY` (required to issue and validate agency JWT access tokens)
- `NAS_JWT_ISSUER` (default `nas-api`)
- `NAS_JWT_AUDIENCE` (default `nas-agency-api`)
- `NAS_JWT_ACCESS_TTL_SECONDS` (default `3600`)

## Worker Run

```bash
python -m backend.app.workers.queue_consumer
```

## DB Migrations

```bash
alembic upgrade head
```

Docker Compose runs the same step through the `db-migrate` service before `api` and `worker` start.

## Monitoring

- API exposes Prometheus metrics at `/metrics`.
- Worker exposes Prometheus metrics on `WORKER_METRICS_PORT`.
- Docker Compose includes `prometheus` and `grafana` services with starter config and dashboards.

## Agency Ingest Auth

- `POST /api/v1/auth/token` mints short-lived JWT access tokens from agency API credentials.
- `/api/v1/ingest/*` routes accept `Authorization: Bearer <jwt>` as the primary auth model.
- DB-backed agency keys are the production API key model.
- Ingest jobs and multipart upload sessions persist `agency_id` and are filtered per agency.
- `Idempotency-Key` is supported on upload, retry-failed-rows, start, and multipart-initiate endpoints.
- OpenAPI examples for the ingest contract are published through FastAPI docs.

## Agency API Key Lifecycle

- Admin routes are protected by `X-Admin-Token`, backed by `NAS_ADMIN_TOKEN`.
- `POST /api/v1/admin/agencies/{agency_id}/api-keys` creates a new one-time plaintext key in `key_id.secret` format.
- `GET /api/v1/admin/agencies/{agency_id}/api-keys` lists stored metadata, never the plaintext secret.
- `POST /api/v1/admin/api-keys/{key_id}/revoke` revokes a key.
- `POST /api/v1/admin/api-keys/{key_id}/rotate` revokes the old key and issues a new one.
- Database stores hashed secrets plus metadata such as `last_used_at`, `expires_at`, and `revoked_at`.
- `GET /api/v1/admin/lookups/states` lists curated reference states from `LOOKUP_SCHEMA`.
- `GET/POST/PATCH` admin lookup routes now cover `districts`, `mukim`, `localities`, and `postcodes`.
- Each admin lookup mutation updates the curated lookup source and mirrors the same row into the runtime schema used by normalized ingest.
- After lookup changes that affect already-loaded canonical addresses, run `python -m backend.app.maintenance.backfill_lookup_refs --entity <district|mukim|locality|postcode> ...` to remap `standardized_address` admin-reference IDs.
- Backfill preserves existing NASKod values. The identifier remains stable even if the admin geography lookup changes later.
- Boundary governance is separate from lookup CRUD. Admin boundary routes support snapshot upload, version listing, and activation for `state_boundary`, `district_boundary`, `mukim_boundary`, `postcode_boundary`, and `pbt`.
- Boundary activation republishes the selected snapshot into the active canonical boundary tables in `LOOKUP_SCHEMA`.
- Spatial reassignment is a separate operational step: run `python -m backend.app.maintenance.backfill_spatial_refs` after activating new polygons if you want coordinates to be reclassified against the updated boundaries.
- Spatial reassignment updates admin IDs for rows with coordinates and preserves existing NASKod codes.

For production, run worker as separate deployment/container and keep API stateless.

## Queue Worker Mode For Existing Ingest Endpoints

When `INGEST_EXECUTION_MODE=queue_worker`:

- `POST /api/v1/ingest/upload` queues job event instead of running ETL in the API process.
- `POST /api/v1/ingest/jobs/{job_id}/start` requeues job event for worker.
- Worker process picks event and runs pipeline.

## Dependencies

`requirements.txt` now includes:

- `psycopg[binary]` for PostgreSQL/PostGIS repository access.

## ETL Boundaries

- Address normalization, replacement rules, and confidence scoring live under `etl/transform/address/`.
- API endpoints validate request/response contracts through `backend/app/schemas/` and service-layer checks.
- Backend runtime code should not import ETL transform modules except when explicitly launching an ingest job through the worker/service boundary.
