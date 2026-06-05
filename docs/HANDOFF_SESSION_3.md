# Session 3 Handoff — NAS Microservices Refactoring

## Status: COMPLETE — All services extracted and running independently

## What was accomplished

The NAS monolith has been fully decomposed into independent microservices.
The backend/ API service is retired from docker-compose. All business logic
routes now live in purpose-built services with Keycloak RS256 auth.

## What was completed this session

### ingestion-api (port 3000) — FULLY EXTRACTED
- All 11 ingest routes working (upload, retry, start, jobs CRUD, multipart, failed-rows download)
- Keycloak RS256 auth (replaces hand-rolled HS256)
- Redis routing keystore
- End-to-end upload verified: token → S3 (MinIO) → PostgreSQL
- pandas + pyarrow added for download_failed_rows

### nas-processor-api (port 8001) — FULLY EXTRACTED
- Address search (OpenSearch, public endpoint)
- Address DB lookup (3 routes, public)
- Admin lookup CRUD (17 operations, platform_admin auth)
- Admin boundary management (4 routes, platform_admin auth)
- Keycloak auth with realm_roles + require_platform_admin

### backend (port 8000) — RETIRED
- Removed from docker-compose.yml
- All business routes deprecated with 410 Gone
- Only ops routes (health/metrics/root) remained before removal
- Port 8000 no longer accessible

### Worker extraction — COMPLETE
- TASK 1 DONE: INDEX_MAPPING moved from backend/ to etl/pipeline/index_mapping.py
- TASK 2 DONE: consumer.py created in nas-processor/src/worker/
  - IngestWorker class (Valkey stream consumer loop)
  - Zero backend/ imports
  - DI-based with JobRunner, JobStateRepository, env config
  - Search sync deferred (logs warning, skips)
- TASK 3 DONE: Dockerfile.worker created, docker-compose worker updated
  - Worker starts with nas_processor.src.worker.consumer entry point
  - backend/ NOT in worker image (verified)
  - End-to-end job processing verified: upload → queue → worker picks up → status=running

## Files created this session

### ingestion-api/
- src/auth/ (keycloak.py, keystore.py, dependencies.py, jwks_cache.py)
- src/errors.py
- src/storage/object_store.py
- src/repositories/ (multipart, idempotency)
- src/state/ (job_repository.py, job_state.py)
- src/schemas/ (events.py, requests.py, responses.py)
- src/services/ingest_service.py
- src/queue/producer.py
- src/routes/ingest.py
- config.py (with _build_dsn)
- app.py (lifespan, ServiceError handler, router registration)
- Dockerfile, requirements.txt

### nas-processor/nas_processor/src/api/
- auth/ (keycloak.py, keystore.py, dependencies.py, jwks_cache.py)
- config.py, errors.py, app.py
- repositories/ (address_read, lookup_admin, boundary_admin, job_repository)
- services/ (search_service, address_service, lookup_admin_service, boundary_admin_service)
- schemas/ (boundary.py)
- routes/ (search.py, address.py, admin_lookup.py, admin_boundary.py)
- Dockerfile (for read API)

### nas-processor/nas_processor/src/worker/
- consumer.py (IngestWorker + main)
- object_store.py (copied from ingestion-api)
- runner.py (already existed from Priority 6)

### Infrastructure
- docker/keycloak/master-realm.json
- scripts/keycloak_seed.py (idempotent: realm, clients, roles, audience mapping, Redis routing)
- etl/pipeline/index_mapping.py (INDEX_MAPPING moved from backend/)

### Deferred items (docs/deferred/)
- address_parse_service_extraction.md (async event-driven design)
- backend_auth_migration.md (done — backend retired)
- download_failed_rows.md (done — moved to ingestion-api)
- shared_package_docker_build.md (_build_dsn duplication)

## Docker services (current docker-compose.yml)

| Service | Port | Image/Build | Status |
|---------|------|-------------|--------|
| postgres | 5432 | postgis/postgis:16-3.4 | Running |
| opensearch | 9200 | opensearchproject/opensearch:2.13.0 | Running |
| valkey | 6379 | valkey/valkey:7-alpine | Running |
| keycloak | 8080 | quay.io/keycloak/keycloak:24.0 | Running |
| minio | 9000/9001 | minio/minio:latest | Running |
| db-migrate | — | Dockerfile.backend (one-shot) | Exited |
| db-bootstrap | — | Dockerfile.backend (one-shot) | Exited |
| ingestion-api | 3000 | ./ingestion-api | Healthy |
| nas-processor-api | 8001 | ./nas-processor (api Dockerfile) | Healthy |
| worker | — | Dockerfile.worker | Running |
| frontend | 80 | ./frontend-vue | Running |

**REMOVED:** backend/api service (port 8000) — fully retired

## Next steps (for next session)

1. **Move Alembic migrations out of backend/** → nas-processor/alembic/
   - Create Dockerfile.migrate (minimal)
   - Update docker-compose db-migrate/db-bootstrap
   - Then delete backend/ and Dockerfile.backend

2. **Move etl/ inside nas-processor/** (optional, can be done with step 1)
   - Update runner.py import: `from etl.pipeline.pipeline import ...`
   - Update Dockerfile.worker COPY paths

3. **Implement search sync in worker** (currently logs warning and skips)
   - Move SearchSyncService logic into worker
   - Or create separate search-sync worker

4. **Implement address parse async endpoint**
   - POST /api/v1/address/parse → publishes event → 202
   - Worker handles parse job type
   - GET /api/v1/address/parse/jobs/{id} → returns results

5. **Frontend update** — point VITE_API_BASE_URL at ingestion-api/nas-processor-api

6. **Production deployment prep**
   - Real AWS credentials (not MinIO)
   - Real Keycloak (not start-dev)
   - Remove test secrets from seed script

## Keycloak setup

- URL: http://localhost:8080
- Admin: admin/admin
- Realm: org-realm
- Clients:
  - ingestion-api (bearer-only)
  - nas-processor-api (bearer-only, has api.access role)
  - nas-agency-a (confidential, service account, secret: test-secret-agency-a)
  - nas-admin (confidential, service account, secret: test-secret-admin, has platform_admin realm role)
- Seed script: scripts/keycloak_seed.py (fully idempotent)

## Key architecture decisions

- _build_dsn() duplicated in each service (documented in docs/deferred/)
- ObjectStoreSettings + build_s3_client duplicated in ingestion-api and nas-processor worker
- Auth layer (JWKS + Redis keystore) duplicated per service (same pattern, separate containers)
- ServiceError + global exception handler pattern in each FastAPI app
- No module-level singletons — all DI via constructor + FastAPI lifespan
- Keycloak audience mapping via api.access client role trick
