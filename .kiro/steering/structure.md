# Project Structure

```
├── backend/                  # FastAPI API server and queue worker
│   └── app/
│       ├── api/v1/           # Route handlers (versioned API)
│       ├── core/             # App settings (Pydantic)
│       ├── db/               # Alembic migrations config
│       ├── maintenance/      # Admin maintenance scripts (reindex, backfill)
│       ├── queue/            # Valkey stream producer
│       ├── repositories/     # Data access layer (SQL queries)
│       ├── runtime/          # Runtime state (job tracking, settings)
│       ├── schemas/          # Pydantic request/response models
│       ├── search/           # OpenSearch indexer and query logic
│       ├── services/         # Business logic layer
│       ├── workers/          # Queue consumer and ingest runner
│       ├── dependencies.py   # FastAPI dependency injection
│       ├── main.py           # App entrypoint
│       ├── monitoring.py     # Prometheus metrics
│       ├── object_store.py   # S3/MinIO client
│       └── security.py       # Auth helpers (JWT, API key)
│
├── etl/                      # ETL pipeline (standalone, also called by worker)
│   ├── audit/                # JSONL audit logging
│   ├── bootstrap/            # Lookup table seeding
│   ├── extract/              # File reading (CSV, Excel, JSON)
│   ├── jobs/                 # Retry-failed-rows logic
│   ├── load/                 # Postgres bulk loader
│   ├── pipeline/             # Orchestrator and pipeline stages
│   ├── repository/           # Lookup and boundary DB access for ETL
│   └── transform/            # Address parsing, lookup matching, LLM enrichment
│       ├── address/          # Core address parsing logic
│       ├── lookup/           # Reference data matching
│       └── llm/              # Optional LLM-based correction
│
├── nas_core/                 # Shared config/env helpers (used by backend + ETL)
│
├── frontend-vue/             # Vue 3 upload portal (Vite)
│   └── src/
│
├── config/                   # Versioned ingest config (config.json)
├── data/
│   ├── boundary/             # GeoJSON boundary files (5 layers)
│   ├── lookups_clean/        # Reference CSV files (11 tables)
│   └── raw/                  # Sample source data
│
├── docker/                   # Additional Docker configs
├── docs/                     # Operational runbooks
├── monitoring/               # Monitoring configs (Prometheus/Grafana)
├── scripts/                  # Utility scripts
├── logs/                     # Audit logs output
├── output/                   # ETL pipeline output (cleaned/failed)
│
├── docker-compose.yml        # Full stack orchestration
├── Dockerfile.backend        # Backend image
├── alembic.ini               # Migration config
├── requirements.txt          # Python dependencies
└── bootstrap_lookups_if_needed.py  # DB seeding entrypoint
```

## Architecture Patterns

- **Layered backend:** Routes → Services → Repositories → DB
- **Dependency injection:** FastAPI `Depends()` for DB sessions, settings, auth
- **Async API, sync ETL:** API routes are async; ETL pipeline uses synchronous pandas/geopandas
- **Event-driven ingest:** API publishes to Valkey stream → Worker consumes and runs ETL
- **Repository pattern:** All SQL access goes through repository classes
- **Schema separation:** `nas` (application) and `nas_lookup` (reference data) are separate Postgres schemas
- **Versioned API:** All endpoints under `/api/v1/`
- **Idempotency:** Upload and job-start endpoints support `Idempotency-Key` header
