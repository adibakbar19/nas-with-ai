# Tech Stack

## Backend (Python)

- **Framework:** FastAPI (uvicorn)
- **Database:** PostgreSQL 16 with PostGIS 3.4 (via SQLAlchemy + GeoAlchemy2)
- **Migrations:** Alembic
- **Search:** OpenSearch 2.13 (security disabled in POC)
- **Queue:** Valkey 7 (Redis-compatible streams for async job processing)
- **Object Storage:** S3 (via boto3)
- **Data Processing:** pandas, geopandas, shapely, pyarrow, openpyxl
- **Monitoring:** prometheus-client
- **Settings:** Pydantic BaseModel with env var overrides
- **Python version:** 3.14+ (based on pycache artifacts)

## Frontend (JavaScript)

- **Framework:** Vue 3
- **Build Tool:** Vite 5
- **Serving:** nginx (in Docker)

## Infrastructure

- Docker Compose for local/POC deployment
- PostGIS/PostgreSQL, OpenSearch, Valkey as containerized services
- S3 for file storage (real AWS or compatible endpoint)

## Common Commands

### Backend

```bash
# Install dependencies
python -m pip install -r requirements.txt

# Run API server locally
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload

# Run queue worker
python -m backend.app.workers.queue_consumer

# Run DB migrations
alembic upgrade head

# Run ETL pipeline
python -m etl.pipeline --input <file> --config config/config.json --success output/cleaned/ --failed output/failed/

# Reindex OpenSearch
python -m backend.app.maintenance.reindex_search --schema nas --es-url http://localhost:9200 --index nas_addresses --recreate-index

# Full pipeline + load + reindex
# ETL pipeline is now triggered via the ingest API (POST /api/v1/ingest/upload).
# The worker processes it automatically.
```

### Frontend

```bash
cd frontend-vue
npm install
npm run dev      # dev server on :5173
npm run build    # production build
```

### Docker

```bash
docker compose up -d              # start full stack
docker compose up -d --scale worker=2  # scale workers
curl http://localhost:8000/api/v1/health  # health check
```

### Maintenance Scripts

```bash
# Backfill lookup references after admin changes
python -m backend.app.maintenance.backfill_lookup_refs --entity district --from-id 7 --to-id 19 --apply

# Reassign spatial boundaries after geometry updates
python -m backend.app.maintenance.backfill_spatial_refs --apply
```
