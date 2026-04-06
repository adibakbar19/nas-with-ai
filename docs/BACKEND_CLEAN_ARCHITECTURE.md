# Backend Runtime Architecture (NAS)

Runtime API is unified under `backend/app/main.py` to avoid duplicate surfaces in Swagger.

Core modules still follow layered separation:

- Shared domain (pure Python, reusable in API/ETL/worker): `domain/`
- Ingest orchestration service: `backend/app/services/ingest_service.py`
- Ops/Search API services: `backend/app/services/ops_service.py`, `backend/app/services/search_api_service.py`
- DB read service + repository: `backend/app/services/address_read_service.py`, `backend/app/repositories/address_read_repository.py`
- Queue producer: `backend/app/queue/producer.py`
- Worker consumer: `backend/app/workers/queue_consumer.py`
- Dependency wiring: `backend/app/dependencies.py`
- Runtime settings: `backend/app/core/settings.py`

## Environment Variables

- `POSTGRES_DSN` (optional override)
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSCHEMA`
- `POSTCODE_BOUNDARY_TABLE` (default `postcode_boundary`)
- `ES_URL`, `ES_INDEX`
- `QUEUE_BACKEND` (`redis_stream`, `log`, or `sqs`)
- `QUEUE_EVENT_LOG` (default `logs/queue/bulk_ingest_events.jsonl`)
- `QUEUE_OFFSET_FILE` (default `logs/queue/bulk_ingest_events.offset`)
- `INGEST_EXECUTION_MODE` (`local_thread` or `queue_worker`)
- `SQS_QUEUE_URL` (required when `QUEUE_BACKEND=sqs`)
- `REDIS_URL` (required when `QUEUE_BACKEND=redis_stream`)
- `REDIS_STREAM_KEY` (default `bulk_ingest_events`)
- `REDIS_STREAM_GROUP` (default `bulk_ingest_workers`)
- `REDIS_STREAM_BLOCK_MS` (default `5000`)
- `REDIS_STREAM_CLAIM_IDLE_MS` (default `60000`, reclaim stale pending messages after 60s idle)
- `REDIS_STREAM_CLAIM_COUNT` (default `10`, max stale pending messages to reclaim per poll)
- `AWS_REGION` (default `ap-southeast-5`)

## Worker Run

```bash
python -m backend.app.workers.queue_consumer
```

For production, run worker as separate deployment/container and keep API stateless.

## Queue Worker Mode For Existing Ingest Endpoints

When `INGEST_EXECUTION_MODE=queue_worker`:

- `POST /api/v1/ingest/upload` queues job event instead of running Spark in API process.
- `POST /api/v1/ingest/jobs/{job_id}/start` requeues job event for worker.
- Worker process picks event and runs pipeline.

## Dependencies

`requirements.txt` now includes:

- `psycopg[binary]` for PostgreSQL/PostGIS repository access.
- `boto3` for SQS producer/consumer.

## Domain Reuse

- API and worker services call shared rules from `domain/` for normalization/scoring/validation.
- ETL uses the same domain logic for row-level address normalization and confidence scoring via Spark UDF wrappers.
- `domain/` has no FastAPI/Spark/DB/queue imports.
