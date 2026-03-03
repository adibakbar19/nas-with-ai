# Backend Runtime Architecture (NAS)

Runtime API is unified under `backend/app/main.py` to avoid duplicate surfaces in Swagger.

Core modules still follow layered separation:

- Service layer: `backend/app/services/address_service.py`
- Repository layer: `backend/app/repositories/address_repository.py`
- Search gateway: `backend/app/search/elasticsearch_gateway.py`
- Queue producer: `backend/app/queue/producer.py`
- Worker consumer: `backend/app/workers/queue_consumer.py`
- Dependency wiring: `backend/app/dependencies.py`
- Runtime settings: `backend/app/core/settings.py`

## Environment Variables

- `POSTGRES_DSN` (optional override)
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSCHEMA`
- `POSTCODE_BOUNDARY_TABLE` (default `postcode_boundary`)
- `ES_URL`, `ES_INDEX`
- `QUEUE_BACKEND` (`log` or `sqs`)
- `QUEUE_EVENT_LOG` (default `logs/queue/bulk_ingest_events.jsonl`)
- `QUEUE_OFFSET_FILE` (default `logs/queue/bulk_ingest_events.offset`)
- `INGEST_EXECUTION_MODE` (`local_thread` or `queue_worker`)
- `SQS_QUEUE_URL` (required when `QUEUE_BACKEND=sqs`)
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
