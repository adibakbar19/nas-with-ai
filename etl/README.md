# ETL Package Layout

The ETL package is organized around a production boundary:

- `pipeline/`: orchestration. Owns flow control, checkpoints, and calls into repositories plus transform functions.
- `extract/`: source file ingestion into data frames.
- `transform/`: address, lookup, spatial, and LLM transformation logic. Production pipeline code should pass reference data in rather than letting transforms fetch from the DB.
- `repository/`: DB access for lookup and boundary reference data.
- `load/`: final ETL writes to parquet and PostgreSQL.
- `jobs/`: bulk-upload recovery jobs only, such as retrying failed ingest rows.
- `audit/`: audit log helpers.

Shared config loading and environment validation live in `nas_core/config/` because they are used by backend, ETL, and scripts.

Use module entrypoints such as `python -m etl.pipeline`, `python -m etl.load.postgres`, and `python -m etl.jobs.retry_failed`. Avoid adding new flat modules under `etl/`.

Lookup/boundary maintenance and Elasticsearch indexing belong to the backend/API side. Temporary backend-owned maintenance commands live under `backend/app/maintenance/`; one-off environment/bootstrap utilities live under `scripts/`.
