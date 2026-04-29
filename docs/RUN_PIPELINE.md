# Pipeline Run Guide

This is the production bulk-ingest runner. It does not provide file fallback, skip flags, or a parquet-only path.

## Prerequisites

- Python environment is set up (`.venv/`, or set `PYTHON_BIN`).
- Dependencies are installed with `pip install -r requirements.txt`.
- `.env` contains the production Postgres, Elasticsearch, Valkey, object storage, and audit settings.
- DB-backed lookup and boundary tables already exist in `LOOKUP_SCHEMA`.
- `PIPELINE_INPUT` points to the agency source file or glob.

## Required Runtime Path

`run_all.sh` always does this:

1. Run `etl.pipeline`.
2. Write cleaned, warning, and failed parquet outputs.
3. Load cleaned output into Postgres with `etl.load.postgres`.
4. Reindex Elasticsearch from Postgres with `backend.app.maintenance.reindex_search`.

## Run

CSV:

```bash
PIPELINE_INPUT="data/raw/agency-batch.csv" PIPELINE_SOURCE_TYPE=csv bash run_all.sh
```

JSON:

```bash
PIPELINE_INPUT="data/raw/agency-batch.json" PIPELINE_SOURCE_TYPE=json bash run_all.sh
```

Excel:

```bash
PIPELINE_INPUT="data/raw/agency-batch.xlsx" PIPELINE_SOURCE_TYPE=excel PIPELINE_SHEET=0 bash run_all.sh
```

## Resume

```bash
PIPELINE_INPUT="data/raw/agency-batch.csv" \
PIPELINE_CHECKPOINT_ROOT=output/checkpoints/job1 \
PIPELINE_RESUME=1 \
PIPELINE_RESUME_FAILED_ONLY=1 \
bash run_all.sh
```

## Useful Env Vars

- `PIPELINE_INPUT`: required source file, folder, or glob.
- `PIPELINE_SOURCE_TYPE`: `auto`, `csv`, `json`, `excel`, or `xlsx`.
- `PIPELINE_CONFIG`: default `config/config.json`.
- `PIPELINE_SUCCESS`: default `output/cleaned`.
- `PIPELINE_WARNING`: default `output/warnings`.
- `PIPELINE_FAILED`: default `output/failed`.
- `PIPELINE_CHECKPOINT_ROOT`: default `output/checkpoints/nas_bulk_ingest`.
- `PIPELINE_STATUS_PATH`: default `<checkpoint-root>/90_record_status`.
- `PIPELINE_RESUME`: default `1`.
- `PIPELINE_RESUME_FAILED_ONLY`: default `1`.
- `ES_URL`, `ES_INDEX`, `ES_SCHEMA`, `ES_BATCH_SIZE`.

## Lookup Data

Lookup and boundary data is maintained in Postgres through backend/admin workflows.
