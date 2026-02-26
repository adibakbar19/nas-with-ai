# Pipeline Run Guide

This guide is a practical runbook for running the NAS Spark pipeline locally.

## 1) Prerequisites

- Python virtual environment is set up (`venv/`)
- Dependencies installed:
  - `pip install -r requirements.txt`
- Input data exists in one of:
  - `data/synthetic_data/*.csv`
  - `data/synthetic_data/*.json`
  - `data/synthetic_data/*.xlsx` / `*.xls`

## 2) One-Time Config

`run_all.sh` automatically loads `.env`.

Recommended Spark tuning in `.env`:

```bash
SPARK_DRIVER_MEMORY=6g
SPARK_EXECUTOR_MEMORY=6g
SPARK_SQL_SHUFFLE_PARTITIONS=64
```

## 3) Standard Run

```bash
bash run_all.sh
```

Behavior:
- Auto-detects source type from `data/synthetic_data`
- Uses `config/config.json`
- Runs full clean/validate pipeline
- Writes:
  - success parquet to `output/cleaned`
  - failed parquet to `output/failed`

## 4) Fast Development Run

```bash
FAST_MODE=1 SKIP_LOAD=1 SKIP_LLM=1 bash run_all.sh
```

Behavior:
- Uses `config/config.fast.json`
- Writes:
  - `output/cleaned-fast`
  - `output/failed-fast`

## 5) Run With Explicit Input

JSON:

```bash
PIPELINE_INPUT="data/synthetic_data/*.json" PIPELINE_SOURCE_TYPE=json bash run_all.sh
```

CSV:

```bash
PIPELINE_INPUT="data/synthetic_data/*.csv" PIPELINE_SOURCE_TYPE=csv bash run_all.sh
```

Excel:

```bash
PIPELINE_INPUT="data/raw/Sample Data.xlsx" PIPELINE_SOURCE_TYPE=excel PIPELINE_SHEET=0 bash run_all.sh
```

## 6) Resume From Checkpoints

Use checkpoints to avoid restarting from zero after failure.

First run (create checkpoints):

```bash
PIPELINE_CHECKPOINT_ROOT=output/checkpoints/job1 \
SKIP_LOAD=1 SKIP_LLM=1 \
bash run_all.sh
```

Resume run:

```bash
PIPELINE_CHECKPOINT_ROOT=output/checkpoints/job1 \
PIPELINE_RESUME=1 \
SKIP_LOAD=1 SKIP_LLM=1 \
bash run_all.sh
```

Resume failed/pending records only (record-based):

```bash
PIPELINE_CHECKPOINT_ROOT=output/checkpoints/job1 \
PIPELINE_RESUME=1 \
PIPELINE_RESUME_FAILED_ONLY=1 \
SKIP_LOAD=1 SKIP_LLM=1 \
bash run_all.sh
```

Checkpoint stages:
- `10_extract_raw`
- `20_clean`
- `30_validated_success`
- `31_validated_failed`
- `40_success_final`
- `41_failed_final`

Resume is **stage-level**:
- If a stage has `_SUCCESS`, pipeline skips it.
- If a stage failed halfway, that stage reruns from its start.

`PIPELINE_RESUME_FAILED_ONLY=1` adds **record-based filtering**:
- Pipeline uses `record_id` + status store (`<checkpoint-root>/90_record_status` by default).
- Only records not marked `DONE` are reprocessed.
- Final outputs are merged by `record_id` (when existing outputs include `record_id`).

## 7) Monitor Progress

Audit log file:

```bash
tail -f logs/nas_audit.log
```

Important events:
- `run_start`
- `stage_checkpoint_written`
- `stage_resume`
- `validation_complete`
- `write_complete`
- `run_end`

`run_end` with `"status":"ok"` means success.

## 8) Optional Postgres Load

Enable load:

```bash
SKIP_LOAD=0 bash run_all.sh
```

This runs `load_postgres.py` and writes normalized tables to schema `nas`.

## 9) Common Troubleshooting

- `_corrupt_record` only:
  - Ensure source type is JSON for `.json` input.
  - Keep `PIPELINE_JSON_MULTILINE=1` (default in `run_all.sh`).

- Long runtime:
  - Use `FAST_MODE=1` for development.
  - Use checkpoints + resume.

- `OutOfMemoryError` / heavy spatial joins:
  - Increase Spark memory in `.env`
  - Reduce workload (FAST mode / smaller input)
  - Move full run to larger cloud machine for production workloads.
