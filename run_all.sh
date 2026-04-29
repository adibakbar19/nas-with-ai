#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ -f ".env" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^\s*#' .env | xargs)
fi

PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
if [ ! -x "${PYTHON_BIN}" ] && [ -x "venv/bin/python" ]; then
  PYTHON_BIN="venv/bin/python"
fi

if [ -z "${PIPELINE_INPUT:-}" ]; then
  echo "PIPELINE_INPUT is required for production bulk ingest."
  exit 1
fi

PIPELINE_CONFIG="${PIPELINE_CONFIG:-config/config.json}"
PIPELINE_SUCCESS="${PIPELINE_SUCCESS:-output/cleaned}"
PIPELINE_WARNING="${PIPELINE_WARNING:-output/warnings}"
PIPELINE_FAILED="${PIPELINE_FAILED:-output/failed}"
PIPELINE_CHECKPOINT_ROOT="${PIPELINE_CHECKPOINT_ROOT:-output/checkpoints/nas_bulk_ingest}"
PIPELINE_STATUS_PATH="${PIPELINE_STATUS_PATH:-${PIPELINE_CHECKPOINT_ROOT}/90_record_status}"
PIPELINE_RESUME="${PIPELINE_RESUME:-1}"
PIPELINE_RESUME_FAILED_ONLY="${PIPELINE_RESUME_FAILED_ONLY:-1}"
ES_INDEX="${ES_INDEX:-nas_addresses}"
ES_BATCH_SIZE="${ES_BATCH_SIZE:-1000}"
ES_SCHEMA="${ES_SCHEMA:-${PGSCHEMA:-nas}}"

echo "Validating required environment variables..."
"${PYTHON_BIN}" -m scripts.validate_env --target run_all

case "${PIPELINE_SOURCE_TYPE:-auto}" in
  auto)
    case "${PIPELINE_INPUT}" in
      *.json|*.jsonl|*.ndjson)
        SOURCE_TYPE="json"
        ;;
      *.xlsx|*.xls)
        SOURCE_TYPE="excel"
        ;;
      *)
        SOURCE_TYPE="csv"
        ;;
    esac
    ;;
  csv|json|excel|xlsx)
    SOURCE_TYPE="${PIPELINE_SOURCE_TYPE}"
    ;;
  *)
    echo "Unsupported PIPELINE_SOURCE_TYPE=${PIPELINE_SOURCE_TYPE}. Use auto/csv/json/excel/xlsx."
    exit 1
    ;;
esac

echo "Running production ETL (${SOURCE_TYPE}) from ${PIPELINE_INPUT}..."
PIPELINE_ARGS=(
  --input "${PIPELINE_INPUT}"
  --source-type "${SOURCE_TYPE}"
  --success "${PIPELINE_SUCCESS}"
  --warning "${PIPELINE_WARNING}"
  --failed "${PIPELINE_FAILED}"
  --config "${PIPELINE_CONFIG}"
  --checkpoint-root "${PIPELINE_CHECKPOINT_ROOT}"
  --status-path "${PIPELINE_STATUS_PATH}"
)
if [ "${SOURCE_TYPE}" = "excel" ] || [ "${SOURCE_TYPE}" = "xlsx" ]; then
  PIPELINE_ARGS+=(--sheet "${PIPELINE_SHEET:-0}")
elif [ "${SOURCE_TYPE}" = "json" ] && [ "${PIPELINE_JSON_MULTILINE:-1}" = "1" ]; then
  PIPELINE_ARGS+=(--multiline)
fi
if [ "${PIPELINE_RESUME}" = "1" ]; then
  PIPELINE_ARGS+=(--resume)
fi
if [ "${PIPELINE_RESUME_FAILED_ONLY}" = "1" ]; then
  PIPELINE_ARGS+=(--resume-failed-only)
fi
"${PYTHON_BIN}" -m etl.pipeline "${PIPELINE_ARGS[@]}"

if [ -n "${OPENAI_API_KEY:-}" ]; then
  echo "Running LLM enrichment on failed/low-confidence rows..."
  "${PYTHON_BIN}" -m etl.transform.llm.enrich \
    --input "${PIPELINE_FAILED}" \
    --output data/llm_corrections.csv \
    --min-confidence 60 \
    --limit "${LLM_LIMIT:-200}"

  if [ -f data/llm_corrections.csv ]; then
    echo "Applying LLM corrections..."
    "${PYTHON_BIN}" -m etl.jobs.retry_failed \
      --failed "${PIPELINE_FAILED}" \
      --corrections data/llm_corrections.csv \
      --success output/cleaned-llm \
      --warning-out output/warning-llm \
      --failed-out output/failed-llm \
      --require-mukim
    PIPELINE_SUCCESS="output/cleaned-llm"
    PIPELINE_WARNING="output/warning-llm"
    PIPELINE_FAILED="output/failed-llm"
  fi
fi

echo "Loading cleaned data into Postgres..."
"${PYTHON_BIN}" -m etl.load.postgres \
  --input "${PIPELINE_SUCCESS}" \
  --table standardized_address \
  --mode overwrite \
  --normalized \
  --pbt-dir "data/boundary/Sempadan Kawalan PBT"

echo "Reindexing Elasticsearch from Postgres..."
"${PYTHON_BIN}" -m backend.app.maintenance.reindex_search \
  --es-url "${ES_URL}" \
  --index "${ES_INDEX}" \
  --schema "${ES_SCHEMA}" \
  --batch-size "${ES_BATCH_SIZE}" \
  --recreate-index

echo "Done."
