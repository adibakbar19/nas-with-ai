#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ -f ".env" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^\s*#' .env | xargs)
fi

export SPARK_JARS_PACKAGES="${SPARK_JARS_PACKAGES:-org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.8.1,org.datasyslab:geotools-wrapper:1.8.1-33.1,org.postgresql:postgresql:42.7.3}"
# Keep Spark driver on loopback for long local runs to avoid NIC/IP changes.
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

FAST_MODE="${FAST_MODE:-0}"
FAST_SAMPLE_ROWS="${FAST_SAMPLE_ROWS:-0}"

if [ "${FAST_MODE}" = "1" ]; then
  SKIP_LLM="${SKIP_LLM:-1}"
  SKIP_LOAD="${SKIP_LOAD:-1}"
  SKIP_ES="${SKIP_ES:-1}"
  PIPELINE_CONFIG="${PIPELINE_CONFIG:-config/config.json}"
  PIPELINE_SUCCESS="${PIPELINE_SUCCESS:-output/cleaned-fast}"
  PIPELINE_FAILED="${PIPELINE_FAILED:-output/failed-fast}"
  echo "FAST_MODE enabled: using config/config.json with lighter runtime flags."
else
  SKIP_LLM="${SKIP_LLM:-0}"
  SKIP_LOAD="${SKIP_LOAD:-0}"
  SKIP_ES="${SKIP_ES:-1}"
  PIPELINE_CONFIG="${PIPELINE_CONFIG:-config/config.json}"
  PIPELINE_SUCCESS="${PIPELINE_SUCCESS:-output/cleaned}"
  PIPELINE_FAILED="${PIPELINE_FAILED:-output/failed}"
fi

if [ "${SKIP_ENV_CHECK:-0}" != "1" ]; then
  echo "Validating required environment variables..."
  venv/bin/python validate_env.py \
    --target run_all \
    --skip-load "${SKIP_LOAD}" \
    --skip-es "${SKIP_ES}" \
    --skip-llm "${SKIP_LLM}"
fi

if [ -n "${PIPELINE_INPUT:-}" ]; then
  INPUT_PATH="${PIPELINE_INPUT}"
  SELECTED_SOURCE="${PIPELINE_SOURCE_TYPE:-auto}"
  if [ "${SELECTED_SOURCE}" = "auto" ]; then
    case "${INPUT_PATH}" in
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
  else
    SOURCE_TYPE="${SELECTED_SOURCE}"
  fi
else
  SYNTH_DIR="data/synthetic_data"
  SELECTED_SOURCE="${PIPELINE_SOURCE_TYPE:-auto}"
  if [ -d "${SYNTH_DIR}" ]; then
    case "${SELECTED_SOURCE}" in
      auto)
        if compgen -G "${SYNTH_DIR}/*.csv" > /dev/null; then
          INPUT_PATH="${SYNTH_DIR}/*.csv"
          SOURCE_TYPE="csv"
        elif compgen -G "${SYNTH_DIR}/*.json" > /dev/null; then
          INPUT_PATH="${SYNTH_DIR}/*.json"
          SOURCE_TYPE="json"
        elif compgen -G "${SYNTH_DIR}/*.xlsx" > /dev/null; then
          INPUT_PATH="$(ls -1 "${SYNTH_DIR}"/*.xlsx | head -n 1)"
          SOURCE_TYPE="excel"
        elif compgen -G "${SYNTH_DIR}/*.xls" > /dev/null; then
          INPUT_PATH="$(ls -1 "${SYNTH_DIR}"/*.xls | head -n 1)"
          SOURCE_TYPE="excel"
        else
          echo "No supported synthetic files found in ${SYNTH_DIR} (csv/json/xlsx/xls)."
          exit 1
        fi
        ;;
      csv)
        if compgen -G "${SYNTH_DIR}/*.csv" > /dev/null; then
          INPUT_PATH="${SYNTH_DIR}/*.csv"
          SOURCE_TYPE="csv"
        else
          echo "PIPELINE_SOURCE_TYPE=csv but no CSV files found in ${SYNTH_DIR}."
          exit 1
        fi
        ;;
      json)
        if compgen -G "${SYNTH_DIR}/*.json" > /dev/null; then
          INPUT_PATH="${SYNTH_DIR}/*.json"
          SOURCE_TYPE="json"
        else
          echo "PIPELINE_SOURCE_TYPE=json but no JSON files found in ${SYNTH_DIR}."
          exit 1
        fi
        ;;
      excel|xlsx)
        if compgen -G "${SYNTH_DIR}/*.xlsx" > /dev/null; then
          INPUT_PATH="$(ls -1 "${SYNTH_DIR}"/*.xlsx | head -n 1)"
          SOURCE_TYPE="excel"
        elif compgen -G "${SYNTH_DIR}/*.xls" > /dev/null; then
          INPUT_PATH="$(ls -1 "${SYNTH_DIR}"/*.xls | head -n 1)"
          SOURCE_TYPE="excel"
        else
          echo "PIPELINE_SOURCE_TYPE=${SELECTED_SOURCE} but no Excel files found in ${SYNTH_DIR}."
          exit 1
        fi
        ;;
      *)
        echo "Unsupported PIPELINE_SOURCE_TYPE=${SELECTED_SOURCE}. Use auto/csv/json/excel/xlsx."
        exit 1
        ;;
    esac
  else
    INPUT_PATH="data/raw/Sample Data.xlsx"
    SOURCE_TYPE="excel"
  fi
fi

SHEET_VALUE="${PIPELINE_SHEET:-0}"
JSON_MULTILINE="${PIPELINE_JSON_MULTILINE:-1}"
RESUME_MODE="${PIPELINE_RESUME:-0}"
CHECKPOINT_ROOT="${PIPELINE_CHECKPOINT_ROOT:-}"
STATUS_PATH="${PIPELINE_STATUS_PATH:-}"
RESUME_FAILED_ONLY="${PIPELINE_RESUME_FAILED_ONLY:-0}"

if [ "${FAST_MODE}" = "1" ] && [ "${FAST_SAMPLE_ROWS}" -gt 0 ] && [ "${SOURCE_TYPE}" = "csv" ]; then
  FIRST_CSV=""
  if [ -d "${INPUT_PATH}" ]; then
    FIRST_CSV="$(find "${INPUT_PATH}" -maxdepth 1 -name '*.csv' | head -n 1 || true)"
  else
    FIRST_CSV="$(compgen -G "${INPUT_PATH}" | head -n 1 || true)"
  fi
  if [ -n "${FIRST_CSV}" ]; then
    SAMPLE_PATH="${TMPDIR:-/tmp}/nas_fast_sample.csv"
    head -n "$((FAST_SAMPLE_ROWS + 1))" "${FIRST_CSV}" > "${SAMPLE_PATH}"
    INPUT_PATH="${SAMPLE_PATH}"
    echo "FAST_MODE sample enabled: using ${FAST_SAMPLE_ROWS} rows from ${FIRST_CSV}."
  fi
fi

if [ "${SKIP_LOAD}" != "1" ]; then
  echo "Starting Postgres with docker compose..."
  docker compose up -d postgres
else
  echo "Skipping Postgres startup (SKIP_LOAD=${SKIP_LOAD})."
fi

if [ "${SKIP_ES}" != "1" ]; then
  echo "Starting Elasticsearch with docker compose profile 'search'..."
  docker compose --profile search up -d elasticsearch
else
  echo "Skipping Elasticsearch startup (SKIP_ES=${SKIP_ES})."
fi

echo "Running Spark pipeline (${SOURCE_TYPE}) from ${INPUT_PATH}..."
PIPELINE_ARGS=(
  --input "${INPUT_PATH}"
  --source-type "${SOURCE_TYPE}"
  --success "${PIPELINE_SUCCESS}"
  --failed "${PIPELINE_FAILED}"
  --config "${PIPELINE_CONFIG}"
)
if [ "${SOURCE_TYPE}" = "excel" ] || [ "${SOURCE_TYPE}" = "xlsx" ]; then
  PIPELINE_ARGS+=(--sheet "${SHEET_VALUE}")
elif [ "${SOURCE_TYPE}" = "json" ] && [ "${JSON_MULTILINE}" = "1" ]; then
  PIPELINE_ARGS+=(--multiline)
fi
if [ -n "${CHECKPOINT_ROOT}" ]; then
  PIPELINE_ARGS+=(--checkpoint-root "${CHECKPOINT_ROOT}")
fi
if [ -n "${STATUS_PATH}" ]; then
  PIPELINE_ARGS+=(--status-path "${STATUS_PATH}")
fi
if [ "${RESUME_MODE}" = "1" ]; then
  PIPELINE_ARGS+=(--resume)
fi
if [ "${RESUME_FAILED_ONLY}" = "1" ]; then
  PIPELINE_ARGS+=(--resume-failed-only)
fi
venv/bin/python pipeline.py "${PIPELINE_ARGS[@]}"

LLM_INPUT="${LLM_INPUT:-${PIPELINE_FAILED}}"
OPENAI_SET="no"
if [ -n "${OPENAI_API_KEY:-}" ]; then
  OPENAI_SET="yes"
fi
if [ "${SKIP_LLM}" != "1" ] && [ -n "${OPENAI_API_KEY:-}" ]; then
  echo "Running LLM enrichment on failed/low-confidence rows..."
  venv/bin/python llm_enrich.py \
    --input "${LLM_INPUT}" \
    --output data/llm_corrections.csv \
    --min-confidence 60 \
    --limit 200

  if [ -f data/llm_corrections.csv ]; then
    echo "Re-running corrections with retry_failed.py..."
    venv/bin/python retry_failed.py \
      --failed "${LLM_INPUT}" \
      --corrections data/llm_corrections.csv \
      --success output/cleaned-llm \
      --failed-out output/failed-llm \
      --require-mukim
  fi
else
  echo "Skipping LLM enrichment (SKIP_LLM=${SKIP_LLM}, OPENAI_API_KEY set=${OPENAI_SET})."
fi

if [ "${SKIP_LOAD}" != "1" ]; then
  echo "Loading cleaned data into Postgres..."
  venv/bin/python load_postgres.py \
    --input "${PIPELINE_SUCCESS}" \
    --table standardized_address \
    --mode overwrite \
    --normalized \
    --pbt-dir "data/boundary/Sempadan Kawalan PBT"
else
  echo "Skipping Postgres load (SKIP_LOAD=${SKIP_LOAD})."
fi

if [ "${SKIP_ES}" != "1" ]; then
  echo "Loading cleaned data into Elasticsearch..."
  ES_INPUT="${ES_INPUT:-${PIPELINE_SUCCESS}}"
  ES_URL="${ES_URL:-http://localhost:9200}"
  ES_INDEX="${ES_INDEX:-nas_addresses}"
  ES_BATCH_SIZE="${ES_BATCH_SIZE:-1000}"
  ES_RECREATE_INDEX="${ES_RECREATE_INDEX:-1}"
  ES_ARGS=(
    --input "${ES_INPUT}"
    --es-url "${ES_URL}"
    --index "${ES_INDEX}"
    --batch-size "${ES_BATCH_SIZE}"
  )
  if [ "${ES_RECREATE_INDEX}" = "1" ]; then
    ES_ARGS+=(--recreate-index)
  fi
  venv/bin/python load_elasticsearch.py "${ES_ARGS[@]}"
else
  echo "Skipping Elasticsearch load (SKIP_ES=${SKIP_ES})."
fi

echo "Done."
