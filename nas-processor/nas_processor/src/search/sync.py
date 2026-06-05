"""Search sync orchestration — called by the worker after each completed job."""

from __future__ import annotations

import logging
import os

from nas_processor.src.search.config import SearchConfig
from nas_processor.src.search.indexer import (
    build_client,
    bulk_index,
    ensure_index_exists,
    fetch_addresses_for_job,
)

logger = logging.getLogger(__name__)


def sync_job_to_search(
    job_id: str,
    *,
    dsn: str,
    search_config: SearchConfig,
) -> dict:
    """Index all addresses from a completed job into OpenSearch.

    Called by the worker after the pipeline completes.
    Returns {"indexed": N, "errors": M, "skipped": K}.
    Never raises — logs errors and returns counts.
    """
    schema = os.environ.get("PGSCHEMA", "nas").strip() or "nas"

    try:
        client = build_client(search_config.opensearch_url, search_config.timeout_seconds)
    except Exception:
        logger.exception("search_client_init_failed job_id=%s", job_id)
        return {"indexed": 0, "errors": 0, "skipped": 0}

    try:
        ensure_index_exists(client, search_config.index_name)
    except Exception:
        logger.exception("ensure_index_failed job_id=%s index=%s", job_id, search_config.index_name)
        return {"indexed": 0, "errors": 0, "skipped": 0}

    try:
        docs = fetch_addresses_for_job(job_id, dsn=dsn, schema=schema)
    except Exception:
        logger.exception("fetch_addresses_failed job_id=%s", job_id)
        return {"indexed": 0, "errors": 0, "skipped": 0}

    if not docs:
        logger.info("search_sync_no_addresses job_id=%s", job_id)
        return {"indexed": 0, "errors": 0, "skipped": 0}

    result = bulk_index(client, search_config.index_name, docs)
    return {**result, "skipped": 0}
