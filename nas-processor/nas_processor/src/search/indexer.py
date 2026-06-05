"""OpenSearch indexing operations for NAS addresses.

Reads from standardized_address via SQLAlchemy, writes via opensearch-py bulk API.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

import sqlalchemy as sa
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk as os_bulk

logger = logging.getLogger(__name__)

# Fields written to each document (no PostGIS geom column)
_SELECT_COLS = """
    s.record_id, s.naskod, s.address_clean, s.street_name,
    s.building_name, s.sub_locality_1, s.sub_locality_2, s.sub_locality_3,
    s.locality_name, s.postcode, s.postcode_name, s.state_code, s.state_name,
    s.mukim_name, s.district_name, s.pbt_name,
    s.confidence_band, s.confidence_score, s.lifecycle_status, s.address_type_id
"""

# Index mappings — autocomplete must be search_as_you_type so the autocomplete-service
# can query autocomplete._2gram / autocomplete._3gram sub-fields automatically.
_INDEX_MAPPINGS = {
    "mappings": {
        "properties": {
            "autocomplete":      {"type": "search_as_you_type"},
            "address_clean":     {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
            "street_name":       {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "locality_name":     {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "postcode":          {"type": "keyword"},
            "state_code":        {"type": "keyword"},
            "naskod":            {"type": "keyword"},
            "confidence_band":   {"type": "keyword"},
            "confidence_score":  {"type": "float"},
            "lifecycle_status":  {"type": "keyword"},
            "record_id":         {"type": "keyword"},
            "address_type_id":   {"type": "integer"},
        }
    }
}


def build_client(opensearch_url: str, timeout_seconds: int = 30) -> OpenSearch:
    parsed = urlparse(opensearch_url)
    return OpenSearch(
        hosts=[{"host": parsed.hostname or "localhost", "port": parsed.port or 9200}],
        scheme=parsed.scheme or "http",
        http_compress=True,
        use_ssl=(parsed.scheme == "https"),
        verify_certs=False,
        timeout=timeout_seconds,
    )


def ensure_index_exists(client: OpenSearch, index_name: str) -> None:
    """Create the index with correct mappings if it does not already exist."""
    if client.indices.exists(index=index_name):
        return
    client.indices.create(index=index_name, body=_INDEX_MAPPINGS)
    logger.info("opensearch_index_created index=%s", index_name)


def fetch_addresses_for_job(
    job_id: str,
    *,
    dsn: str,
    schema: str = "nas",
) -> list[dict]:
    """Return standardized_address rows that belong to the given ingest job.

    Uses source_ref_id (set at ETL load time) so this works immediately after
    the pipeline completes — before the matcher sets standardized_id.
    Also covers post-match rows via standardized_id for re-index calls.
    """
    sql = sa.text(f"""
        SELECT DISTINCT {_SELECT_COLS}
        FROM "{schema}"."standardized_address" s
        WHERE s.record_id IN (
            SELECT source_ref_id
            FROM "{schema}"."raw_address"
            WHERE job_id = :job_id AND source_ref_id IS NOT NULL
            UNION
            SELECT standardized_id
            FROM "{schema}"."raw_address"
            WHERE job_id = :job_id AND standardized_id IS NOT NULL
        )
        AND s.lifecycle_status NOT IN ('archived', 'inactive')
    """)
    engine = sa.create_engine(dsn, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"job_id": job_id}).mappings().all()
        return [dict(r) for r in rows]
    finally:
        engine.dispose()


def fetch_all_addresses(
    *,
    dsn: str,
    schema: str = "nas",
    offset: int = 0,
    limit: int = 500,
) -> list[dict]:
    """Return a page of all active standardized_address rows for backfill."""
    sql = sa.text(f"""
        SELECT {_SELECT_COLS}
        FROM "{schema}"."standardized_address" s
        WHERE s.lifecycle_status NOT IN ('archived', 'inactive')
        ORDER BY s.record_id
        OFFSET :offset LIMIT :limit
    """)
    engine = sa.create_engine(dsn, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"offset": offset, "limit": limit}).mappings().all()
        return [dict(r) for r in rows]
    finally:
        engine.dispose()


def count_all_addresses(*, dsn: str, schema: str = "nas") -> int:
    """Return total active standardized_address count for backfill progress."""
    sql = sa.text(f"""
        SELECT COUNT(*) FROM "{schema}"."standardized_address"
        WHERE lifecycle_status NOT IN ('archived', 'inactive')
    """)
    engine = sa.create_engine(dsn, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            return conn.execute(sql).scalar() or 0
    finally:
        engine.dispose()


def _to_action(doc: dict, index_name: str) -> dict:
    """Convert a DB row dict to an opensearch-py bulk action."""
    address_clean = doc.get("address_clean") or ""
    return {
        "_index": index_name,
        "_id": doc["record_id"],
        "_source": {
            **doc,
            # autocomplete must be populated for search_as_you_type sub-fields
            "autocomplete": address_clean,
        },
    }


def bulk_index(
    client: OpenSearch,
    index_name: str,
    documents: list[dict],
) -> dict:
    """Bulk index documents into OpenSearch. Returns {"indexed": N, "errors": M}."""
    if not documents:
        return {"indexed": 0, "errors": 0}

    actions = [_to_action(doc, index_name) for doc in documents]

    success = 0
    failed = 0
    try:
        success, errors = os_bulk(
            client,
            actions,
            raise_on_error=False,
            raise_on_exception=False,
        )
        if errors:
            failed = len(errors)
            for err in errors[:5]:
                logger.error("bulk_index_error %s", err)
    except Exception:
        logger.exception("bulk_call_failed count=%d", len(actions))
        failed = len(actions)

    return {"indexed": success, "errors": failed}
