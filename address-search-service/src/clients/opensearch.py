"""OpenSearch client — synchronous, singleton."""

from __future__ import annotations

import logging

from opensearchpy import OpenSearch

from config import settings

logger = logging.getLogger(__name__)

_client: OpenSearch | None = None

_LOCATION_MAPPING = {"properties": {"location": {"type": "geo_point"}}}


def get_client() -> OpenSearch:
    global _client
    if _client is None:
        _client = OpenSearch(
            hosts=[settings.opensearch_url],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            timeout=settings.opensearch_timeout,
        )
    return _client


def ensure_location_mapping() -> None:
    """Add geo_point location field to the index if not already present."""
    try:
        client = get_client()
        mapping = client.indices.get_mapping(index=settings.opensearch_index)
        props = mapping.get(settings.opensearch_index, {}).get("mappings", {}).get("properties", {})
        if "location" not in props:
            client.indices.put_mapping(index=settings.opensearch_index, body=_LOCATION_MAPPING)
            logger.info("opensearch_location_mapping_added index=%s", settings.opensearch_index)
        else:
            logger.debug("opensearch_location_mapping_exists index=%s", settings.opensearch_index)
    except Exception:
        logger.warning("opensearch_ensure_location_mapping_failed", exc_info=True)


def check_health() -> bool:
    try:
        return get_client().ping()
    except Exception:
        return False
