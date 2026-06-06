"""Search service — OpenSearch-backed address search, autocomplete, and suggest."""

from __future__ import annotations

import logging
import time

from src.clients.opensearch import get_client
from config import settings

logger = logging.getLogger(__name__)

_ACTIVE_STATUSES = ["validated", "active"]


def _hit_to_dict(hit: dict, distance_m: float | None = None) -> dict:
    src = hit.get("_source", {})
    return {
        "record_id": src.get("record_id", hit.get("_id", "")),
        "naskod": src.get("naskod"),
        "address_clean": src.get("address_clean"),
        "street_name": src.get("street_name"),
        "building_name": src.get("building_name"),
        "sub_locality_1": src.get("sub_locality_1"),
        "sub_locality_2": src.get("sub_locality_2"),
        "sub_locality_3": src.get("sub_locality_3"),
        "locality_name": src.get("locality_name"),
        "postcode": src.get("postcode"),
        "postcode_name": src.get("postcode_name"),
        "state_code": src.get("state_code"),
        "state_name": src.get("state_name"),
        "mukim_name": src.get("mukim_name"),
        "district_name": src.get("district_name"),
        "pbt_name": src.get("pbt_name"),
        "address_type": None,
        "address_subtype": None,
        "confidence_band": src.get("confidence_band"),
        "lifecycle_status": src.get("lifecycle_status"),
        "latitude": src.get("latitude"),
        "longitude": src.get("longitude"),
        "score": hit.get("_score"),
        "distance_m": distance_m,
    }


def search_addresses(
    q: str | None,
    postcode: str | None = None,
    state_code: str | None = None,
    mukim_id: str | None = None,
    pbt_id: str | None = None,
    address_type: str | None = None,
    confidence_band: str | None = None,
    lifecycle_status: str | None = None,
    limit: int = 10,
    offset: int = 0,
) -> tuple[list[dict], int, int]:
    """Returns (results, total, query_time_ms). Never raises."""
    t0 = time.time()
    try:
        must: list[dict] = []
        filters: list[dict] = [
            {"terms": {"lifecycle_status": _ACTIVE_STATUSES}}
        ]

        if q and q.strip():
            must.append({
                "multi_match": {
                    "query": q.strip(),
                    "fields": [
                        "address_clean^3",
                        "street_name^2",
                        "building_name^2",
                        "locality_name^1.5",
                        "sub_locality_1",
                        "postcode",
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                    "prefix_length": 2,
                }
            })

        if postcode:
            filters.append({"term": {"postcode": postcode}})
        if state_code:
            filters.append({"term": {"state_code": state_code}})
        if confidence_band:
            filters.append({"term": {"confidence_band": confidence_band.upper()}})
        if lifecycle_status:
            filters.append({"term": {"lifecycle_status": lifecycle_status}})

        query_body: dict = {
            "query": {
                "bool": {
                    "must": must if must else [{"match_all": {}}],
                    "filter": filters,
                }
            },
            "from": offset,
            "size": limit,
            "track_total_hits": True,
        }

        if not q:
            query_body["sort"] = [{"address_clean.keyword": {"order": "asc"}}]

        resp = get_client().search(index=settings.opensearch_index, body=query_body)
        hits = resp.get("hits", {})
        total_val = hits.get("total", {})
        total = total_val.get("value", 0) if isinstance(total_val, dict) else int(total_val)
        results = [_hit_to_dict(h) for h in hits.get("hits", [])]
        elapsed_ms = int((time.time() - t0) * 1000)
        return results, total, elapsed_ms

    except Exception:
        logger.exception("search_addresses_failed q=%r", q)
        return [], 0, int((time.time() - t0) * 1000)


def autocomplete_addresses(
    q: str,
    state_code: str | None = None,
    limit: int = 10,
) -> tuple[list[str], int]:
    """Replicate the exact autocomplete-service query for search_as_you_type.
    Returns (suggestions, query_time_ms). Never raises.
    """
    t0 = time.time()
    try:
        normalized = q.strip().lower()
        if not normalized:
            return [], 0

        filters: list[dict] = [{"terms": {"lifecycle_status": _ACTIVE_STATUSES}}]
        if state_code:
            filters.append({"term": {"state_code": state_code}})

        query_body: dict = {
            "size": limit,
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": normalized,
                                "type": "bool_prefix",
                                "fields": [
                                    "autocomplete",
                                    "autocomplete._2gram",
                                    "autocomplete._3gram",
                                ],
                            }
                        }
                    ],
                    "filter": filters,
                }
            },
            "_source": ["address_clean"],
        }

        resp = get_client().search(index=settings.opensearch_index, body=query_body)
        suggestions = [
            h["_source"]["address_clean"]
            for h in resp.get("hits", {}).get("hits", [])
            if h.get("_source", {}).get("address_clean")
        ]
        return suggestions, int((time.time() - t0) * 1000)

    except Exception:
        logger.exception("autocomplete_failed q=%r", q)
        return [], int((time.time() - t0) * 1000)


def suggest_addresses(
    address: str,
    limit: int = 5,
) -> tuple[list[dict], int]:
    """Fuzzy suggestion search. Returns (suggestions, query_time_ms). Never raises."""
    t0 = time.time()
    try:
        if not address.strip():
            return [], 0

        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": address.strip(),
                                "fields": [
                                    "address_clean^3",
                                    "street_name^2",
                                    "locality_name^1.5",
                                    "building_name",
                                ],
                                "fuzziness": 2,
                                "prefix_length": 1,
                            }
                        }
                    ],
                    "filter": [{"terms": {"lifecycle_status": _ACTIVE_STATUSES}}],
                }
            },
            "size": limit,
            "track_total_hits": True,
        }

        resp = get_client().search(index=settings.opensearch_index, body=query_body)
        results = [_hit_to_dict(h) for h in resp.get("hits", {}).get("hits", [])]
        return results, int((time.time() - t0) * 1000)

    except Exception:
        logger.exception("suggest_failed address=%r", address)
        return [], int((time.time() - t0) * 1000)
