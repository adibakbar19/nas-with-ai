"""Spatial service — geo_distance OpenSearch queries and PostGIS boundary queries.

Spatial results are empty when:
  - No addresses have location populated in OpenSearch (no coordinates indexed)
  - No addresses have geom in Postgres
Both cases return empty lists, not errors.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from sqlalchemy import text

from src.clients.cache import cache_get, cache_set
from src.clients.opensearch import get_client
from src.clients.postgres import get_engine
from config import settings

logger = logging.getLogger(__name__)

_ACTIVE_STATUSES = ["validated", "active"]

# Boundary table configuration: type → (table, geom_col, id_col, is_integer_id)
# mukim_id in mukim_boundary is TEXT (loaded from CSV); state/district IDs added as INTEGER
_BOUNDARY_TABLES: dict[str, tuple[str, str, str, bool]] = {
    "mukim":    ("mukim_boundary",    "boundary_geom", "mukim_id",    False),
    "district": ("district_boundary", "boundary_geom", "district_id", True),
    "state":    ("state_boundary",    "boundary_geom", "state_id",    True),
    "pbt":      ("pbt",               "boundary_geom", "pbt_id",      False),
}


def _hit_to_dict(hit: dict, distance_m: float | None = None) -> dict:
    src = hit.get("_source", {})
    sort_vals = hit.get("sort", [])
    dist = float(sort_vals[0]) if sort_vals else distance_m
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
        "distance_m": dist,
    }


def find_nearby(
    lat: float,
    lon: float,
    radius_m: int,
    limit: int,
    offset: int,
) -> tuple[list[dict], int, int]:
    """Find addresses within radius_m metres of the given coordinates.
    Returns (results, total, query_time_ms). Empty when no geo data is indexed.
    """
    t0 = time.time()
    try:
        query_body = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "geo_distance": {
                                "distance": f"{radius_m}m",
                                "location": {"lat": lat, "lon": lon},
                            }
                        },
                        {"terms": {"lifecycle_status": _ACTIVE_STATUSES}},
                    ]
                }
            },
            "sort": [
                {
                    "_geo_distance": {
                        "location": {"lat": lat, "lon": lon},
                        "order": "asc",
                        "unit": "m",
                    }
                }
            ],
            "from": offset,
            "size": limit,
            "track_total_hits": True,
        }

        resp = get_client().search(index=settings.opensearch_index, body=query_body)
        hits = resp.get("hits", {})
        total_val = hits.get("total", {})
        total = total_val.get("value", 0) if isinstance(total_val, dict) else int(total_val)
        results = [_hit_to_dict(h) for h in hits.get("hits", [])]
        return results, total, int((time.time() - t0) * 1000)

    except Exception:
        logger.exception("find_nearby_failed lat=%s lon=%s radius=%s", lat, lon, radius_m)
        return [], 0, int((time.time() - t0) * 1000)


def find_in_boundary(
    boundary_type: str,
    boundary_id: str,
    limit: int,
    offset: int,
) -> tuple[list[dict], int, int]:
    """Find addresses within an administrative boundary using PostGIS.
    Returns (results, total, query_time_ms). Empty when geom data is absent.
    """
    cache_key = f"boundary:{boundary_type}:{boundary_id}:{limit}:{offset}"
    t0 = time.time()

    cached = cache_get(cache_key)
    if cached is not None:
        try:
            data = json.loads(cached)
            return data["results"], data["total"], int((time.time() - t0) * 1000)
        except Exception:
            pass

    cfg = _BOUNDARY_TABLES.get(boundary_type.lower())
    if cfg is None:
        logger.warning("find_in_boundary unknown type=%s", boundary_type)
        return [], 0, int((time.time() - t0) * 1000)

    boundary_table, geom_col, id_col, is_integer_id = cfg
    nas_schema = settings.postgres_schema
    lookup_schema = settings.postgres_lookup_schema

    # Cast boundary_id to the right Python type so psycopg2 binds correctly
    try:
        typed_id: Any = int(boundary_id) if is_integer_id else boundary_id
    except (ValueError, TypeError):
        logger.warning("find_in_boundary invalid id type=%s id=%s", boundary_type, boundary_id)
        return [], 0, int((time.time() - t0) * 1000)

    try:
        stmt = text(f"""
            SELECT
                s.record_id, s.address_clean, s.postcode, s.postcode_name,
                s.state_code, s.state_name, s.locality_name, s.street_name,
                s.building_name, s.sub_locality_1, s.sub_locality_2, s.sub_locality_3,
                s.mukim_name, s.district_name, s.pbt_name,
                s.naskod, s.confidence_band, s.lifecycle_status,
                s.latitude, s.longitude,
                COUNT(*) OVER() AS total_count
            FROM "{nas_schema}"."standardized_address" s
            WHERE s.lifecycle_status NOT IN ('archived', 'inactive')
              AND s.geom IS NOT NULL
              AND ST_Within(
                s.geom,
                (
                    SELECT "{geom_col}"
                    FROM "{lookup_schema}"."{boundary_table}"
                    WHERE "{id_col}" = :boundary_id
                    LIMIT 1
                )
              )
            ORDER BY s.address_clean
            LIMIT :limit OFFSET :offset
        """)

        with get_engine().connect() as conn:
            rows = conn.execute(
                stmt,
                {"boundary_id": typed_id, "limit": limit, "offset": offset},
            ).mappings().all()

        if not rows:
            return [], 0, int((time.time() - t0) * 1000)

        total = int(rows[0]["total_count"])
        results = [
            {
                "record_id": r["record_id"],
                "address_clean": r["address_clean"],
                "postcode": r["postcode"],
                "postcode_name": r["postcode_name"],
                "state_code": r["state_code"],
                "state_name": r["state_name"],
                "locality_name": r["locality_name"],
                "street_name": r["street_name"],
                "building_name": r["building_name"],
                "sub_locality_1": r["sub_locality_1"],
                "sub_locality_2": r["sub_locality_2"],
                "sub_locality_3": r["sub_locality_3"],
                "mukim_name": r["mukim_name"],
                "district_name": r["district_name"],
                "pbt_name": r["pbt_name"],
                "naskod": r["naskod"],
                "address_type": None,
                "address_subtype": None,
                "confidence_band": r["confidence_band"],
                "lifecycle_status": r["lifecycle_status"],
                "latitude": r["latitude"],
                "longitude": r["longitude"],
                "score": None,
                "distance_m": None,
            }
            for r in rows
        ]

        cache_set(cache_key, json.dumps({"results": results, "total": total}))
        return results, total, int((time.time() - t0) * 1000)

    except Exception:
        logger.exception(
            "find_in_boundary_failed type=%s id=%s", boundary_type, boundary_id
        )
        return [], 0, int((time.time() - t0) * 1000)
