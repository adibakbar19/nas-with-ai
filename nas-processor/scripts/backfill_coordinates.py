#!/usr/bin/env python3
"""Backfill coordinates for existing standardized_address rows.

Fetches all rows WHERE latitude IS NULL, enriches from centroid cache,
batch-UPDATEs lat/lon/coordinate_level/geom, then re-indexes to OpenSearch.

Safe to re-run — idempotent (only touches rows with latitude IS NULL).

Usage:
    python scripts/backfill_coordinates.py
    python scripts/backfill_coordinates.py --batch-size 500 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_BATCH_SIZE = 1000
_LAT_MIN, _LAT_MAX = 1.0, 7.5
_LON_MIN, _LON_MAX = 99.5, 119.5


def _build_dsn() -> str:
    for key in ("POSTGRES_DSN", "DATABASE_URL"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    pwd  = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    db   = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


def _load_centroid_cache(cur) -> dict[str, dict[str, tuple[float, float]]]:
    cache: dict[str, dict] = {"mukim": {}, "postcode": {}, "district": {}, "state": {}}

    rows = cur.execute("""
        SELECT mukim_id::text,
               ST_Y(ST_Centroid(geom))::float8,
               ST_X(ST_Centroid(geom))::float8
        FROM nas_lookup.mukim WHERE geom IS NOT NULL
    """).fetchall()
    cache["mukim"] = {r[0]: (r[1], r[2]) for r in rows if r[0]}

    rows = cur.execute("""
        SELECT postcode,
               ST_Y(ST_Centroid(geom))::float8,
               ST_X(ST_Centroid(geom))::float8
        FROM nas_lookup.postcode WHERE geom IS NOT NULL AND postcode IS NOT NULL
    """).fetchall()
    cache["postcode"] = {r[0]: (r[1], r[2]) for r in rows if r[0]}

    rows = cur.execute("""
        SELECT state_code, district_code,
               ST_Y(ST_Centroid(geom))::float8,
               ST_X(ST_Centroid(geom))::float8
        FROM nas_lookup.district
        WHERE geom IS NOT NULL AND state_code IS NOT NULL AND district_code IS NOT NULL
    """).fetchall()
    cache["district"] = {
        f"{r[0]}:{r[1]}": (r[2], r[3]) for r in rows if r[0] and r[1]
    }

    rows = cur.execute("""
        SELECT state_code,
               ST_Y(ST_Centroid(geom))::float8,
               ST_X(ST_Centroid(geom))::float8
        FROM nas_lookup.state WHERE geom IS NOT NULL AND state_code IS NOT NULL
    """).fetchall()
    cache["state"] = {r[0]: (r[1], r[2]) for r in rows if r[0]}

    logger.info(
        "centroid_cache_loaded mukim=%d postcode=%d district=%d state=%d",
        len(cache["mukim"]), len(cache["postcode"]),
        len(cache["district"]), len(cache["state"]),
    )
    return cache


def _enrich_row(row: dict[str, Any], cache: dict) -> tuple[float | None, float | None, str | None]:
    try:
        mukim_id = (row.get("mukim_id") or "").strip()
        if mukim_id:
            hit = cache["mukim"].get(mukim_id)
            if hit:
                return hit[0], hit[1], "mukim_centroid"

        postcode = (row.get("postcode") or "").strip()
        if postcode:
            hit = cache["postcode"].get(postcode)
            if hit:
                return hit[0], hit[1], "postcode_centroid"

        state_code   = (row.get("state_code")   or "").strip()
        district_code = (row.get("district_code") or "").strip()
        if state_code and district_code:
            hit = cache["district"].get(f"{state_code}:{district_code}")
            if hit:
                return hit[0], hit[1], "district_centroid"

        if state_code:
            hit = cache["state"].get(state_code)
            if hit:
                return hit[0], hit[1], "state_centroid"

    except Exception:
        pass

    return None, None, None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=_BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute enrichment but do not write to DB")
    parser.add_argument("--schema", default="nas")
    args = parser.parse_args()

    dsn = _build_dsn()

    import psycopg

    # Try to import indexer for OpenSearch re-index
    try:
        import sys as _sys
        _sys.path.insert(0, "/app/nas-processor")
        from nas_processor.src.search.indexer import (
            build_client, bulk_index, ensure_index_exists
        )
        es_url = os.environ.get("ES_URL", "http://opensearch:9200")
        es_index = os.environ.get("ES_INDEX", "nas_addresses")
        os_client = build_client(es_url)
        ensure_index_exists(os_client, es_index)
        do_opensearch = True
        logger.info("opensearch_connected index=%s", es_index)
    except Exception as exc:
        logger.warning("opensearch_unavailable error=%s skipping_reindex=true", exc)
        do_opensearch = False

    schema = args.schema

    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            # Load centroid cache
            cache = _load_centroid_cache(cur)

            # Count rows to process
            total_null = cur.execute(
                f'SELECT COUNT(*) FROM "{schema}"."standardized_address" WHERE latitude IS NULL'
            ).fetchone()[0]
            logger.info("rows_to_enrich total=%d", total_null)

            if total_null == 0:
                print("No rows with NULL latitude — nothing to backfill.")
                return 0

            # Counters
            level_counts: dict[str, int] = {
                "mukim_centroid": 0, "postcode_centroid": 0,
                "district_centroid": 0, "state_centroid": 0, "none": 0,
            }
            total_enriched = 0
            batch_num = 0

            while True:
                rows = cur.execute(f"""
                    SELECT record_id, mukim_id, postcode, state_code, district_code
                    FROM "{schema}"."standardized_address"
                    WHERE latitude IS NULL
                    LIMIT {args.batch_size}
                """).fetchall()

                if not rows:
                    break

                batch_num += 1
                updates: list[tuple] = []

                for r in rows:
                    row_dict = {
                        "mukim_id":     r[1],
                        "postcode":     r[2],
                        "state_code":   r[3],
                        "district_code": r[4],
                    }
                    lat, lon, level = _enrich_row(row_dict, cache)
                    if lat is not None:
                        updates.append((lat, lon, level, r[0]))
                        level_counts[level or "none"] = level_counts.get(level or "none", 0) + 1
                    else:
                        level_counts["none"] += 1

                batch_enriched = len(updates)
                total_enriched += batch_enriched

                enriched_ids = [u[3] for u in updates]
                level_summary = " ".join(
                    f"{k}={v}" for k, v in sorted(level_counts.items()) if v > 0
                )
                print(
                    f"Batch {batch_num}: enriched {batch_enriched}/{len(rows)} rows "
                    f"({level_summary})"
                )

                if not args.dry_run and updates:
                    # Batch UPDATE lat/lon/coordinate_level
                    cur.executemany(f"""
                        UPDATE "{schema}"."standardized_address"
                        SET latitude         = %s,
                            longitude        = %s,
                            coordinate_level = %s,
                            updated_at       = NOW()
                        WHERE record_id = %s
                    """, updates)

                    # PostGIS geom UPDATE for enriched rows
                    cur.execute(f"""
                        UPDATE "{schema}"."standardized_address"
                        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
                            updated_at = NOW()
                        WHERE record_id = ANY(%s)
                          AND latitude  IS NOT NULL
                          AND longitude IS NOT NULL
                          AND geom IS NULL
                    """, (enriched_ids,))

                    conn.commit()

                    # Re-index to OpenSearch
                    if do_opensearch and enriched_ids:
                        try:
                            from nas_processor.src.search.indexer import fetch_all_addresses
                            import sqlalchemy as sa
                            sa_dsn = dsn.replace("postgresql://", "postgresql+psycopg://")
                            engine = sa.create_engine(sa_dsn)
                            with engine.connect() as sa_conn:
                                docs = sa_conn.execute(
                                    sa.text(f"""
                                        SELECT s.record_id, s.naskod, s.address_clean,
                                               s.street_name, s.building_name,
                                               s.sub_locality_1, s.sub_locality_2, s.sub_locality_3,
                                               s.locality_name, s.postcode, s.postcode_name,
                                               s.state_code, s.state_name,
                                               s.mukim_name, s.district_name, s.pbt_name,
                                               s.confidence_band, s.confidence_score,
                                               s.lifecycle_status, s.address_type_id,
                                               s.latitude, s.longitude, s.coordinate_level
                                        FROM "{schema}"."standardized_address" s
                                        WHERE s.record_id = ANY(:ids)
                                    """),
                                    {"ids": enriched_ids},
                                ).mappings().all()
                            result = bulk_index(os_client, es_index, [dict(r) for r in docs])
                            logger.info("opensearch_indexed batch=%d indexed=%d", batch_num, result["indexed"])
                        except Exception as exc:
                            logger.warning("opensearch_batch_failed error=%s", exc)

    print()
    print(f"Backfill complete: {total_enriched}/{total_null} rows enriched")
    print("Level breakdown:")
    for level, count in sorted(level_counts.items()):
        if count > 0:
            print(f"  {level}: {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
