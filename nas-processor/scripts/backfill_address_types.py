#!/usr/bin/env python3
"""Backfill address_type_id for existing standardized_address rows.

Reads rows where address_type_id IS NULL, runs detect_address_type on each,
and updates the DB. Safe to re-run.

Usage: python scripts/backfill_address_types.py
"""

from __future__ import annotations

import os
import sys


def _build_dsn() -> str:
    for key in ("POSTGRES_DSN", "DATABASE_URL"):
        raw = os.environ.get(key, "").strip()
        if raw:
            return raw
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    password = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    database = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def main() -> int:
    import sqlalchemy as sa
    from nas_processor.etl.pipeline.stages.classify import detect_address_type, load_type_id_map

    dsn = _build_dsn()
    schema = os.environ.get("PGSCHEMA", "nas").strip() or "nas"
    batch_size = 1000

    print(f"Loading address type map from {schema}.address_type...")
    type_id_map = load_type_id_map(dsn=dsn, schema=schema)
    if not type_id_map:
        print("ERROR: address_type table is empty or unreachable.", file=sys.stderr)
        return 1
    print(f"  Loaded {len(type_id_map)} address type entries.")

    engine = sa.create_engine(dsn, pool_pre_ping=True)

    # Count unclassified rows
    with engine.connect() as conn:
        total_unclassified = conn.execute(
            sa.text(f'SELECT COUNT(*) FROM "{schema}".standardized_address WHERE address_type_id IS NULL')
        ).scalar() or 0

    print(f"Rows with address_type_id IS NULL: {total_unclassified}")
    if total_unclassified == 0:
        print("Nothing to backfill.")
        return 0

    fetch_cols = "record_id, address_clean, building_name, street_name, " \
                 "sub_locality_1, sub_locality_2, premise_no, unit_no, floor_no, postcode"

    total_processed = 0
    total_classified = 0
    batch_num = 0
    offset = 0

    while True:
        with engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    f'SELECT {fetch_cols} FROM "{schema}".standardized_address '
                    f"WHERE address_type_id IS NULL "
                    f"ORDER BY record_id "
                    f"LIMIT :limit OFFSET :offset"
                ),
                {"limit": batch_size, "offset": offset},
            ).mappings().all()

        if not rows:
            break

        batch_num += 1
        batch_classified = 0
        updates: list[dict] = []

        for row in rows:
            total_processed += 1
            t, s = detect_address_type(dict(row))
            if t is not None:
                type_id = type_id_map.get((t, s))
                if type_id is not None:
                    updates.append({"record_id": row["record_id"], "type_id": type_id})
                    batch_classified += 1

        if updates:
            with engine.begin() as conn:
                conn.execute(
                    sa.text(
                        f'UPDATE "{schema}".standardized_address '
                        f"SET address_type_id = :type_id "
                        f"WHERE record_id = :record_id AND address_type_id IS NULL"
                    ),
                    updates,
                )

        total_classified += batch_classified
        print(f"Batch {batch_num}: classified {batch_classified}/{len(rows)} rows")

        offset += len(rows)
        if len(rows) < batch_size:
            break

    print(f"\nSummary: {total_processed} rows processed, "
          f"{total_classified} classified, "
          f"{total_processed - total_classified} unclassified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
