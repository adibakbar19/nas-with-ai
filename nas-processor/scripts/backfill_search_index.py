#!/usr/bin/env python3
"""Backfill all standardized_address rows into OpenSearch.

Safe to re-run — uses index with _id=record_id so duplicates
are overwritten, not duplicated.

Usage: python scripts/backfill_search_index.py
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
    from nas_processor.src.search.config import SearchConfig
    from nas_processor.src.search.indexer import (
        build_client,
        bulk_index,
        count_all_addresses,
        ensure_index_exists,
        fetch_all_addresses,
    )

    dsn = _build_dsn()
    schema = os.environ.get("PGSCHEMA", "nas").strip() or "nas"
    config = SearchConfig.from_env()

    print(f"Connecting to OpenSearch: {config.opensearch_url}")
    print(f"Index: {config.index_name}")

    try:
        client = build_client(config.opensearch_url, config.timeout_seconds)
        ensure_index_exists(client, config.index_name)
    except Exception as exc:
        print(f"ERROR: could not connect to OpenSearch: {exc}", file=sys.stderr)
        return 1

    try:
        total = count_all_addresses(dsn=dsn, schema=schema)
    except Exception as exc:
        print(f"ERROR: could not count addresses: {exc}", file=sys.stderr)
        return 1

    print(f"Total addresses to index: {total}")
    if total == 0:
        print("Nothing to index.")
        return 0

    batch_num = 0
    indexed_total = 0
    offset = 0

    while True:
        try:
            docs = fetch_all_addresses(dsn=dsn, schema=schema, offset=offset, limit=config.batch_size)
        except Exception as exc:
            print(f"ERROR: fetch failed at offset {offset}: {exc}", file=sys.stderr)
            return 1

        if not docs:
            break

        batch_num += 1
        result = bulk_index(client, config.index_name, docs)
        indexed_total += result["indexed"]
        print(f"Batch {batch_num}: indexed {result['indexed']} (errors {result['errors']}) — total so far {indexed_total}")

        if result["errors"]:
            print(f"  WARNING: {result['errors']} errors in batch {batch_num}", file=sys.stderr)

        offset += len(docs)
        if len(docs) < config.batch_size:
            break

    print(f"\nBackfill complete: {indexed_total} addresses indexed across {batch_num} batch(es).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
