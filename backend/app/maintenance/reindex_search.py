import argparse
import os

from backend.app.core.settings import get_settings
from backend.app.services.search_sync_service import SearchSyncService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild the Elasticsearch NAS address index from Postgres")
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "nas"), help="Postgres schema to index")
    parser.add_argument("--es-url", default=os.getenv("ES_URL", "http://localhost:9200"), help="Elasticsearch URL")
    parser.add_argument("--index", default=os.getenv("ES_INDEX", "nas_addresses"), help="Elasticsearch index name")
    parser.add_argument("--batch-size", type=int, default=1000, help="Bulk indexing batch size")
    parser.add_argument("--timeout-seconds", type=int, default=60, help="HTTP timeout in seconds")
    parser.add_argument("--recreate-index", action="store_true", help="Delete and recreate index before indexing")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    service = SearchSyncService(
        postgres_dsn=settings.postgres_dsn,
        postgres_schema=settings.postgres_schema,
        es_url=args.es_url.rstrip("/") or settings.es_url,
        es_index=settings.es_index,
    )
    result = service.sync_from_postgres(
        schema_name=args.schema,
        es_index=args.index,
        recreate_index=args.recreate_index,
        batch_size=args.batch_size,
        timeout_seconds=args.timeout_seconds,
    )
    print(f"Indexed documents: {result['indexed']}, failed items: {result['failed']}, index: {result['index']}")
    if int(result["failed"]) > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
