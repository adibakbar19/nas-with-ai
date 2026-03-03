import argparse
import json
import os
from typing import Any

import pyarrow.dataset as ds
import requests


INDEXABLE_FIELDS = {
    "record_id",
    "naskod",
    "address_clean",
    "premise_no",
    "lot_no",
    "unit_no",
    "floor_no",
    "floor_level",
    "building_name",
    "street_name_prefix",
    "street_name",
    "sub_locality_1",
    "sub_locality_2",
    "sub_locality_levels",
    "postcode",
    "postcode_code",
    "postcode_name",
    "locality_name",
    "district_code",
    "district_name",
    "mukim_code",
    "mukim_name",
    "state_code",
    "state_name",
    "address_type",
    "confidence_score",
    "confidence_band",
    "validation_status",
    "reason_codes",
    "latitude",
    "longitude",
    "geom",
    "geometry",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bulk index cleaned NAS addresses into Elasticsearch")
    parser.add_argument("--input", required=True, help="Parquet file/folder path")
    parser.add_argument("--es-url", default=os.getenv("ES_URL", "http://localhost:9200"), help="Elasticsearch URL")
    parser.add_argument("--index", default=os.getenv("ES_INDEX", "nas_addresses"), help="Elasticsearch index name")
    parser.add_argument("--id-field", default="record_id", help="Field used as Elasticsearch _id")
    parser.add_argument("--batch-size", type=int, default=1000, help="Bulk batch size")
    parser.add_argument("--timeout-seconds", type=int, default=60, help="HTTP timeout in seconds")
    parser.add_argument("--recreate-index", action="store_true", help="Delete and recreate index before load")
    return parser.parse_args()


def _req(method: str, url: str, *, timeout: int, **kwargs) -> requests.Response:
    resp = requests.request(method, url, timeout=timeout, **kwargs)
    return resp


def wait_for_es(es_url: str, timeout_seconds: int) -> None:
    resp = _req("GET", es_url, timeout=timeout_seconds)
    if resp.status_code >= 300:
        raise RuntimeError(f"Elasticsearch not reachable: {resp.status_code} {resp.text}")


def ensure_index(es_url: str, index: str, *, recreate: bool, timeout_seconds: int) -> None:
    index_url = f"{es_url.rstrip('/')}/{index}"
    if recreate:
        _req("DELETE", index_url, timeout=timeout_seconds)

    exists = _req("HEAD", index_url, timeout=timeout_seconds)
    if exists.status_code == 200:
        return
    if exists.status_code not in {404}:
        raise RuntimeError(f"Failed checking index: {exists.status_code} {exists.text}")

    payload = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "dynamic": False,
            "properties": {
                "record_id": {"type": "keyword"},
                "naskod": {"type": "keyword"},
                "address_clean": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "autocomplete": {"type": "search_as_you_type"},
                "postcode": {"type": "keyword"},
                "postcode_code": {"type": "keyword"},
                "postcode_name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "locality_name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "district_code": {"type": "keyword"},
                "district_name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "mukim_code": {"type": "keyword"},
                "mukim_name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "state_code": {"type": "keyword"},
                "state_name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "street_name_prefix": {"type": "text"},
                "street_name": {"type": "text"},
                "sub_locality_1": {"type": "text"},
                "sub_locality_2": {"type": "text"},
                "sub_locality_levels": {"type": "keyword"},
                "building_name": {"type": "text"},
                "premise_no": {"type": "keyword"},
                "lot_no": {"type": "keyword"},
                "unit_no": {"type": "keyword"},
                "floor_no": {"type": "keyword"},
                "floor_level": {"type": "keyword"},
                "address_type": {"type": "keyword"},
                "confidence_score": {"type": "integer"},
                "confidence_band": {"type": "keyword"},
                "validation_status": {"type": "keyword"},
                "reason_codes": {"type": "keyword"},
                "latitude": {"type": "float"},
                "longitude": {"type": "float"},
                "geom": {"type": "keyword"},
                "geometry": {"type": "keyword"},
            },
        },
    }
    created = _req("PUT", index_url, timeout=timeout_seconds, json=payload)
    if created.status_code >= 300:
        raise RuntimeError(f"Failed creating index: {created.status_code} {created.text}")


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (value != value):
        return None
    if isinstance(value, str):
        v = value.strip()
        return v if v else None
    if isinstance(value, list):
        out = []
        for item in value:
            cleaned = _clean_value(item)
            if cleaned is not None:
                out.append(cleaned)
        return out or None
    return value


def _autocomplete_text(doc: dict[str, Any]) -> str:
    pieces = []
    for key in [
        "address_clean",
        "building_name",
        "street_name",
        "sub_locality_1",
        "sub_locality_2",
        "locality_name",
        "postcode",
        "postcode_code",
        "postcode_name",
        "district_name",
        "mukim_name",
        "state_name",
        "naskod",
    ]:
        val = doc.get(key)
        if isinstance(val, str) and val:
            pieces.append(val)
    return " ".join(dict.fromkeys(pieces))


def _iter_docs(parquet_path: str, batch_size: int):
    dataset = ds.dataset(parquet_path, format="parquet")
    selected_cols = [c for c in dataset.schema.names if c in INDEXABLE_FIELDS]
    for batch in dataset.to_batches(columns=selected_cols, batch_size=batch_size):
        for row in batch.to_pylist():
            doc = {}
            for k, v in row.items():
                cleaned = _clean_value(v)
                if cleaned is not None:
                    doc[k] = cleaned
            if not doc:
                continue
            doc["autocomplete"] = _autocomplete_text(doc)
            yield doc


def bulk_index(
    es_url: str,
    index: str,
    docs: list[dict[str, Any]],
    *,
    id_field: str,
    timeout_seconds: int,
) -> tuple[int, int]:
    if not docs:
        return 0, 0
    lines: list[str] = []
    for doc in docs:
        meta: dict[str, Any] = {"index": {"_index": index}}
        if id_field in doc:
            meta["index"]["_id"] = str(doc[id_field])
        lines.append(json.dumps(meta, ensure_ascii=True))
        lines.append(json.dumps(doc, ensure_ascii=True))
    payload = "\n".join(lines) + "\n"
    resp = _req(
        "POST",
        f"{es_url.rstrip('/')}/_bulk",
        timeout=timeout_seconds,
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Bulk index failed: {resp.status_code} {resp.text}")
    body = resp.json()
    failed = 0
    if body.get("errors"):
        for item in body.get("items", []):
            action = item.get("index", {})
            if action.get("error"):
                failed += 1
    return len(docs), failed


def main() -> None:
    args = parse_args()
    wait_for_es(args.es_url, args.timeout_seconds)
    ensure_index(
        args.es_url,
        args.index,
        recreate=args.recreate_index,
        timeout_seconds=args.timeout_seconds,
    )

    batch = []
    total = 0
    failed = 0
    for doc in _iter_docs(args.input, args.batch_size):
        batch.append(doc)
        if len(batch) >= args.batch_size:
            indexed, failed_now = bulk_index(
                args.es_url,
                args.index,
                batch,
                id_field=args.id_field,
                timeout_seconds=args.timeout_seconds,
            )
            total += indexed
            failed += failed_now
            batch.clear()
    if batch:
        indexed, failed_now = bulk_index(
            args.es_url,
            args.index,
            batch,
            id_field=args.id_field,
            timeout_seconds=args.timeout_seconds,
        )
        total += indexed
        failed += failed_now

    refresh = _req("POST", f"{args.es_url.rstrip('/')}/{args.index}/_refresh", timeout=args.timeout_seconds)
    if refresh.status_code >= 300:
        raise RuntimeError(f"Failed refreshing index: {refresh.status_code} {refresh.text}")
    print(f"Indexed documents: {total}, failed items: {failed}, index: {args.index}")
    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
