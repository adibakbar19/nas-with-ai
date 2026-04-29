import json
from collections.abc import Iterable
from typing import Any

import requests


INDEX_MAPPING = {
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


def _request(method: str, url: str, *, timeout_seconds: int, **kwargs) -> requests.Response:
    return requests.request(method, url, timeout=timeout_seconds, **kwargs)


def wait_for_search(es_url: str, *, timeout_seconds: int) -> None:
    response = _request("GET", es_url.rstrip("/"), timeout_seconds=timeout_seconds)
    if response.status_code >= 300:
        raise RuntimeError(f"Elasticsearch not reachable: {response.status_code} {response.text}")


def ensure_index(es_url: str, index: str, *, recreate: bool, timeout_seconds: int) -> None:
    index_url = f"{es_url.rstrip('/')}/{index}"
    if recreate:
        _request("DELETE", index_url, timeout_seconds=timeout_seconds)

    exists = _request("HEAD", index_url, timeout_seconds=timeout_seconds)
    if exists.status_code == 200:
        return
    if exists.status_code != 404:
        raise RuntimeError(f"Failed checking index: {exists.status_code} {exists.text}")

    created = _request("PUT", index_url, timeout_seconds=timeout_seconds, json=INDEX_MAPPING)
    if created.status_code >= 300:
        raise RuntimeError(f"Failed creating index: {created.status_code} {created.text}")


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

    response = _request(
        "POST",
        f"{es_url.rstrip('/')}/_bulk",
        timeout_seconds=timeout_seconds,
        data="\n".join(lines) + "\n",
        headers={"Content-Type": "application/x-ndjson"},
    )
    if response.status_code >= 300:
        raise RuntimeError(f"Bulk index failed: {response.status_code} {response.text}")

    body = response.json()
    failed = 0
    if body.get("errors"):
        for item in body.get("items", []):
            action = item.get("index", {})
            if action.get("error"):
                failed += 1
    return len(docs), failed


def refresh_index(es_url: str, index: str, *, timeout_seconds: int) -> None:
    response = _request("POST", f"{es_url.rstrip('/')}/{index}/_refresh", timeout_seconds=timeout_seconds)
    if response.status_code >= 300:
        raise RuntimeError(f"Failed refreshing index: {response.status_code} {response.text}")


def index_documents(
    docs: Iterable[dict[str, Any]],
    *,
    es_url: str,
    index: str,
    batch_size: int,
    id_field: str = "record_id",
    recreate_index: bool = False,
    timeout_seconds: int = 60,
) -> dict[str, int | str]:
    wait_for_search(es_url, timeout_seconds=timeout_seconds)
    ensure_index(es_url, index, recreate=recreate_index, timeout_seconds=timeout_seconds)

    batch: list[dict[str, Any]] = []
    total = 0
    failed = 0
    for doc in docs:
        batch.append(doc)
        if len(batch) >= batch_size:
            indexed, failed_now = bulk_index(
                es_url,
                index,
                batch,
                id_field=id_field,
                timeout_seconds=timeout_seconds,
            )
            total += indexed
            failed += failed_now
            batch.clear()

    if batch:
        indexed, failed_now = bulk_index(
            es_url,
            index,
            batch,
            id_field=id_field,
            timeout_seconds=timeout_seconds,
        )
        total += indexed
        failed += failed_now

    refresh_index(es_url, index, timeout_seconds=timeout_seconds)
    return {"indexed": total, "failed": failed, "index": index}
