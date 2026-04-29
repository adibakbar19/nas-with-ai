from typing import Any

import requests


SEARCH_SOURCE_FIELDS = [
    "record_id",
    "naskod",
    "address_clean",
    "postcode",
    "postcode_code",
    "postcode_name",
    "locality_name",
    "district_name",
    "mukim_name",
    "state_name",
    "confidence_score",
]


def autocomplete(
    *,
    es_url: str,
    index: str,
    query: str,
    size: int,
    timeout_seconds: int = 20,
) -> list[dict[str, Any]]:
    payload = {
        "size": size,
        "_source": SEARCH_SOURCE_FIELDS,
        "query": {
            "multi_match": {
                "query": query,
                "type": "bool_prefix",
                "fields": ["autocomplete", "autocomplete._2gram", "autocomplete._3gram"],
            }
        },
        "sort": [
            {"_score": {"order": "desc"}},
            {"confidence_score": {"order": "desc", "missing": "_last"}},
        ],
    }
    response = requests.post(
        f"{es_url.rstrip('/')}/{index}/_search",
        json=payload,
        timeout=timeout_seconds,
    )
    if response.status_code >= 300:
        raise RuntimeError(f"Elasticsearch query failed: {response.status_code} {response.text}")

    hits = []
    for item in response.json().get("hits", {}).get("hits", []):
        source = item.get("_source", {})
        source["_score"] = item.get("_score")
        hits.append(source)
    return hits
