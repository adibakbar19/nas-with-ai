"""OpenSearch address search query — autocomplete + fuzzy matching."""

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

FUZZY_SEARCH_FIELDS = [
    "address_clean",
    "postcode_name",
    "locality_name",
    "district_name",
    "mukim_name",
    "state_name",
]


def _execute_search(es_url: str, index: str, payload: dict, timeout_seconds: int) -> list[dict[str, Any]]:
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


def search_address(
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
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": query,
                            "type": "bool_prefix",
                            "fields": ["autocomplete", "autocomplete._2gram", "autocomplete._3gram"],
                            "boost": 2.0,
                        }
                    },
                    {
                        "multi_match": {
                            "query": query,
                            "fields": FUZZY_SEARCH_FIELDS,
                            "fuzziness": "AUTO",
                            "prefix_length": 1,
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
        "sort": [
            {"_score": {"order": "desc"}},
            {"confidence_score": {"order": "desc", "missing": "_last", "unmapped_type": "float"}},
        ],
    }
    return _execute_search(es_url, index, payload, timeout_seconds)
