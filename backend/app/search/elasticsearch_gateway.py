from typing import Any

import requests


class ElasticsearchSearchGateway:
    def __init__(self, es_url: str, index: str) -> None:
        self._es_url = es_url.rstrip("/")
        self._index = index

    def autocomplete(self, query: str, size: int) -> list[dict[str, Any]]:
        payload = {
            "size": size,
            "_source": [
                "record_id",
                "naskod",
                "address_clean",
                "postcode",
                "locality_name",
                "district_name",
                "state_name",
                "confidence_score",
            ],
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
            f"{self._es_url}/{self._index}/_search",
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        body = response.json()
        results: list[dict[str, Any]] = []
        for item in body.get("hits", {}).get("hits", []):
            source = item.get("_source", {})
            source["score"] = item.get("_score")
            results.append(source)
        return results
