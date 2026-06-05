"""OpenSearch query logic for autocomplete."""

from __future__ import annotations

import httpx


async def query_opensearch(
    *,
    opensearch_url: str,
    domain_config: dict,
    query: str,
    limit: int,
) -> list[str]:
    """Query OpenSearch and return suggestion strings."""
    index = domain_config["index"]
    query_type = domain_config["query_type"]
    query_fields = domain_config["query_fields"]
    result_field = domain_config["result_field"]

    if query_type == "bool_prefix":
        os_query = {
            "size": limit,
            "query": {
                "multi_match": {
                    "query": query,
                    "type": "bool_prefix",
                    "fields": query_fields,
                }
            },
            "_source": [result_field],
        }
    else:
        # match_phrase_prefix for simpler indices
        os_query = {
            "size": limit,
            "query": {
                "match_phrase_prefix": {
                    query_fields[0]: query,
                }
            },
            "_source": [result_field],
        }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{opensearch_url}/{index}/_search",
            json=os_query,
        )

    if response.status_code == 404:
        # Index not found — treat as empty results
        return []
    if response.status_code >= 500:
        raise httpx.HTTPStatusError(
            f"OpenSearch error {response.status_code}",
            request=response.request,
            response=response,
        )
    if response.status_code >= 400:
        raise httpx.HTTPStatusError(
            f"OpenSearch bad request {response.status_code}: {response.text}",
            request=response.request,
            response=response,
        )

    data = response.json()
    hits = data.get("hits", {}).get("hits", [])

    results = []
    for hit in hits:
        value = hit.get("_source", {}).get(result_field)
        if value and isinstance(value, str):
            results.append(value)

    return results
