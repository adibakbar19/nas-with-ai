import argparse
import json
import os

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query NAS address autocomplete from Elasticsearch")
    parser.add_argument("--q", required=True, help="Query string")
    parser.add_argument("--es-url", default=os.getenv("ES_URL", "http://localhost:9200"), help="Elasticsearch URL")
    parser.add_argument("--index", default=os.getenv("ES_INDEX", "nas_addresses"), help="Elasticsearch index")
    parser.add_argument("--size", type=int, default=10, help="Number of suggestions")
    parser.add_argument("--timeout-seconds", type=int, default=30, help="HTTP timeout in seconds")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = {
        "size": args.size,
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
                "query": args.q,
                "type": "bool_prefix",
                "fields": ["autocomplete", "autocomplete._2gram", "autocomplete._3gram"],
            }
        },
        "sort": [{"_score": "desc"}, {"confidence_score": {"order": "desc", "missing": "_last"}}],
    }
    resp = requests.post(
        f"{args.es_url.rstrip('/')}/{args.index}/_search",
        json=payload,
        timeout=args.timeout_seconds,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Search failed: {resp.status_code} {resp.text}")
    body = resp.json()
    hits = body.get("hits", {}).get("hits", [])
    for h in hits:
        src = h.get("_source", {})
        src["_score"] = h.get("_score")
        print(json.dumps(src, ensure_ascii=True))


if __name__ == "__main__":
    main()
