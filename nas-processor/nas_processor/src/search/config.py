"""OpenSearch connection settings for the search sync module."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class SearchConfig:
    opensearch_url: str = ""
    index_name: str = "nas_addresses"
    batch_size: int = 500
    timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> "SearchConfig":
        return cls(
            opensearch_url=os.environ.get("OPENSEARCH_URL", "http://opensearch:9200"),
            index_name=os.environ.get("OPENSEARCH_INDEX", "nas_addresses"),
        )
