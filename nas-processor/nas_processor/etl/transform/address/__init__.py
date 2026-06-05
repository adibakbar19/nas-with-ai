"""Address parsing, normalization, and lookup — Polars version."""

from .normalise import normalise_chunk, normalise_text, detect_address_column
from .lookup import enrich_lookup

__all__ = [
    "normalise_chunk",
    "normalise_text",
    "detect_address_column",
    "enrich_lookup",
]
