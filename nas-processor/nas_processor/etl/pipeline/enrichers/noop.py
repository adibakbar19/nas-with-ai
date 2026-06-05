"""NoOpEnricher — default passthrough, no external calls."""

from __future__ import annotations

import polars as pl

from .base import AddressEnricher


class NoOpEnricher:
    """Passthrough enricher. Does nothing.

    Used when AI enrichment is disabled (default).
    Satisfies AddressEnricher protocol.
    Zero external dependencies.
    """

    def enrich(self, df: pl.DataFrame) -> pl.DataFrame:
        return df
