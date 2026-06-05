"""AddressEnricher Protocol — the only interface the pipeline knows about."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class AddressEnricher(Protocol):
    """Protocol for optional address enrichment.

    Any class implementing enrich(df) → df satisfies this protocol.

    The pipeline only depends on this interface, never on a concrete
    implementation.

    To add a new enricher:
    1. Create a new class with def enrich(self, df) -> pl.DataFrame
    2. Wire it in pipeline.py main() — no other files need changing

    To remove Bedrock:
    1. Pass NoOpEnricher() instead of BedrockEnricher() in pipeline.py
    2. Delete enrichers/bedrock.py if desired
    3. Nothing else changes
    """

    def enrich(self, df: pl.DataFrame) -> pl.DataFrame:
        """Enrich a DataFrame chunk with AI-assisted address resolution.

        Receives: DataFrame after lookup enrichment
        Expected to: fill in missing fields for low-confidence rows
        Must return: DataFrame with same schema (may add columns)
        Must be: idempotent and null-safe
        """
        ...
