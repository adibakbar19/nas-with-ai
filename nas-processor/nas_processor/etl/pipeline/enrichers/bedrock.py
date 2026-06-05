"""BedrockEnricher — optional AWS Bedrock AI enrichment.

NOT imported by __init__.py intentionally.
Only instantiated when BEDROCK_ADDRESS_PARSER_ENABLED=1.

To remove Bedrock from the system entirely:
1. Delete this file
2. Remove BedrockEnricher instantiation from pipeline.py main()
3. Nothing else changes — pipeline uses AddressEnricher Protocol
"""

from __future__ import annotations

import logging
import os
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


class BedrockEnricher:
    """AWS Bedrock AI enrichment for low-confidence address rows.

    Only processes rows where confidence_score < threshold.
    Rows above threshold are passed through unchanged.

    Requires:
    - boto3 installed
    - AWS credentials configured
    - BEDROCK_MODEL_ID env var (default: anthropic.claude-3-haiku-20240307-v1:0)
    - BEDROCK_ADDRESS_PARSER_ENABLED=1
    - BEDROCK_CONFIDENCE_THRESHOLD env var (default: 50)

    If boto3 is not installed or credentials fail:
    → logs warning and returns df unchanged (graceful degradation)
    """

    def __init__(
        self,
        *,
        model_id: str | None = None,
        confidence_threshold: int | None = None,
        region: str | None = None,
    ) -> None:
        self._model_id = (
            model_id
            or os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
        )
        self._threshold = (
            confidence_threshold
            or int(os.environ.get("BEDROCK_CONFIDENCE_THRESHOLD", "50"))
        )
        self._region = (
            region
            or os.environ.get("AWS_REGION", "ap-southeast-1")
        )
        self._client: Any = None

    def _get_client(self):
        """Lazy boto3 client initialization."""
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client(
                    "bedrock-runtime", region_name=self._region
                )
            except ImportError:
                logger.warning(
                    "bedrock_unavailable: boto3 not installed. "
                    "Install boto3 to enable AI enrichment."
                )
                return None
            except Exception as exc:
                logger.warning(
                    "bedrock_client_error: %s. "
                    "AI enrichment disabled.", exc
                )
                return None
        return self._client

    def enrich(self, df: pl.DataFrame) -> pl.DataFrame:
        """Enrich low-confidence rows via Bedrock Claude.

        Only processes rows with confidence_score < threshold.
        High-confidence rows pass through unchanged.
        If Bedrock is unavailable, returns df unchanged.
        """
        if "confidence_score" not in df.columns:
            return df

        client = self._get_client()
        if client is None:
            return df

        # Only process low-confidence rows
        low_conf_mask = pl.col("confidence_score") < self._threshold
        low_conf_count = df.filter(low_conf_mask).height

        if low_conf_count == 0:
            logger.debug("bedrock_skip no_low_confidence_rows")
            return df

        logger.info(
            "bedrock_enrich low_confidence_rows=%d threshold=%d",
            low_conf_count, self._threshold,
        )

        # TODO: Implement actual Bedrock API calls
        # This is a stub — implement when Bedrock integration is decided.
        # Pattern:
        # 1. Extract low-confidence rows
        # 2. Batch into groups of 20-50 addresses per prompt
        # 3. Send to Bedrock Claude with structured prompt
        # 4. Parse response → fill missing fields
        # 5. Merge results back using pl.coalesce pattern
        # 6. Re-score confidence for enriched rows

        logger.warning(
            "bedrock_stub: AI enrichment not yet implemented. "
            "Returning df unchanged."
        )
        return df
