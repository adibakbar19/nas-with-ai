"""Shared constants and helpers for retry job modules."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from ..load.loader import load_success_warning_failed
from ..transform import clean_text_addresses, enrich_spatial_components, validate_addresses
from ..transform.address.naskod_utils import add_standard_naskod

if TYPE_CHECKING:
    from ..pipeline.orchestrator import ReferenceData


DERIVED_COLUMNS = {
    "address_clean",
    "premise_no",
    "street_name",
    "locality_name",
    "postcode",
    "state_code",
    "state_name",
    "district_code",
    "district_name",
    "mukim_code",
    "mukim_name",
    "mukim_id",
    "pbt_id",
    "pbt_name",
    "confidence_score",
    "validation_status",
    "error_reason",
    "error_reasons",
    "warning_reason",
    "warning_reasons",
    "reason_codes",
    "_address_norm",
    "seg2_norm",
    "seg3_norm",
    "seg4_norm",
    "seg5_norm",
}


def run_retry_pipeline(
    retry_df: pd.DataFrame,
    *,
    refs: "ReferenceData",
    address_col: str | None,
    require_mukim: bool,
    success_path: str,
    warning_path: str,
    failed_path: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    clean = clean_text_addresses(retry_df, address_col=address_col, config=refs.config, lookups=refs.lookups)
    enriched = enrich_spatial_components(
        clean,
        config=refs.config,
        postcode_boundaries=refs.postcode_boundaries,
        admin_boundaries=refs.admin_boundaries,
        pbt_boundaries=refs.pbt_boundaries,
    )
    success, warning, failed_out = validate_addresses(enriched, require_mukim=require_mukim)
    success = add_standard_naskod(success, output_col="naskod")
    warning = add_standard_naskod(warning, output_col="naskod")
    failed_out = add_standard_naskod(failed_out, output_col="naskod")
    load_success_warning_failed(success, warning, failed_out, success_path, warning_path, failed_path)
    return success, warning, failed_out
