from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from etl.repository.lookup_repository import load_lookup_frames
from etl.transform.address.input_normalizer import normalize_address_inputs
from etl.transform.address.unified import parse_unified_addresses
from etl.transform.llm.bedrock import bedrock_mapper_from_env
from nas_core.config.config_loader import load_config

from backend.app.services.errors import ServiceError


@lru_cache(maxsize=1)
def _load_parser_references(config_path: str) -> tuple[dict[str, Any], Any]:
    config = load_config(config_path)
    lookups = load_lookup_frames(config=config)
    return config, lookups


class AddressParseService:
    def __init__(self, *, config_path: str | None = None) -> None:
        self._config_path = config_path or os.getenv("INGEST_DEFAULT_CONFIG_PATH", "config/config.json")
        self._parser_mode = str(os.getenv("ADDRESS_PARSER_MODE", "ai_required")).strip().lower() or "ai_required"

    def _mapper_for_request(self, *, use_ai: bool):
        if self._parser_mode in {"deterministic", "legacy"}:
            return None
        if not use_ai and self._parser_mode == "ai_required":
            raise ServiceError(status_code=400, detail="AI parsing is required in production mode")
        if not use_ai:
            return None
        mapper = bedrock_mapper_from_env(enabled_by_default=self._parser_mode == "ai_required")
        if mapper is None and self._parser_mode == "ai_required":
            raise ServiceError(status_code=503, detail="AI address parser is not configured")
        return mapper

    def parse(
        self,
        *,
        address: str | None = None,
        addresses: list[str] | None = None,
        text: str | None = None,
        csv_text: str | None = None,
        csv_address_column: str | None = None,
        use_ai: bool = True,
        require_mukim: bool = True,
        ai_min_confidence: float = 0.85,
        max_records: int = 100,
    ) -> dict[str, Any]:
        try:
            records = normalize_address_inputs(
                address=address,
                addresses=addresses,
                text=text,
                csv_text=csv_text,
                csv_address_column=csv_address_column,
                max_records=max_records,
            )
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        if not records:
            raise ServiceError(status_code=400, detail="at least one address input is required")

        try:
            config, lookups = _load_parser_references(self._config_path)
            mapper = self._mapper_for_request(use_ai=use_ai)
            return parse_unified_addresses(
                records,
                config=config,
                lookups=lookups,
                llm_mapper=mapper,
                ai_min_confidence=int(ai_min_confidence * 100),
                require_mukim=require_mukim,
            )
        except ServiceError:
            raise
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"address parsing failed: {exc}") from exc
