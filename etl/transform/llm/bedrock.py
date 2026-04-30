from __future__ import annotations

import json
import os
from typing import Any


def _as_json_dict(payload: Any) -> dict[str, Any] | None:
    return payload if isinstance(payload, dict) else None


def _as_json_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        payload = payload["items"]
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


class BedrockClaudeAddressMapper:
    def __init__(
        self,
        *,
        model_id: str | None = None,
        region_name: str | None = None,
        max_tokens: int = 4000,
        batch_size: int | None = None,
    ) -> None:
        import boto3

        self.model_id = model_id or os.getenv("BEDROCK_CLAUDE_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
        self.max_tokens = int(max_tokens)
        self.batch_size = int(batch_size or os.getenv("BEDROCK_ADDRESS_BATCH_SIZE", "25"))
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=(
                region_name
                or os.getenv("BEDROCK_AWS_REGION")
                or os.getenv("BEDROCK_REGION")
                or os.getenv("AWS_REGION")
                or os.getenv("AWS_DEFAULT_REGION")
            ),
        )

    @staticmethod
    def _extract_json(text: str) -> Any | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            object_start = raw.find("{")
            object_end = raw.rfind("}")
            array_start = raw.find("[")
            array_end = raw.rfind("]")
            if object_start >= 0 and (array_start < 0 or object_start < array_start):
                start, end = object_start, object_end
            elif array_start >= 0 and array_end > array_start:
                start, end = array_start, array_end
            else:
                start, end = object_start, object_end
            if start < 0 or end <= start:
                return None
            try:
                return json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                return None

    @staticmethod
    def _system_prompt() -> str:
        return (
            "You normalize and map Malaysian addresses. Return JSON only. "
            "Resolve common abbreviations and spelling variations such as Jln/Jalan, Psn/Persiaran, "
            "Kg/Kampung, and state abbreviations. Preserve meaning and do not invent fields. "
            "Use null when a component cannot be inferred. Keep one output item per input record."
        )

    @staticmethod
    def _user_prompt(address: str, context: dict[str, Any]) -> str:
        return json.dumps(
            {
                "task": "Normalize the address and infer Malaysian state, district, mukim, locality, and postcode where possible.",
                "address": address,
                "current_parser_context": context,
                "response_schema": {
                    "corrected_address": "string",
                    "state_name": "string or null",
                    "district_name": "string or null",
                    "mukim_name": "string or null",
                    "locality_name": "string or null",
                    "postcode": "5 digit string or null",
                    "confidence_score": "number from 0.0 to 1.0",
                    "reason": "short string",
                },
            },
            ensure_ascii=True,
        )

    @staticmethod
    def _batch_prompt(records: list[dict[str, str]], context: dict[str, Any] | None = None) -> str:
        return json.dumps(
            {
                "task": (
                    "Parse these Malaysian addresses into structured JSON. "
                    "Return exactly one item per input record in the same order."
                ),
                "records": records,
                "context": context or {},
                "response_schema": {
                    "items": [
                        {
                            "record_id": "same record_id from input",
                            "corrected_address": "string",
                            "premise_no": "string or null",
                            "lot_no": "string or null",
                            "unit_no": "string or null",
                            "floor_no": "string or null",
                            "building_name": "string or null",
                            "street_name": "string or null",
                            "locality_name": "string or null",
                            "district_name": "string or null",
                            "mukim_name": "string or null",
                            "state_name": "string or null",
                            "postcode": "5 digit string or null",
                            "confidence_score": "number from 0.0 to 1.0",
                            "reason": "short string",
                        }
                    ]
                },
            },
            ensure_ascii=True,
        )

    def _invoke_text(self, text: str) -> Any | None:
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": self.max_tokens,
            "temperature": 0,
            "system": self._system_prompt(),
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": text}],
                }
            ],
        }
        response = self._client.invoke_model(
            modelId=self.model_id,
            body=json.dumps(body).encode("utf-8"),
            contentType="application/json",
            accept="application/json",
        )
        payload = json.loads(response["body"].read().decode("utf-8"))
        content = payload.get("content") or []
        response_text = "\n".join(str(item.get("text", "")) for item in content if isinstance(item, dict))
        return self._extract_json(response_text)

    def map_address(self, *, address: str, context: dict[str, Any]) -> dict[str, Any] | None:
        payload = self._invoke_text(self._user_prompt(address, context))
        return _as_json_dict(payload)

    def map_addresses(
        self,
        *,
        records: list[dict[str, str]],
        context: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        batch_size = max(1, self.batch_size)
        for offset in range(0, len(records), batch_size):
            chunk = records[offset : offset + batch_size]
            payload = self._invoke_text(self._batch_prompt(chunk, context=context))
            items.extend(_as_json_list(payload))
        return items


def bedrock_mapper_from_env(*, enabled_by_default: bool = False) -> BedrockClaudeAddressMapper | None:
    default = "1" if enabled_by_default else ""
    enabled = str(os.getenv("BEDROCK_ADDRESS_PARSER_ENABLED", default)).strip().lower()
    if enabled not in {"1", "true", "yes", "y", "on"}:
        return None
    return BedrockClaudeAddressMapper()
