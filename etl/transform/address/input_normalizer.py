from __future__ import annotations

from dataclasses import dataclass, field
from io import StringIO
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class AddressInputRecord:
    record_id: str
    address: str
    metadata: dict[str, Any] = field(default_factory=dict)


def _clean_address(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _append_address(
    records: list[AddressInputRecord],
    value: Any,
    *,
    source: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    address = _clean_address(value)
    if not address:
        return
    record_id = f"addr-{len(records) + 1}"
    records.append(
        AddressInputRecord(
            record_id=record_id,
            address=address,
            metadata={"input_source": source, **(metadata or {})},
        )
    )


def _records_from_csv_text(csv_text: str, *, address_column: str | None = None) -> list[AddressInputRecord]:
    text = str(csv_text or "").strip()
    if not text:
        return []
    df = pd.read_csv(StringIO(text), dtype=str)
    if df.empty:
        return []

    selected_col = address_column
    if selected_col and selected_col not in df.columns:
        raise ValueError(f"CSV address column not found: {selected_col}")
    if not selected_col:
        preferred = ["address", "full_address", "alamat", "raw_address", "source_address", "address_clean"]
        by_lower = {str(col).strip().lower(): col for col in df.columns}
        selected_col = next((by_lower[name] for name in preferred if name in by_lower), None)
    if not selected_col:
        if len(df.columns) == 1:
            selected_col = str(df.columns[0])
        else:
            raise ValueError(
                "CSV input must include an address column or specify csv_address_column. "
                f"Columns found: {list(df.columns)}"
            )

    records: list[AddressInputRecord] = []
    for idx, row in df.iterrows():
        metadata = {str(col): row.get(col) for col in df.columns if col != selected_col and pd.notna(row.get(col))}
        _append_address(records, row.get(selected_col), source="csv", metadata={"csv_row": int(idx) + 1, **metadata})
    return records


def normalize_address_inputs(
    *,
    address: str | None = None,
    addresses: list[str] | None = None,
    text: str | None = None,
    csv_text: str | None = None,
    csv_address_column: str | None = None,
    max_records: int | None = None,
) -> list[AddressInputRecord]:
    records: list[AddressInputRecord] = []
    _append_address(records, address, source="single")

    for value in addresses or []:
        _append_address(records, value, source="array")

    if text:
        for line_no, line in enumerate(str(text).splitlines(), start=1):
            _append_address(records, line, source="line_text", metadata={"line_no": line_no})

    if csv_text:
        records.extend(_records_from_csv_text(csv_text, address_column=csv_address_column))

    if max_records is not None and len(records) > max_records:
        raise ValueError(f"Too many addresses: {len(records)} supplied, max_records={max_records}")
    return records


def records_to_dataframe(records: list[AddressInputRecord]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "record_id": record.record_id,
                "address": record.address,
                "input_metadata": record.metadata,
            }
            for record in records
        ]
    )
