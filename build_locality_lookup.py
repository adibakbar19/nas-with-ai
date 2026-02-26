import argparse
import csv
import json
import os
import re
from glob import glob
from typing import Iterable, Iterator, Tuple, Dict


def _normalize(text: str) -> str:
    cleaned = text.strip().upper()
    cleaned = re.sub(r"[^A-Z0-9 ]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _iter_items(folder: str, source: str) -> Iterator[Tuple[str, str, str, str]]:
    for path in sorted(glob(os.path.join(folder, "*.json"))):
        state_name = os.path.splitext(os.path.basename(path))[0].upper()
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)

        items = None
        if isinstance(data, dict) and data:
            items = next(iter(data.values()))
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, str):
                continue
            value = item.strip()
            if not value:
                continue
            yield state_name, value, _normalize(value), source


def _load_state_code_map(lookups_dir: str) -> Dict[str, str]:
    path = os.path.join(lookups_dir, "state_codes.csv")
    mapping: Dict[str, str] = {}
    if not os.path.exists(path):
        return mapping
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            code = (row.get("state_code") or "").strip()
            name = (row.get("state_name") or "").strip()
            if code and name:
                mapping[code] = name.upper()
    return mapping


def _iter_postcode_localities(lookups_dir: str) -> Iterator[Tuple[str, str, str, str]]:
    path = os.path.join(lookups_dir, "postcodes.csv")
    if not os.path.exists(path):
        return iter(())
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            city = (row.get("city") or "").strip()
            state = (row.get("state") or "").strip()
            if not city or not state:
                continue
            state_name = state.upper()
            yield state_name, city, _normalize(city), "POSTCODE_CITY"


def _iter_district_localities(lookups_dir: str, state_map: Dict[str, str]) -> Iterator[Tuple[str, str, str, str]]:
    path = os.path.join(lookups_dir, "district_codes.csv")
    if not os.path.exists(path):
        return iter(())
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            district = (row.get("district_name") or "").strip()
            state_code = (row.get("state_code") or "").strip()
            if not district or not state_code:
                continue
            state_name = state_map.get(state_code)
            if not state_name:
                continue
            yield state_name, district, _normalize(district), "DISTRICT"


def _iter_mukim_localities(lookups_dir: str, state_map: Dict[str, str]) -> Iterator[Tuple[str, str, str, str]]:
    path = os.path.join(lookups_dir, "mukim_codes.csv")
    if not os.path.exists(path):
        return iter(())
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            mukim = (row.get("mukim_name") or "").strip()
            state_code = (row.get("state_code") or "").strip()
            if not mukim or not state_code:
                continue
            state_name = state_map.get(state_code)
            if not state_name:
                continue
            yield state_name, mukim, _normalize(mukim), "MUKIM"


def _iter_manual_localities(path: str) -> Iterator[Tuple[str, str, str, str]]:
    if not os.path.exists(path):
        return iter(())
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            state = (row.get("state_name") or "").strip()
            name = (row.get("locality_name") or "").strip()
            if not state or not name:
                continue
            yield state.upper(), name, _normalize(name), "MANUAL"


def build_locality_rows(root: str, lookups_dir: str, manual_path: str | None) -> Iterable[Tuple[str, str, str, str]]:
    city_dir = os.path.join(root, "CITY")
    section_dir = os.path.join(root, "SECTION")
    rows = []
    if os.path.isdir(city_dir):
        rows.extend(_iter_items(city_dir, "CITY"))
    if os.path.isdir(section_dir):
        rows.extend(_iter_items(section_dir, "SECTION"))
    state_map = _load_state_code_map(lookups_dir)
    rows.extend(_iter_postcode_localities(lookups_dir))
    rows.extend(_iter_district_localities(lookups_dir, state_map))
    rows.extend(_iter_mukim_localities(lookups_dir, state_map))
    if manual_path:
        rows.extend(_iter_manual_localities(manual_path))
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build locality lookup from granite_map_info-master.")
    parser.add_argument("--input", required=True, help="Path to granite_map_info-master")
    parser.add_argument("--output", default="data/lookups/locality_lookup.csv")
    parser.add_argument("--lookups-dir", default="data/lookups")
    parser.add_argument("--manual", default="data/lookups/locality_manual.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = build_locality_rows(args.input, args.lookups_dir, args.manual)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    seen = set()
    with open(args.output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["state_name", "locality_name", "locality_norm", "source"])
        for row in rows:
            key = (row[0], row[2])
            if key in seen:
                continue
            seen.add(key)
            writer.writerow(row)


if __name__ == "__main__":
    main()
