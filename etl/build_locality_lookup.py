import argparse
import csv
import json
import os
import re
from glob import glob
from typing import Dict, Iterable, Iterator, Tuple

from domain.replacements import (
    LOCALITY_ABBREVIATION_REPLACEMENTS,
    LOCALITY_INVALID_PREFIXES,
    LOCALITY_PLACEHOLDER_VALUES,
)


SOURCE_CONFIDENCE: Dict[str, int] = {
    # Manual curation is strongest.
    "MANUAL": 100,
    # Structured master sources are generally cleaner than free-form CITY strings.
    "DISTRICT": 90,
    "POSTCODE_CITY": 85,
    "MUKIM": 80,
    "CITY": 70,
}


def _normalize(text: str) -> str:
    cleaned = text.strip().upper()
    # Normalize common R&R variants before punctuation cleanup.
    cleaned = re.sub(r"\bR\s*&\s*R\b", "R&R", cleaned)
    cleaned = re.sub(r"\bR\s+R\b", "R&R", cleaned)
    for src, dst in LOCALITY_ABBREVIATION_REPLACEMENTS:
        cleaned = re.sub(rf"\b{re.escape(src)}\b", dst, cleaned)
    cleaned = re.sub(r"[^A-Z0-9& ]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    # OCR/import artifacts: numeric prefix before long alpha token (e.g. "0KUALA").
    cleaned = re.sub(r"\b\d+([A-Z]{4,})\b", r"\1", cleaned)
    # OCR/import artifacts: split single-letter token before a long token (e.g. "L ANAS" -> "LANAS").
    cleaned = re.sub(r"(?<!&)\b([A-Z])\s+([A-Z]{3,})\b", r"\1\2", cleaned)
    cleaned = cleaned.strip()
    return cleaned


def _is_valid_locality_norm(value: str) -> bool:
    if not value:
        return False
    # Must contain letters; pure numeric tokens are not locality names.
    if not re.search(r"[A-Z]", value):
        return False
    # Drop short code-like entries such as "59A", "1611B".
    if re.fullmatch(r"\d+[A-Z]?", value):
        return False
    # Drop obvious placeholders.
    if value in LOCALITY_PLACEHOLDER_VALUES:
        return False
    first_token = value.split(" ", 1)[0]
    if first_token in LOCALITY_INVALID_PREFIXES:
        return False
    return True


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            ins = curr[j - 1] + 1
            delete = prev[j] + 1
            replace = prev[j - 1] + (0 if ca == cb else 1)
            curr.append(min(ins, delete, replace))
        prev = curr
    return prev[-1]


def _is_typo_variant(a_norm: str, b_norm: str) -> bool:
    if a_norm == b_norm:
        return False
    # Spacing-only variants (e.g. "AYER LANAS" vs "AYERL ANAS" after cleanup).
    if a_norm.replace(" ", "") == b_norm.replace(" ", ""):
        return True
    a_parts = a_norm.split()
    b_parts = b_norm.split()

    # Single-token names: allow only long-token near matches.
    if len(a_parts) == 1 and len(b_parts) == 1:
        if min(len(a_norm), len(b_norm)) < 7:
            return False
        return _levenshtein(a_norm, b_norm) <= 2

    # Multi-token names: only one token may differ, and differing token must be long.
    if len(a_parts) != len(b_parts):
        return False

    diff_count = 0
    for ta, tb in zip(a_parts, b_parts):
        if ta == tb:
            continue
        if max(len(ta), len(tb)) < 6:
            return False
        if _levenshtein(ta, tb) > 2:
            return False
        diff_count += 1
        if diff_count > 1:
            return False
    return diff_count == 1


def _dedupe_typo_localities(rows: list[Tuple[str, str, str, str]]) -> list[Tuple[str, str, str, str]]:
    by_state: Dict[str, Dict[str, Dict[str, object]]] = {}
    for state_name, locality_name, locality_norm, source in rows:
        state_bucket = by_state.setdefault(state_name, {})
        item = state_bucket.setdefault(
            locality_norm,
            {
                "name": locality_name,
                "sources": set(),
                "count": 0,
            },
        )
        item["sources"].add(source)
        item["count"] = int(item["count"]) + 1

    def score(norm: str, item: Dict[str, object]) -> tuple[int, int, int]:
        sources = item["sources"]
        confidence = sum(SOURCE_CONFIDENCE.get(str(src), 0) for src in sources)
        support = int(item["count"])
        length = len(norm)
        return confidence, support, length

    # Build canonical mapping per state using conservative typo-variant clustering.
    canonical_map: Dict[Tuple[str, str], str] = {}
    for state_name, entries in by_state.items():
        norms = list(entries.keys())
        visited: set[str] = set()
        for seed in norms:
            if seed in visited:
                continue
            stack = [seed]
            component: list[str] = []
            visited.add(seed)
            while stack:
                cur = stack.pop()
                component.append(cur)
                for other in norms:
                    if other in visited:
                        continue
                    if _is_typo_variant(cur, other):
                        visited.add(other)
                        stack.append(other)

            # Pick canonical by source confidence, then support, then length.
            best = max(component, key=lambda n: score(n, entries[n]))
            for name_norm in component:
                canonical_map[(state_name, name_norm)] = best

    # Collapse variants and keep a single best row per (state, canonical_norm).
    merged: Dict[Tuple[str, str], Dict[str, object]] = {}
    for state_name, locality_name, locality_norm, source in rows:
        canon_norm = canonical_map.get((state_name, locality_norm), locality_norm)
        key = (state_name, canon_norm)
        row = merged.setdefault(
            key,
            {
                "state_name": state_name,
                "locality_name": canon_norm,
                "locality_norm": canon_norm,
                "source": source,
                "_score": SOURCE_CONFIDENCE.get(source, 0),
            },
        )
        src_score = SOURCE_CONFIDENCE.get(source, 0)
        if src_score > int(row["_score"]):
            row["source"] = source
            row["_score"] = src_score

    out: list[Tuple[str, str, str, str]] = []
    for row in merged.values():
        out.append(
            (
                str(row["state_name"]),
                str(row["locality_name"]),
                str(row["locality_norm"]),
                str(row["source"]),
            )
        )
    return out


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
            norm = _normalize(value)
            if not _is_valid_locality_norm(norm):
                continue
            yield state_name, norm, norm, source


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
            norm = _normalize(city)
            if not _is_valid_locality_norm(norm):
                continue
            yield state_name, norm, norm, "POSTCODE_CITY"


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
            norm = _normalize(district)
            if not _is_valid_locality_norm(norm):
                continue
            yield state_name, norm, norm, "DISTRICT"


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
            norm = _normalize(mukim)
            if not _is_valid_locality_norm(norm):
                continue
            yield state_name, norm, norm, "MUKIM"


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
            norm = _normalize(name)
            if not _is_valid_locality_norm(norm):
                continue
            yield state.upper(), norm, norm, "MANUAL"


def build_locality_rows(root: str, lookups_dir: str, manual_path: str | None) -> Iterable[Tuple[str, str, str, str]]:
    city_dir = os.path.join(root, "CITY")
    rows = []
    # Locality = CITY level (plus curated fallback sources), not SECTION.
    if os.path.isdir(city_dir):
        rows.extend(_iter_items(city_dir, "CITY"))
    state_map = _load_state_code_map(lookups_dir)
    rows.extend(_iter_postcode_localities(lookups_dir))
    rows.extend(_iter_district_localities(lookups_dir, state_map))
    rows.extend(_iter_mukim_localities(lookups_dir, state_map))
    if manual_path:
        rows.extend(_iter_manual_localities(manual_path))
    return _dedupe_typo_localities(rows)


def _iter_manual_sublocalities(path: str) -> Iterator[Tuple[str, str, str, str]]:
    if not os.path.exists(path):
        return iter(())
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            state = (row.get("state_name") or "").strip()
            name = (row.get("sub_locality_name") or row.get("locality_name") or "").strip()
            if not state or not name:
                continue
            norm = _normalize(name)
            if not _is_valid_locality_norm(norm):
                continue
            yield state.upper(), norm, norm, "MANUAL"


def build_sublocality_rows(root: str, manual_path: str | None = None) -> Iterable[Tuple[str, str, str, str]]:
    section_dir = os.path.join(root, "SECTION")
    rows = []
    if os.path.isdir(section_dir):
        rows.extend(_iter_items(section_dir, "SECTION"))
    if manual_path:
        rows.extend(_iter_manual_sublocalities(manual_path))
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build locality/sublocality lookups from granite_map_info-master.")
    parser.add_argument("--input", required=True, help="Path to granite_map_info-master")
    parser.add_argument("--output", default="data/lookups/locality_lookup.csv")
    parser.add_argument("--sublocality-output", default="data/lookups/sublocality_lookup.csv")
    parser.add_argument("--lookups-dir", default="data/lookups")
    parser.add_argument("--manual", default="data/lookups/locality_manual.csv")
    parser.add_argument("--sublocality-manual", default="data/lookups/sublocality_manual.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = build_locality_rows(args.input, args.lookups_dir, args.manual)
    sublocality_rows = build_sublocality_rows(args.input, args.sublocality_manual)

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

    os.makedirs(os.path.dirname(args.sublocality_output), exist_ok=True)
    seen_sub = set()
    with open(args.sublocality_output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["state_name", "sub_locality_name", "sub_locality_norm", "source"])
        for state_name, sub_locality_name, sub_locality_norm, source in sublocality_rows:
            key = (state_name, sub_locality_norm)
            if key in seen_sub:
                continue
            seen_sub.add(key)
            writer.writerow([state_name, sub_locality_name, sub_locality_norm, source])


if __name__ == "__main__":
    main()
