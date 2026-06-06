"""Address type classification stage — DMS 2039 based detection.

detect_address_type() applies keyword rules in strict priority order:
  po_box → rural → government → poi → commercial → residential → None

Never raises — returns (None, None) on any error or ambiguous input.
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# Fields used for classification (from the pipeline row)
_CLASSIFY_FIELDS = [
    "address_clean",
    "full_address",   # pre-load alias for address_clean
    "building_name",
    "street_name",
    "sub_locality_1",
    "sub_locality_2",
    "premise_no",
    "unit_no",
    "floor_no",
    "postcode",
]


def detect_address_type(row: dict) -> tuple[str, str] | tuple[None, None]:
    """Detect (address_type, address_subtype) from a row dict.

    Keys used: address_clean (or full_address), building_name, street_name,
    sub_locality_1, sub_locality_2, premise_no, unit_no, floor_no, postcode.
    All keys are optional — missing/null values are treated as empty.

    Never raises. Returns (None, None) if type cannot be determined.
    """
    try:
        # Build a single lowercase search string from all relevant text fields
        fields = [
            row.get("address_clean") or row.get("full_address"),
            row.get("building_name"),
            row.get("street_name"),
            row.get("sub_locality_1"),
            row.get("sub_locality_2"),
            row.get("premise_no"),
        ]
        parts = [str(f).strip() for f in fields if f]
        if not parts:
            return (None, None)

        search = " ".join(parts).lower()

        # ── 1. PO BOX (strongest signal — very specific strings) ──────────────
        if "p.o. box" in search or "po box" in search or "peti surat" in search:
            return ("po_box", "po_box")
        if "locked bag" in search or "karung berkunci" in search:
            return ("po_box", "locked_bag")
        if search.startswith("wdt ") or " wdt " in search:
            return ("po_box", "wdt")

        # ── 2. RURAL (kampung/felda/estate keywords) ──────────────────────────
        # Check compound phrases before simpler ones
        if "kampung atas air" in search:
            return ("rural", "kampung_atas_air")
        if "felda" in search:
            return ("rural", "felda")
        if "felcra" in search:
            return ("rural", "felcra")
        if "rumah panjang" in search:
            return ("rural", "rumah_panjang")
        if "tanah rancangan" in search:
            return ("rural", "tanah_rancangan")
        if "orang asli" in search or " rps " in search:
            return ("rural", "orang_asli")
        if "ladang" in search or " estate " in search or search.endswith(" estate"):
            return ("rural", "estate")
        if "kampung" in search or " kg " in search or "kg." in search:
            return ("rural", "kampung")

        # ── 3. GOVERNMENT (institutional keyword signals) ─────────────────────
        if "balai bomba" in search:
            return ("government", "federal")
        if "balai polis" in search or " polis " in search or search.startswith("polis "):
            return ("government", "police")
        if "hospital" in search or "klinik kesihatan" in search:
            return ("government", "hospital")
        if (
            "sekolah" in search
            or " sk " in search
            or " smk " in search
            or search.startswith("smk ")
            or search.startswith("sk ")
            or "sekolah kebangsaan" in search
        ):
            return ("government", "school")
        if "universiti" in search or "politeknik" in search or "kolej universiti" in search:
            return ("government", "university")
        if "mahkamah" in search:
            return ("government", "court")
        if "penjara" in search:
            return ("government", "prison")
        if (
            "majlis bandaraya" in search
            or "majlis perbandaran" in search
            or "majlis daerah" in search
        ):
            return ("government", "pbt")
        if (
            "jabatan" in search
            or "kementerian" in search
            or "perbadanan" in search
            or "lembaga" in search
        ):
            return ("government", "federal")
        if "tentera" in search or " kem " in search or "markas" in search:
            return ("government", "military")

        # ── 4. POI (check transport first to avoid "Stesen LRT Masjid Jamek" → mosque) ──
        if "lapangan terbang" in search or "airport" in search:
            return ("poi", "airport")
        if "stesen" in search:
            return ("poi", "train_station")
        if "terminal" in search:
            return ("poi", "bus_terminal")
        if "stadium" in search:
            return ("poi", "stadium")
        if "masjid" in search:
            return ("poi", "mosque")
        if "surau" in search:
            return ("poi", "mosque")
        if "gereja" in search or "church" in search:
            return ("poi", "church")
        if "kuil" in search or "tokong" in search or "temple" in search or " wat " in search:
            return ("poi", "temple")
        if "pasar" in search or "market" in search:
            return ("poi", "market")
        if "klinik" in search:
            return ("poi", "clinic")
        if (
            "petronas" in search
            or "petrol" in search
            or "shell" in search
            or "caltex" in search
            or "bpetrol" in search
        ):
            return ("poi", "petrol_station")
        if "plaza tol" in search or "plaza toll" in search or "lebuhraya" in search:
            return ("poi", "toll")

        # ── 5. COMMERCIAL (building/brand signals) ────────────────────────────
        if "hotel" in search or " inn " in search or "resort" in search or "motel" in search:
            return ("commercial", "hotel")
        if "mall" in search or "shopping" in search or "pasaraya" in search:
            return ("commercial", "mall")
        if "soho" in search or "sovo" in search or "sofo" in search:
            return ("commercial", "soho")
        if "menara" in search or "wisma" in search or "plaza" in search or "kompleks" in search:
            return ("commercial", "office")

        # ── 6. RESIDENTIAL (last resort) ─────────────────────────────────────
        if (
            "pangsapuri" in search
            or "apartment" in search
            or "kondominium" in search
            or "kondo" in search
        ):
            return ("residential", "apartment")
        if " flat " in search or search.startswith("flat "):
            return ("residential", "flat")

        unit_no = row.get("unit_no")
        floor_no = row.get("floor_no")
        premise_no = row.get("premise_no")

        if unit_no and floor_no:
            return ("residential", "apartment")
        if unit_no:
            return ("residential", "apartment")
        if premise_no:
            return ("residential", "landed")

        # If address_clean starts with a digit it's likely a numbered street address
        address_clean = str(row.get("address_clean") or row.get("full_address") or "").strip()
        if address_clean and address_clean[0].isdigit():
            return ("residential", "landed")

        # "taman" in sub-locality → housing area → landed residential
        sub1 = str(row.get("sub_locality_1") or "").lower()
        sub2 = str(row.get("sub_locality_2") or "").lower()
        if "taman" in sub1 or "taman" in sub2:
            return ("residential", "landed")

        return (None, None)

    except Exception:
        return (None, None)


def classify_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Apply detect_address_type to every row, adding _detected_type and _detected_subtype columns.

    Uses map_elements for row-wise application. Returns (None, None) for any row that
    cannot be classified. All input columns treated as optional.
    """
    available = [c for c in _CLASSIFY_FIELDS if c in df.columns]

    if not available:
        return df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("_detected_type"),
            pl.lit(None).cast(pl.Utf8).alias("_detected_subtype"),
        ])

    def _classify_encoded(row: dict) -> str | None:
        try:
            # Normalize: accept full_address as alias for address_clean
            if "full_address" in row and "address_clean" not in row:
                row = dict(row, address_clean=row["full_address"])
            t, s = detect_address_type(row)
            if t is None:
                return None
            return f"{t}||{s}"
        except Exception:
            return None

    df = df.with_columns(
        pl.struct(available)
        .map_elements(_classify_encoded, return_dtype=pl.Utf8)
        .alias("_detected_class")
    )

    df = df.with_columns([
        pl.when(pl.col("_detected_class").is_not_null())
          .then(pl.col("_detected_class").str.split("||").list.get(0))
          .otherwise(pl.lit(None).cast(pl.Utf8))
          .alias("_detected_type"),
        pl.when(pl.col("_detected_class").is_not_null())
          .then(pl.col("_detected_class").str.split("||").list.get(1))
          .otherwise(pl.lit(None).cast(pl.Utf8))
          .alias("_detected_subtype"),
    ]).drop("_detected_class")

    return df


def load_type_id_map(dsn: str, schema: str) -> dict[tuple[str, str], int]:
    """Load address_type reference table into memory as {(type, subtype): id}.

    Called once at pipeline startup. Returns empty dict on any error.
    """
    try:
        import sqlalchemy as sa
        engine = sa.create_engine(dsn, pool_pre_ping=True)
        with engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    f'SELECT address_type_id, address_type, address_subtype '
                    f'FROM "{schema}".address_type'
                )
            ).mappings().all()
        result = {(r["address_type"], r["address_subtype"]): int(r["address_type_id"]) for r in rows}
        engine.dispose()
        return result
    except Exception as exc:
        logger.warning("load_type_id_map_failed: %s", exc)
        return {}


def resolve_type_ids(
    df: pl.DataFrame,
    type_id_map: dict[tuple[str, str], int],
) -> pl.DataFrame:
    """Map _detected_type + _detected_subtype to address_type_id integer.

    Drops the intermediate _detected_type and _detected_subtype columns.
    address_type_id is Int32, nullable.
    """
    if "_detected_type" not in df.columns or "_detected_subtype" not in df.columns:
        if "address_type_id" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Int32).alias("address_type_id"))
        return df

    def _lookup(row: dict) -> int | None:
        t = row.get("_detected_type")
        s = row.get("_detected_subtype")
        if t is None or s is None:
            return None
        return type_id_map.get((t, s))

    df = df.with_columns(
        pl.struct(["_detected_type", "_detected_subtype"])
        .map_elements(_lookup, return_dtype=pl.Int32)
        .alias("address_type_id")
    ).drop(["_detected_type", "_detected_subtype"])

    return df
