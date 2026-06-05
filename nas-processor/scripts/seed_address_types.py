#!/usr/bin/env python3
"""Seed nas.address_type reference data.

Safe to re-run — uses INSERT ... ON CONFLICT DO NOTHING.
Usage: python scripts/seed_address_types.py
"""

from __future__ import annotations

import os
import sys

import sqlalchemy as sa


def _build_dsn() -> str:
    for key in ("POSTGRES_DSN", "DATABASE_URL"):
        raw = os.environ.get(key, "").strip()
        if raw:
            return raw
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    password = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    database = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


# (address_type, address_subtype, property_code)
_SEED_ROWS: list[tuple[str, str, str]] = [
    # RESIDENTIAL
    ("residential", "landed",        "RES-LND"),
    ("residential", "apartment",     "RES-APT"),
    ("residential", "condominium",   "RES-CON"),
    ("residential", "flat",          "RES-FLT"),
    ("residential", "townhouse",     "RES-TWN"),
    ("residential", "semi_detached", "RES-SEM"),
    ("residential", "bungalow",      "RES-BUN"),
    ("residential", "quarters",      "RES-QTR"),
    ("residential", "rumah_pangsa",  "RES-PAN"),
    # COMMERCIAL
    ("commercial", "shop",         "COM-SHP"),
    ("commercial", "office",       "COM-OFF"),
    ("commercial", "mall",         "COM-MAL"),
    ("commercial", "hotel",        "COM-HTL"),
    ("commercial", "restaurant",   "COM-RST"),
    ("commercial", "soho",         "COM-SOH"),
    ("commercial", "serviced_apt", "COM-SVC"),
    # INDUSTRIAL
    ("industrial", "factory",   "IND-FCT"),
    ("industrial", "warehouse", "IND-WRH"),
    ("industrial", "port",      "IND-PRT"),
    ("industrial", "logistics", "IND-LOG"),
    # GOVERNMENT
    ("government", "federal",    "GOV-FED"),
    ("government", "state",      "GOV-STA"),
    ("government", "pbt",        "GOV-PBT"),
    ("government", "military",   "GOV-MIL"),
    ("government", "police",     "GOV-POL"),
    ("government", "hospital",   "GOV-HOS"),
    ("government", "school",     "GOV-SCH"),
    ("government", "university", "GOV-UNI"),
    ("government", "court",      "GOV-CRT"),
    ("government", "prison",     "GOV-PRN"),
    # POI
    ("poi", "mosque",            "POI-MSQ"),
    ("poi", "temple",            "POI-TPL"),
    ("poi", "church",            "POI-CHR"),
    ("poi", "surau",             "POI-SRU"),
    ("poi", "petrol_station",    "POI-PTR"),
    ("poi", "toll",              "POI-TOL"),
    ("poi", "highway_rest_area", "POI-HRA"),
    ("poi", "cemetery",          "POI-CMT"),
    ("poi", "park",              "POI-PRK"),
    ("poi", "stadium",           "POI-STD"),
    ("poi", "airport",           "POI-APT"),
    ("poi", "train_station",     "POI-TRN"),
    ("poi", "bus_terminal",      "POI-BUS"),
    ("poi", "jetty",             "POI-JTY"),
    ("poi", "clinic",            "POI-CLN"),
    ("poi", "market",            "POI-MKT"),
    # RURAL (DMS 2039 clause 6 non-street addresses)
    ("rural", "kampung",          "RUR-KPG"),
    ("rural", "felda",            "RUR-FLD"),
    ("rural", "felcra",           "RUR-FCR"),
    ("rural", "rumah_panjang",    "RUR-RPJ"),
    ("rural", "kampung_atas_air", "RUR-KAA"),
    ("rural", "estate",           "RUR-EST"),
    ("rural", "tanah_rancangan",  "RUR-TRN"),
    ("rural", "orang_asli",       "RUR-OAS"),
    # PO_BOX (DMS 2039 clause 7)
    ("po_box", "po_box",     "POB-POB"),
    ("po_box", "locked_bag", "POB-LCK"),
    ("po_box", "wdt",        "POB-WDT"),
]


def main() -> int:
    dsn = _build_dsn()
    engine = sa.create_engine(dsn)

    # Ensure the UNIQUE constraint exists before inserting — ignore if already present
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(
                "ALTER TABLE nas.address_type "
                "ADD CONSTRAINT address_type_subtype_unique "
                "UNIQUE (address_type, address_subtype)"
            ))
    except sa.exc.ProgrammingError:
        pass  # constraint already exists

    try:
        with engine.begin() as conn:
            before = conn.execute(sa.text("SELECT COUNT(*) FROM nas.address_type")).scalar()

            values_sql = ",\n        ".join(
                f"('{at}', '{ast}', '{pc}')"
                for at, ast, pc in _SEED_ROWS
            )
            conn.execute(sa.text(f"""
                INSERT INTO nas.address_type (address_type, address_subtype, property_code)
                VALUES
                {values_sql}
                ON CONFLICT (address_type, address_subtype) DO NOTHING
            """))

            after = conn.execute(sa.text("SELECT COUNT(*) FROM nas.address_type")).scalar()

        inserted = after - before
        skipped = len(_SEED_ROWS) - inserted
        print(f"address_type seed complete: {inserted} rows inserted, {skipped} rows already existed")
        return 0

    except Exception as exc:
        print(f"ERROR: seed failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
