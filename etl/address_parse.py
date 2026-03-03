import re
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    element_at,
    length,
    lit,
    size,
    regexp_extract,
    regexp_replace,
    split,
    trim,
    upper,
    when,
)


def normalize_for_match(address_col):
    cleaned = upper(trim(address_col.cast("string")))
    cleaned = regexp_replace(cleaned, r"[\r\n]+", " ")
    cleaned = regexp_replace(cleaned, r"[^A-Z0-9 ]", " ")
    cleaned = regexp_replace(cleaned, r"\s+", " ")
    return concat(lit(" "), cleaned, lit(" "))


def normalize_segment(seg_col):
    seg = upper(trim(seg_col))
    seg = regexp_replace(seg, r"[^A-Z0-9 ]", " ")
    seg = regexp_replace(seg, r"\s+", " ")
    return seg


def parse_full_address(df: DataFrame, address_col: str, replacements: Optional[dict] = None) -> DataFrame:
    address_clean = trim(regexp_replace(col(address_col).cast("string"), r"\s+", " "))
    address_clean = regexp_replace(address_clean, r"[;|]+", ",")
    if replacements:
        for src, dst in replacements.items():
            key = src.strip()
            if not key:
                continue
            escaped = re.escape(key)
            if re.fullmatch(r"[A-Za-z]+", key):
                pattern = rf"(?i)(?<![A-Z0-9]){escaped}(?:\\.)?(?![A-Z0-9])"
            else:
                pattern = rf"(?i)(?<![A-Z0-9]){escaped}(?![A-Z0-9])"
            address_clean = regexp_replace(address_clean, pattern, dst)
    segments = split(address_clean, r"\s*,\s*")

    seg1 = trim(when(size(segments) >= 1, element_at(segments, 1)))
    seg2 = trim(when(size(segments) >= 2, element_at(segments, 2)))
    seg3 = trim(when(size(segments) >= 3, element_at(segments, 3)))
    seg4 = trim(when(size(segments) >= 4, element_at(segments, 4)))
    seg5 = trim(when(size(segments) >= 5, element_at(segments, 5)))

    seg1_up = upper(seg1)
    seg2_up = upper(seg2)

    lot_no = regexp_extract(seg1_up, r"\bLOT\s*([0-9A-Z/-]+)", 1)
    lot_fallback = regexp_extract(seg1_up, r"\bPT\s*([0-9A-Z/-]+)", 1)
    lot_no = when((lot_no == "") | lot_no.isNull(), lot_fallback).otherwise(lot_no)

    unit_no = regexp_extract(seg1_up, r"\b(?:UNIT|APT|APARTMENT|SUITE|STE)\s*([0-9A-Z/-]+)", 1)
    unit_fallback = regexp_extract(seg1_up, r"\b[A-Z]?\d+-\d+(?:-\d+)?\b", 0)
    unit_no = when((unit_no == "") | unit_no.isNull(), unit_fallback).otherwise(unit_no)

    floor_no = regexp_extract(seg1_up, r"\b(?:FLOOR|LEVEL|LVL|TINGKAT|TKT|ARAS)\s*([0-9A-Z/-]+)", 1)

    premise_no = regexp_extract(seg1_up, r"\bNO\.?\s*([0-9A-Z/-]+)", 1)
    premise_fallback = regexp_extract(seg1_up, r"^([0-9A-Z/-]+)", 1)
    premise_no = when((premise_no == "") | premise_no.isNull(), premise_fallback).otherwise(premise_no)
    premise_no = when((lot_no != "") | (unit_no != "") | (floor_no != ""), lit(None)).otherwise(premise_no)

    df = df.withColumn("address_clean", address_clean)
    df = df.withColumn("premise_no", when((premise_no == "") | premise_no.isNull(), lit(None)).otherwise(premise_no))
    df = df.withColumn("lot_no", when((lot_no == "") | lot_no.isNull(), lit(None)).otherwise(lot_no))
    df = df.withColumn("unit_no", when((unit_no == "") | unit_no.isNull(), lit(None)).otherwise(unit_no))
    df = df.withColumn("floor_no", when((floor_no == "") | floor_no.isNull(), lit(None)).otherwise(floor_no))
    def _strip_postcode(seg_col):
        cleaned = regexp_replace(seg_col, r"(?<!\d)\d{5}(?!\d)", "")
        cleaned = regexp_replace(cleaned, r"\s+", " ")
        return trim(cleaned)

    seg3_no_zip = _strip_postcode(seg3)
    seg4_no_zip = _strip_postcode(seg4)
    seg5_no_zip = _strip_postcode(seg5)

    tail_base = trim(regexp_replace(address_clean, r"(?<!\d)\d{5}(?!\d)", ""))
    tail_parts = split(tail_base, r"\s+")
    tail2 = when(
        size(tail_parts) >= 2,
        concat_ws(" ", element_at(tail_parts, -2), element_at(tail_parts, -1)),
    )
    tail3 = when(
        size(tail_parts) >= 3,
        concat_ws(
            " ",
            element_at(tail_parts, -3),
            element_at(tail_parts, -2),
            element_at(tail_parts, -1),
        ),
    )
    tail4 = when(
        size(tail_parts) >= 4,
        concat_ws(
            " ",
            element_at(tail_parts, -4),
            element_at(tail_parts, -3),
            element_at(tail_parts, -2),
            element_at(tail_parts, -1),
        ),
    )

    seg3_up = when(length(seg3_no_zip) > 0, upper(seg3_no_zip))
    seg4_up = when(length(seg4_no_zip) > 0, upper(seg4_no_zip))
    seg5_up = when(length(seg5_no_zip) > 0, upper(seg5_no_zip))

    locality_name = when(length(seg5) > 0, seg5_up).when(length(seg4) > 0, seg4_up).otherwise(seg3_up)
    sub_locality_1 = when(length(seg4) > 0, seg3_up)
    sub_locality_2 = when(length(seg5) > 0, seg4_up)

    street_prefix_pattern = r"(JALAN\\s+RAYA|JALANRAYA|JALAN|JLN|LORONG|LRG|PERSIARAN|LEBUHRAYA|LEBUH)"
    street_prefix = regexp_extract(seg2_up, rf"^{street_prefix_pattern}\\b", 1)
    street_name_clean = regexp_replace(seg2_up, rf"^{street_prefix_pattern}\\s+", "")
    street_name = when(length(seg2) > 0, seg2_up)
    street_name = when((street_prefix != "") & (length(street_name_clean) > 0), street_name_clean).otherwise(street_name)

    building_pattern = (
        r"\\b(APARTMENT|APARTMEN|PANGSAPURI|KONDOMINIUM|CONDOMINIUM|KONDO|CONDO|FLAT|"
        r"RUMAH\\s+PANGSA|MENARA|TOWER|WISMA|PLAZA|KOMPLEKS|BLOK|BLOCK|RESIDENSI|"
        r"RESIDENCE|HOTEL|MALL|ARKED|PPR|PPRT|PPA1M)\\b"
    )
    building_name = when(seg1_up.rlike(building_pattern), seg1_up).when(seg2_up.rlike(building_pattern), seg2_up)

    df = df.withColumn("street_name_prefix", when(street_prefix != "", street_prefix))
    df = df.withColumn("street_name", street_name)
    df = df.withColumn("building_name", building_name)
    df = df.withColumn("locality_name", locality_name)
    df = df.withColumn("sub_locality_1", sub_locality_1)
    df = df.withColumn("sub_locality_2", sub_locality_2)
    df = df.withColumn("postcode", regexp_extract(normalize_for_match(address_clean), r"\b(\d{5})\b", 1))
    df = df.withColumn("seg2_norm", normalize_segment(seg2))
    df = df.withColumn("seg3_norm", normalize_segment(seg3))
    df = df.withColumn("seg4_norm", normalize_segment(seg4))
    df = df.withColumn("seg5_norm", normalize_segment(seg5))
    df = df.withColumn("tail2_norm", normalize_segment(tail2))
    df = df.withColumn("tail3_norm", normalize_segment(tail3))
    df = df.withColumn("tail4_norm", normalize_segment(tail4))
    df = df.withColumn("_address_norm", normalize_for_match(col("address_clean")))

    return df
