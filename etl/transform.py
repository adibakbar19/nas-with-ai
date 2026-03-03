import json
import os
from glob import glob
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    array,
    broadcast,
    col,
    coalesce,
    concat,
    concat_ws,
    count,
    expr,
    first,
    initcap,
    isnan,
    least,
    length,
    levenshtein,
    lit,
    lower,
    monotonically_increasing_id,
    row_number,
    regexp_extract,
    size,
    trim,
    upper,
    when,
)

from .address_parse import parse_full_address
from .lookup_match import match_lookup_exact, match_lookup_fuzzy, match_mukim_fuzzy
from .sedona_utils import register_sedona


def _normalize_col_name(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _load_config(config_path: Optional[str]) -> dict:
    if not config_path:
        return {}
    with open(config_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _config_bool(config: dict, key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(value)


def _detect_address_col(df: DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    norm_cols = {_normalize_col_name(c): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
        norm = _normalize_col_name(cand)
        if norm in norm_cols:
            return norm_cols[norm]
    # Fallback: pick a single column that contains address/alamat
    matches = []
    for raw, norm in ((c, _normalize_col_name(c)) for c in df.columns):
        if not ("alamat" in norm or "address" in norm):
            continue
        if any(
            token in norm
            for token in [
                "addresstype",
                "addresssource",
                "addressid",
            ]
        ):
            continue
        if norm in {"addressclean"}:
            continue
        if "new" in norm and "address" in norm:
            # Keep new-address style columns for explicit new-address detection.
            continue
        if "old" in norm and "address" in norm:
            # Keep old-address style columns for explicit candidate matching.
            continue
        if "alamat" in norm or "addressline" in norm or "fulladdress" in norm or "rawaddress" in norm or norm == "address":
            matches.append(raw)
    if len(matches) == 1:
        return matches[0]
    return None


def load_lookups(spark, lookups_dir: str = "data/lookups"):
    state_df = spark.read.csv(os.path.join(lookups_dir, "state_codes.csv"), header=True)
    district_df = spark.read.csv(os.path.join(lookups_dir, "district_codes.csv"), header=True)
    mukim_df = spark.read.csv(os.path.join(lookups_dir, "mukim_codes.csv"), header=True)
    postcode_df = spark.read.csv(os.path.join(lookups_dir, "postcodes.csv"), header=True)
    district_alias_path = os.path.join(lookups_dir, "district_aliases.csv")
    district_alias_df = None
    if os.path.exists(district_alias_path):
        district_alias_df = spark.read.csv(district_alias_path, header=True)
    return state_df, district_df, mukim_df, postcode_df, district_alias_df


def _detect_new_address_col(df: DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    norm_cols = {_normalize_col_name(c): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
        norm = _normalize_col_name(cand)
        if norm in norm_cols:
            return norm_cols[norm]
    return None


def _detect_col(df: DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    norm_cols = {_normalize_col_name(c): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
        norm = _normalize_col_name(cand)
        if norm in norm_cols:
            return norm_cols[norm]
    return None


def _ensure_column(df: DataFrame, name: str) -> DataFrame:
    if name in df.columns:
        return df
    return df.withColumn(name, lit(None).cast("string"))


def _rename_if_exists(df: DataFrame, name: str, new_name: str) -> DataFrame:
    if name in df.columns:
        return df.withColumnRenamed(name, new_name)
    return df


def _clean_text_expr(value_col):
    txt = trim(value_col.cast("string"))
    txt_norm = lower(txt)
    return when(
        txt.isNull() | txt_norm.isin("", "nan", "null", "none", "na", "n/a"),
        lit(None).cast("string"),
    ).otherwise(txt)


def _standardize_common_columns(df: DataFrame, config: dict) -> DataFrame:
    alias_defaults = {
        "state_name": ["state", "negeri", "nama_negeri", "nm_negeri"],
        "district_name": ["district", "daerah", "nama_daerah", "nm_daerah"],
        "mukim_name": ["mukim", "nama_mukim", "nm_mukim"],
        "locality_name": ["locality", "city", "town", "bandar", "pekan", "nama_bandar"],
        "postcode": ["postcode", "poskod", "postal_code", "zip", "zip_code"],
        "premise_no": ["house_no", "house_number", "no_rumah", "nombor_rumah", "building_no"],
        "lot_no": ["lot", "lot_number", "no_lot", "nombor_lot"],
        "unit_no": ["unit", "unit_number", "no_unit", "nombor_unit"],
        "street_name": ["street", "road_name", "nama_jalan", "jalan"],
        "building_name": ["building", "premise_name", "nama_premis", "nama_premi"],
        "address_type": ["building_type", "property_type", "jenis_bangunan"],
        "country": ["negara"],
    }
    overrides = config.get("column_aliases", {})
    for canonical, defaults in alias_defaults.items():
        if canonical in df.columns:
            continue
        candidates = overrides.get(canonical, defaults)
        detected = _detect_col(df, candidates)
        if detected and detected != canonical:
            df = df.withColumnRenamed(detected, canonical)
    return df


def _compose_structured_address(df: DataFrame, *, source_cols: list[str], target_col: str) -> DataFrame:
    parts = [_clean_text_expr(col(c)) for c in source_cols if c in df.columns]
    if not parts:
        return df.withColumn(target_col, lit(None).cast("string"))
    merged = concat_ws(", ", *parts)
    return df.withColumn(
        target_col,
        when(length(merged) > 0, merged).otherwise(lit(None).cast("string")),
    )


def _point_wkt_from_lat_lon(lat_col, lon_col):
    lat_num = lat_col.cast("double")
    lon_num = lon_col.cast("double")
    has_valid_coords = (
        lat_num.isNotNull()
        & lon_num.isNotNull()
        & (~isnan(lat_num))
        & (~isnan(lon_num))
    )
    return when(
        has_valid_coords,
        concat(
            lit("POINT("),
            lon_num.cast("string"),
            lit(" "),
            lat_num.cast("string"),
            lit(")"),
        ),
    ).otherwise(lit(None).cast("string"))


def _load_pbt_boundaries(spark, pbt_dir: str, *, simplify_tolerance: Optional[float] = None) -> DataFrame:
    shp_paths = sorted(glob(os.path.join(pbt_dir, "*.shp")))
    if not shp_paths:
        raise ValueError(f"No shapefiles found in PBT directory: {pbt_dir}")

    frames = []
    for shp_path in shp_paths:
        df = spark.read.format("shapefile").load(shp_path)
        geom_col = "geometry" if "geometry" in df.columns else "geom"
        if geom_col != "geom":
            df = df.withColumnRenamed(geom_col, "geom")

        prj_path = os.path.splitext(shp_path)[0] + ".prj"
        if os.path.exists(prj_path):
            with open(prj_path, "r", encoding="utf-8") as handle:
                prj_wkt = handle.read().strip()
            if prj_wkt:
                prj_wkt_sql = prj_wkt.replace("'", "''")
                df = df.withColumn(
                    "geom",
                    expr(f"ST_Transform(geom, '{prj_wkt_sql}', 'EPSG:4326')"),
                )
        if simplify_tolerance and simplify_tolerance > 0:
            df = df.withColumn("geom", expr(f"ST_SimplifyPreserveTopology(geom, {float(simplify_tolerance)})"))
        frames.append(df)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def _assign_pbt(df: DataFrame, pbt_df: DataFrame, *, pbt_id_col: str, pbt_name_col: str) -> DataFrame:
    df = _rename_if_exists(df, "pbt_id", "_pbt_id_orig")
    df = _rename_if_exists(df, "pbt_name", "_pbt_name_orig")

    df = df.withColumn(
        "_point_geom",
        expr("ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))"),
    )

    pbt_select = pbt_df.select(
        col(pbt_id_col).alias("_pbt_id"),
        col(pbt_name_col).alias("_pbt_name"),
        col("geom").alias("_pbt_geom"),
    )

    joined = df.join(
        broadcast(pbt_select),
        expr("ST_Intersects(_pbt_geom, _point_geom)"),
        "left",
    )
    window = Window.partitionBy("_row_id").orderBy(
        col("_pbt_id").asc_nulls_last(),
        col("_pbt_name").asc_nulls_last(),
    )
    joined = joined.withColumn("_pbt_rank", row_number().over(window)).filter(col("_pbt_rank") == 1)

    joined = joined.withColumn("pbt_id", coalesce(col("_pbt_id"), col("_pbt_id_orig")))
    joined = joined.withColumn("pbt_name", coalesce(col("_pbt_name"), col("_pbt_name_orig")))
    return joined.drop(
        "_pbt_id",
        "_pbt_name",
        "_pbt_geom",
        "_point_geom",
        "_pbt_rank",
        "_pbt_id_orig",
        "_pbt_name_orig",
    )


def _assign_postcode_from_boundary(
    df: DataFrame,
    postcode_boundary_df: DataFrame,
    *,
    postcode_col: str,
    city_col: str,
    state_col: str,
) -> DataFrame:
    df = _ensure_column(df, "postcode_code")
    df = _ensure_column(df, "postcode_name")
    df = _ensure_column(df, "postcode_city")
    df = _ensure_column(df, "postcode_state_name")
    df = _ensure_column(df, "locality_name")

    df = df.withColumn(
        "_point_geom",
        expr("ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))"),
    )
    postcode_select = (
        postcode_boundary_df.select(
            regexp_extract(trim(col(postcode_col).cast("string")), r"(\\d{5})", 1).alias("_b_postcode_code"),
            upper(trim(col(city_col).cast("string"))).alias("_b_postcode_city"),
            upper(trim(col(state_col).cast("string"))).alias("_b_postcode_state"),
            col("geom").alias("_b_postcode_geom"),
        )
        .dropna(subset=["_b_postcode_code"])
        .dropDuplicates(["_b_postcode_code", "_b_postcode_city", "_b_postcode_state"])
    )

    joined = df.join(
        broadcast(postcode_select),
        expr("ST_Intersects(_b_postcode_geom, _point_geom)"),
        "left",
    )
    window = Window.partitionBy("_row_id").orderBy(
        col("_b_postcode_code").asc_nulls_last(),
        col("_b_postcode_city").asc_nulls_last(),
    )
    joined = joined.withColumn("_b_postcode_rank", row_number().over(window)).filter(col("_b_postcode_rank") == 1)

    existing_postcode_code = regexp_extract(coalesce(col("postcode_code"), lit("")), r"(\\d{5})", 1)
    postcode_conflict = (
        col("_b_postcode_code").isNotNull()
        & (length(existing_postcode_code) == 5)
        & (existing_postcode_code != col("_b_postcode_code"))
    )
    postcode_apply = col("_b_postcode_code").isNotNull() & ~postcode_conflict

    joined = joined.withColumn("_postcode_boundary_conflict", postcode_conflict)
    joined = joined.withColumn("_postcode_from_boundary", postcode_apply)
    joined = joined.withColumn(
        "postcode_code",
        when(postcode_apply, col("_b_postcode_code")).otherwise(col("postcode_code")),
    )
    joined = joined.withColumn(
        "postcode_city",
        when(postcode_apply, coalesce(col("postcode_city"), col("_b_postcode_city"))).otherwise(col("postcode_city")),
    )
    joined = joined.withColumn(
        "postcode_state_name",
        when(postcode_apply, coalesce(col("postcode_state_name"), col("_b_postcode_state"))).otherwise(col("postcode_state_name")),
    )
    joined = joined.withColumn(
        "postcode_name",
        when(postcode_apply, coalesce(col("postcode_name"), initcap(lower(col("_b_postcode_city"))))).otherwise(col("postcode_name")),
    )
    joined = joined.withColumn("locality_name", coalesce(col("locality_name"), col("postcode_city")))

    return joined.drop(
        "_point_geom",
        "_b_postcode_code",
        "_b_postcode_city",
        "_b_postcode_state",
        "_b_postcode_geom",
        "_b_postcode_rank",
    )


def _pick_boundary_col(df: DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def _assign_admin_from_boundaries(
    df: DataFrame,
    *,
    state_df: DataFrame,
    district_df: DataFrame,
    mukim_df: DataFrame,
) -> DataFrame:
    # Use point-in-polygon as authoritative admin resolver when coordinates exist.
    df = _ensure_column(df, "latitude")
    df = _ensure_column(df, "longitude")
    df = df.withColumn(
        "_point_geom",
        expr("ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))"),
    )

    state_name_col = _pick_boundary_col(state_df, ["state", "state_name"])
    state_code_col = _pick_boundary_col(state_df, ["state_code", "statecode"])
    if state_name_col and state_code_col:
        state_select = state_df.select(
            upper(col(state_name_col)).alias("_b_state_name"),
            col(state_code_col).alias("_b_state_code"),
            col("geom").alias("_b_state_geom"),
        )
        state_join = df.join(
            broadcast(state_select),
            expr("ST_Intersects(_b_state_geom, _point_geom)"),
            "left",
        )
        state_win = Window.partitionBy("_row_id").orderBy(
            col("_b_state_code").asc_nulls_last(),
            col("_b_state_name").asc_nulls_last(),
        )
        state_join = state_join.withColumn("_b_state_rank", row_number().over(state_win)).filter(col("_b_state_rank") == 1)
        state_conflict = (
            col("_b_state_code").isNotNull() & col("state_code").isNotNull() & (col("_b_state_code") != col("state_code"))
        )
        state_apply = col("_b_state_code").isNotNull() & ~state_conflict
        df = state_join.withColumn("_state_boundary_conflict", state_conflict)
        df = df.withColumn("_state_from_boundary", state_apply)
        df = df.withColumn("state_code", when(state_apply, col("_b_state_code")).otherwise(col("state_code")))
        df = df.withColumn("state_name", when(state_apply, coalesce(col("_b_state_name"), col("state_name"))).otherwise(col("state_name")))
        df = df.drop("_b_state_code", "_b_state_name", "_b_state_geom", "_b_state_rank")
    else:
        df = df.withColumn("_state_from_boundary", lit(False))
        df = df.withColumn("_state_boundary_conflict", lit(False))

    district_name_col = _pick_boundary_col(district_df, ["district", "district_name"])
    district_code_col = _pick_boundary_col(district_df, ["district_c", "district_code", "districtcode"])
    district_state_code_col = _pick_boundary_col(district_df, ["state_code", "statecode"])
    if district_name_col and district_code_col:
        district_select = district_df.select(
            upper(col(district_name_col)).alias("_b_district_name"),
            col(district_code_col).alias("_b_district_code"),
            col(district_state_code_col).alias("_b_district_state_code") if district_state_code_col else lit(None).cast("string").alias("_b_district_state_code"),
            col("geom").alias("_b_district_geom"),
        )
        district_cond = expr("ST_Intersects(_b_district_geom, _point_geom)") & (
            col("state_code").isNull()
            | col("_b_district_state_code").isNull()
            | (col("state_code") == col("_b_district_state_code"))
        )
        district_join = df.join(broadcast(district_select), district_cond, "left")
        district_win = Window.partitionBy("_row_id").orderBy(
            when(col("state_code") == col("_b_district_state_code"), lit(0)).otherwise(lit(1)).asc(),
            col("_b_district_code").asc_nulls_last(),
            col("_b_district_name").asc_nulls_last(),
        )
        district_join = district_join.withColumn("_b_district_rank", row_number().over(district_win)).filter(
            col("_b_district_rank") == 1
        )
        district_conflict = (
            col("_b_district_code").isNotNull()
            & col("district_code").isNotNull()
            & (col("_b_district_code") != col("district_code"))
        )
        district_apply = col("_b_district_code").isNotNull() & ~district_conflict
        df = district_join.withColumn("_district_boundary_conflict", district_conflict)
        df = df.withColumn("_district_from_boundary", district_apply)
        df = df.withColumn("state_code", when(district_apply, coalesce(col("_b_district_state_code"), col("state_code"))).otherwise(col("state_code")))
        df = df.withColumn("district_code", when(district_apply, col("_b_district_code")).otherwise(col("district_code")))
        df = df.withColumn("district_name", when(district_apply, coalesce(col("_b_district_name"), col("district_name"))).otherwise(col("district_name")))
        df = df.drop(
            "_b_district_name",
            "_b_district_code",
            "_b_district_state_code",
            "_b_district_geom",
            "_b_district_rank",
        )
    else:
        df = df.withColumn("_district_from_boundary", lit(False))
        df = df.withColumn("_district_boundary_conflict", lit(False))

    mukim_name_col = _pick_boundary_col(mukim_df, ["mukim", "mukim_name"])
    mukim_code_col = _pick_boundary_col(mukim_df, ["mukim_code", "mukimcode"])
    mukim_id_col = _pick_boundary_col(mukim_df, ["id", "mukim_id", "mukimid"])
    mukim_district_code_col = _pick_boundary_col(mukim_df, ["district_c", "district_code", "districtcode"])
    mukim_district_name_col = _pick_boundary_col(mukim_df, ["district", "district_name"])
    mukim_state_code_col = _pick_boundary_col(mukim_df, ["state_code", "statecode"])
    if mukim_name_col and mukim_code_col:
        mukim_select = mukim_df.select(
            upper(col(mukim_name_col)).alias("_b_mukim_name"),
            col(mukim_code_col).alias("_b_mukim_code"),
            col(mukim_id_col).cast("int").alias("_b_mukim_id") if mukim_id_col else lit(None).cast("int").alias("_b_mukim_id"),
            col(mukim_district_code_col).alias("_b_mukim_district_code") if mukim_district_code_col else lit(None).cast("string").alias("_b_mukim_district_code"),
            upper(col(mukim_district_name_col)).alias("_b_mukim_district_name") if mukim_district_name_col else lit(None).cast("string").alias("_b_mukim_district_name"),
            col(mukim_state_code_col).alias("_b_mukim_state_code") if mukim_state_code_col else lit(None).cast("string").alias("_b_mukim_state_code"),
            col("geom").alias("_b_mukim_geom"),
        )
        mukim_cond = expr("ST_Intersects(_b_mukim_geom, _point_geom)") & (
            col("state_code").isNull()
            | col("_b_mukim_state_code").isNull()
            | (col("state_code") == col("_b_mukim_state_code"))
        ) & (
            col("district_code").isNull()
            | col("_b_mukim_district_code").isNull()
            | (col("district_code") == col("_b_mukim_district_code"))
        )
        mukim_join = df.join(broadcast(mukim_select), mukim_cond, "left")
        mukim_win = Window.partitionBy("_row_id").orderBy(
            when(col("district_code") == col("_b_mukim_district_code"), lit(0)).otherwise(lit(1)).asc(),
            when(col("state_code") == col("_b_mukim_state_code"), lit(0)).otherwise(lit(1)).asc(),
            col("_b_mukim_code").asc_nulls_last(),
            col("_b_mukim_name").asc_nulls_last(),
        )
        mukim_join = mukim_join.withColumn("_b_mukim_rank", row_number().over(mukim_win)).filter(col("_b_mukim_rank") == 1)
        mukim_conflict = (
            col("_b_mukim_code").isNotNull() & col("mukim_code").isNotNull() & (col("_b_mukim_code") != col("mukim_code"))
        )
        mukim_apply = col("_b_mukim_code").isNotNull() & ~mukim_conflict
        df = mukim_join.withColumn("_mukim_boundary_conflict", mukim_conflict)
        df = df.withColumn("_mukim_from_boundary", mukim_apply)
        df = df.withColumn("state_code", when(mukim_apply, coalesce(col("_b_mukim_state_code"), col("state_code"))).otherwise(col("state_code")))
        df = df.withColumn("district_code", when(mukim_apply, coalesce(col("_b_mukim_district_code"), col("district_code"))).otherwise(col("district_code")))
        df = df.withColumn("district_name", when(mukim_apply, coalesce(col("_b_mukim_district_name"), col("district_name"))).otherwise(col("district_name")))
        df = df.withColumn("mukim_code", when(mukim_apply, col("_b_mukim_code")).otherwise(col("mukim_code")))
        df = df.withColumn("mukim_name", when(mukim_apply, coalesce(col("_b_mukim_name"), col("mukim_name"))).otherwise(col("mukim_name")))
        df = df.withColumn("mukim_id", when(mukim_apply, coalesce(col("_b_mukim_id"), col("mukim_id"))).otherwise(col("mukim_id")))
        df = df.drop(
            "_b_mukim_name",
            "_b_mukim_code",
            "_b_mukim_id",
            "_b_mukim_district_code",
            "_b_mukim_district_name",
            "_b_mukim_state_code",
            "_b_mukim_geom",
            "_b_mukim_rank",
        )
    else:
        df = df.withColumn("_mukim_from_boundary", lit(False))
        df = df.withColumn("_mukim_boundary_conflict", lit(False))

    return df.drop("_point_geom")


def _attach_admin_candidates(df: DataFrame, mukim_df: DataFrame, *, top_k: int = 3) -> DataFrame:
    candidates = (
        mukim_df.select(
            col("state_code").alias("_cand_state_code"),
            upper(col("state_name")).alias("_cand_state_name"),
            col("district_code").alias("_cand_district_code"),
            upper(col("district_name")).alias("_cand_district_name"),
            col("mukim_code").alias("_cand_mukim_code"),
            upper(col("mukim_name")).alias("_cand_mukim_name"),
            col("mukim_id").cast("int").alias("_cand_mukim_id"),
        )
        .dropna(subset=["_cand_state_code", "_cand_district_code", "_cand_mukim_code"])
        .dropDuplicates(["_cand_state_code", "_cand_district_code", "_cand_mukim_code"])
    )

    scoped = (
        df.withColumn("_query_mukim", upper(coalesce(col("mukim_name"), lit(""))))
        .withColumn("_query_locality", upper(coalesce(col("locality_name"), lit(""))))
        .withColumn("_query_district", upper(coalesce(col("district_name"), lit(""))))
    )
    join_cond = (
        col("state_code").isNotNull()
        & (col("state_code") == col("_cand_state_code"))
        & (col("district_code").isNull() | (col("district_code") == col("_cand_district_code")))
    )
    scoped = scoped.join(broadcast(candidates), join_cond, "left")

    name_dist = least(
        coalesce(levenshtein(col("_cand_mukim_name"), col("_query_mukim")), lit(999)),
        coalesce(levenshtein(col("_cand_mukim_name"), col("_query_locality")), lit(999)),
        coalesce(levenshtein(col("_cand_mukim_name"), col("_query_district")), lit(999)),
    )
    district_penalty = when(
        col("district_code").isNotNull() & (col("district_code") != col("_cand_district_code")),
        lit(3),
    ).otherwise(lit(0))
    scoped = scoped.withColumn(
        "_cand_score",
        when(col("_cand_mukim_code").isNull(), lit(999)).otherwise(name_dist + district_penalty),
    )
    scoped = scoped.withColumn(
        "_cand_label",
        concat_ws(
            " :: ",
            col("_cand_state_name"),
            col("_cand_district_name"),
            col("_cand_mukim_name"),
            col("_cand_mukim_code"),
        ),
    )

    rank_window = Window.partitionBy("_row_id").orderBy(
        col("_cand_score").asc(),
        length(col("_cand_mukim_name")).desc_nulls_last(),
    )
    ranked = scoped.withColumn("_cand_rank", row_number().over(rank_window)).filter(
        (col("_cand_rank") <= lit(top_k)) & col("_cand_mukim_code").isNotNull()
    )
    ranked_summary = ranked.groupBy("_row_id").agg(
        expr(
            "transform(sort_array(collect_list(named_struct('r', _cand_rank, 'v', _cand_label))), x -> x.v)"
        ).alias("top_3_candidates"),
        expr("max(CASE WHEN _cand_rank = 1 THEN _cand_score END)").alias("_top_cand_score"),
        expr("max(CASE WHEN _cand_rank = 1 THEN _cand_district_code END)").alias("_top_district_code"),
        expr("max(CASE WHEN _cand_rank = 1 THEN _cand_district_name END)").alias("_top_district_name"),
        expr("max(CASE WHEN _cand_rank = 1 THEN _cand_mukim_code END)").alias("_top_mukim_code"),
        expr("max(CASE WHEN _cand_rank = 1 THEN _cand_mukim_name END)").alias("_top_mukim_name"),
        expr("max(CASE WHEN _cand_rank = 1 THEN _cand_mukim_id END)").alias("_top_mukim_id"),
    )

    df = df.join(ranked_summary, "_row_id", "left")
    df = df.withColumn("top_3_candidates", coalesce(col("top_3_candidates"), expr("array()")))
    top_candidate_strong = col("_top_cand_score").isNotNull() & (col("_top_cand_score") <= lit(2))
    district_from_candidate = col("district_code").isNull() & top_candidate_strong & col("_top_district_code").isNotNull()
    mukim_from_candidate = col("mukim_code").isNull() & top_candidate_strong & col("_top_mukim_code").isNotNull()

    df = df.withColumn("_district_from_candidate", district_from_candidate)
    df = df.withColumn("_mukim_from_candidate", mukim_from_candidate)
    df = df.withColumn(
        "district_code",
        when(district_from_candidate, col("_top_district_code")).otherwise(col("district_code")),
    )
    df = df.withColumn(
        "district_name",
        when(col("district_name").isNull() & district_from_candidate, col("_top_district_name")).otherwise(col("district_name")),
    )
    df = df.withColumn(
        "mukim_code",
        when(mukim_from_candidate, col("_top_mukim_code")).otherwise(col("mukim_code")),
    )
    df = df.withColumn(
        "mukim_name",
        when(col("mukim_name").isNull() & mukim_from_candidate, col("_top_mukim_name")).otherwise(col("mukim_name")),
    )
    df = df.withColumn(
        "mukim_id",
        when(col("mukim_id").isNull() & mukim_from_candidate, col("_top_mukim_id")).otherwise(col("mukim_id")),
    )
    df = df.withColumn("candidate_match_score", col("_top_cand_score").cast("int"))

    return df.drop(
        "_query_mukim",
        "_query_locality",
        "_query_district",
        "_top_cand_score",
        "_top_district_code",
        "_top_district_name",
        "_top_mukim_code",
        "_top_mukim_name",
        "_top_mukim_id",
    )


def clean_addresses(
    df: DataFrame,
    address_col: Optional[str] = None,
    lookups_dir: str = "data/lookups",
    config_path: Optional[str] = None,
) -> DataFrame:
    config = _load_config(config_path)
    spark = df.sparkSession
    state_df, district_df, mukim_df, postcode_df, district_alias_df = load_lookups(spark, lookups_dir)
    df = _standardize_common_columns(df, config)
    preserve_after_parse = [
        "premise_no",
        "lot_no",
        "unit_no",
        "floor_no",
        "building_name",
        "street_name",
        "locality_name",
        "mukim_name",
        "district_name",
        "state_name",
        "postcode",
        "address_type",
    ]

    address_candidates = [
        "address",
        "full_address",
        "fulladdress",
        "addr",
        "alamat_penuh",
        "alamat",
        "alamat_lama",
        "alamatlama",
        "alamat_asal",
        "alamatasal",
        "old_address",
        "address_old",
        "source_address",
        "raw_address",
        "address_full",
        "address_line",
    ]
    new_address_candidates = [
        "new_address",
        "alamat_baru",
        "alamat_bar",
        "alamatbar",
        "alamatbaharu",
        "alamatbaru",
        "address_new",
        "newalamat",
    ]
    address_candidates = config.get("address_columns", address_candidates)
    new_address_candidates = config.get("new_address_columns", new_address_candidates)

    if address_col is None:
        address_col = _detect_address_col(
            df,
            address_candidates,
        )
    new_address_col = _detect_new_address_col(
        df,
        new_address_candidates,
    )
    if not address_col:
        structured_address_cols = config.get(
            "structured_address_columns",
            [
                "premise_no",
                "house_no",
                "lot_no",
                "unit_no",
                "floor_no",
                "building_name",
                "street_name",
                "sub_locality_1",
                "sub_locality_2",
                "locality_name",
                "mukim_name",
                "district_name",
                "state_name",
                "postcode",
                "country",
            ],
        )
        usable_cols = [c for c in structured_address_cols if c in df.columns]
        if usable_cols:
            df = _compose_structured_address(
                df,
                source_cols=usable_cols,
                target_col="_structured_source_address",
            )
            address_col = "_structured_source_address"
    if not address_col:
        raise ValueError(
            "Address column not found and no structured address columns available. "
            "Provide address_col or update --config (address_columns/structured_address_columns). "
            f"Columns found: {df.columns}"
        )

    lat_candidates = [
        "latitude",
        "lat",
        "y",
        "coord_lat",
        "lat_deg",
        "latitude_deg",
    ]
    lon_candidates = [
        "longitude",
        "lon",
        "lng",
        "long",
        "x",
        "coord_lon",
        "lon_deg",
        "lng_deg",
        "longitude_deg",
    ]
    lat_candidates = config.get("latitude_columns", lat_candidates)
    lon_candidates = config.get("longitude_columns", lon_candidates)

    lat_col = _detect_col(df, lat_candidates)
    lon_col = _detect_col(df, lon_candidates)
    if lat_col and lat_col != "latitude" and "latitude" not in df.columns:
        df = df.withColumnRenamed(lat_col, "latitude")
    if lon_col and lon_col != "longitude" and "longitude" not in df.columns:
        df = df.withColumnRenamed(lon_col, "longitude")

    df = df.withColumn("_row_id", monotonically_increasing_id())
    df = df.withColumn("source_address_old", col(address_col).cast("string"))
    if new_address_col:
        df = df.withColumn("source_address_new", col(new_address_col).cast("string"))
    else:
        df = df.withColumn("source_address_new", lit(None).cast("string"))

    df = _ensure_column(df, "latitude")
    df = _ensure_column(df, "longitude")
    df = df.withColumn("latitude", col("latitude").cast("double"))
    df = df.withColumn("longitude", col("longitude").cast("double"))
    df = df.withColumn("geom", _point_wkt_from_lat_lon(col("latitude"), col("longitude")))
    df = df.withColumn("geometry", col("geom"))

    old_norm = lower(trim(col("source_address_old")))
    new_norm = lower(trim(col("source_address_new")))
    old_missing = old_norm.isNull() | old_norm.isin("", "nan", "null", "none", "na", "n/a")
    new_missing = new_norm.isNull() | new_norm.isin("", "nan", "null", "none", "na", "n/a")

    df = df.withColumn("source_address_old", when(old_missing, lit(None)).otherwise(col("source_address_old")))
    df = df.withColumn("source_address_new", when(new_missing, lit(None)).otherwise(col("source_address_new")))

    df = df.withColumn(
        "address_for_parse",
        when(~new_missing, col("source_address_new")).otherwise(col("source_address_old")),
    )
    df = df.withColumn(
        "address_source",
        when(~new_missing, lit("new")).otherwise(lit("old")),
    )
    df = df.withColumn("is_missing_address", old_missing & new_missing)

    replacements = config.get("text_replacements")
    preserve_lot_no_from_source = _config_bool(config, "preserve_lot_no_from_source", False)
    for preserve_col in preserve_after_parse:
        if preserve_col in df.columns:
            df = df.withColumn(f"_orig_{preserve_col}", col(preserve_col))

    df = parse_full_address(df, "address_for_parse", replacements=replacements)
    df = _ensure_column(df, "postcode")
    df = df.withColumn("postcode_code", _clean_text_expr(col("postcode")))
    for preserve_col in preserve_after_parse:
        orig_col = f"_orig_{preserve_col}"
        if orig_col in df.columns:
            if preserve_col == "lot_no" and not preserve_lot_no_from_source:
                # Avoid injecting lot numbers from legacy source columns when the parsed
                # selected address does not explicitly contain LOT/PT markers.
                lot_from_source = _clean_text_expr(col(orig_col))
                parse_has_lot_marker = upper(coalesce(col("address_for_parse"), lit(""))).rlike(r"\b(?:LOT|PT)\b")
                df = (
                    df.withColumn(
                        "lot_no",
                        when(parse_has_lot_marker, coalesce(col("lot_no"), lot_from_source)).otherwise(col("lot_no")),
                    ).drop(orig_col)
                )
            else:
                df = df.withColumn(preserve_col, coalesce(_clean_text_expr(col(orig_col)), col(preserve_col))).drop(orig_col)

    df = _ensure_column(df, "street_name")
    df = _ensure_column(df, "sub_locality_1")
    df = _ensure_column(df, "sub_locality_2")
    df = _ensure_column(df, "locality_name")
    df = _ensure_column(df, "mukim_name")

    street_like_pattern = r"(?i)\\b(JALAN\\s+RAYA|JALANRAYA|JALAN|JLN|LORONG|LRG|PERSIARAN|LEBUHRAYA|LEBUH)\\b"
    bandar_like_pattern = r"(?i)\\b(BANDAR(?:\\s+BARU)?|BANDARAYA|PEKAN|CITY)\\b"
    mukim_like_pattern = r"(?i)\\bMUKIM\\b"

    sub1 = col("sub_locality_1")
    sub2 = col("sub_locality_2")
    locality = col("locality_name")
    street = col("street_name")
    mukim = col("mukim_name")

    street_from_sub = when(sub2.rlike(street_like_pattern), sub2).when(sub1.rlike(street_like_pattern), sub1)
    locality_from_sub = when(sub2.rlike(bandar_like_pattern), sub2).when(sub1.rlike(bandar_like_pattern), sub1)
    mukim_from_locality = when(locality.rlike(mukim_like_pattern), locality)
    mukim_from_sub = when(sub2.rlike(mukim_like_pattern), sub2).when(sub1.rlike(mukim_like_pattern), sub1)

    street_is_road = street.rlike(street_like_pattern)
    df = df.withColumn(
        "street_name",
        when(street.isNull() | (~street_is_road), coalesce(street_from_sub, street)).otherwise(street),
    )
    df = df.withColumn("mukim_name", coalesce(mukim, mukim_from_locality, mukim_from_sub))
    df = df.withColumn("locality_name", when(locality.rlike(mukim_like_pattern), lit(None).cast("string")).otherwise(locality))
    df = df.withColumn("locality_name", coalesce(col("locality_name"), locality_from_sub))

    sub_invalid_pattern = (
        r"(?i)\\b(JALAN\\s+RAYA|JALANRAYA|JALAN|JLN|LORONG|LRG|PERSIARAN|LEBUHRAYA|LEBUH|"
        r"BANDAR(?:\\s+BARU)?|BANDARAYA|PEKAN|CITY|MUKIM)\\b"
    )
    df = df.withColumn("sub_locality_1", when(col("sub_locality_1").rlike(sub_invalid_pattern), lit(None)).otherwise(col("sub_locality_1")))
    df = df.withColumn("sub_locality_2", when(col("sub_locality_2").rlike(sub_invalid_pattern), lit(None)).otherwise(col("sub_locality_2")))
    # Keep sub-locality optional and non-redundant with street/locality/mukim.
    df = df.withColumn("_sub1_clean", _clean_text_expr(col("sub_locality_1")))
    df = df.withColumn("_sub2_clean", _clean_text_expr(col("sub_locality_2")))
    sub1_same_core = (
        col("_sub1_clean").isNotNull()
        & (
            (upper(col("_sub1_clean")) == upper(coalesce(col("street_name"), lit(""))))
            | (upper(col("_sub1_clean")) == upper(coalesce(col("building_name"), lit(""))))
            | (upper(col("_sub1_clean")) == upper(coalesce(col("locality_name"), lit(""))))
            | (upper(col("_sub1_clean")) == upper(coalesce(col("mukim_name"), lit(""))))
        )
    )
    sub2_same_core = (
        col("_sub2_clean").isNotNull()
        & (
            (upper(col("_sub2_clean")) == upper(coalesce(col("street_name"), lit(""))))
            | (upper(col("_sub2_clean")) == upper(coalesce(col("building_name"), lit(""))))
            | (upper(col("_sub2_clean")) == upper(coalesce(col("locality_name"), lit(""))))
            | (upper(col("_sub2_clean")) == upper(coalesce(col("mukim_name"), lit(""))))
        )
    )
    df = df.withColumn("_sub1_clean", when(sub1_same_core, lit(None).cast("string")).otherwise(col("_sub1_clean")))
    df = df.withColumn("_sub2_clean", when(sub2_same_core, lit(None).cast("string")).otherwise(col("_sub2_clean")))
    df = df.withColumn(
        "sub_locality_levels",
        expr(
            "array_distinct(filter(array(_sub1_clean, _sub2_clean), x -> x is not null and length(trim(x)) > 0))"
        ),
    )
    df = df.withColumn("sub_locality_1", expr("try_element_at(sub_locality_levels, 1)"))
    df = df.withColumn("sub_locality_2", expr("try_element_at(sub_locality_levels, 2)"))
    df = df.drop("_sub1_clean", "_sub2_clean")

    df = _ensure_column(df, "state_name")
    df = _ensure_column(df, "state_code")
    df = _ensure_column(df, "district_name")
    df = _ensure_column(df, "district_code")
    df = _ensure_column(df, "mukim_name")
    df = _ensure_column(df, "mukim_code")
    df = _ensure_column(df, "mukim_id")
    df = _ensure_column(df, "pbt_id")
    df = _ensure_column(df, "pbt_name")
    df = _ensure_column(df, "_locality_state_name")

    postcode_lookup = postcode_df.select(
        col("postcode").alias("postcode_lookup"),
        upper(col("city")).alias("postcode_city"),
        upper(col("state")).alias("postcode_state_name"),
    )
    df = df.join(postcode_lookup, col("postcode_code") == col("postcode_lookup"), "left").drop("postcode_lookup")
    df = df.withColumn("locality_name", coalesce(col("locality_name"), col("postcode_city")))
    df = df.withColumn("postcode_name", initcap(lower(coalesce(col("postcode_city"), col("locality_name")))))
    df = df.withColumn(
        "postcode",
        when(
            col("postcode_code").isNotNull() & col("postcode_name").isNotNull(),
            concat_ws(" ", col("postcode_code"), col("postcode_name")),
        ).otherwise(col("postcode_code")),
    )

    postcode_boundary_enabled = config.get("postcode_boundary_enabled", True)
    strict_boundary_validation = _config_bool(config, "strict_boundary_validation", True)
    strict_postcode_boundary = _config_bool(config, "strict_postcode_boundary", strict_boundary_validation)
    strict_admin_boundary = _config_bool(config, "strict_admin_boundary", strict_boundary_validation)
    postcode_boundary_dir = config.get("postcode_boundary_dir", os.path.join("data", "boundary", "02_Postcode_boundary"))
    postcode_boundary_simplify_tolerance = float(config.get("postcode_boundary_simplify_tolerance", 0.0))
    if postcode_boundary_enabled and postcode_boundary_dir and os.path.exists(postcode_boundary_dir):
        try:
            register_sedona(spark)
            postcode_boundary_df = _load_pbt_boundaries(
                spark,
                postcode_boundary_dir,
                simplify_tolerance=postcode_boundary_simplify_tolerance,
            )
            boundary_cols = {c.lower(): c for c in postcode_boundary_df.columns}
            postcode_boundary_postcode_col = config.get("postcode_boundary_postcode_column", "POSTCODE")
            postcode_boundary_city_col = config.get("postcode_boundary_city_column", "TOWN_CITY")
            postcode_boundary_state_col = config.get("postcode_boundary_state_column", "STATE")
            postcode_boundary_postcode_col = boundary_cols.get(postcode_boundary_postcode_col.lower())
            postcode_boundary_city_col = boundary_cols.get(postcode_boundary_city_col.lower())
            postcode_boundary_state_col = boundary_cols.get(postcode_boundary_state_col.lower())
            if not postcode_boundary_postcode_col or not postcode_boundary_city_col or not postcode_boundary_state_col:
                raise ValueError(
                    "Postcode boundary shapefile missing required columns. "
                    f"Found: {sorted(postcode_boundary_df.columns)}"
                )
            df = _assign_postcode_from_boundary(
                df,
                postcode_boundary_df,
                postcode_col=postcode_boundary_postcode_col,
                city_col=postcode_boundary_city_col,
                state_col=postcode_boundary_state_col,
            )
        except Exception as exc:
            if strict_postcode_boundary:
                raise RuntimeError(
                    "Postcode boundary validation is enabled but failed. "
                    "Fix Sedona/shapefile setup or set strict_postcode_boundary=false."
                ) from exc
            print(f"Warning: Postcode boundary mapping skipped due to Sedona/shapefile issue: {exc}")
            df = df.withColumn("_postcode_from_boundary", lit(False))
            df = df.withColumn("_postcode_boundary_conflict", lit(False))
    else:
        if postcode_boundary_enabled and strict_postcode_boundary:
            raise FileNotFoundError(
                "Postcode boundary validation is enabled but boundary directory is unavailable: "
                f"{postcode_boundary_dir}"
            )
        df = df.withColumn("_postcode_from_boundary", lit(False))
        df = df.withColumn("_postcode_boundary_conflict", lit(False))

    df = df.withColumn("postcode_name", initcap(lower(coalesce(col("postcode_name"), col("postcode_city"), col("locality_name")))))
    df = df.withColumn(
        "postcode",
        when(
            col("postcode_code").isNotNull() & col("postcode_name").isNotNull(),
            concat_ws(" ", col("postcode_code"), col("postcode_name")),
        ).otherwise(col("postcode_code")),
    )

    locality_enabled = config.get("locality_enabled", True)
    locality_lookup_path = config.get(
        "locality_lookup_path",
        os.path.join(lookups_dir, "locality_lookup.csv"),
    )
    if locality_enabled and locality_lookup_path and os.path.exists(locality_lookup_path):
        locality_df = spark.read.csv(locality_lookup_path, header=True)
        locality_lookup = locality_df.select(
            col("locality_name"),
            col("state_name").alias("_lk_locality_state_name"),
        )

        df = _rename_if_exists(df, "locality_name", "_locality_name_orig")
        df = match_lookup_exact(df, locality_lookup, "locality_name", ["locality_name", "_lk_locality_state_name"])
        df = df.withColumn("locality_name", coalesce(col("locality_name"), col("_locality_name_orig")))
        df = df.withColumn(
            "_locality_state_name",
            coalesce(col("_locality_state_name"), col("_lk_locality_state_name")),
        ).drop("_lk_locality_state_name")

        df = _rename_if_exists(df, "locality_name", "_locality_name_exact")
        # Guard against duplicate locality_name columns before fuzzy join.
        if "locality_name" in df.columns:
            df = df.drop("locality_name")
        df = match_lookup_fuzzy(
            df,
            locality_lookup,
            "locality_name",
            ["locality_name", "_lk_locality_state_name"],
            state_name_col="state_name",
            lookup_state_name_col="_lk_locality_state_name",
            seg_cols=["seg2_norm", "seg3_norm", "seg4_norm", "seg5_norm", "tail2_norm", "tail3_norm", "tail4_norm"],
            max_dist_short=1,
            max_dist_long=2,
        )
        df = df.withColumn("locality_name", coalesce(col("locality_name"), col("_locality_name_exact")))
        df = df.withColumn(
            "_locality_state_name",
            coalesce(col("_locality_state_name"), col("_lk_locality_state_name")),
        ).drop("_lk_locality_state_name", "_locality_name_orig", "_locality_name_exact")

    df = _ensure_column(df, "floor_level")
    df = df.withColumn("floor_level", coalesce(col("floor_level"), col("floor_no")))
    df = _ensure_column(df, "country")
    df = df.withColumn("country", coalesce(col("country"), lit("MALAYSIA")))

    df = _ensure_column(df, "address_type")
    df = df.withColumn("address_type_raw", _clean_text_expr(col("address_type")))
    addr_up = upper(col("address_clean"))
    bldg_up = upper(coalesce(col("building_name"), lit("")))
    source_type_up = upper(coalesce(col("address_type_raw"), lit("")))

    highrise_pattern = (
        r"(APARTMENT|APARTMEN|PANGSAPURI|KONDOMINIUM|CONDOMINIUM|KONDO|CONDO|"
        r"FLAT|RUMAH PANGSA|MENARA|TOWER|RESIDENSI|RESIDENCE|SOHO|SOFO|SERVICED RESIDENCE|PPR|PPA1M)"
    )
    institutional_pattern = r"(HOSPITAL|SCHOOL|MOSQUE|SURAU|POLICE STATION|BALAI POLIS|GOVERNMENT BUILDING|PEJABAT)"
    industrial_pattern = r"(FACTORY|INDUSTRIAL LOT|PERINDUSTRIAN|INDUSTRI|WAREHOUSE|GUDANG)"
    commercial_pattern = r"(SHOPLOT|SHOP LOT|SHOPPING MALL|MALL|PLAZA|KOMPLEKS|OFFICE TOWER|WISMA|ARKED)"
    rural_pattern = r"(ORANG ASLI SETTLEMENT|KAMPUNG|KG|FELDA|FELCRA|LADANG|ESTET|RKT|RPT|RPS)"
    residential_pattern = r"(BUNGALOW|SEMI-D|SEMI D|TERRACE HOUSE|KAMPUNG HOUSE|FELDA HOUSE|ESTATE HOUSE|RUMAH)"

    signal_text = concat_ws(" ", bldg_up, addr_up)

    derived_from_text = when(
        signal_text.rlike(highrise_pattern),
        lit("HIGHRISE"),
    ).when(
        signal_text.rlike(institutional_pattern),
        lit("INSTITUTIONAL"),
    ).when(
        signal_text.rlike(industrial_pattern),
        lit("INDUSTRIAL"),
    ).when(
        signal_text.rlike(commercial_pattern),
        lit("COMMERCIAL"),
    ).when(
        signal_text.rlike(rural_pattern),
        lit("RURAL"),
    ).when(
        signal_text.rlike(residential_pattern),
        lit("RESIDENTIAL"),
    )

    mapped_from_source = when(
        source_type_up.rlike(highrise_pattern),
        lit("HIGHRISE"),
    ).when(
        source_type_up.rlike(institutional_pattern),
        lit("INSTITUTIONAL"),
    ).when(
        source_type_up.rlike(industrial_pattern),
        lit("INDUSTRIAL"),
    ).when(
        source_type_up.rlike(commercial_pattern),
        lit("COMMERCIAL"),
    ).when(
        source_type_up.rlike(rural_pattern),
        lit("RURAL"),
    ).when(
        source_type_up.rlike(residential_pattern),
        lit("RESIDENTIAL"),
    )
    df = df.withColumn("address_type", coalesce(derived_from_text, mapped_from_source, lit("UNKNOWN")))

    df = _rename_if_exists(df, "state_code", "_state_code_orig")
    df = _rename_if_exists(df, "state_name", "_state_name_orig")
    df = match_lookup_exact(df, state_df, "state_name", ["state_code", "state_name"])
    if "_state_code_orig" in df.columns:
        df = df.withColumn("state_code", coalesce(col("state_code"), col("_state_code_orig"))).drop(
            "_state_code_orig"
        )
    if "_state_name_orig" in df.columns:
        df = df.withColumn("state_name", coalesce(col("state_name"), col("_state_name_orig"))).drop(
            "_state_name_orig"
        )
    df = df.withColumn(
        "state_name",
        coalesce(col("state_name"), col("postcode_state_name"), col("_locality_state_name")),
    )
    df = df.withColumn(
        "district_name",
        when(
            col("district_name").isNull()
            & (upper(col("locality_name")) == lit("MELAKA"))
            & ((col("state_code") == lit("04")) | (upper(col("state_name")) == lit("MELAKA"))),
            lit("MELAKA TENGAH"),
        ).otherwise(col("district_name")),
    )
    df = _rename_if_exists(df, "district_code", "_district_code_orig")
    df = _rename_if_exists(df, "district_name", "_district_name_orig")

    district_lookup_df = district_df
    if district_alias_df is not None:
        alias_rows = district_alias_df.select(
            col("state_code"),
            col("district_code"),
            col("district_alias").alias("district_name"),
        )
        district_lookup_df = district_df.unionByName(alias_rows)

    df = match_lookup_fuzzy(
        df,
        district_lookup_df,
        "district_name",
        ["district_code", "district_name"],
        state_code_col="state_code",
        lookup_state_code_col="state_code",
        seg_cols=["seg2_norm", "seg3_norm", "seg4_norm", "seg5_norm", "tail2_norm", "tail3_norm", "tail4_norm"],
        max_dist_short=1,
        max_dist_long=2,
    )
    if "_district_code_orig" in df.columns:
        df = df.withColumn("district_code", coalesce(col("district_code"), col("_district_code_orig"))).drop(
            "_district_code_orig"
        )
    if "_district_name_orig" in df.columns:
        df = df.withColumn("district_name", coalesce(col("district_name"), col("_district_name_orig"))).drop(
            "_district_name_orig"
        )

    district_canonical = district_df.select(
        col("state_code").alias("_dc_state_code"),
        col("district_code").alias("_dc_district_code"),
        col("district_name").alias("_dc_district_name"),
    )
    df = df.join(
        district_canonical,
        (col("state_code") == col("_dc_state_code"))
        & (col("district_code") == col("_dc_district_code")),
        "left",
    )
    df = df.withColumn("district_lookup_valid", col("_dc_district_name").isNotNull())
    df = df.withColumn("district_name", coalesce(col("_dc_district_name"), col("district_name")))
    df = df.drop("_dc_state_code", "_dc_district_code", "_dc_district_name")
    df = _rename_if_exists(df, "mukim_code", "_mukim_code_orig")
    df = _rename_if_exists(df, "mukim_name", "_mukim_name_orig")
    df = _rename_if_exists(df, "mukim_id", "_mukim_id_orig")
    df = match_mukim_fuzzy(df, mukim_df)
    if "_mukim_code_orig" in df.columns:
        df = df.withColumn("mukim_code", coalesce(col("mukim_code"), col("_mukim_code_orig"))).drop(
            "_mukim_code_orig"
        )
    if "_mukim_name_orig" in df.columns:
        df = df.withColumn("mukim_name", coalesce(col("mukim_name"), _clean_text_expr(col("_mukim_name_orig")))).drop(
            "_mukim_name_orig"
        )
    if "_mukim_id_orig" in df.columns:
        df = df.withColumn("mukim_id", coalesce(col("mukim_id"), col("_mukim_id_orig"))).drop("_mukim_id_orig")

    admin_boundary_enabled = config.get("admin_boundary_enabled", True)
    state_boundary_dir = config.get("state_boundary_dir", os.path.join("data", "boundary", "01_State_boundary"))
    district_boundary_dir = config.get("district_boundary_dir", os.path.join("data", "boundary", "03_District_boundary"))
    mukim_boundary_dir = config.get("mukim_boundary_dir", os.path.join("data", "boundary", "05_Mukim_boundary"))
    admin_boundary_simplify_tolerance = float(config.get("admin_boundary_simplify_tolerance", 0.0))
    admin_boundary_missing: list[str] = []
    if admin_boundary_enabled:
        for path in [state_boundary_dir, district_boundary_dir, mukim_boundary_dir]:
            if not os.path.exists(path):
                admin_boundary_missing.append(path)
    if admin_boundary_enabled and not admin_boundary_missing:
        try:
            register_sedona(spark)
            state_boundary_df = _load_pbt_boundaries(
                spark,
                state_boundary_dir,
                simplify_tolerance=admin_boundary_simplify_tolerance,
            )
            district_boundary_df = _load_pbt_boundaries(
                spark,
                district_boundary_dir,
                simplify_tolerance=admin_boundary_simplify_tolerance,
            )
            mukim_boundary_df = _load_pbt_boundaries(
                spark,
                mukim_boundary_dir,
                simplify_tolerance=admin_boundary_simplify_tolerance,
            )
            df = _assign_admin_from_boundaries(
                df,
                state_df=state_boundary_df,
                district_df=district_boundary_df,
                mukim_df=mukim_boundary_df,
            )
        except Exception as exc:
            if strict_admin_boundary:
                raise RuntimeError(
                    "Admin boundary validation is enabled but failed. "
                    "Fix Sedona/shapefile setup or set strict_admin_boundary=false."
                ) from exc
            print(f"Warning: Admin boundary mapping skipped due to Sedona/shapefile issue: {exc}")
            df = df.withColumn("_state_from_boundary", lit(False))
            df = df.withColumn("_district_from_boundary", lit(False))
            df = df.withColumn("_mukim_from_boundary", lit(False))
            df = df.withColumn("_state_boundary_conflict", lit(False))
            df = df.withColumn("_district_boundary_conflict", lit(False))
            df = df.withColumn("_mukim_boundary_conflict", lit(False))
    else:
        if admin_boundary_enabled and strict_admin_boundary:
            raise FileNotFoundError(
                "Admin boundary validation is enabled but required directories are unavailable: "
                + ", ".join(admin_boundary_missing)
            )
        df = df.withColumn("_state_from_boundary", lit(False))
        df = df.withColumn("_district_from_boundary", lit(False))
        df = df.withColumn("_mukim_from_boundary", lit(False))
        df = df.withColumn("_state_boundary_conflict", lit(False))
        df = df.withColumn("_district_boundary_conflict", lit(False))
        df = df.withColumn("_mukim_boundary_conflict", lit(False))

    unique_mukim = (
        mukim_df.groupBy("state_code", "district_code")
        .agg(
            count(lit(1)).alias("_mukim_count"),
            first("mukim_code").alias("_mukim_code_only"),
            first("mukim_name").alias("_mukim_name_only"),
            first("mukim_id").alias("_mukim_id_only"),
        )
        .filter(col("_mukim_count") == 1)
    )
    df = df.join(unique_mukim, ["state_code", "district_code"], "left")
    df = df.withColumn("mukim_code", coalesce(col("mukim_code"), col("_mukim_code_only")))
    df = df.withColumn("mukim_name", coalesce(col("mukim_name"), col("_mukim_name_only")))
    df = df.withColumn("mukim_id", coalesce(col("mukim_id"), col("_mukim_id_only")))
    df = df.drop("_mukim_count", "_mukim_code_only", "_mukim_name_only", "_mukim_id_only")

    default_window = Window.partitionBy("state_code", "district_code").orderBy(
        col("mukim_code").asc_nulls_last(),
        col("mukim_name").asc_nulls_last(),
    )
    default_mukim = (
        mukim_df.withColumn("_rn", row_number().over(default_window))
        .filter(col("_rn") == 1)
        .select(
            col("state_code"),
            col("district_code"),
            col("mukim_code").alias("_mukim_code_default"),
            col("mukim_name").alias("_mukim_name_default"),
            col("mukim_id").alias("_mukim_id_default"),
        )
    )
    df = df.join(default_mukim, ["state_code", "district_code"], "left")
    df = df.withColumn("mukim_code", coalesce(col("mukim_code"), col("_mukim_code_default")))
    df = df.withColumn("mukim_name", coalesce(col("mukim_name"), col("_mukim_name_default")))
    df = df.withColumn("mukim_id", coalesce(col("mukim_id"), col("_mukim_id_default")))
    df = df.drop("_mukim_code_default", "_mukim_name_default", "_mukim_id_default")
    df = _attach_admin_candidates(df, mukim_df, top_k=3)

    mukim_canonical = (
        mukim_df.select(
            col("state_code").alias("_mk_state_code"),
            col("district_code").alias("_mk_district_code"),
            col("mukim_code").alias("_mk_mukim_code"),
            upper(col("mukim_name")).alias("_mk_mukim_name"),
            col("mukim_id").cast("int").alias("_mk_mukim_id"),
        )
        .dropDuplicates(["_mk_state_code", "_mk_district_code", "_mk_mukim_code"])
    )
    df = df.join(
        mukim_canonical,
        (col("state_code") == col("_mk_state_code"))
        & (col("district_code") == col("_mk_district_code"))
        & (col("mukim_code") == col("_mk_mukim_code")),
        "left",
    )
    df = df.withColumn("mukim_name", coalesce(col("_mk_mukim_name"), col("mukim_name")))
    df = df.withColumn("mukim_id", coalesce(col("mukim_id"), col("_mk_mukim_id")))
    df = df.withColumn("mukim_lookup_valid", col("_mk_mukim_name").isNotNull())
    df = df.drop("_mk_state_code", "_mk_district_code", "_mk_mukim_code", "_mk_mukim_name", "_mk_mukim_id")

    pbt_enabled = config.get("pbt_enabled", True)
    pbt_dir = config.get("pbt_dir", os.path.join("data", "Sempadan Kawalan PBT"))
    pbt_simplify_tolerance = float(config.get("pbt_simplify_tolerance", 0.0))
    if pbt_enabled and pbt_dir and os.path.exists(pbt_dir):
        try:
            register_sedona(spark)
            pbt_df = _load_pbt_boundaries(spark, pbt_dir, simplify_tolerance=pbt_simplify_tolerance)
            pbt_cols = {c.lower(): c for c in pbt_df.columns}
            pbt_id_col = config.get("pbt_id_column", "pbt_id")
            pbt_name_col = config.get("pbt_name_column", "NAMA_PBT")
            pbt_id_col = pbt_cols.get(pbt_id_col.lower())
            pbt_name_col = pbt_cols.get(pbt_name_col.lower())
            if not pbt_id_col or not pbt_name_col:
                raise ValueError(
                    "PBT shapefile missing required columns. "
                    f"Found: {sorted(pbt_df.columns)}"
                )
            df = _assign_pbt(
                df,
                pbt_df,
                pbt_id_col=pbt_id_col,
                pbt_name_col=pbt_name_col,
            )
        except Exception as exc:
            print(f"Warning: PBT mapping skipped due to Sedona/shapefile issue: {exc}")

    state_map = state_df.select(
        upper(col("state_name")).alias("_state_name_lookup"),
        col("state_code").alias("_state_code_lookup"),
    )
    df = df.join(state_map, upper(col("state_name")) == col("_state_name_lookup"), "left")
    df = df.withColumn("state_code", coalesce(col("state_code"), col("_state_code_lookup")))
    state_canonical = state_df.select(
        col("state_code").alias("_sc_state_code"),
        upper(col("state_name")).alias("_sc_state_name"),
    )
    df = df.join(state_canonical, col("state_code") == col("_sc_state_code"), "left")
    df = df.withColumn("state_name", coalesce(col("_sc_state_name"), col("state_name")))
    df = df.withColumn("state_lookup_valid", col("_sc_state_name").isNotNull())
    has_postcode = (col("postcode_code").isNotNull()) & (length(col("postcode_code")) == 5)
    has_state = col("state_code").isNotNull()
    has_district = col("district_code").isNotNull()
    has_mukim = col("mukim_code").isNotNull()
    has_locality = col("locality_name").isNotNull()
    has_pbt = col("pbt_id").isNotNull()
    has_postcode_boundary = col("_postcode_from_boundary") == lit(True)
    has_postcode_boundary_conflict = col("_postcode_boundary_conflict") == lit(True)
    has_state_boundary = col("_state_from_boundary") == lit(True)
    has_district_boundary = col("_district_from_boundary") == lit(True)
    has_mukim_boundary = col("_mukim_from_boundary") == lit(True)
    has_state_boundary_conflict = col("_state_boundary_conflict") == lit(True)
    has_district_boundary_conflict = col("_district_boundary_conflict") == lit(True)
    has_mukim_boundary_conflict = col("_mukim_boundary_conflict") == lit(True)

    state_from_postcode = has_postcode & col("postcode_state_name").isNotNull() & (
        upper(col("state_name")) == upper(col("postcode_state_name"))
    )
    state_from_locality = col("_locality_state_name").isNotNull() & (
        upper(col("state_name")) == upper(col("_locality_state_name"))
    )
    state_conflict_postcode = has_postcode & has_state & col("postcode_state_name").isNotNull() & (
        upper(col("state_name")) != upper(col("postcode_state_name"))
    )
    df = df.withColumn("_state_postcode_conflict", state_conflict_postcode)

    sub1_norm = upper(coalesce(col("sub_locality_1"), lit("")))
    sub2_norm = upper(coalesce(col("sub_locality_2"), lit("")))
    locality_norm = upper(coalesce(col("locality_name"), lit("")))
    suspicious_sub_has_street = sub1_norm.rlike(
        r"\\b(JALAN\\s+RAYA|JALANRAYA|JALAN|JLN|LORONG|LRG|PERSIARAN|LEBUHRAYA|LEBUH)\\b"
    ) | sub2_norm.rlike(r"\\b(JALAN\\s+RAYA|JALANRAYA|JALAN|JLN|LORONG|LRG|PERSIARAN|LEBUHRAYA|LEBUH)\\b")
    suspicious_sub_has_bandar = sub1_norm.rlike(r"\\b(BANDAR(?:\\s+BARU)?|BANDARAYA|PEKAN|CITY)\\b") | sub2_norm.rlike(
        r"\\b(BANDAR(?:\\s+BARU)?|BANDARAYA|PEKAN|CITY)\\b"
    )
    suspicious_locality_has_mukim = locality_norm.rlike(r"\\bMUKIM\\b")
    suspicious_missing_street = col("street_name").isNull() | (length(trim(col("street_name"))) == 0)

    confidence = lit(0)
    confidence = confidence + when(has_postcode, lit(10)).otherwise(lit(0))
    confidence = confidence + when(has_state, lit(20)).otherwise(lit(0))
    confidence = confidence + when(has_district, lit(20)).otherwise(lit(0))
    confidence = confidence + when(has_mukim, lit(15)).otherwise(lit(0))
    confidence = confidence + when(has_locality, lit(10)).otherwise(lit(0))
    confidence = confidence + when(has_pbt, lit(10)).otherwise(lit(0))
    confidence = confidence + when(has_postcode_boundary, lit(10)).otherwise(lit(0))
    confidence = confidence + when(state_from_postcode, lit(5)).otherwise(lit(0))
    confidence = confidence + when(state_from_locality, lit(5)).otherwise(lit(0))
    confidence = confidence + when(has_district & has_mukim, lit(5)).otherwise(lit(0))
    confidence = confidence + when(has_pbt & has_state, lit(5)).otherwise(lit(0))
    confidence = confidence + when(has_state_boundary, lit(10)).otherwise(lit(0))
    confidence = confidence + when(has_district_boundary, lit(10)).otherwise(lit(0))
    confidence = confidence + when(has_mukim_boundary, lit(10)).otherwise(lit(0))
    confidence = confidence - when(has_state_boundary_conflict, lit(35)).otherwise(lit(0))
    confidence = confidence - when(has_district_boundary_conflict, lit(35)).otherwise(lit(0))
    confidence = confidence - when(has_mukim_boundary_conflict, lit(35)).otherwise(lit(0))
    confidence = confidence - when(has_postcode_boundary_conflict, lit(35)).otherwise(lit(0))
    confidence = confidence - when(state_conflict_postcode, lit(25)).otherwise(lit(0))
    confidence = confidence - when(suspicious_locality_has_mukim, lit(20)).otherwise(lit(0))
    confidence = confidence - when(suspicious_sub_has_street, lit(15)).otherwise(lit(0))
    confidence = confidence - when(suspicious_sub_has_bandar, lit(10)).otherwise(lit(0))
    confidence = confidence - when(suspicious_missing_street, lit(10)).otherwise(lit(0))

    df = df.withColumn(
        "confidence_score",
        when(confidence < lit(0), lit(0))
        .when(confidence > lit(100), lit(100))
        .otherwise(confidence)
        .cast("int"),
    )
    df = df.withColumn(
        "correction_notes",
        expr(
            "filter(array("
            "CASE WHEN _district_from_candidate THEN 'district_inferred_from_candidates' END, "
            "CASE WHEN _mukim_from_candidate THEN 'mukim_inferred_from_candidates' END, "
            "CASE WHEN _postcode_from_boundary THEN 'postcode_from_boundary' END, "
            "CASE WHEN _postcode_boundary_conflict THEN 'postcode_boundary_conflict' END, "
            "CASE WHEN _state_from_boundary THEN 'state_from_boundary' END, "
            "CASE WHEN _district_from_boundary THEN 'district_from_boundary' END, "
            "CASE WHEN _mukim_from_boundary THEN 'mukim_from_boundary' END, "
            "CASE WHEN _state_boundary_conflict THEN 'state_boundary_conflict' END, "
            "CASE WHEN _district_boundary_conflict THEN 'district_boundary_conflict' END, "
            "CASE WHEN _mukim_boundary_conflict THEN 'mukim_boundary_conflict' END, "
            "CASE WHEN _state_postcode_conflict THEN 'state_postcode_conflict' END, "
            "CASE WHEN postcode_code is not null AND postcode_name is not null THEN 'postcode_standardized_with_name' END"
            "), x -> x is not null)"
        ),
    )

    # Canonicalize address_clean into NAS component order.
    lot_part = when(_clean_text_expr(col("lot_no")).isNotNull(), concat(lit("LOT "), _clean_text_expr(col("lot_no"))))
    premise_part = when(
        _clean_text_expr(col("premise_no")).isNotNull(),
        concat(lit("NO "), _clean_text_expr(col("premise_no"))),
    )
    unit_part = when(_clean_text_expr(col("unit_no")).isNotNull(), concat(lit("UNIT "), _clean_text_expr(col("unit_no"))))
    level_part = when(
        _clean_text_expr(coalesce(col("floor_level"), col("floor_no"))).isNotNull(),
        concat(lit("LEVEL "), _clean_text_expr(coalesce(col("floor_level"), col("floor_no")))),
    )
    building_part = when(
        (col("address_type") == lit("HIGHRISE")) | _clean_text_expr(col("building_name")).isNotNull(),
        _clean_text_expr(col("building_name")),
    )
    street_prefix_clean = _clean_text_expr(col("street_name_prefix"))
    street_name_clean = _clean_text_expr(col("street_name"))
    street_part = (
        when(
            street_prefix_clean.isNotNull()
            & street_name_clean.isNotNull()
            & upper(street_name_clean).rlike(r"^(JALAN\\s+RAYA|JALANRAYA|JALAN|JLN|LORONG|LRG|PERSIARAN|LEBUHRAYA|LEBUH)\\b"),
            street_name_clean,
        )
        .when(
            street_prefix_clean.isNotNull() & street_name_clean.isNotNull(),
            concat(street_prefix_clean, lit(" "), street_name_clean),
        )
        .otherwise(street_name_clean)
    )
    canonical_address = concat_ws(
        ", ",
        lot_part,
        premise_part,
        unit_part,
        level_part,
        building_part,
        street_part,
        _clean_text_expr(col("sub_locality_1")),
        _clean_text_expr(col("sub_locality_2")),
        _clean_text_expr(col("locality_name")),
        _clean_text_expr(coalesce(col("postcode"), col("postcode_code"))),
        _clean_text_expr(col("state_name")),
    )
    df = df.withColumn(
        "address_clean",
        upper(coalesce(_clean_text_expr(canonical_address), _clean_text_expr(col("address_clean")))),
    )

    df = df.drop(
        "_state_name_lookup",
        "_state_code_lookup",
        "_sc_state_code",
        "_sc_state_name",
        "postcode_city",
        "postcode_state_name",
        "_locality_state_name",
        "_state_from_boundary",
        "_district_from_boundary",
        "_mukim_from_boundary",
        "_district_from_candidate",
        "_mukim_from_candidate",
    )

    return df.drop(
        "_row_id",
        "_address_norm",
        "seg2_norm",
        "seg3_norm",
        "seg4_norm",
        "seg5_norm",
        "tail2_norm",
        "tail3_norm",
        "tail4_norm",
        "sub_locality_name",
    )


def validate_addresses(
    df: DataFrame,
    *,
    require_mukim: bool = False,
    dedupe_on: str = "address_clean",
    min_confidence: int = 75,
) -> tuple[DataFrame, DataFrame]:
    df = _ensure_column(df, "_postcode_boundary_conflict")
    df = _ensure_column(df, "_state_postcode_conflict")
    df = _ensure_column(df, "_state_boundary_conflict")
    df = _ensure_column(df, "_district_boundary_conflict")
    df = _ensure_column(df, "_mukim_boundary_conflict")
    df = df.withColumn("_postcode_boundary_conflict", coalesce(col("_postcode_boundary_conflict").cast("boolean"), lit(False)))
    df = df.withColumn("_state_postcode_conflict", coalesce(col("_state_postcode_conflict").cast("boolean"), lit(False)))
    df = df.withColumn("_state_boundary_conflict", coalesce(col("_state_boundary_conflict").cast("boolean"), lit(False)))
    df = df.withColumn(
        "_district_boundary_conflict", coalesce(col("_district_boundary_conflict").cast("boolean"), lit(False))
    )
    df = df.withColumn("_mukim_boundary_conflict", coalesce(col("_mukim_boundary_conflict").cast("boolean"), lit(False)))
    postcode_col = col("postcode_code") if "postcode_code" in df.columns else col("postcode")
    window = Window.partitionBy(col(dedupe_on)).orderBy(col(dedupe_on))
    df = df.withColumn("_dup_rank", row_number().over(window))
    df = df.withColumn(
        "confidence_band",
        when(col("confidence_score") >= lit(95), lit("VERIFIED"))
        .when(col("confidence_score") >= lit(85), lit("HIGH"))
        .when(col("confidence_score") >= lit(70), lit("REVIEW"))
        .otherwise(lit("REJECT")),
    )

    reasons = array(
        when(col("is_missing_address") | col("address_clean").isNull() | (length(col("address_clean")) == 0), lit("missing_address")),
        when(postcode_col.isNull() | (length(postcode_col) != 5), lit("invalid_postcode")),
        when(col("_postcode_boundary_conflict") == lit(True), lit("postcode_boundary_conflict")),
        when(col("state_code").isNull(), lit("missing_state")),
        when(col("state_lookup_valid") == lit(False), lit("invalid_state_lookup")),
        when(col("district_code").isNull(), lit("missing_district")),
        when(col("district_lookup_valid") == lit(False), lit("invalid_district_lookup")),
        when(lit(require_mukim) & col("mukim_code").isNull(), lit("missing_mukim")),
        when(lit(require_mukim) & (col("mukim_lookup_valid") == lit(False)), lit("invalid_mukim_lookup")),
        when(col("_state_postcode_conflict") == lit(True), lit("state_postcode_conflict")),
        when(col("_state_boundary_conflict") == lit(True), lit("state_boundary_conflict")),
        when(col("_district_boundary_conflict") == lit(True), lit("district_boundary_conflict")),
        when(col("_mukim_boundary_conflict") == lit(True), lit("mukim_boundary_conflict")),
        when(col("confidence_score").isNull() | (col("confidence_score") < lit(min_confidence)), lit("low_confidence")),
        when(col("_dup_rank") > 1, lit("duplicate_address")),
    )
    df = df.withColumn("_reasons", reasons)
    df = df.withColumn("reason_codes", expr("filter(_reasons, x -> x is not null)")).drop("_reasons")
    df = df.withColumn("error_reasons", col("reason_codes"))
    df = df.withColumn("error_reason", concat_ws("|", col("reason_codes")))
    df = df.withColumn(
        "validation_status",
        when(coalesce(size(col("reason_codes")), lit(0)) == 0, lit("PASS")).otherwise(lit("FAIL")),
    )

    success = df.filter(col("validation_status") == "PASS").drop("_dup_rank")
    failed = df.filter(col("validation_status") == "FAIL")

    return success, failed
