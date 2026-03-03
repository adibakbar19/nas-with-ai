from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    broadcast,
    col,
    concat,
    instr,
    least,
    length,
    levenshtein,
    lit,
    row_number,
    upper,
    coalesce,
    when,
)


def _min_distance(name_col, seg_cols: list[str]):
    distances = [coalesce(levenshtein(name_col, col(seg)), lit(999)) for seg in seg_cols]
    return least(*distances)


def match_lookup_exact(df: DataFrame, lookup_df: DataFrame, name_col: str, cols_to_keep: list[str]) -> DataFrame:
    lookup = lookup_df.select(
        upper(col(name_col)).alias("_lk_name"),
        *[col(c) for c in cols_to_keep],
    )
    condition = instr(col("_address_norm"), concat(lit(" "), col("_lk_name"), lit(" "))) > 0

    joined = df.join(broadcast(lookup), condition, "left")
    window = Window.partitionBy("_row_id").orderBy(length(col("_lk_name")).desc())

    return (
        joined.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn", "_lk_name")
    )


def match_lookup_fuzzy(
    df: DataFrame,
    lookup_df: DataFrame,
    name_col: str,
    cols_to_keep: list[str],
    *,
    state_name_col: Optional[str] = None,
    lookup_state_name_col: Optional[str] = None,
    state_code_col: Optional[str] = None,
    district_code_col: Optional[str] = None,
    lookup_state_code_col: Optional[str] = None,
    lookup_district_code_col: Optional[str] = None,
    seg_cols: Optional[list[str]] = None,
    max_dist_short: int = 1,
    max_dist_long: int = 2,
) -> DataFrame:
    select_exprs = [upper(col(name_col)).alias("_lk_name")]
    select_exprs.extend([col(c) for c in cols_to_keep])

    state_name_ref = None
    state_ref = None
    district_ref = None
    temp_cols = []
    if lookup_state_name_col:
        if lookup_state_name_col in cols_to_keep:
            state_name_ref = lookup_state_name_col
        else:
            select_exprs.append(col(lookup_state_name_col).alias("_lk_state_name"))
            state_name_ref = "_lk_state_name"
            temp_cols.append("_lk_state_name")
    if lookup_state_code_col:
        if lookup_state_code_col in cols_to_keep:
            state_ref = lookup_state_code_col
        else:
            select_exprs.append(col(lookup_state_code_col).alias("_lk_state_code"))
            state_ref = "_lk_state_code"
            temp_cols.append("_lk_state_code")
    if lookup_district_code_col:
        if lookup_district_code_col in cols_to_keep:
            district_ref = lookup_district_code_col
        else:
            select_exprs.append(col(lookup_district_code_col).alias("_lk_district_code"))
            district_ref = "_lk_district_code"
            temp_cols.append("_lk_district_code")

    lookup = lookup_df.select(*select_exprs)

    exact_match = instr(col("_address_norm"), concat(lit(" "), col("_lk_name"), lit(" "))) > 0
    if seg_cols is None:
        seg_cols = ["seg2_norm", "seg3_norm", "seg4_norm", "seg5_norm"]
    distance = _min_distance(col("_lk_name"), seg_cols)
    max_dist = when(length(col("_lk_name")) <= 5, lit(max_dist_short)).otherwise(lit(max_dist_long))
    fuzzy_match = distance <= max_dist

    cond = exact_match | fuzzy_match
    if state_name_col and state_name_ref and (state_name_col in df.columns):
        cond = cond & (
            col(state_name_col).isNull()
            | (upper(col(state_name_col)) == upper(col(state_name_ref)))
        )
    if state_code_col and state_ref:
        cond = cond & (col(state_code_col).isNull() | (col(state_code_col) == col(state_ref)))
    if district_code_col and district_ref:
        cond = cond & (col(district_code_col).isNull() | (col(district_code_col) == col(district_ref)))

    joined = df.join(broadcast(lookup), cond, "left")
    window = Window.partitionBy("_row_id").orderBy(
        exact_match.desc(),
        distance.asc(),
        length(col("_lk_name")).desc(),
    )

    result = (
        joined.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn", "_lk_name")
    )
    if temp_cols:
        result = result.drop(*temp_cols)
    return result


def match_mukim_fuzzy(df: DataFrame, mukim_df: DataFrame) -> DataFrame:
    lookup = mukim_df.select(
        col("mukim_code"),
        col("mukim_name"),
        col("mukim_id"),
        col("district_code").alias("_lk_district_code"),
        col("district_name").alias("_lk_district_name"),
        col("state_code").alias("_lk_state_code"),
        col("state_name").alias("_lk_state_name"),
    )

    df = match_lookup_fuzzy(
        df,
        lookup,
        "mukim_name",
        [
            "mukim_code",
            "mukim_name",
            "mukim_id",
            "_lk_district_code",
            "_lk_district_name",
            "_lk_state_code",
            "_lk_state_name",
        ],
        state_code_col="state_code",
        district_code_col="district_code",
        lookup_state_code_col="_lk_state_code",
        lookup_district_code_col="_lk_district_code",
        seg_cols=["seg2_norm", "seg3_norm", "seg4_norm", "seg5_norm", "tail2_norm", "tail3_norm", "tail4_norm"],
        max_dist_short=1,
        max_dist_long=2,
    )

    df = df.withColumn("district_code", coalesce(col("district_code"), col("_lk_district_code")))
    df = df.withColumn("district_name", coalesce(col("district_name"), col("_lk_district_name")))
    df = df.withColumn("state_code", coalesce(col("state_code"), col("_lk_state_code")))
    df = df.withColumn("state_name", coalesce(col("state_name"), col("_lk_state_name")))

    return df.drop("_lk_district_code", "_lk_district_name", "_lk_state_code", "_lk_state_name")
