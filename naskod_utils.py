from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, concat, create_map, expr, lit, lpad, monotonically_increasing_id, row_number, trim, upper, when


STATE_ABBR_MAP = {
    "01": "JHR",
    "02": "KDH",
    "03": "KTN",
    "04": "MLK",
    "05": "NSN",
    "06": "PHG",
    "07": "PNG",
    "08": "PRK",
    "09": "PLS",
    "10": "SGR",
    "11": "TRG",
    "12": "SBH",
    "13": "SWK",
    "14": "KL",
    "15": "LAB",
    "16": "PJY",
}


def add_standard_naskod(
    df: DataFrame,
    *,
    output_col: str = "naskod",
    overwrite_existing: bool = True,
    source_col: str = "source_naskod",
) -> DataFrame:
    if output_col in df.columns:
        if not overwrite_existing:
            return df
        if source_col and source_col not in df.columns:
            df = df.withColumn(source_col, col(output_col).cast("string"))
        df = df.drop(output_col)
    if "state_code" not in df.columns or "district_code" not in df.columns:
        return df.withColumn(output_col, lit(None).cast("string"))

    map_expr = create_map([lit(x) for kv in STATE_ABBR_MAP.items() for x in kv])
    state_num = expr("try_cast(state_code as int)")
    district_num = expr("try_cast(district_code as int)")
    out = df.withColumn(
        "_naskod_state_code",
        when(state_num.isNotNull(), lpad(state_num.cast("string"), 2, "0")).otherwise(upper(trim(col("state_code").cast("string")))),
    )
    out = out.withColumn(
        "_naskod_district_code",
        when(district_num.isNotNull(), lpad(district_num.cast("string"), 2, "0")).otherwise(lit(None).cast("string")),
    )
    out = out.withColumn("_naskod_state_abbr", map_expr.getItem(col("_naskod_state_code")))
    out = out.withColumn(
        "_naskod_state_abbr",
        when(
            col("_naskod_state_abbr").isNull() & col("_naskod_state_code").rlike(r"^[A-Z]{2,3}$"),
            col("_naskod_state_code"),
        ).otherwise(col("_naskod_state_abbr")),
    )

    naskod_seq = Window.partitionBy("_naskod_state_abbr", "_naskod_district_code").orderBy(
        monotonically_increasing_id()
    )
    out = out.withColumn("_naskod_seq", row_number().over(naskod_seq))
    out = out.withColumn("_naskod_suffix", lpad(col("_naskod_seq").cast("string"), 6, "0"))
    out = out.withColumn(
        output_col,
        when(
            col("_naskod_state_abbr").isNotNull() & col("_naskod_district_code").isNotNull(),
            concat(
                lit("NAS-"),
                col("_naskod_state_abbr"),
                lit("-"),
                col("_naskod_district_code"),
                lit("-"),
                col("_naskod_suffix"),
            ),
        ),
    )
    return out.drop(
        "_naskod_state_code",
        "_naskod_district_code",
        "_naskod_state_abbr",
        "_naskod_seq",
        "_naskod_suffix",
    )
