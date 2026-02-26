from typing import Iterable, Optional

from pyspark.sql.dataframe import DataFrame

def load_parquet(
    df: DataFrame,
    output_path: str,
    *,
    mode: str = "overwrite",
    partition_by: Optional[Iterable[str]] = None,
    coalesce: Optional[int] = None,
):
    if coalesce:
        df = df.coalesce(coalesce)

    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(output_path)


def load_success_failed(
    success_df: DataFrame,
    failed_df: DataFrame,
    success_path: str,
    failed_path: str,
    *,
    mode: str = "overwrite",
    partition_by: Optional[Iterable[str]] = None,
    coalesce: Optional[int] = None,
):
    load_parquet(success_df, success_path, mode=mode, partition_by=partition_by, coalesce=coalesce)
    load_parquet(failed_df, failed_path, mode=mode, partition_by=partition_by, coalesce=coalesce)
