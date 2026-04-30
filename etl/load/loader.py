import os
import shutil

import pandas as pd


DataFrame = pd.DataFrame


def _prepare_output_path(path: str, mode: str) -> None:
    if mode == "overwrite" and os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def load_parquet(df: DataFrame, output_path: str, *, mode: str = "overwrite") -> None:
    _prepare_output_path(output_path, mode)
    # Coerce mixed-type object columns to string to prevent PyArrow serialization errors.
    # Excel files commonly produce columns like "6C" alongside plain integers.
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        if len({type(v).__name__ for v in df[col].dropna()}) > 1:
            df[col] = df[col].astype(str)
    df.to_parquet(output_path, index=False)


def load_success_failed(
    success_df: DataFrame,
    failed_df: DataFrame,
    success_path: str,
    failed_path: str,
    *,
    mode: str = "overwrite",
) -> None:
    load_parquet(success_df, success_path, mode=mode)
    load_parquet(failed_df, failed_path, mode=mode)


def load_success_warning_failed(
    success_df: DataFrame,
    warning_df: DataFrame,
    failed_df: DataFrame,
    success_path: str,
    warning_path: str,
    failed_path: str,
    *,
    mode: str = "overwrite",
) -> None:
    load_parquet(success_df, success_path, mode=mode)
    load_parquet(warning_df, warning_path, mode=mode)
    load_parquet(failed_df, failed_path, mode=mode)
