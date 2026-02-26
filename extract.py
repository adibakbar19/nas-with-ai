import argparse
import glob
import json
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def _resolve_input_path(path: str):
    if any(token in path for token in ["*", "?", "["]):
        matches = sorted(glob.glob(path))
        if not matches:
            raise FileNotFoundError(f"No files matched input pattern: {path}")
        return matches
    return path


def extract_csv(
    spark: SparkSession,
    path: str,
    *,
    header: bool = True,
    infer_schema: bool = True,
    sep: str = ",",
    encoding: str = "utf-8",
) -> DataFrame:
    resolved = _resolve_input_path(path)
    return (
        spark.read.csv(
            resolved,
            header=header,
            inferSchema=infer_schema,
            sep=sep,
            encoding=encoding,
        )
    )


def extract_json(spark: SparkSession, path: str, *, multiline: bool = False) -> DataFrame:
    resolved = _resolve_input_path(path)
    return spark.read.json(resolved, multiLine=multiline)


def extract_excel(
    spark: SparkSession,
    path: str,
    *,
    sheet_name: Any = 0,
) -> DataFrame:
    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "pandas is required for Excel input. Install with: pip install pandas openpyxl"
        ) from exc

    pdf = pd.read_excel(path, sheet_name=sheet_name)
    return spark.createDataFrame(pdf)


def extract_api_json(
    spark: SparkSession,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
) -> DataFrame:
    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"

    req = urllib.request.Request(url, headers=headers or {}, method=method)
    with urllib.request.urlopen(req) as resp:
        payload = resp.read().decode("utf-8")

    data = json.loads(payload)
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        records = data["data"]
    elif isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = [data]
    else:
        raise ValueError("API JSON response not in an expected format (list or dict).")

    return spark.createDataFrame(records)


def extract_data(
    spark: SparkSession,
    source_type: str,
    source: str,
    **kwargs,
) -> DataFrame:
    source_type = source_type.lower()
    if source_type == "csv":
        return extract_csv(spark, source, **kwargs)
    if source_type == "json":
        return extract_json(spark, source, **kwargs)
    if source_type in {"excel", "xlsx"}:
        return extract_excel(spark, source, **kwargs)
    if source_type == "api":
        return extract_api_json(spark, source, **kwargs)
    raise ValueError(f"Unsupported source_type: {source_type}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract data from CSV/Excel/JSON/API into a Spark DataFrame.")
    parser.add_argument("--source-type", required=True, choices=["csv", "json", "excel", "xlsx", "api"])
    parser.add_argument("--source", required=True, help="Path or URL")
    parser.add_argument("--sheet", default="0", help="Excel sheet name or index (excel only)")
    parser.add_argument("--multiline", action="store_true", help="JSON multiline (json only)")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter")
    parser.add_argument("--encoding", default="utf-8", help="CSV encoding")
    parser.add_argument("--output", required=True, help="Output parquet path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("NAS Extract").getOrCreate()

    if args.source_type in {"excel", "xlsx"}:
        sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
        df = extract_data(spark, args.source_type, args.source, sheet_name=sheet)
    elif args.source_type == "json":
        df = extract_data(spark, args.source_type, args.source, multiline=args.multiline)
    elif args.source_type == "csv":
        df = extract_data(
            spark,
            args.source_type,
            args.source,
            sep=args.delimiter,
            encoding=args.encoding,
        )
    else:
        df = extract_data(spark, args.source_type, args.source)

    df.write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
