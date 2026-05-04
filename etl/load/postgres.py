import argparse
import os
from typing import Iterable

import pandas as pd
import sqlalchemy as sa


def _db_url_from_env() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "postgres")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def _read_inputs(paths: Iterable[str]) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in paths]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _clean_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    out = df.copy()
    for col_name in out.columns:
        if out[col_name].map(lambda value: isinstance(value, (list, dict, np.ndarray))).any():
            out[col_name] = out[col_name].map(
                lambda value: str(list(value)) if isinstance(value, np.ndarray) else (
                    str(value) if isinstance(value, (list, dict)) else (None if pd.isna(value) else value)
                )
            )
    return out.where(pd.notna(out), None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load parquet output into PostgreSQL with Pandas/SQLAlchemy.")
    parser.add_argument("--input", nargs="+", required=True, help="Input parquet path(s)")
    parser.add_argument("--table", required=True, help="Destination table name")
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "public"))
    parser.add_argument("--mode", default="append", choices=["append", "overwrite", "replace"])
    parser.add_argument("--db-url", default=None, help="SQLAlchemy database URL")
    parser.add_argument("--chunksize", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = _clean_for_sql(_read_inputs(args.input))
    if df.empty:
        print("No rows to load.")
        return
    engine = sa.create_engine(args.db_url or _db_url_from_env())
    if_exists = "replace" if args.mode in {"overwrite", "replace"} else "append"
    with engine.begin() as conn:
        if args.schema:
            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{args.schema}"'))
        df.to_sql(args.table, conn, schema=args.schema or None, if_exists=if_exists, index=False, chunksize=args.chunksize)
    print(f"Loaded {len(df)} rows into {args.schema}.{args.table}")


if __name__ == "__main__":
    main()
