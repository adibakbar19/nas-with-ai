"""Extract stage — streams source files in chunks.

Never loads the full file into memory (for CSV).
Attaches record_id (SHA-256 of raw row content) to every row.
record_id is deterministic: same row content = same id across uploads.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

import polars as pl

if TYPE_CHECKING:
    from nas_processor.etl.pipeline.progress import ProgressCallback

logger = logging.getLogger(__name__)

_DEFAULT_CHUNK_SIZE = 500_000


def _hash_series(s: pl.Series) -> pl.Series:
    """Hash a series of strings to SHA-256 hex digests.

    Uses map_elements — faster than Python df.apply()
    but still row-level. Acceptable for record_id generation
    which runs once at extract time.
    """
    return s.map_elements(
        lambda v: hashlib.sha256(v.encode("utf-8", errors="replace")).hexdigest(),
        return_dtype=pl.Utf8,
    )


def _attach_record_id(df: pl.DataFrame) -> pl.DataFrame:
    """Add record_id column as SHA-256 of concatenated row content.

    Deterministic: identical row content produces identical record_id.
    Collision-resistant: SHA-256 gives 2^256 space.
    """
    return df.with_columns(
        _hash_series(
            pl.concat_str(pl.all(), separator="|", ignore_nulls=True)
        ).alias("record_id")
    )


def _cast_all_utf8(df: pl.DataFrame) -> pl.DataFrame:
    """Cast all columns to Utf8 string type.

    Raw extraction treats everything as text.
    Type inference happens in the clean stage.
    """
    return df.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in df.columns])


def _chunk_dataframe(df: pl.DataFrame, chunk_size: int) -> Iterator[pl.DataFrame]:
    """Slice a fully-loaded DataFrame into chunks."""
    total = len(df)
    for offset in range(0, total, chunk_size):
        yield df.slice(offset, min(chunk_size, total - offset))


def _extract_csv(
    file_path: Path,
    chunk_size: int,
) -> Iterator[pl.DataFrame]:
    """Stream CSV in chunks using lazy scan."""
    lazy = pl.scan_csv(
        file_path,
        infer_schema_length=0,  # treat all columns as Utf8
        ignore_errors=True,
        encoding="utf8-lossy",
    )
    total_rows = lazy.select(pl.len()).collect().item()

    if total_rows == 0:
        logger.warning("extract_empty_file path=%s", file_path)
        return

    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    logger.info(
        "extract_csv path=%s total_rows=%d chunks=%d chunk_size=%d",
        file_path, total_rows, num_chunks, chunk_size,
    )

    for offset in range(0, total_rows, chunk_size):
        chunk = lazy.slice(offset, chunk_size).collect()
        yield chunk


def _extract_excel(
    file_path: Path,
    chunk_size: int,
) -> Iterator[pl.DataFrame]:
    """Load Excel file fully then chunk.

    Excel has no streaming API — must load into memory.
    Warns if file exceeds chunk_size rows.
    """
    df = pl.read_excel(file_path)
    df = _cast_all_utf8(df)

    if len(df) > chunk_size:
        logger.warning(
            "extract_excel_large path=%s rows=%d "
            "memory_pressure=high consider_csv_instead=true",
            file_path, len(df),
        )

    logger.info("extract_excel path=%s total_rows=%d", file_path, len(df))
    yield from _chunk_dataframe(df, chunk_size)


def _extract_json(
    file_path: Path,
    chunk_size: int,
) -> Iterator[pl.DataFrame]:
    """Load JSON/JSONL fully then chunk."""
    suffix = file_path.suffix.lower()
    if suffix in (".jsonl", ".ndjson"):
        df = pl.read_ndjson(file_path)
    else:
        df = pl.read_json(file_path)

    df = _cast_all_utf8(df)
    logger.info("extract_json path=%s total_rows=%d", file_path, len(df))
    yield from _chunk_dataframe(df, chunk_size)


def extract_chunks(
    file_path: Path,
    *,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
    config: dict | None = None,
    progress: "ProgressCallback | None" = None,
) -> Iterator[pl.DataFrame]:
    """Stream source file in chunks with record_id attached.

    Yields pl.DataFrame chunks of at most chunk_size rows.
    Each row has a record_id column (SHA-256 of raw content).
    All columns are Utf8 strings.

    Args:
        file_path: Path to source file.
        chunk_size: Rows per chunk. Default 500_000.
        config: Pipeline config dict (reserved for future use).
        progress: Optional progress callback.

    Yields:
        pl.DataFrame chunks with record_id column added.

    Raises:
        FileNotFoundError: If file_path does not exist.
        ValueError: If file format is not supported.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls", ".json", ".jsonl", ".ndjson"}:
        raise ValueError(
            f"Unsupported file format: {suffix!r}. "
            f"Supported: .csv .xlsx .xls .json .jsonl .ndjson"
        )

    if progress:
        progress.on_stage_start("extract")

    chunks_yielded = 0
    rows_yielded = 0

    if suffix == ".csv":
        raw_chunks = _extract_csv(file_path, chunk_size)
    elif suffix in (".xlsx", ".xls"):
        raw_chunks = _extract_excel(file_path, chunk_size)
    else:
        raw_chunks = _extract_json(file_path, chunk_size)

    for chunk in raw_chunks:
        chunk = _cast_all_utf8(chunk)
        chunk = _attach_record_id(chunk)
        chunks_yielded += 1
        rows_yielded += len(chunk)
        logger.info(
            "extract_chunk chunk=%d rows=%d total_so_far=%d",
            chunks_yielded, len(chunk), rows_yielded,
        )
        yield chunk

    if progress:
        progress.on_stage_complete(
            "extract",
            rows_processed=rows_yielded,
            rows_failed=0,
            duration_ms=0,
        )

    logger.info(
        "extract_complete total_rows=%d total_chunks=%d",
        rows_yielded, chunks_yielded,
    )
