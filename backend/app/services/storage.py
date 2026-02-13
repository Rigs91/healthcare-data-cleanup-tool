from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from app.config import ARROW_BLOCK_SIZE, RAW_DIR

SUPPORTED_EXTENSIONS = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".txt": "csv",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
}


def supported_extensions() -> list[str]:
    return sorted({ext.lstrip(".") for ext in SUPPORTED_EXTENSIONS.keys() if ext})


def is_supported_filename(filename: str) -> bool:
    return Path(filename).suffix.lower() in SUPPORTED_EXTENSIONS


def detect_file_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return SUPPORTED_EXTENSIONS.get(ext, "csv")


def _detect_delimiter(sample: str) -> str:
    candidates = [",", "\t", ";", "|"]
    try:
        dialect = csv.Sniffer().sniff(sample, candidates)
        return dialect.delimiter
    except csv.Error:
        for delimiter in candidates:
            if delimiter in sample:
                return delimiter
    return ","


def detect_delimiter_from_file(path: Path, file_type: str) -> str:
    if file_type == "tsv":
        return "\t"
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        sample = handle.read(5000)
    return _detect_delimiter(sample)


def save_upload_to_disk(file_obj, filename: str, dataset_id: str) -> Tuple[Path, int]:
    safe_name = filename.replace("..", ".").replace("/", "_").replace("\\", "_")
    path = RAW_DIR / f"{dataset_id}__{safe_name}"

    size_bytes = 0
    with path.open("wb") as out:
        while True:
            chunk = file_obj.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            size_bytes += len(chunk)

    return path, size_bytes


def estimate_row_count(
    path: Path,
    file_type: str,
    *,
    size_bytes: Optional[int] = None,
    sample_bytes: int = 5 * 1024 * 1024,
) -> Optional[int]:
    if file_type == "parquet":
        try:
            import pyarrow.parquet as pq
        except Exception:
            return None

        try:
            parquet_file = pq.ParquetFile(path)
            return int(parquet_file.metadata.num_rows)
        except Exception:
            return None

    if file_type not in {"csv", "tsv", "jsonl"}:
        return None

    total_size = size_bytes or path.stat().st_size
    if total_size <= sample_bytes * 2:
        count = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                count += chunk.count(b"\n")
    else:
        with path.open("rb") as handle:
            sample = handle.read(sample_bytes)
        count = sample.count(b"\n")
        if count == 0:
            return None
        avg_bytes = sample_bytes / count
        count = int(total_size / avg_bytes)

    if file_type in {"csv", "tsv"}:
        return max(0, count - 1)
    return count


def _read_parquet_window(
    path: Path,
    *,
    max_rows: Optional[int],
    offset: int,
) -> pd.DataFrame:
    try:
        import pyarrow.parquet as pq
    except Exception:
        df = pd.read_parquet(path)
        if offset > 0:
            end = offset + max_rows if max_rows is not None else None
            df = df.iloc[offset:end]
        elif max_rows is not None:
            df = df.head(max_rows)
        return df

    parquet_file = pq.ParquetFile(path)

    if max_rows is None and offset == 0:
        return parquet_file.read().to_pandas()

    target_start = max(0, offset)
    target_end = target_start + max_rows if max_rows is not None else None
    seen = 0
    frames: list[pd.DataFrame] = []

    for batch in parquet_file.iter_batches(batch_size=10000):
        batch_df = batch.to_pandas()
        batch_len = len(batch_df)
        batch_start = seen
        batch_end = seen + batch_len

        if batch_end <= target_start:
            seen = batch_end
            continue

        local_start = max(0, target_start - batch_start)
        if target_end is None:
            local_end = batch_len
        else:
            local_end = max(0, min(batch_len, target_end - batch_start))

        if local_start < local_end:
            frames.append(batch_df.iloc[local_start:local_end])

        seen = batch_end
        if target_end is not None and seen >= target_end:
            break

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def read_dataframe(
    path: Path,
    *,
    file_type: str,
    max_rows: Optional[int] = None,
    offset: int = 0,
) -> Tuple[pd.DataFrame, Optional[str]]:
    if file_type in {"csv", "tsv"}:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            sample = handle.read(5000)
        delimiter = "\t" if file_type == "tsv" else _detect_delimiter(sample)
        skiprows = None
        if offset > 0:
            skiprows = range(1, offset + 1)
        df = pd.read_csv(
            path,
            sep=delimiter,
            engine="c",
            dtype=str,
            keep_default_na=False,
            na_values=[],
            na_filter=False,
            low_memory=False,
            encoding="utf-8",
            encoding_errors="replace",
            nrows=max_rows,
            skiprows=skiprows,
        )
        return df, delimiter

    if file_type == "jsonl":
        read_rows = max_rows
        if max_rows is not None and offset > 0:
            read_rows = max_rows + offset
        df = pd.read_json(
            path,
            lines=True,
            dtype=False,
            encoding="utf-8",
            nrows=read_rows,
        )
        if offset > 0:
            end = offset + max_rows if max_rows else None
            df = df.iloc[offset:end]
        elif max_rows is not None:
            df = df.head(max_rows)
        return df, None

    if file_type == "parquet":
        try:
            df = _read_parquet_window(path, max_rows=max_rows, offset=offset)
        except Exception as exc:
            raise ValueError("Parquet support requires pyarrow or fastparquet.") from exc
        return df, None

    df = pd.read_csv(path, dtype=str)
    return df, ","


def iter_csv_chunks(
    path: Path,
    *,
    delimiter: str,
    chunksize: int = 50000,
) -> pd.io.parsers.TextFileReader:
    return pd.read_csv(
        path,
        sep=delimiter,
        engine="c",
        dtype=str,
        keep_default_na=False,
        na_values=[],
        na_filter=False,
        low_memory=False,
        encoding="utf-8",
        encoding_errors="replace",
        chunksize=chunksize,
    )


def iter_csv_chunks_arrow(
    path: Path,
    *,
    delimiter: str,
    block_size: int = ARROW_BLOCK_SIZE,
) -> pd.io.parsers.TextFileReader:
    try:
        import pyarrow.csv as pacsv
    except Exception:
        yield from iter_csv_chunks(path, delimiter=delimiter, chunksize=50000)
        return

    read_options = pacsv.ReadOptions(block_size=block_size, use_threads=True)
    parse_options = pacsv.ParseOptions(delimiter=delimiter)
    convert_options = pacsv.ConvertOptions(strings_can_be_null=True)

    try:
        reader = pacsv.open_csv(
            path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )
        for batch in reader:
            yield batch.to_pandas()
    except Exception:
        # Fallback to pandas if pyarrow parsing fails.
        yield from iter_csv_chunks(path, delimiter=delimiter, chunksize=50000)
