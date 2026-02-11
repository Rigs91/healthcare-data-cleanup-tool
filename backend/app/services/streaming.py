from __future__ import annotations

from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd

from app.services.cleaning import clean_dataframe, normalize_missing, standardize_columns
from app.config import CSV_CHUNK_SIZE
from app.services.storage import detect_delimiter_from_file, iter_csv_chunks, iter_csv_chunks_arrow


def stream_clean_csv(
    path: Path,
    *,
    file_type: str,
    column_metadata: Dict[str, Dict[str, str]],
    output_path: Path,
    output_format: str,
    privacy_mode: str,
    remove_duplicates: bool,
    drop_empty_columns: bool,
    deidentify: bool,
    normalize_phone: bool,
    normalize_zip: bool,
    normalize_gender: bool,
    text_case: str,
    coercion_mode: str,
    performance_mode: str = "balanced",
    parallel_workers: int = 1,
    use_pyarrow: bool | None = None,
    preview_limit: int = 50,
    chunksize: int = CSV_CHUNK_SIZE,
    row_count_estimate: int | None = None,
    progress_callback: Callable[[int], None] | None = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], pd.DataFrame, List[str]]:
    delimiter = detect_delimiter_from_file(path, file_type)

    if performance_mode not in {"balanced", "fast", "ultra_fast"}:
        raise ValueError("Unsupported performance mode.")
    if output_format not in {"csv", "jsonl", "parquet"}:
        raise ValueError("Streaming output supports CSV, JSONL, or Parquet.")

    missing_counts_raw: Dict[str, int] = defaultdict(int)
    missing_counts_clean: Dict[str, int] = defaultdict(int)
    invalid_counts: Dict[str, int] = defaultdict(int)
    total_raw = 0
    total_cleaned = 0
    preview_rows: List[Dict[str, Any]] = []

    warnings: List[str] = []
    if performance_mode == "ultra_fast":
        warnings.append("Performance mode: ultra fast (reduced type coercion and validation).")
    elif performance_mode == "fast":
        warnings.append("Performance mode: fast (validation checks reduced).")
    if remove_duplicates:
        warnings.append("Streaming mode: duplicate removal disabled.")
        remove_duplicates = False
    if drop_empty_columns:
        warnings.append("Streaming mode: empty-column drop disabled.")
        drop_empty_columns = False

    column_map: Dict[str, str] | None = None
    first_write = True
    parquet_writer = None
    pa = None
    pq = None

    if output_format == "jsonl":
        output_path.write_text("", encoding="utf-8")
    elif output_format == "parquet":
        try:
            import pyarrow as pa_module
            import pyarrow.parquet as pq_module
        except Exception as exc:
            raise ValueError("Parquet output requires pyarrow.") from exc
        pa = pa_module
        pq = pq_module
        output_path.unlink(missing_ok=True)

    normalize_missing_values = performance_mode != "ultra_fast"
    sanitize_strings = performance_mode != "ultra_fast"
    if not normalize_missing_values:
        warnings.append("Ultra fast mode: missing-value normalization skipped; missing counts may be understated.")

    if use_pyarrow is None:
        use_pyarrow = performance_mode in {"fast", "ultra_fast"}

    chunk_iter = (
        iter_csv_chunks_arrow(path, delimiter=delimiter)
        if use_pyarrow
        else iter_csv_chunks(path, delimiter=delimiter, chunksize=chunksize)
    )

    def _process_standardized_chunk(standardized_chunk: pd.DataFrame) -> tuple:
        raw_len = len(standardized_chunk)
        if normalize_missing_values:
            normalized_raw = normalize_missing(standardized_chunk)
        else:
            normalized_raw = standardized_chunk

        missing_raw = {col: int(normalized_raw[col].isna().sum()) for col in normalized_raw.columns}
        cleaned_chunk, report = clean_dataframe(
            normalized_raw,
            column_metadata,
            column_map=column_map,
            already_standardized=True,
            normalize_missing_values=False,
            sanitize_strings=sanitize_strings,
            remove_duplicates=remove_duplicates,
            drop_empty_columns=drop_empty_columns,
            deidentify=deidentify,
            normalize_phone=normalize_phone,
            normalize_zip=normalize_zip,
            normalize_gender=normalize_gender,
            text_case=text_case if performance_mode != "ultra_fast" else "none",
            coercion_mode=coercion_mode,
            privacy_mode=privacy_mode,
            performance_mode=performance_mode,
        )

        missing_clean = {col: int(cleaned_chunk[col].isna().sum()) for col in cleaned_chunk.columns}
        invalid_local = {
            col: int(details.get("invalid", 0)) for col, details in report.get("conversion", {}).items()
        }
        return cleaned_chunk, report, missing_raw, missing_clean, invalid_local, raw_len

    def _consume_result(result: tuple) -> None:
        nonlocal total_raw, total_cleaned, first_write, parquet_writer
        cleaned_chunk, report, missing_raw, missing_clean, invalid_local, raw_len = result

        total_raw += raw_len
        total_cleaned += len(cleaned_chunk)

        for col, count in missing_raw.items():
            missing_counts_raw[col] += count
        for col, count in missing_clean.items():
            missing_counts_clean[col] += count
        for col, count in invalid_local.items():
            invalid_counts[col] += count

        if len(preview_rows) < preview_limit:
            remaining = preview_limit - len(preview_rows)
            preview_rows.extend(cleaned_chunk.head(remaining).to_dict(orient="records"))

        if output_format == "csv":
            cleaned_chunk.to_csv(output_path, mode="w" if first_write else "a", index=False, header=first_write)
        elif output_format == "jsonl":
            json_lines = cleaned_chunk.to_json(orient="records", lines=True)
            with output_path.open("a", encoding="utf-8") as handle:
                handle.write(json_lines)
                handle.write("\n")
        elif output_format == "parquet":
            parquet_chunk = cleaned_chunk.copy()
            for col in parquet_chunk.columns:
                parquet_chunk[col] = parquet_chunk[col].astype("string")
            table = pa.Table.from_pandas(parquet_chunk, preserve_index=False)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(output_path, table.schema)
            parquet_writer.write_table(table)
        else:
            raise ValueError("Streaming output supports CSV, JSONL, or Parquet.")

        first_write = False

        if progress_callback and row_count_estimate:
            pct = int(min(99, (total_raw / max(1, row_count_estimate)) * 100))
            progress_callback(pct)

    try:
        try:
            first_chunk = next(chunk_iter)
        except StopIteration:
            first_chunk = None

        if first_chunk is None:
            cleaning_report = {
                "column_map": {},
                "conversion": {},
                "empty_columns_removed": [],
                "duplicate_rows_removed": 0,
                "streaming": True,
            }
            qc_payload = {
                "row_count_raw": 0,
                "row_count_cleaned": 0,
                "missing_counts_raw": {},
                "missing_counts_cleaned": {},
                "invalid_counts": {},
                "empty_columns_removed": [],
                "duplicate_rows_removed": 0,
            }
            return cleaning_report, qc_payload, pd.DataFrame(), warnings

        standardized_first, column_map = standardize_columns(first_chunk.copy())
        _consume_result(_process_standardized_chunk(standardized_first))

        if parallel_workers > 1:
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                pending: deque = deque()
                for chunk in chunk_iter:
                    standardized = chunk.rename(columns=column_map)
                    pending.append(executor.submit(_process_standardized_chunk, standardized))
                    if len(pending) >= parallel_workers:
                        _consume_result(pending.popleft().result())
                while pending:
                    _consume_result(pending.popleft().result())
        else:
            for chunk in chunk_iter:
                standardized = chunk.rename(columns=column_map)
                _consume_result(_process_standardized_chunk(standardized))
    finally:
        if parquet_writer is not None:
            parquet_writer.close()

    cleaning_report = {
        "column_map": column_map or {},
        "conversion": {col: {"invalid": count} for col, count in invalid_counts.items()},
        "empty_columns_removed": [],
        "duplicate_rows_removed": 0,
        "streaming": True,
    }

    preview_df = pd.DataFrame(preview_rows)

    qc_payload = {
        "row_count_raw": total_raw,
        "row_count_cleaned": total_cleaned,
        "missing_counts_raw": dict(missing_counts_raw),
        "missing_counts_cleaned": dict(missing_counts_clean),
        "invalid_counts": dict(invalid_counts),
        "empty_columns_removed": [],
        "duplicate_rows_removed": 0,
    }

    return cleaning_report, qc_payload, preview_df, warnings
