import argparse
import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
BACKEND_PATH = ROOT / "backend"
if str(BACKEND_PATH) not in sys.path:
    sys.path.insert(0, str(BACKEND_PATH))

from app.config import (
    CLEANED_DIR,
    PROFILE_SAMPLE_ROWS,
    STREAMING_THRESHOLD_MB,
    CSV_CHUNK_SIZE,
    CSV_CHUNK_SIZE_FAST,
    CSV_CHUNK_SIZE_ULTRA,
    STREAMING_MAX_WORKERS_FAST,
    STREAMING_MAX_WORKERS_ULTRA,
)
from app.services.cleaning import standardize_columns
from app.services.profiling import build_profile, infer_hints_from_profile
from app.services.storage import detect_file_type, estimate_row_count, read_dataframe
from app.services.streaming import stream_clean_csv


def run_benchmark(path: Path, mode: str, output_format: str) -> float:
    file_type = detect_file_type(path.name)
    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    row_estimate = estimate_row_count(path, file_type, size_bytes=size_bytes)
    sampled = size_mb > STREAMING_THRESHOLD_MB

    raw_df, _delimiter = read_dataframe(
        path,
        file_type=file_type,
        max_rows=PROFILE_SAMPLE_ROWS if sampled else None,
    )
    _, column_map = standardize_columns(raw_df.copy())
    profile = build_profile(raw_df, column_map, total_rows=row_estimate, sampled=sampled)
    column_metadata = infer_hints_from_profile(profile)

    output_path = CLEANED_DIR / f"benchmark_{mode}_{path.stem}.{output_format}"

    start = time.perf_counter()
    if size_mb > STREAMING_THRESHOLD_MB and file_type in {"csv", "tsv"}:
        if mode == "ultra_fast":
            chunksize = CSV_CHUNK_SIZE_ULTRA
            parallel_workers = STREAMING_MAX_WORKERS_ULTRA
            use_pyarrow = True
        elif mode == "fast":
            chunksize = CSV_CHUNK_SIZE_FAST
            parallel_workers = STREAMING_MAX_WORKERS_FAST
            use_pyarrow = True
        else:
            chunksize = CSV_CHUNK_SIZE
            parallel_workers = 1
            use_pyarrow = False
        stream_clean_csv(
            path,
            file_type=file_type,
            column_metadata=column_metadata,
            output_path=output_path,
            output_format=output_format,
            privacy_mode="safe_harbor",
            performance_mode=mode,
            parallel_workers=parallel_workers,
            use_pyarrow=use_pyarrow,
            remove_duplicates=True,
            drop_empty_columns=True,
            deidentify=True,
            normalize_phone=True,
            normalize_zip=True,
            normalize_gender=True,
            text_case="none",
            coercion_mode="safe",
            row_count_estimate=row_estimate,
            chunksize=chunksize,
        )
    else:
        # For small files, force streaming path for comparability.
        if mode == "ultra_fast":
            chunksize = CSV_CHUNK_SIZE_ULTRA
            parallel_workers = STREAMING_MAX_WORKERS_ULTRA
            use_pyarrow = True
        elif mode == "fast":
            chunksize = CSV_CHUNK_SIZE_FAST
            parallel_workers = STREAMING_MAX_WORKERS_FAST
            use_pyarrow = True
        else:
            chunksize = CSV_CHUNK_SIZE
            parallel_workers = 1
            use_pyarrow = False
        stream_clean_csv(
            path,
            file_type=file_type,
            column_metadata=column_metadata,
            output_path=output_path,
            output_format=output_format,
            privacy_mode="safe_harbor",
            performance_mode=mode,
            parallel_workers=parallel_workers,
            use_pyarrow=use_pyarrow,
            remove_duplicates=True,
            drop_empty_columns=True,
            deidentify=True,
            normalize_phone=True,
            normalize_zip=True,
            normalize_gender=True,
            text_case="none",
            coercion_mode="safe",
            row_count_estimate=row_estimate,
            chunksize=chunksize,
        )
    elapsed = time.perf_counter() - start
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark streaming cleaning performance.")
    parser.add_argument(
        "--file",
        default=r"D:\Local_LLM_Work\Games fooling\HcDataCleanUpAi\alpha test file 2.csv",
        help="Path to file for benchmark.",
    )
    parser.add_argument("--output-format", default="csv", choices=["csv", "jsonl"])
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    print(f"Benchmark file: {path}")
    print("Running balanced mode...")
    balanced_time = run_benchmark(path, "balanced", args.output_format)
    print(f"Balanced: {balanced_time:.2f}s")

    print("Running fast mode...")
    fast_time = run_benchmark(path, "fast", args.output_format)
    print(f"Fast: {fast_time:.2f}s")

    print("Running ultra fast mode...")
    ultra_time = run_benchmark(path, "ultra_fast", args.output_format)
    print(f"Ultra fast: {ultra_time:.2f}s")

    if fast_time > 0:
        speedup = balanced_time / fast_time
        print(f"Speedup (fast): {speedup:.2f}x")
    if ultra_time > 0:
        speedup_ultra = balanced_time / ultra_time
        print(f"Speedup (ultra): {speedup_ultra:.2f}x")


if __name__ == "__main__":
    main()
