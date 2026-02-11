import argparse
import math
import os
import time
from pathlib import Path
import sys

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
BACKEND_PATH = ROOT / "backend"
if str(BACKEND_PATH) not in sys.path:
    sys.path.insert(0, str(BACKEND_PATH))

from app.main import app


def human_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    size = float(value)
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.1f} {units[idx]}"


def is_ok(resp) -> bool:
    return 200 <= resp.status_code < 300


def main() -> None:
    parser = argparse.ArgumentParser(description="Exercise the chunked upload + cleaning pipeline.")
    parser.add_argument(
        "--file",
        dest="file_path",
        default=r"D:\Local_LLM_Work\Games fooling\HcDataCleanUpAi\alpha test file 2.csv",
        help="Path to the source file to upload.",
    )
    parser.add_argument("--chunk-size-mb", type=int, default=10, help="Chunk size in MB.")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Job polling interval (seconds).")
    args = parser.parse_args()

    file_path = args.file_path
    if not os.path.exists(file_path):
        raise SystemExit(f"File not found: {file_path}")

    total_size = os.path.getsize(file_path)
    chunk_size = args.chunk_size_mb * 1024 * 1024
    total_chunks = math.ceil(total_size / chunk_size)

    print(f"Testing upload for: {file_path}")
    print(f"Size: {human_bytes(total_size)} | Chunks: {total_chunks} @ {args.chunk_size_mb}MB")

    client = TestClient(app)

    payload = {
        "filename": os.path.basename(file_path),
        "name": "alpha test file 2",
        "usage_intent": "training",
        "output_format": "csv",
        "privacy_mode": "safe_harbor",
        "file_size": total_size,
        "total_chunks": total_chunks,
    }

    start = client.post("/api/uploads/start", json=payload)
    if not is_ok(start):
        raise SystemExit(f"Upload start failed: {start.status_code} {start.text}")
    upload_id = start.json()["upload_id"]
    print(f"Upload session: {upload_id}")

    with open(file_path, "rb") as handle:
        for index in range(total_chunks):
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            files = {"chunk": (f"chunk_{index}", chunk, "application/octet-stream")}
            data = {"index": str(index)}
            resp = client.post(f"/api/uploads/{upload_id}/chunk", data=data, files=files)
            if not is_ok(resp):
                raise SystemExit(f"Chunk {index} failed: {resp.status_code} {resp.text}")
            if index % 10 == 0 or index == total_chunks - 1:
                uploaded = min(total_size, (index + 1) * chunk_size)
                print(f"Uploaded {index + 1}/{total_chunks} ({human_bytes(uploaded)})")

    complete = client.post(f"/api/uploads/{upload_id}/complete")
    if not is_ok(complete):
        raise SystemExit(f"Upload complete failed: {complete.status_code} {complete.text}")
    dataset = complete.json()
    dataset_id = dataset["id"]
    print(f"Dataset created: {dataset_id}")

    options = {
        "remove_duplicates": True,
        "drop_empty_columns": True,
        "privacy_mode": "safe_harbor",
        "normalize_phone": True,
        "normalize_zip": True,
        "normalize_gender": True,
        "text_case": "none",
        "output_format": "csv",
        "coercion_mode": "safe",
    }

    job = client.post(f"/api/datasets/{dataset_id}/clean-jobs", json=options)
    if not is_ok(job):
        raise SystemExit(f"Cleaning job start failed: {job.status_code} {job.text}")
    job_id = job.json()["id"]
    print(f"Cleaning job: {job_id}")

    while True:
        status = client.get(f"/api/clean-jobs/{job_id}")
        if not is_ok(status):
            raise SystemExit(f"Job status failed: {status.status_code} {status.text}")
        payload = status.json()
        print(f"Status: {payload['status']} | {payload['progress']}% | {payload['message']}")
        if payload["status"] == "completed":
            result = payload["result"]
            preview_rows = len(result.get("preview", {}).get("rows", []))
            print(f"Cleaning complete. Preview rows: {preview_rows}")
            break
        if payload["status"] == "failed":
            raise SystemExit(f"Cleaning failed: {payload.get('error')}")
        time.sleep(args.poll_interval)

    print("Pipeline test completed successfully.")


if __name__ == "__main__":
    main()
