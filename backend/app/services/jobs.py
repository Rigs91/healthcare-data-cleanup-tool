from __future__ import annotations

from datetime import datetime
from threading import Lock
from uuid import uuid4

_jobs = {}
_lock = Lock()


def create_job(job_type: str, dataset_id: str) -> dict:
    job_id = uuid4().hex
    job = {
        "id": job_id,
        "type": job_type,
        "dataset_id": dataset_id,
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "result": None,
        "error": None,
    }
    with _lock:
        _jobs[job_id] = job
    return job


def get_job(job_id: str) -> dict | None:
    with _lock:
        job = _jobs.get(job_id)
        if not job:
            return None
        return dict(job)


def update_job(job_id: str, **fields) -> None:
    with _lock:
        if job_id not in _jobs:
            return
        _jobs[job_id].update(fields)
        _jobs[job_id]["updated_at"] = datetime.utcnow().isoformat()


def finish_job(job_id: str, result: dict) -> None:
    update_job(job_id, status="completed", progress=100, message="Completed", result=result)


def fail_job(job_id: str, error: str) -> None:
    update_job(job_id, status="failed", message=error, error=error)
