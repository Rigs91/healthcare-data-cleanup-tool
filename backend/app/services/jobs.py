from __future__ import annotations

from datetime import datetime, timedelta
from threading import Lock
from uuid import uuid4

_jobs = {}
_job_cancel_requests = set()
_lock = Lock()
MAX_COMPLETED_JOBS = 500
JOB_TTL_HOURS = 24


def _parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.utcnow()


def _prune_jobs(now: datetime) -> None:
    if len(_jobs) <= MAX_COMPLETED_JOBS:
        return

    cutoff = now - timedelta(hours=JOB_TTL_HOURS)
    expired = [
        job_id
        for job_id, payload in _jobs.items()
        if payload.get("status") in {"completed", "failed"} and _parse_timestamp(payload.get("updated_at")) < cutoff
    ]
    for job_id in expired:
        _jobs.pop(job_id, None)

    if len(_jobs) <= MAX_COMPLETED_JOBS:
        return

    # Keep a bounded backlog so the in-memory job store does not grow unbounded.
    candidates = [
        (job_id, _parse_timestamp(payload.get("created_at")))
        for job_id, payload in _jobs.items()
        if payload.get("status") not in {"running"}
    ]
    candidates.sort(key=lambda item: item[1])
    excess = len(_jobs) - MAX_COMPLETED_JOBS
    for job_id, _ in candidates[:max(0, excess)]:
        _jobs.pop(job_id, None)
    for canceled_id in list(_job_cancel_requests):
        if canceled_id not in _jobs:
            _job_cancel_requests.discard(canceled_id)


def create_job(job_type: str, dataset_id: str) -> dict:
    job_id = uuid4().hex
    now = datetime.utcnow().isoformat()
    job = {
        "id": job_id,
        "type": job_type,
        "dataset_id": dataset_id,
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "created_at": now,
        "updated_at": now,
        "result": None,
        "error": None,
    }
    with _lock:
        _jobs[job_id] = job
        _prune_jobs(datetime.utcnow())
    return job


def is_job_cancel_requested(job_id: str) -> bool:
    return job_id in _job_cancel_requests


def request_job_cancel(job_id: str) -> bool:
    with _lock:
        if job_id not in _jobs:
            _job_cancel_requests.discard(job_id)
            return False
        status = _jobs[job_id].get("status")
        if status in {"completed", "failed", "cancelled"}:
            _job_cancel_requests.discard(job_id)
            return False
        _job_cancel_requests.add(job_id)
        return True


def get_job(job_id: str) -> dict | None:
    with _lock:
        job = _jobs.get(job_id)
        if not job:
            return None
        payload = dict(job)
        payload["cancel_requested"] = job_id in _job_cancel_requests
        return payload


def update_job(job_id: str, **fields) -> None:
    with _lock:
        if job_id not in _jobs:
            return
        _jobs[job_id].update(fields)
        _jobs[job_id]["updated_at"] = datetime.utcnow().isoformat()


def finish_job(job_id: str, result: dict) -> None:
    update_job(job_id, status="completed", progress=100, message="Completed", result=result)
    _job_cancel_requests.discard(job_id)


def cancel_job(job_id: str, message: str | None = None) -> None:
    update_job(job_id, status="cancelled", progress=100, message=message or "Cancelled")
    _job_cancel_requests.discard(job_id)


def fail_job(job_id: str, error: str) -> None:
    update_job(job_id, status="failed", message=error, error=error)
    _job_cancel_requests.discard(job_id)
