from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.models import CleanRun, Dataset
from app.db.session import SessionLocal
from app.services.runs import run_to_dict

router = APIRouter(prefix="/api", tags=["runs"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/datasets/{dataset_id}/runs")
async def list_dataset_runs(
    dataset_id: str,
    limit: int = 20,
    db: Session = Depends(get_db),
) -> dict:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    safe_limit = max(1, min(200, int(limit)))
    runs = (
        db.query(CleanRun)
        .filter(CleanRun.dataset_id == dataset_id)
        .order_by(CleanRun.created_at.desc())
        .limit(safe_limit)
        .all()
    )
    return {"items": [run_to_dict(run) for run in runs]}


@router.get("/runs/{run_id}")
async def get_run(run_id: str, db: Session = Depends(get_db)) -> dict:
    run = db.query(CleanRun).filter(CleanRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run_to_dict(run)


@router.get("/runs/{run_id}/outcomes")
async def get_run_outcomes(run_id: str, db: Session = Depends(get_db)) -> dict:
    run = db.query(CleanRun).filter(CleanRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    payload = run_to_dict(run)
    return {
        "run_id": run.id,
        "dataset_id": run.dataset_id,
        "status": run.status,
        "quality_gate": payload.get("quality_gate"),
        "outcomes": payload.get("outcomes") or [],
    }
