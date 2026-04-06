from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List
from uuid import uuid4

from sqlalchemy.orm import Session

from app.db.models import CleanRun


def _serialize_json(value: Dict[str, Any] | List[Dict[str, Any]] | None) -> str | None:
    return json.dumps(value) if value is not None else None


def _deserialize_json(value: str | None) -> Any:
    if not value:
        return None
    return json.loads(value)


def create_clean_run(
    *,
    db: Session,
    dataset_id: str,
    performance_mode: str,
    privacy_mode: str,
    output_format: str,
    cleanup_mode: str | None,
    llm_provider: str | None,
    llm_model: str | None,
    llm_plan: Dict[str, Any] | None,
    profile_snapshot: Dict[str, Any] | None,
    assessment: Dict[str, Any] | None,
) -> CleanRun:
    run = CleanRun(
        id=uuid4().hex,
        dataset_id=dataset_id,
        status="running",
        performance_mode=performance_mode,
        privacy_mode=privacy_mode,
        output_format=output_format,
        cleanup_mode=cleanup_mode,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_plan_json=_serialize_json(llm_plan),
        started_at=datetime.utcnow(),
        profile_snapshot_json=_serialize_json(profile_snapshot),
        assessment_json=_serialize_json(assessment),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def complete_clean_run(
    *,
    db: Session,
    run: CleanRun,
    duration_ms: int,
    qc: Dict[str, Any],
    outcomes: List[Dict[str, Any]],
    rag_readiness: Dict[str, Any] | None,
    quality_gate: Dict[str, Any] | None,
    warnings: List[str] | None,
) -> CleanRun:
    run.status = "completed"
    run.completed_at = datetime.utcnow()
    run.duration_ms = duration_ms
    run.qc_json = _serialize_json(qc)
    run.outcomes_json = _serialize_json(outcomes)
    run.rag_readiness_json = _serialize_json(rag_readiness)
    run.quality_gate_json = _serialize_json(quality_gate)
    run.warnings_json = _serialize_json({"warnings": warnings or []})
    db.commit()
    db.refresh(run)
    return run


def fail_clean_run(
    *,
    db: Session,
    run: CleanRun,
    error_message: str,
    duration_ms: int | None = None,
) -> CleanRun:
    run.status = "failed"
    run.completed_at = datetime.utcnow()
    run.error = error_message
    if duration_ms is not None:
        run.duration_ms = duration_ms
    db.commit()
    db.refresh(run)
    return run


def cancel_clean_run(
    *,
    db: Session,
    run: CleanRun,
    error_message: str,
    duration_ms: int | None = None,
) -> CleanRun:
    run.status = "cancelled"
    run.completed_at = datetime.utcnow()
    run.error = error_message
    if duration_ms is not None:
        run.duration_ms = duration_ms
    db.commit()
    db.refresh(run)
    return run


def run_to_dict(run: CleanRun) -> Dict[str, Any]:
    return {
        "id": run.id,
        "dataset_id": run.dataset_id,
        "status": run.status,
        "performance_mode": run.performance_mode,
        "privacy_mode": run.privacy_mode,
        "output_format": run.output_format,
        "cleanup_mode": run.cleanup_mode,
        "llm_provider": run.llm_provider,
        "llm_model": run.llm_model,
        "llm_plan": _deserialize_json(run.llm_plan_json),
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "duration_ms": run.duration_ms,
        "profile_snapshot": _deserialize_json(run.profile_snapshot_json),
        "assessment": _deserialize_json(run.assessment_json),
        "qc": _deserialize_json(run.qc_json),
        "outcomes": _deserialize_json(run.outcomes_json) or [],
        "rag_readiness": _deserialize_json(run.rag_readiness_json),
        "quality_gate": _deserialize_json(run.quality_gate_json),
        "warnings": (_deserialize_json(run.warnings_json) or {}).get("warnings", []),
        "error": run.error,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    }
