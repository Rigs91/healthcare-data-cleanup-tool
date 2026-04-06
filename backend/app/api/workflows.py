from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.datasets import (
    AutopilotRequest,
    _create_dataset_from_path,
    _dataset_to_detail,
    _run_autopilot_for_dataset,
    get_db,
)
from app.db.models import Dataset
from app.services.storage import is_supported_filename, save_upload_to_disk

router = APIRouter(prefix="/api/v2/workflows", tags=["workflows_v2"])


class WorkflowAutopilotRequest(BaseModel):
    target_score: int = Field(default=95, ge=70, le=100)
    output_format: str | None = None
    privacy_mode: str | None = None
    performance_mode: str | None = None
    cleanup_mode: str | None = None
    llm_model: str | None = None


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None and str(item).strip()]


def _derive_stage(dataset: Dataset, profile: dict[str, Any], qc: dict[str, Any]) -> str:
    status = str(dataset.status or "").lower()
    if status in {"running", "processing"}:
        return "running"
    if status in {"failed", "error"}:
        return "failed"
    if dataset.cleaned_path and qc:
        return "completed"
    if profile:
        return "prechecked"
    return "uploaded"


def _next_actions_for_stage(stage: str) -> list[str]:
    mapping = {
        "uploaded": ["review_precheck", "refresh_workflow"],
        "prechecked": ["run_autopilot", "refresh_workflow", "view_history"],
        "running": ["refresh_workflow"],
        "completed": ["download_result", "start_new_workflow", "view_history"],
        "failed": ["run_autopilot", "refresh_workflow", "view_history"],
    }
    return mapping.get(stage, ["refresh_workflow"])


def _build_precheck_summary(profile: dict[str, Any]) -> dict[str, Any] | None:
    if not profile:
        return None

    decision = profile.get("preclean_decision") if isinstance(profile.get("preclean_decision"), dict) else {}
    rag = profile.get("rag_readiness") if isinstance(profile.get("rag_readiness"), dict) else {}
    blockers = _as_string_list(decision.get("reasons"))

    if not blockers:
        checks = rag.get("checks") if isinstance(rag.get("checks"), list) else []
        for check in checks:
            if not isinstance(check, dict):
                continue
            status = str(check.get("status") or "").lower()
            if status in {"not_ready", "blocked", "partial", "needs_attention"}:
                reason = check.get("summary") or check.get("name") or "Check needs attention"
                blockers.append(str(reason))
            if len(blockers) >= 5:
                break

    return {
        "decision_status": decision.get("status") or "needs_review",
        "decision_reasons": blockers[:5],
        "recommended_actions": _as_string_list(decision.get("actions"))[:5],
        "readiness_score": rag.get("score"),
        "readiness_band": rag.get("band") or rag.get("label"),
        "primary_domain": profile.get("primary_domain"),
        "is_sampled": bool(profile.get("sampled")),
        "llm_summary": ((profile.get("llm_assist") or {}).get("summary") if isinstance(profile.get("llm_assist"), dict) else None),
        "llm_plan_status": ((profile.get("llm_assist") or {}).get("status") if isinstance(profile.get("llm_assist"), dict) else None),
        "llm_acceptance_status": ((profile.get("llm_assist") or {}).get("acceptance_status") if isinstance(profile.get("llm_assist"), dict) else None),
        "llm_validation_notes": _as_string_list(((profile.get("llm_assist") or {}).get("validation_notes") if isinstance(profile.get("llm_assist"), dict) else []))[:5],
    }


def _build_result_summary(workflow_id: str, qc: dict[str, Any]) -> dict[str, Any] | None:
    if not qc:
        return None

    postclean_decision = qc.get("postclean_decision") if isinstance(qc.get("postclean_decision"), dict) else {}
    rag = qc.get("rag_readiness") if isinstance(qc.get("rag_readiness"), dict) else {}
    rag_comparison = qc.get("rag_readiness_comparison") if isinstance(qc.get("rag_readiness_comparison"), dict) else {}
    quality_gate = qc.get("quality_gate") if isinstance(qc.get("quality_gate"), dict) else {}

    return {
        "decision_status": postclean_decision.get("status") or "warn",
        "release_recommendation": postclean_decision.get("release_recommendation") or "Review diagnostics before sharing.",
        "quality_gate_status": quality_gate.get("status") or "warn",
        "rag_score": rag.get("score"),
        "rag_score_delta": rag_comparison.get("score_delta"),
        "warnings": _as_string_list(qc.get("warnings")),
        "blockers": _as_string_list(postclean_decision.get("blockers")),
        "actions": _as_string_list(postclean_decision.get("actions")),
        "download_url": f"/api/v2/workflows/{workflow_id}/export",
    }


def _build_execution_summary(detail_payload: dict[str, Any]) -> dict[str, Any]:
    latest_run = detail_payload.get("latest_run") if isinstance(detail_payload.get("latest_run"), dict) else {}
    latest_plan = latest_run.get("llm_plan") if isinstance(latest_run.get("llm_plan"), dict) else {}
    dataset_plan = detail_payload.get("llm_plan") if isinstance(detail_payload.get("llm_plan"), dict) else {}
    active_plan = latest_plan or dataset_plan
    return {
        "cleanup_mode": latest_run.get("cleanup_mode") or detail_payload.get("cleanup_mode") or "deterministic",
        "llm_provider": latest_run.get("llm_provider") or detail_payload.get("llm_provider"),
        "llm_model": latest_run.get("llm_model") or detail_payload.get("llm_model"),
        "llm_plan_status": active_plan.get("status") if isinstance(active_plan, dict) else None,
        "llm_acceptance_status": active_plan.get("acceptance_status") if isinstance(active_plan, dict) else None,
        "llm_summary": active_plan.get("summary") if isinstance(active_plan, dict) else None,
        "llm_validation_notes": _as_string_list(active_plan.get("validation_notes"))[:5] if isinstance(active_plan, dict) else [],
    }


def _build_workflow_response(dataset: Dataset, *, db: Session) -> dict[str, Any]:
    dataset_detail = _dataset_to_detail(dataset, db=db)
    detail_payload = dataset_detail.model_dump(mode="json")
    profile = detail_payload.get("profile") if isinstance(detail_payload.get("profile"), dict) else {}
    qc = detail_payload.get("qc") if isinstance(detail_payload.get("qc"), dict) else {}
    stage = _derive_stage(dataset, profile, qc)

    return {
        "workflow_id": dataset.id,
        "dataset_id": dataset.id,
        "stage": stage,
        "next_actions": _next_actions_for_stage(stage),
        "dataset": detail_payload,
        "precheck_summary": _build_precheck_summary(profile),
        "result_summary": _build_result_summary(dataset.id, qc),
        "execution": _build_execution_summary(detail_payload),
    }


def _require_workflow(db: Session, workflow_id: str) -> Dataset:
    dataset = db.query(Dataset).filter(Dataset.id == workflow_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return dataset


@router.get("")
@router.get("/")
async def list_workflows(limit: int = 25, db: Session = Depends(get_db)) -> dict[str, Any]:
    safe_limit = max(1, min(100, int(limit)))
    datasets = db.query(Dataset).order_by(Dataset.created_at.desc()).limit(safe_limit).all()
    return {"items": [_build_workflow_response(dataset, db=db) for dataset in datasets]}


@router.post("/upload")
async def create_workflow_upload(
    file: UploadFile = File(...),
    name: str | None = Form(None),
    usage_intent: str | None = Form(None),
    output_format: str | None = Form(None),
    privacy_mode: str | None = Form(None),
    cleanup_mode: str | None = Form(None),
    llm_model: str | None = Form(None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    filename = (file.filename or "").strip()
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required.")
    if not is_supported_filename(filename):
        raise HTTPException(status_code=400, detail="Unsupported file extension.")

    dataset_id = uuid4().hex
    saved_path, size_bytes = save_upload_to_disk(file.file, filename, dataset_id)
    dataset = _create_dataset_from_path(
        db=db,
        dataset_id=dataset_id,
        saved_path=saved_path,
        filename=filename,
        size_bytes=size_bytes,
        name=name,
        usage_intent=usage_intent,
        output_format=output_format,
        privacy_mode=privacy_mode,
        cleanup_mode=cleanup_mode,
        llm_model=llm_model,
    )
    return _build_workflow_response(dataset, db=db)


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    dataset = _require_workflow(db, workflow_id)
    return _build_workflow_response(dataset, db=db)


@router.post("/{workflow_id}/autopilot-run")
async def run_workflow_autopilot(
    workflow_id: str,
    payload: WorkflowAutopilotRequest,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    dataset = _require_workflow(db, workflow_id)

    request_payload = AutopilotRequest(
        dataset_id=workflow_id,
        target_score=payload.target_score,
        output_format=payload.output_format,
        privacy_mode=payload.privacy_mode,
        performance_mode=payload.performance_mode,
        cleanup_mode=payload.cleanup_mode,
        llm_model=payload.llm_model,
    )
    _run_autopilot_for_dataset(dataset=dataset, payload=request_payload, db=db)
    db.refresh(dataset)

    return _build_workflow_response(dataset, db=db)


@router.get("/{workflow_id}/result")
async def get_workflow_result(workflow_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    dataset = _require_workflow(db, workflow_id)
    payload = _build_workflow_response(dataset, db=db)
    if payload.get("stage") != "completed":
        raise HTTPException(status_code=409, detail="Workflow has not completed cleaning yet.")

    dataset_payload = payload.get("dataset") if isinstance(payload.get("dataset"), dict) else {}
    return {
        "workflow_id": workflow_id,
        "stage": payload.get("stage"),
        "result_summary": payload.get("result_summary"),
        "execution": payload.get("execution"),
        "qc": dataset_payload.get("qc") if isinstance(dataset_payload.get("qc"), dict) else None,
        "latest_run": dataset_payload.get("latest_run") if isinstance(dataset_payload.get("latest_run"), dict) else None,
    }


@router.get("/{workflow_id}/export")
async def export_workflow_result(workflow_id: str, db: Session = Depends(get_db)) -> FileResponse:
    dataset = _require_workflow(db, workflow_id)
    if not dataset.cleaned_path:
        raise HTTPException(status_code=409, detail="Workflow has not produced a cleaned file yet.")

    path = Path(dataset.cleaned_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Cleaned file not found.")

    return FileResponse(str(path), filename=path.name)
