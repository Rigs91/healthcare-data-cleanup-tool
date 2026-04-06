from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class OutcomeMetric(BaseModel):
    id: str
    label: str
    status: str
    target: str
    observed_value: Any
    unit: str
    evidence: str
    recommended_action: str


class QualityGateResult(BaseModel):
    mode: str
    status: str
    failed_outcomes: List[str]
    summary: str


class CleanRunDetail(BaseModel):
    id: str
    dataset_id: str
    status: str
    performance_mode: Optional[str] = None
    privacy_mode: Optional[str] = None
    output_format: Optional[str] = None
    cleanup_mode: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    llm_plan: Optional[Dict[str, Any]] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    assessment: Optional[Dict[str, Any]] = None
    qc: Optional[Dict[str, Any]] = None
    outcomes: Optional[List[OutcomeMetric]] = None
    rag_readiness: Optional[Dict[str, Any]] = None
    quality_gate: Optional[QualityGateResult] = None
    warnings: Optional[List[str]] = None
    error: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class DatasetBase(BaseModel):
    id: str
    name: str
    original_filename: str
    status: str
    created_at: datetime
    updated_at: datetime
    source_type: Optional[str] = None
    file_type: Optional[str] = None
    usage_intent: Optional[str] = None
    output_format: Optional[str] = None
    privacy_mode: Optional[str] = None
    cleanup_mode: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    file_size_bytes: Optional[int] = None
    row_count_estimate: Optional[int] = None
    latest_run_id: Optional[str] = None


class DatasetDetail(DatasetBase):
    profile: Optional[Dict[str, Any]] = None
    qc: Optional[Dict[str, Any]] = None
    column_map: Optional[Dict[str, str]] = None
    raw_path: Optional[str] = None
    cleaned_path: Optional[str] = None
    llm_plan: Optional[Dict[str, Any]] = None
    latest_run: Optional[CleanRunDetail] = None

    model_config = ConfigDict(from_attributes=True)


class DatasetList(BaseModel):
    items: list[DatasetBase]

    model_config = ConfigDict(from_attributes=True)
