from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, Text

from app.db.session import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    original_filename = Column(String, nullable=False)
    status = Column(String, nullable=False, default="ingested")
    raw_path = Column(String, nullable=False)
    cleaned_path = Column(String, nullable=True)
    profile_json = Column(Text, nullable=True)
    qc_json = Column(Text, nullable=True)
    column_map_json = Column(Text, nullable=True)
    source_type = Column(String, nullable=True)
    file_type = Column(String, nullable=True)
    usage_intent = Column(String, nullable=True)
    output_format = Column(String, nullable=True)
    privacy_mode = Column(String, nullable=True)
    cleanup_mode = Column(String, nullable=True)
    llm_provider = Column(String, nullable=True)
    llm_model = Column(String, nullable=True)
    llm_plan_json = Column(Text, nullable=True)
    file_size_bytes = Column(Integer, nullable=True)
    row_count_estimate = Column(Integer, nullable=True)
    latest_run_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CleanRun(Base):
    __tablename__ = "clean_runs"

    id = Column(String, primary_key=True, index=True)
    dataset_id = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, default="running")
    performance_mode = Column(String, nullable=True)
    privacy_mode = Column(String, nullable=True)
    output_format = Column(String, nullable=True)
    cleanup_mode = Column(String, nullable=True)
    llm_provider = Column(String, nullable=True)
    llm_model = Column(String, nullable=True)
    llm_plan_json = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)
    profile_snapshot_json = Column(Text, nullable=True)
    assessment_json = Column(Text, nullable=True)
    qc_json = Column(Text, nullable=True)
    outcomes_json = Column(Text, nullable=True)
    rag_readiness_json = Column(Text, nullable=True)
    quality_gate_json = Column(Text, nullable=True)
    warnings_json = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
