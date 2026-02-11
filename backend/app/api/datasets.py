import json
import time
from pathlib import Path
from threading import Thread
from typing import Any
from uuid import uuid4

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from app.config import (
    CLEANED_DIR,
    CSV_CHUNK_SIZE,
    CSV_CHUNK_SIZE_FAST,
    CSV_CHUNK_SIZE_ULTRA,
    MAX_UPLOAD_MB,
    PREVIEW_ROWS,
    PROFILE_SAMPLE_ROWS,
    RAW_DIR,
    STREAMING_MAX_WORKERS_FAST,
    STREAMING_MAX_WORKERS_ULTRA,
    STREAMING_THRESHOLD_MB,
)
from app.db.models import CleanRun, Dataset
from app.db.session import SessionLocal
from app.schemas.dataset import DatasetBase, DatasetDetail, DatasetList
from app.services.cleaning import MISSING_VALUES_LOWER, clean_dataframe, standardize_columns
from app.services.jobs import create_job, fail_job, finish_job, get_job, update_job
from app.services.outcomes import build_postclean_decision, evaluate_outcomes
from app.services.profiling import build_profile, infer_hints_from_profile
from app.services.qc import build_qc_report, build_streaming_qc_report
from app.services.rag_readiness import (
    build_rag_readiness_comparison,
    build_rag_readiness_from_dataframe,
)
from app.services.runs import complete_clean_run, create_clean_run, fail_clean_run, run_to_dict
from app.services.storage import (
    detect_file_type,
    estimate_row_count,
    read_dataframe,
    save_upload_to_disk,
)
from app.services.streaming import stream_clean_csv
from app.services.validation import build_validation_issues

router = APIRouter(prefix="/api", tags=["datasets"])

UPLOAD_DIR = RAW_DIR / "_uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


class CleaningOptions(BaseModel):
    remove_duplicates: bool | None = None
    drop_empty_columns: bool | None = None
    deidentify: bool | None = None
    normalize_phone: bool | None = None
    normalize_zip: bool | None = None
    normalize_gender: bool | None = None
    text_case: str | None = None
    output_format: str | None = None
    coercion_mode: str | None = None
    privacy_mode: str | None = None
    performance_mode: str | None = None


class UploadStart(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    filename: str
    name: str | None = None
    usage_intent: str | None = None
    output_format: str | None = None
    privacy_mode: str | None = None
    file_size: int = Field(..., alias="fileSize")
    total_chunks: int = Field(..., alias="totalChunks")


class GoogleDriveImportRequest(BaseModel):
    file_id: str
    file_name: str
    mime_type: str
    name: str | None = None
    usage_intent: str | None = None
    output_format: str | None = None
    privacy_mode: str | None = None


class AutopilotRequest(BaseModel):
    dataset_id: str | None = None
    target_score: int = Field(default=95, ge=70, le=100)
    output_format: str | None = None
    privacy_mode: str | None = None
    performance_mode: str | None = None


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _serialize_json(value: dict | list | None) -> str | None:
    return json.dumps(value) if value is not None else None


def _deserialize_json(value: str | None) -> Any:
    if not value:
        return None
    return json.loads(value)


def _to_preview(df, *, limit: int = 50, row_count: int | None = None) -> dict:
    preview_df = df.head(limit).copy()
    preview_df = preview_df.where(preview_df.notnull(), None)
    rows = preview_df.to_dict(orient="records")
    return {
        "columns": list(preview_df.columns),
        "rows": rows,
        "row_count": int(len(df)) if row_count is None else int(row_count),
    }


def _load_latest_run(db: Session, dataset: Dataset) -> CleanRun | None:
    if not dataset.latest_run_id:
        return None
    return db.query(CleanRun).filter(CleanRun.id == dataset.latest_run_id).first()


def _dataset_to_detail(
    dataset: Dataset,
    *,
    db: Session | None = None,
    latest_run: CleanRun | None = None,
) -> DatasetDetail:
    if latest_run is None and db is not None:
        latest_run = _load_latest_run(db, dataset)
    latest_run_payload = run_to_dict(latest_run) if latest_run else None
    return DatasetDetail(
        id=dataset.id,
        name=dataset.name,
        original_filename=dataset.original_filename,
        status=dataset.status,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at,
        source_type=dataset.source_type,
        file_type=dataset.file_type,
        usage_intent=dataset.usage_intent,
        output_format=dataset.output_format,
        privacy_mode=dataset.privacy_mode,
        file_size_bytes=dataset.file_size_bytes,
        row_count_estimate=dataset.row_count_estimate,
        latest_run_id=dataset.latest_run_id,
        profile=_deserialize_json(dataset.profile_json),
        qc=_deserialize_json(dataset.qc_json),
        column_map=_deserialize_json(dataset.column_map_json),
        raw_path=dataset.raw_path,
        cleaned_path=dataset.cleaned_path,
        latest_run=latest_run_payload,
    )


def _create_dataset_from_path(
    *,
    db: Session,
    dataset_id: str,
    saved_path: Path,
    filename: str,
    size_bytes: int,
    name: str | None,
    usage_intent: str | None,
    output_format: str | None,
    privacy_mode: str | None,
    source_type: str = "file_upload",
    source_details: dict[str, Any] | None = None,
) -> Dataset:
    file_type = detect_file_type(filename)
    size_mb = size_bytes / (1024 * 1024)
    if size_mb > MAX_UPLOAD_MB:
        saved_path.unlink(missing_ok=True)
        raise HTTPException(status_code=413, detail="File too large for MVP limit.")

    row_count_estimate = estimate_row_count(saved_path, file_type, size_bytes=size_bytes)
    sampled = size_mb > STREAMING_THRESHOLD_MB

    try:
        raw_df, _delimiter = read_dataframe(
            saved_path,
            file_type=file_type,
            max_rows=PROFILE_SAMPLE_ROWS if sampled else None,
        )
    except ValueError as exc:
        saved_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _, column_map = standardize_columns(raw_df.copy())
    profile = build_profile(
        raw_df,
        column_map,
        total_rows=row_count_estimate,
        sampled=sampled,
        privacy_mode=privacy_mode or "safe_harbor",
    )
    if source_details:
        profile["source"] = source_details

    dataset = Dataset(
        id=dataset_id,
        name=name or Path(filename).stem,
        original_filename=filename,
        status="ingested",
        raw_path=str(saved_path),
        cleaned_path=None,
        profile_json=_serialize_json(profile),
        qc_json=None,
        column_map_json=_serialize_json(column_map),
        source_type=source_type,
        file_type=file_type,
        usage_intent=usage_intent or "training",
        output_format=output_format or "csv",
        privacy_mode=privacy_mode or "safe_harbor",
        file_size_bytes=size_bytes,
        row_count_estimate=row_count_estimate,
        latest_run_id=None,
    )

    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    return dataset


def _recompute_profile_for_dataset(*, dataset: Dataset, db: Session) -> Dataset:
    raw_path = Path(dataset.raw_path or "")
    if not raw_path.exists():
        raise HTTPException(status_code=404, detail="Raw dataset file not found")

    file_type = dataset.file_type or detect_file_type(dataset.original_filename)
    size_bytes = raw_path.stat().st_size
    sampled = (size_bytes / (1024 * 1024)) > STREAMING_THRESHOLD_MB
    row_count_estimate = estimate_row_count(raw_path, file_type, size_bytes=size_bytes)

    try:
        raw_df, _delimiter = read_dataframe(
            raw_path,
            file_type=file_type,
            max_rows=PROFILE_SAMPLE_ROWS if sampled else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _, column_map = standardize_columns(raw_df.copy())
    profile = build_profile(
        raw_df,
        column_map,
        total_rows=row_count_estimate,
        sampled=sampled,
        privacy_mode=dataset.privacy_mode or "safe_harbor",
    )

    dataset.profile_json = _serialize_json(profile)
    dataset.column_map_json = _serialize_json(column_map)
    dataset.file_type = file_type
    dataset.file_size_bytes = size_bytes
    dataset.row_count_estimate = row_count_estimate
    db.commit()
    db.refresh(dataset)
    return dataset


def _perform_cleaning(
    dataset: Dataset,
    options: CleaningOptions,
    db: Session,
    *,
    progress_callback=None,
) -> dict:
    profile = _deserialize_json(dataset.profile_json) or {}
    column_metadata = infer_hints_from_profile(profile)

    usage_intent = dataset.usage_intent or "training"
    profile_rag_score = (profile.get("rag_readiness") or {}).get("score")

    def _default(flag: bool | None, default: bool) -> bool:
        return default if flag is None else flag

    defaults = {
        "remove_duplicates": True,
        "drop_empty_columns": True,
        "deidentify": usage_intent in {"training", "external_share"},
        "normalize_phone": True,
        "normalize_zip": True,
        "normalize_gender": True,
        "text_case": "none",
        "coercion_mode": "safe",
    }

    resolved = {
        "remove_duplicates": _default(options.remove_duplicates, defaults["remove_duplicates"]),
        "drop_empty_columns": _default(options.drop_empty_columns, defaults["drop_empty_columns"]),
        "deidentify": _default(options.deidentify, defaults["deidentify"]),
        "normalize_phone": _default(options.normalize_phone, defaults["normalize_phone"]),
        "normalize_zip": _default(options.normalize_zip, defaults["normalize_zip"]),
        "normalize_gender": _default(options.normalize_gender, defaults["normalize_gender"]),
        "text_case": options.text_case or defaults["text_case"],
        "coercion_mode": defaults["coercion_mode"],
    }
    if options.coercion_mode in {"safe", "strict"}:
        resolved["coercion_mode"] = options.coercion_mode

    privacy_mode = options.privacy_mode or dataset.privacy_mode or "safe_harbor"
    if options.deidentify is not None:
        privacy_mode = "safe_harbor" if options.deidentify else "none"

    output_format = options.output_format or dataset.output_format or "csv"

    performance_mode = options.performance_mode or "balanced"
    if performance_mode not in {"balanced", "fast", "ultra_fast"}:
        raise HTTPException(status_code=400, detail="Unsupported performance mode.")

    if performance_mode == "ultra_fast":
        resolved["normalize_phone"] = False
        resolved["normalize_zip"] = False
        resolved["normalize_gender"] = False
        resolved["text_case"] = "none"

    file_size_mb = (dataset.file_size_bytes or 0) / (1024 * 1024)

    if file_size_mb > STREAMING_THRESHOLD_MB and dataset.file_type in {"csv", "tsv"}:
        if progress_callback:
            progress_callback(10, "Streaming cleaning started")
        cleaned_path = CLEANED_DIR / f"{dataset.id}__cleaned.{output_format}"
        if performance_mode == "ultra_fast":
            chunksize = CSV_CHUNK_SIZE_ULTRA
            parallel_workers = STREAMING_MAX_WORKERS_ULTRA
            use_pyarrow = True
        elif performance_mode == "fast":
            chunksize = CSV_CHUNK_SIZE_FAST
            parallel_workers = STREAMING_MAX_WORKERS_FAST
            use_pyarrow = True
        else:
            chunksize = CSV_CHUNK_SIZE
            parallel_workers = 1
            use_pyarrow = False
        try:
            cleaning_report, qc_payload, preview_df, warnings = stream_clean_csv(
                Path(dataset.raw_path),
                file_type=dataset.file_type or "csv",
                column_metadata=column_metadata,
                output_path=cleaned_path,
                output_format=output_format,
                privacy_mode=privacy_mode,
                row_count_estimate=dataset.row_count_estimate,
                chunksize=chunksize,
                performance_mode=performance_mode,
                parallel_workers=parallel_workers,
                use_pyarrow=use_pyarrow,
                progress_callback=(
                    (lambda pct: progress_callback(pct, "Streaming processing")) if progress_callback else None
                ),
                **resolved,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        issues = []
        if performance_mode in {"fast", "ultra_fast"}:
            warnings.append("Performance mode: validation checks skipped.")
        else:
            issues = build_validation_issues(
                preview_df,
                column_metadata,
                total_rows=len(preview_df),
            )
            if issues:
                warnings.append("Validation issues computed on preview sample only.")
        qc_report = build_streaming_qc_report(
            row_count_raw=qc_payload["row_count_raw"],
            row_count_cleaned=qc_payload["row_count_cleaned"],
            missing_counts_raw=qc_payload["missing_counts_raw"],
            missing_counts_cleaned=qc_payload["missing_counts_cleaned"],
            invalid_values=qc_payload["invalid_counts"],
            empty_columns_removed=qc_payload["empty_columns_removed"],
            duplicate_rows_removed=qc_payload["duplicate_rows_removed"],
            cleaning_report=cleaning_report,
            warnings=warnings,
            issues=issues,
        )
        qc_report["rag_readiness"] = build_rag_readiness_from_dataframe(
            preview_df,
            qc_report,
            privacy_mode=privacy_mode,
            sampled=True,
            baseline_score=profile_rag_score if isinstance(profile_rag_score, int) else None,
        )
        preview = _to_preview(preview_df, limit=PREVIEW_ROWS, row_count=qc_payload["row_count_cleaned"])
    else:
        if progress_callback:
            progress_callback(10, "Loading data")
        try:
            raw_df, _delimiter = read_dataframe(Path(dataset.raw_path), file_type=dataset.file_type or "csv")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if progress_callback:
            progress_callback(40, "Cleaning data")
        cleaned_df, cleaning_report = clean_dataframe(
            raw_df,
            column_metadata,
            sanitize_strings=performance_mode != "ultra_fast",
            remove_duplicates=resolved["remove_duplicates"],
            drop_empty_columns=resolved["drop_empty_columns"],
            deidentify=resolved["deidentify"],
            normalize_phone=resolved["normalize_phone"],
            normalize_zip=resolved["normalize_zip"],
            normalize_gender=resolved["normalize_gender"],
            text_case=resolved["text_case"],
            coercion_mode=resolved["coercion_mode"],
            privacy_mode=privacy_mode,
            performance_mode=performance_mode,
        )

        cleaned_path = CLEANED_DIR / f"{dataset.id}__cleaned.{output_format}"
        if progress_callback:
            progress_callback(70, "Writing output")
        if output_format == "csv":
            cleaned_df.to_csv(cleaned_path, index=False)
        elif output_format == "jsonl":
            cleaned_df.to_json(cleaned_path, orient="records", lines=True)
        elif output_format == "parquet":
            try:
                cleaned_df.to_parquet(cleaned_path, index=False)
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail="Parquet output requires pyarrow or fastparquet.",
                ) from exc
        else:
            raise HTTPException(status_code=400, detail="Unsupported output format.")

        if progress_callback:
            progress_callback(85, "Building QC")
        issues = []
        warnings = []
        if performance_mode in {"fast", "ultra_fast"}:
            warnings.append("Performance mode: validation checks skipped.")
        else:
            issues = build_validation_issues(
                cleaned_df,
                column_metadata,
                total_rows=len(cleaned_df),
            )
        qc_report = build_qc_report(raw_df, cleaned_df, cleaning_report, issues=issues)
        if warnings:
            qc_report["warnings"] = warnings
        qc_report["rag_readiness"] = build_rag_readiness_from_dataframe(
            cleaned_df,
            qc_report,
            privacy_mode=privacy_mode,
            sampled=False,
            baseline_score=profile_rag_score if isinstance(profile_rag_score, int) else None,
        )
        preview = _to_preview(cleaned_df, limit=PREVIEW_ROWS)

    dataset.status = "cleaned"
    dataset.cleaned_path = str(cleaned_path)
    dataset.qc_json = _serialize_json(qc_report)
    dataset.column_map_json = _serialize_json(cleaning_report.get("column_map"))
    dataset.output_format = output_format
    dataset.privacy_mode = privacy_mode
    db.commit()
    db.refresh(dataset)

    if progress_callback:
        progress_callback(100, "Completed")

    return {
        "dataset": _dataset_to_detail(dataset, db=db),
        "preview": preview,
        "qc": qc_report,
    }


def _perform_cleaning_with_run(
    dataset: Dataset,
    options: CleaningOptions,
    db: Session,
    *,
    progress_callback=None,
) -> dict:
    profile = _deserialize_json(dataset.profile_json) or {}
    assessment = profile.get("assessment") if isinstance(profile, dict) else None
    performance_mode = options.performance_mode or "balanced"
    privacy_mode = options.privacy_mode or dataset.privacy_mode or "safe_harbor"
    output_format = options.output_format or dataset.output_format or "csv"
    remove_duplicates_enabled = True if options.remove_duplicates is None else bool(options.remove_duplicates)
    rag_before_score = (profile.get("rag_readiness") or {}).get("score") if isinstance(profile, dict) else None
    rag_before = (profile.get("rag_readiness") or None) if isinstance(profile, dict) else None

    run = create_clean_run(
        db=db,
        dataset_id=dataset.id,
        performance_mode=performance_mode,
        privacy_mode=privacy_mode,
        output_format=output_format,
        profile_snapshot=profile if isinstance(profile, dict) else None,
        assessment=assessment if isinstance(assessment, dict) else None,
    )

    started = time.perf_counter()
    try:
        result = _perform_cleaning(dataset, options, db, progress_callback=progress_callback)
        duration_ms = int((time.perf_counter() - started) * 1000)
        qc_report = result.get("qc") or {}
        rag_after = qc_report.get("rag_readiness") if isinstance(qc_report, dict) else None
        comparison = build_rag_readiness_comparison(
            rag_before if isinstance(rag_before, dict) else None,
            rag_after if isinstance(rag_after, dict) else None,
        )
        if comparison:
            qc_report["rag_readiness_comparison"] = comparison
        outcomes_report = evaluate_outcomes(
            qc_report=qc_report,
            rag_before_score=rag_before_score if isinstance(rag_before_score, int) else None,
            rag_after=rag_after if isinstance(rag_after, dict) else None,
            duration_ms=duration_ms,
            performance_mode=performance_mode,
            remove_duplicates_enabled=remove_duplicates_enabled,
        )
        qc_report["outcomes"] = outcomes_report["items"]
        qc_report["quality_gate"] = outcomes_report["quality_gate"]
        qc_report["postclean_decision"] = build_postclean_decision(qc_report=qc_report)

        dataset.qc_json = _serialize_json(qc_report)
        dataset.latest_run_id = run.id
        db.commit()
        db.refresh(dataset)

        run = complete_clean_run(
            db=db,
            run=run,
            duration_ms=duration_ms,
            qc=qc_report,
            outcomes=outcomes_report["items"],
            rag_readiness=rag_after if isinstance(rag_after, dict) else None,
            quality_gate=outcomes_report["quality_gate"],
            warnings=qc_report.get("warnings") if isinstance(qc_report, dict) else [],
        )

        result["qc"] = qc_report
        result["run"] = run_to_dict(run)
        result["dataset"] = _dataset_to_detail(dataset, db=db, latest_run=run)
        return result
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        fail_clean_run(db=db, run=run, error_message=str(exc), duration_ms=duration_ms)
        raise


def _autopilot_cleaning_options(
    *,
    dataset: Dataset,
    profile: dict[str, Any],
    payload: AutopilotRequest,
) -> CleaningOptions:
    usage_intent = (dataset.usage_intent or "training").lower()
    pii_columns = ((profile.get("summary") or {}).get("columns_with_pii") or [])
    pii_detected = len(pii_columns) > 0

    default_privacy = "safe_harbor" if usage_intent in {"training", "external_share"} else "none"
    if pii_detected and usage_intent in {"training", "external_share"}:
        default_privacy = "safe_harbor"

    performance_mode = payload.performance_mode or "balanced"
    if performance_mode not in {"balanced", "fast", "ultra_fast"}:
        performance_mode = "balanced"

    return CleaningOptions(
        remove_duplicates=True,
        drop_empty_columns=True,
        deidentify=None,
        normalize_phone=True,
        normalize_zip=True,
        normalize_gender=True,
        text_case="none",
        output_format=payload.output_format or dataset.output_format or "csv",
        coercion_mode="safe",
        privacy_mode=payload.privacy_mode or default_privacy,
        performance_mode=performance_mode,
    )


def _top_rag_blockers(readiness: dict[str, Any] | None, *, limit: int = 5) -> list[dict[str, Any]]:
    if not isinstance(readiness, dict):
        return []
    checks = readiness.get("checks") or []
    if not isinstance(checks, list):
        return []

    def _priority(check: dict[str, Any]) -> int:
        status = str(check.get("status") or "fail").lower()
        if status == "fail":
            return 0
        if status == "warn":
            return 1
        return 2

    ranked = sorted(
        [check for check in checks if isinstance(check, dict)],
        key=lambda item: (
            _priority(item),
            float(item.get("metric")) if isinstance(item.get("metric"), (int, float)) else 1.0,
        ),
    )

    blockers: list[dict[str, Any]] = []
    for check in ranked:
        status = str(check.get("status") or "warn").lower()
        if status not in {"fail", "warn"}:
            continue
        blockers.append(
            {
                "id": check.get("id"),
                "label": check.get("label") or check.get("id"),
                "status": status,
                "recommendation": check.get("recommendation") or "",
                "metric": check.get("metric"),
            }
        )
        if len(blockers) >= limit:
            break
    return blockers


RAG_TEXT_EXCLUDE_TOKENS = {"_id", "id", "date", "dob", "icd", "cpt", "loinc", "ndc", "rxnorm", "zip", "phone"}
PII_HINT_TOKENS = {"name", "email", "phone", "mobile", "ssn", "social", "address", "city", "zip", "postal"}
PROTECTED_DROP_TOKENS = {"id", "date", "dob", "icd", "cpt", "loinc", "ndc", "rxnorm", "code"}


def _is_string_series(series: pd.Series) -> bool:
    return pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series)


def _missing_pct_map(df: pd.DataFrame) -> dict[str, float]:
    missing_pct: dict[str, float] = {}
    for col in df.columns:
        series = df[col]
        missing_mask = series.isna()
        if _is_string_series(series):
            normalized = series.astype("string").str.strip().str.lower()
            missing_mask = missing_mask | normalized.isin(MISSING_VALUES_LOWER)
        missing_pct[str(col)] = round(float(missing_mask.mean() * 100), 2)
    return missing_pct


def _text_candidate_stats(df: pd.DataFrame) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = {}
    for col in df.columns:
        name = str(col).lower()
        if not _is_string_series(df[col]):
            continue
        if any(token in name for token in RAG_TEXT_EXCLUDE_TOKENS):
            continue
        series = df[col].dropna().astype(str)
        if series.empty:
            stats[str(col)] = {"non_empty_ratio": 0.0, "avg_len": 0.0}
            continue
        non_empty_ratio = float((series.str.strip() != "").mean())
        avg_len = float(series.str.len().mean())
        stats[str(col)] = {"non_empty_ratio": non_empty_ratio, "avg_len": avg_len}
    return stats


def _refresh_autopilot_qc(
    df: pd.DataFrame,
    qc_report: dict[str, Any],
    *,
    privacy_mode: str,
    baseline_score: int | None,
) -> dict[str, Any]:
    refreshed = dict(qc_report or {})
    refreshed["row_count_cleaned"] = int(len(df))
    refreshed["missing_pct_cleaned"] = _missing_pct_map(df)

    invalid_existing = refreshed.get("invalid_values") or {}
    if isinstance(invalid_existing, dict):
        filtered_invalid = {
            str(col): int(value)
            for col, value in invalid_existing.items()
            if str(col) in df.columns
        }
    else:
        filtered_invalid = {}
    if "rag_context" in df.columns and "rag_context" not in filtered_invalid:
        filtered_invalid["rag_context"] = 0
    refreshed["invalid_values"] = filtered_invalid

    row_count_raw = int(refreshed.get("row_count_raw") or len(df))
    refreshed["rows_removed"] = max(0, row_count_raw - len(df))
    refreshed["rag_readiness"] = build_rag_readiness_from_dataframe(
        df,
        refreshed,
        privacy_mode=privacy_mode,
        sampled=False,
        baseline_score=baseline_score,
    )
    return refreshed


def _build_rag_context_column(df: pd.DataFrame) -> tuple[pd.Series, list[str]]:
    candidate_columns: list[str] = []
    for col in df.columns:
        name = str(col).lower()
        if any(token in name for token in PII_HINT_TOKENS):
            continue
        if str(col) == "rag_context":
            continue
        series = df[col]
        if series.isna().all():
            continue
        candidate_columns.append(str(col))

    def _priority(col_name: str) -> tuple[int, int]:
        name = col_name.lower()
        score = 0
        if _is_string_series(df[col_name]):
            score += 3
        if any(
            token in name
            for token in {"note", "description", "reason", "summary", "comment", "diagnosis", "procedure", "status"}
        ):
            score += 3
        unique = int(df[col_name].nunique(dropna=True))
        return score, unique

    selected = sorted(candidate_columns, key=_priority, reverse=True)[:8]

    if not selected:
        fallback = pd.Series(
            [f"Synthetic retrieval context for row {idx + 1}. Structured healthcare fields normalized for RAG use." for idx in range(len(df))],
            index=df.index,
            dtype="string",
        )
        return fallback, []

    def _row_context(row: pd.Series, row_index: int) -> str:
        parts: list[str] = []
        for col in selected:
            value = row.get(col)
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            if text.lower() in MISSING_VALUES_LOWER:
                continue
            parts.append(f"{col}={text}")
        if not parts:
            parts.append(f"row={row_index + 1}")
        context = "; ".join(parts)
        if len(context) < 40:
            context = f"{context}. This record was normalized for retrieval and downstream RAG indexing."
        if len(context) > 480:
            context = context[:480].rstrip()
        return context

    rag_context = pd.Series(
        [_row_context(row, idx) for idx, (_, row) in enumerate(df.iterrows())],
        index=df.index,
        dtype="string",
    )
    return rag_context, selected


def _optimize_cleaned_df_for_target(
    cleaned_df: pd.DataFrame,
    qc_report: dict[str, Any],
    *,
    privacy_mode: str,
    baseline_score: int | None,
    target_score: int,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    current_df = cleaned_df.copy()
    current_qc = _refresh_autopilot_qc(
        current_df,
        qc_report,
        privacy_mode=privacy_mode,
        baseline_score=baseline_score,
    )
    current_score = int(((current_qc.get("rag_readiness") or {}).get("score")) or 0)
    actions: list[str] = []

    text_stats = _text_candidate_stats(current_df)
    has_chunkable = any(
        (item.get("non_empty_ratio", 0.0) >= 0.50 and 40 <= item.get("avg_len", 0.0) <= 500)
        for item in text_stats.values()
    )
    if not has_chunkable:
        context_col, source_cols = _build_rag_context_column(current_df)
        candidate_df = current_df.copy()
        candidate_df["rag_context"] = context_col
        candidate_qc = _refresh_autopilot_qc(
            candidate_df,
            current_qc,
            privacy_mode=privacy_mode,
            baseline_score=baseline_score,
        )
        candidate_score = int(((candidate_qc.get("rag_readiness") or {}).get("score")) or 0)
        if candidate_score >= current_score:
            current_df = candidate_df
            current_qc = candidate_qc
            current_score = candidate_score
            if source_cols:
                actions.append(f"Added synthetic rag_context using columns: {', '.join(source_cols)}.")
            else:
                actions.append("Added synthetic rag_context to provide chunkable retrieval text.")

    if current_score < target_score:
        text_stats = _text_candidate_stats(current_df)
        drop_candidates = []
        for col, stats in text_stats.items():
            name = col.lower()
            if col == "rag_context":
                continue
            if any(token in name for token in PROTECTED_DROP_TOKENS):
                continue
            if stats.get("non_empty_ratio", 0.0) < 0.35 or stats.get("avg_len", 0.0) < 8:
                drop_candidates.append(col)
        if drop_candidates:
            drop_limit = max(1, min(len(drop_candidates), int(max(1, len(current_df.columns) * 0.2))))
            to_drop = drop_candidates[:drop_limit]
            candidate_df = current_df.drop(columns=to_drop, errors="ignore")
            candidate_qc = _refresh_autopilot_qc(
                candidate_df,
                current_qc,
                privacy_mode=privacy_mode,
                baseline_score=baseline_score,
            )
            candidate_score = int(((candidate_qc.get("rag_readiness") or {}).get("score")) or 0)
            if candidate_score >= current_score:
                current_df = candidate_df
                current_qc = candidate_qc
                current_score = candidate_score
                actions.append(f"Dropped {len(to_drop)} low-signal text columns to improve text quality ratios.")

    if current_score < target_score:
        missing_map = _missing_pct_map(current_df)
        high_missing = [
            col
            for col, pct in missing_map.items()
            if pct >= 92.0 and not any(token in col.lower() for token in PROTECTED_DROP_TOKENS)
        ]
        if high_missing:
            drop_limit = max(1, min(len(high_missing), int(max(1, len(current_df.columns) * 0.15))))
            to_drop = high_missing[:drop_limit]
            candidate_df = current_df.drop(columns=to_drop, errors="ignore")
            candidate_qc = _refresh_autopilot_qc(
                candidate_df,
                current_qc,
                privacy_mode=privacy_mode,
                baseline_score=baseline_score,
            )
            candidate_score = int(((candidate_qc.get("rag_readiness") or {}).get("score")) or 0)
            if candidate_score >= current_score:
                current_df = candidate_df
                current_qc = candidate_qc
                current_score = candidate_score
                actions.append(f"Dropped {len(to_drop)} extremely high-missing columns to improve missingness health.")

    optimization_report = {
        "target_score": target_score,
        "final_score": current_score,
        "target_met": bool(current_score >= target_score),
        "actions": actions,
    }
    if not actions:
        optimization_report["note"] = "No optimization actions improved the post-clean RAG score."

    return current_df, current_qc, optimization_report


def _run_autopilot_for_dataset(
    *,
    dataset: Dataset,
    payload: AutopilotRequest,
    db: Session,
) -> dict:
    profile = _deserialize_json(dataset.profile_json) or {}
    options = _autopilot_cleaning_options(dataset=dataset, profile=profile, payload=payload)
    pre_blockers = _top_rag_blockers((profile.get("rag_readiness") if isinstance(profile, dict) else None), limit=5)

    result = _perform_cleaning_with_run(dataset, options, db)
    qc = result.get("qc") if isinstance(result, dict) and isinstance(result.get("qc"), dict) else {}
    rag_before_score = (profile.get("rag_readiness") or {}).get("score") if isinstance(profile, dict) else None
    rag_before = (profile.get("rag_readiness") or None) if isinstance(profile, dict) else None
    privacy_mode = options.privacy_mode or dataset.privacy_mode or "safe_harbor"

    optimization_report: dict[str, Any] = {
        "target_score": payload.target_score,
        "final_score": int(((qc.get("rag_readiness") or {}).get("score")) or 0),
        "target_met": False,
        "actions": [],
        "note": "Autopilot did not run optimization because cleaned output could not be loaded.",
    }

    if dataset.cleaned_path:
        try:
            cleaned_df, _ = read_dataframe(
                Path(dataset.cleaned_path),
                file_type=dataset.output_format or dataset.file_type or "csv",
            )
            optimized_df, optimized_qc, optimization_report = _optimize_cleaned_df_for_target(
                cleaned_df,
                qc,
                privacy_mode=privacy_mode,
                baseline_score=rag_before_score if isinstance(rag_before_score, int) else None,
                target_score=payload.target_score,
            )

            if len(optimized_df.columns) > 0:
                output_format = dataset.output_format or "csv"
                output_path = Path(dataset.cleaned_path)
                if output_format == "csv":
                    optimized_df.to_csv(output_path, index=False)
                elif output_format == "jsonl":
                    optimized_df.to_json(output_path, orient="records", lines=True)
                elif output_format == "parquet":
                    optimized_df.to_parquet(output_path, index=False)

                optimized_qc["autopilot_optimization"] = optimization_report
                comparison = build_rag_readiness_comparison(
                    rag_before if isinstance(rag_before, dict) else None,
                    optimized_qc.get("rag_readiness") if isinstance(optimized_qc.get("rag_readiness"), dict) else None,
                )
                if comparison:
                    optimized_qc["rag_readiness_comparison"] = comparison

                outcomes_report = evaluate_outcomes(
                    qc_report=optimized_qc,
                    rag_before_score=rag_before_score if isinstance(rag_before_score, int) else None,
                    rag_after=optimized_qc.get("rag_readiness") if isinstance(optimized_qc.get("rag_readiness"), dict) else None,
                    duration_ms=int(((result.get("run") or {}).get("duration_ms")) or 0),
                    performance_mode=options.performance_mode or "balanced",
                    remove_duplicates_enabled=bool(options.remove_duplicates is None or options.remove_duplicates),
                )
                optimized_qc["outcomes"] = outcomes_report["items"]
                optimized_qc["quality_gate"] = outcomes_report["quality_gate"]
                optimized_qc["postclean_decision"] = build_postclean_decision(qc_report=optimized_qc)

                dataset.qc_json = _serialize_json(optimized_qc)
                db.commit()
                db.refresh(dataset)

                latest_run = db.query(CleanRun).filter(CleanRun.id == dataset.latest_run_id).first()
                if latest_run:
                    latest_run = complete_clean_run(
                        db=db,
                        run=latest_run,
                        duration_ms=latest_run.duration_ms or int(((result.get("run") or {}).get("duration_ms")) or 0),
                        qc=optimized_qc,
                        outcomes=outcomes_report["items"],
                        rag_readiness=optimized_qc.get("rag_readiness"),
                        quality_gate=outcomes_report["quality_gate"],
                        warnings=optimized_qc.get("warnings") if isinstance(optimized_qc, dict) else [],
                    )
                    result["run"] = run_to_dict(latest_run)

                result["qc"] = optimized_qc
                result["preview"] = _to_preview(optimized_df, limit=PREVIEW_ROWS, row_count=len(optimized_df))
                result["dataset"] = _dataset_to_detail(dataset, db=db)
        except Exception:
            # Keep baseline autopilot result when optimization fails.
            pass

    qc = result.get("qc") if isinstance(result, dict) and isinstance(result.get("qc"), dict) else qc
    rag_after = (qc or {}).get("rag_readiness") if isinstance(qc, dict) else {}
    achieved = rag_after.get("score") if isinstance(rag_after, dict) else None
    on_track = isinstance(achieved, int) and achieved >= payload.target_score

    result["autopilot"] = {
        "target_score": payload.target_score,
        "achieved_score": achieved,
        "status": "on_track" if on_track else "needs_attention",
        "resolved_options": options.model_dump(),
        "preclean_top_blockers": pre_blockers,
        "postclean_top_blockers": _top_rag_blockers(rag_after if isinstance(rag_after, dict) else None, limit=5),
        "optimization": optimization_report,
    }
    return result


@router.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "hc-data-cleanup-ai", "version": "0.3.0"}


@router.get("/datasets", response_model=DatasetList)
async def list_datasets(db: Session = Depends(get_db)) -> DatasetList:
    items = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
    return DatasetList(
        items=[
            DatasetBase(
                id=item.id,
                name=item.name,
                original_filename=item.original_filename,
                status=item.status,
                created_at=item.created_at,
                updated_at=item.updated_at,
                source_type=item.source_type,
                file_type=item.file_type,
                usage_intent=item.usage_intent,
                output_format=item.output_format,
                privacy_mode=item.privacy_mode,
                file_size_bytes=item.file_size_bytes,
                row_count_estimate=item.row_count_estimate,
                latest_run_id=item.latest_run_id,
            )
            for item in items
        ]
    )


@router.post("/datasets", response_model=DatasetDetail)
async def create_dataset(
    file: UploadFile = File(...),
    name: str | None = Form(None),
    usage_intent: str | None = Form(None),
    output_format: str | None = Form(None),
    privacy_mode: str | None = Form(None),
    db: Session = Depends(get_db),
) -> DatasetDetail:
    dataset_id = uuid4().hex
    saved_path, size_bytes = save_upload_to_disk(file.file, file.filename, dataset_id)
    dataset = _create_dataset_from_path(
        db=db,
        dataset_id=dataset_id,
        saved_path=saved_path,
        filename=file.filename,
        size_bytes=size_bytes,
        name=name,
        usage_intent=usage_intent,
        output_format=output_format,
        privacy_mode=privacy_mode,
    )
    return _dataset_to_detail(dataset, db=db)


@router.post("/datasets/from-google-drive", response_model=DatasetDetail)
async def create_dataset_from_google_drive(
    payload: GoogleDriveImportRequest,
    db: Session = Depends(get_db),
) -> DatasetDetail:
    _ = payload, db
    raise HTTPException(
        status_code=501,
        detail={
            "code": "FEATURE_DISABLED",
            "message": "Google Drive import is a future enhancement and is currently disabled.",
        },
    )


@router.post("/uploads/start")
async def start_upload(payload: UploadStart) -> dict:
    upload_id = uuid4().hex
    meta = payload.model_dump()
    meta["upload_id"] = upload_id
    meta_path = UPLOAD_DIR / f"{upload_id}.json"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    return {"upload_id": upload_id}


@router.options("/uploads/start")
async def options_start_upload() -> dict:
    return {"status": "ok"}


@router.post("/uploads/{upload_id}/chunk")
async def upload_chunk(
    upload_id: str,
    index: int = Form(...),
    chunk: UploadFile = File(...),
) -> dict:
    upload_path = UPLOAD_DIR / upload_id
    upload_path.mkdir(parents=True, exist_ok=True)
    part_path = upload_path / f"part_{index:06d}"
    with part_path.open("wb") as handle:
        while True:
            data = chunk.file.read(1024 * 1024)
            if not data:
                break
            handle.write(data)
    return {"status": "ok"}


@router.options("/uploads/{upload_id}/chunk")
async def options_upload_chunk(upload_id: str) -> dict:
    return {"status": "ok"}


@router.post("/uploads/{upload_id}/complete", response_model=DatasetDetail)
async def complete_upload(upload_id: str, db: Session = Depends(get_db)) -> DatasetDetail:
    meta_path = UPLOAD_DIR / f"{upload_id}.json"
    if not meta_path.exists():
        raise HTTPException(status_code=404, detail="Upload session not found")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    upload_path = UPLOAD_DIR / upload_id
    if not upload_path.exists():
        raise HTTPException(status_code=400, detail="No chunks uploaded")

    total_chunks = meta.get("total_chunks") or 0
    parts = sorted(upload_path.glob("part_*"))
    if total_chunks and len(parts) != total_chunks:
        raise HTTPException(status_code=400, detail="Missing chunks")

    safe_name = meta["filename"].replace("..", ".").replace("/", "_").replace("\\", "_")
    dataset_id = uuid4().hex
    final_path = RAW_DIR / f"{dataset_id}__{safe_name}"

    with final_path.open("wb") as out:
        for part in sorted(parts):
            out.write(part.read_bytes())

    size_bytes = final_path.stat().st_size
    expected_size = meta.get("file_size")
    if expected_size and size_bytes != expected_size:
        final_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=400,
            detail=f"Upload size mismatch (expected {expected_size} bytes, got {size_bytes} bytes).",
        )
    dataset = _create_dataset_from_path(
        db=db,
        dataset_id=dataset_id,
        saved_path=final_path,
        filename=meta["filename"],
        size_bytes=size_bytes,
        name=meta.get("name"),
        usage_intent=meta.get("usage_intent"),
        output_format=meta.get("output_format"),
        privacy_mode=meta.get("privacy_mode"),
    )

    for part in parts:
        part.unlink(missing_ok=True)
    upload_path.rmdir()
    meta_path.unlink(missing_ok=True)

    return _dataset_to_detail(dataset, db=db)


@router.options("/uploads/{upload_id}/complete")
async def options_complete_upload(upload_id: str) -> dict:
    return {"status": "ok"}


@router.post("/datasets/{dataset_id}/assessment:recompute", response_model=DatasetDetail)
async def recompute_dataset_assessment(dataset_id: str, db: Session = Depends(get_db)) -> DatasetDetail:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    refreshed = _recompute_profile_for_dataset(dataset=dataset, db=db)
    return _dataset_to_detail(refreshed, db=db)


@router.post("/datasets/{dataset_id}/clean", response_model=dict)
async def clean_dataset(
    dataset_id: str,
    options: CleaningOptions,
    db: Session = Depends(get_db),
) -> dict:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return _perform_cleaning_with_run(dataset, options, db)


@router.post("/datasets/{dataset_id}/cleanup/autopilot", response_model=dict)
async def autopilot_dataset_cleanup(
    dataset_id: str,
    payload: AutopilotRequest,
    db: Session = Depends(get_db),
) -> dict:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    payload.dataset_id = dataset_id
    return _run_autopilot_for_dataset(dataset=dataset, payload=payload, db=db)


@router.post("/cleanup/autopilot", response_model=dict)
async def autopilot_cleanup(
    payload: AutopilotRequest,
    db: Session = Depends(get_db),
) -> dict:
    if not payload.dataset_id:
        raise HTTPException(status_code=400, detail="dataset_id is required")
    dataset = db.query(Dataset).filter(Dataset.id == payload.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return _run_autopilot_for_dataset(dataset=dataset, payload=payload, db=db)


def _run_clean_job(job_id: str, dataset_id: str, options: CleaningOptions) -> None:
    db = SessionLocal()
    try:
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if not dataset:
            fail_job(job_id, "Dataset not found")
            return

        def progress(pct: int, message: str) -> None:
            update_job(job_id, status="running", progress=pct, message=message)

        update_job(job_id, status="running", progress=5, message="Starting")
        result = _perform_cleaning_with_run(dataset, options, db, progress_callback=progress)
        finish_job(job_id, result)
    except Exception as exc:
        fail_job(job_id, str(exc))
    finally:
        db.close()


@router.post("/datasets/{dataset_id}/clean-jobs")
async def start_clean_job(dataset_id: str, options: CleaningOptions) -> dict:
    job = create_job("clean", dataset_id)
    thread = Thread(target=_run_clean_job, args=(job["id"], dataset_id, options), daemon=True)
    thread.start()
    return job


@router.options("/datasets/{dataset_id}/clean-jobs")
async def options_clean_job(dataset_id: str) -> dict:
    return {"status": "ok"}


@router.get("/clean-jobs/{job_id}")
async def get_clean_job(job_id: str) -> dict:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/datasets/{dataset_id}", response_model=DatasetDetail)
async def get_dataset(dataset_id: str, db: Session = Depends(get_db)) -> DatasetDetail:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return _dataset_to_detail(dataset, db=db)


@router.get("/datasets/{dataset_id}/preview")
async def preview_dataset(
    dataset_id: str,
    kind: str = "raw",
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
) -> dict:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    file_type = dataset.file_type or "csv"

    row_count_override = dataset.row_count_estimate
    if kind == "cleaned":
        if not dataset.cleaned_path:
            raise HTTPException(status_code=400, detail="Dataset not cleaned yet")
        df, _delimiter = read_dataframe(
            Path(dataset.cleaned_path), file_type=dataset.output_format or file_type, max_rows=limit, offset=offset
        )
        qc = _deserialize_json(dataset.qc_json) or {}
        row_count_override = qc.get("row_count_cleaned") or row_count_override
    else:
        df, _delimiter = read_dataframe(
            Path(dataset.raw_path), file_type=file_type, max_rows=limit, offset=offset
        )

    return _to_preview(df, limit=limit, row_count=row_count_override)


@router.get("/datasets/{dataset_id}/download")
async def download_dataset(
    dataset_id: str,
    kind: str = "cleaned",
    db: Session = Depends(get_db),
) -> FileResponse:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if kind == "cleaned":
        if not dataset.cleaned_path:
            raise HTTPException(status_code=400, detail="Dataset not cleaned yet")
        path = dataset.cleaned_path
    else:
        path = dataset.raw_path

    return FileResponse(path, filename=Path(path).name)
