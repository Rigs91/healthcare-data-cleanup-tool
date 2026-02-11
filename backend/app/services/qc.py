from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from app.services.cleaning import MISSING_VALUES_LOWER


def _missing_summary(df: pd.DataFrame) -> Dict[str, float]:
    summary: Dict[str, float] = {}
    for col in df.columns:
        series = df[col]
        missing_mask = series.isna()
        if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
            normalized = series.astype("string").str.strip().str.lower()
            missing_mask = missing_mask | normalized.isin(MISSING_VALUES_LOWER)
        summary[col] = round(float(missing_mask.mean() * 100), 2)
    return summary


def _severity_rank(severity: str) -> int:
    if severity == "high":
        return 3
    if severity == "medium":
        return 2
    return 1


def _severity_legend() -> Dict[str, str]:
    return {
        "high": "High impact quality or safety risk. Triggered by high severity score or critical hard rules.",
        "medium": "Moderate impact issue. Rate and impact weight indicate meaningful cleanup priority.",
        "low": "Low impact issue. Monitor and fix if it affects your downstream use case.",
    }


def _severity_summary(issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for issue in issues:
        severity = issue.get("severity") or "low"
        if severity not in counts:
            severity = "low"
        counts[severity] += 1

    ranked = sorted(
        issues,
        key=lambda item: (
            _severity_rank(str(item.get("severity") or "low")),
            float(item.get("severity_score") or 0.0),
            int(item.get("count") or 0),
        ),
        reverse=True,
    )
    top_issues = [
        {
            "column": issue.get("column"),
            "message": issue.get("message"),
            "severity": issue.get("severity"),
            "count": issue.get("count"),
            "rate_pct": issue.get("rate_pct"),
            "severity_reason": issue.get("severity_reason"),
        }
        for issue in ranked[:5]
    ]
    return {
        "counts": counts,
        "total": len(issues),
        "top_issues": top_issues,
    }


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value:
            return None
        return float(value)
    if hasattr(value, "item"):
        try:
            return _to_jsonable(value.item())
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _build_column_rename_summary(cleaning_report: Dict[str, Any]) -> Dict[str, Any]:
    column_map = cleaning_report.get("column_map") or {}
    renamed = [
        {"from": str(source), "to": str(target)}
        for source, target in column_map.items()
        if str(source) != str(target)
    ]
    return {
        "count": len(renamed),
        "items": renamed[:50],
    }


def _build_conversion_summary(cleaning_report: Dict[str, Any]) -> Dict[str, Any]:
    conversion = cleaning_report.get("conversion") or {}
    by_type: Dict[str, int] = {}
    invalid_total = 0
    invalid_columns: List[Dict[str, Any]] = []
    coercion_skipped: List[str] = []

    for col, details in conversion.items():
        if not isinstance(details, dict):
            continue
        target_type = str(details.get("type") or "unknown")
        by_type[target_type] = by_type.get(target_type, 0) + 1
        invalid = int(details.get("invalid") or 0)
        invalid_total += invalid
        if invalid > 0:
            invalid_columns.append({"column": str(col), "invalid": invalid})
        if details.get("coercion_skipped"):
            coercion_skipped.append(str(col))

    return {
        "count": len(conversion),
        "by_type": by_type,
        "invalid_values_total": invalid_total,
        "invalid_columns": invalid_columns[:50],
        "coercion_skipped_columns": coercion_skipped[:50],
    }


def _build_normalization_summary(cleaning_report: Dict[str, Any]) -> Dict[str, Any]:
    privacy_mode = cleaning_report.get("privacy_mode")
    privacy_report = cleaning_report.get("privacy_report") or {}
    conversion = cleaning_report.get("conversion") or {}

    typed_columns = [
        str(col)
        for col, details in conversion.items()
        if isinstance(details, dict) and str(details.get("type") or "") in {"number", "date", "boolean"}
    ]

    privacy_transforms: List[Dict[str, Any]] = []
    if isinstance(privacy_report, dict):
        for key, columns in privacy_report.items():
            if not isinstance(columns, list) or not columns:
                continue
            privacy_transforms.append(
                {
                    "transform": str(key),
                    "count": len(columns),
                    "columns": [str(col) for col in columns[:50]],
                }
            )

    notes: List[str] = []
    if typed_columns:
        notes.append(f"Type coercion/normalization applied to {len(typed_columns)} columns.")
    if privacy_mode == "safe_harbor" and privacy_transforms:
        notes.append("Safe Harbor privacy transformations applied.")
    elif privacy_mode == "safe_harbor":
        notes.append("Safe Harbor mode enabled; no explicit column-level privacy transforms reported.")

    return {
        "privacy_mode": privacy_mode or "none",
        "typed_columns": typed_columns[:50],
        "privacy_transforms": privacy_transforms,
        "notes": notes,
    }


def _build_row_level_diffs(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    *,
    column_map: Dict[str, str],
    sample_size: int = 5,
) -> List[Dict[str, Any]]:
    if sample_size <= 0:
        return []

    raw_standardized = raw_df.rename(columns=column_map or {})
    comparable_columns = list(dict.fromkeys(list(raw_standardized.columns) + list(cleaned_df.columns)))
    scan_limit = min(500, max(len(raw_standardized), len(cleaned_df)))
    diffs: List[Dict[str, Any]] = []

    for row_index in range(scan_limit):
        before_row = raw_standardized.iloc[row_index] if row_index < len(raw_standardized) else None
        after_row = cleaned_df.iloc[row_index] if row_index < len(cleaned_df) else None

        row_changes: List[Dict[str, Any]] = []
        note: str | None = None

        if before_row is None and after_row is not None:
            note = "Row exists only in cleaned output."
        elif after_row is None and before_row is not None:
            note = "Row removed during cleaning."
        elif before_row is not None and after_row is not None:
            for col in comparable_columns:
                before_val = _to_jsonable(before_row[col]) if col in raw_standardized.columns else None
                after_val = _to_jsonable(after_row[col]) if col in cleaned_df.columns else None
                if before_val == after_val:
                    continue
                row_changes.append(
                    {
                        "column": str(col),
                        "before": before_val,
                        "after": after_val,
                    }
                )

        if row_changes or note:
            item: Dict[str, Any] = {"row_index": row_index}
            if row_changes:
                item["change_count"] = len(row_changes)
                item["changes"] = row_changes[:20]
            if note:
                item["note"] = note
            diffs.append(item)
        if len(diffs) >= sample_size:
            break

    while len(diffs) < sample_size:
        diffs.append(
            {
                "row_index": len(diffs),
                "note": "No row-level change detected in sampled comparison window.",
                "changes": [],
                "change_count": 0,
            }
        )

    return diffs[:sample_size]


def _build_change_summary(
    *,
    cleaning_report: Dict[str, Any],
    rows_removed: int,
    empty_columns_removed: List[str],
    streaming_mode: bool,
    raw_df: pd.DataFrame | None = None,
    cleaned_df: pd.DataFrame | None = None,
) -> Dict[str, Any]:
    column_renames = _build_column_rename_summary(cleaning_report)
    type_conversions = _build_conversion_summary(cleaning_report)
    normalizations = _build_normalization_summary(cleaning_report)
    columns_removed = [str(col) for col in (empty_columns_removed or [])]

    changed_columns = {str(col) for col in (cleaning_report.get("conversion") or {}).keys()}
    for item in column_renames.get("items", []):
        if isinstance(item, dict):
            to_value = item.get("to")
            if to_value:
                changed_columns.add(str(to_value))
    for transform in normalizations.get("privacy_transforms", []):
        if isinstance(transform, dict):
            for col in transform.get("columns", []):
                changed_columns.add(str(col))
    for col in normalizations.get("typed_columns", []):
        changed_columns.add(str(col))
    for col in columns_removed:
        changed_columns.add(col)

    summary: Dict[str, Any] = {
        "column_renames": column_renames,
        "type_conversions": type_conversions,
        "normalizations": normalizations,
        "rows_removed": int(rows_removed),
        "columns_removed": columns_removed,
        "cols_removed": len(columns_removed),
        "changed_fields_count": len(changed_columns),
    }

    if streaming_mode:
        summary["row_level_diffs"] = []
        summary["row_level_diffs_note"] = "Streaming mode enabled; row-level diffs unavailable."
        summary["note"] = "Row-level examples are unavailable in streaming mode."
        return summary

    if raw_df is None or cleaned_df is None:
        summary["row_level_diffs"] = []
        summary["row_level_diffs_note"] = "Row-level diff context unavailable."
        summary["note"] = "Row-level examples could not be generated for this run."
        return summary

    summary["row_level_diffs"] = _build_row_level_diffs(
        raw_df,
        cleaned_df,
        column_map=cleaning_report.get("column_map") or {},
        sample_size=5,
    )
    summary["note"] = "Sampled row-level examples shown below."
    return summary


def build_qc_report(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    cleaning_report: Dict[str, Any],
    *,
    warnings: Optional[List[str]] = None,
    streaming_mode: bool = False,
    issues: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    raw_rows = int(len(raw_df))
    cleaned_rows = int(len(cleaned_df))

    removed_rows = raw_rows - cleaned_rows

    conversion = cleaning_report.get("conversion", {})

    invalid_values = {
        col: details.get("invalid", 0) for col, details in conversion.items()
    }

    issue_list = issues or []
    change_summary = _build_change_summary(
        cleaning_report=cleaning_report,
        rows_removed=removed_rows,
        empty_columns_removed=cleaning_report.get("empty_columns_removed", []),
        streaming_mode=streaming_mode,
        raw_df=raw_df,
        cleaned_df=cleaned_df,
    )

    return {
        "row_count_raw": raw_rows,
        "row_count_cleaned": cleaned_rows,
        "rows_removed": removed_rows,
        "duplicate_rows_removed": cleaning_report.get("duplicate_rows_removed", 0),
        "empty_columns_removed": cleaning_report.get("empty_columns_removed", []),
        "missing_pct_raw": _missing_summary(raw_df),
        "missing_pct_cleaned": _missing_summary(cleaned_df),
        "invalid_values": invalid_values,
        "streaming_mode": streaming_mode,
        "warnings": warnings or [],
        "issues": issue_list,
        "severity_legend": _severity_legend(),
        "severity_summary": _severity_summary(issue_list),
        "change_summary": change_summary,
    }


def build_streaming_qc_report(
    *,
    row_count_raw: int,
    row_count_cleaned: int,
    missing_counts_raw: Dict[str, int],
    missing_counts_cleaned: Dict[str, int],
    invalid_values: Dict[str, int],
    empty_columns_removed: List[str],
    duplicate_rows_removed: int,
    cleaning_report: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    issues: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    def _pct(count: int, total: int) -> float:
        if total == 0:
            return 0.0
        return round(count / total * 100, 2)

    missing_pct_raw = {col: _pct(count, row_count_raw) for col, count in missing_counts_raw.items()}
    missing_pct_cleaned = {
        col: _pct(count, row_count_cleaned) for col, count in missing_counts_cleaned.items()
    }

    issue_list = issues or []
    report_source = cleaning_report or {}
    change_summary = _build_change_summary(
        cleaning_report=report_source,
        rows_removed=row_count_raw - row_count_cleaned,
        empty_columns_removed=empty_columns_removed,
        streaming_mode=True,
    )

    return {
        "row_count_raw": row_count_raw,
        "row_count_cleaned": row_count_cleaned,
        "rows_removed": row_count_raw - row_count_cleaned,
        "duplicate_rows_removed": duplicate_rows_removed,
        "empty_columns_removed": empty_columns_removed,
        "missing_pct_raw": missing_pct_raw,
        "missing_pct_cleaned": missing_pct_cleaned,
        "invalid_values": invalid_values,
        "streaming_mode": True,
        "warnings": warnings or [],
        "issues": issue_list,
        "severity_legend": _severity_legend(),
        "severity_summary": _severity_summary(issue_list),
        "change_summary": change_summary,
    }
