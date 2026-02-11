from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

import pandas as pd


RAG_CHECK_WEIGHTS = {
    "key_fields_presence": 0.20,
    "text_density": 0.15,
    "chunkable_text_quality": 0.15,
    "missingness_health": 0.15,
    "dedup_health": 0.10,
    "normalization_health": 0.10,
    "pii_safety": 0.10,
    "schema_clarity": 0.05,
}

STATUS_POINTS = {"pass": 1.0, "warn": 0.6, "fail": 0.2}

NON_TEXT_SEMANTICS = {
    "id",
    "claim_id",
    "member_id",
    "email",
    "phone",
    "ssn",
    "postal_code",
    "code",
    "date",
    "dob",
    "admit_date",
    "discharge_date",
    "encounter_date",
    "boolean",
}

PII_NAME_HINTS = {
    "name",
    "email",
    "phone",
    "mobile",
    "ssn",
    "social",
    "address",
    "city",
    "zip",
    "postal",
    "mrn",
    "member_id",
    "patient_id",
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b")
SSN_RE = re.compile(r"\b\d{3}-?\d{2}-?\d{4}\b")


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _status_from_metric(metric: float, pass_min: float, warn_min: float) -> str:
    if metric >= pass_min:
        return "pass"
    if metric >= warn_min:
        return "warn"
    return "fail"


def _severity_from_status(status: str) -> str:
    if status == "fail":
        return "high"
    if status == "warn":
        return "medium"
    return "low"


def _readiness_band(score: int) -> str:
    if score >= 80:
        return "ready"
    if score >= 60:
        return "partial"
    return "not_ready"


def _status_rank(status: str | None) -> int:
    if status == "pass":
        return 2
    if status == "warn":
        return 1
    return 0


def _status_delta(before: str | None, after: str | None) -> str:
    before_rank = _status_rank(before)
    after_rank = _status_rank(after)
    if after_rank > before_rank:
        return "improved"
    if after_rank < before_rank:
        return "regressed"
    return "unchanged"


def _to_float_or_none(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _format_threshold(pass_min: float, warn_min: float, label: str = "metric") -> str:
    return f"pass >= {pass_min:.2f}, warn >= {warn_min:.2f} ({label})"


def _build_check(
    *,
    check_id: str,
    label: str,
    metric: float,
    pass_min: float,
    warn_min: float,
    recommendation: str,
    metric_label: str = "ratio",
) -> Dict[str, Any]:
    status = _status_from_metric(metric, pass_min, warn_min)
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "metric": round(_clamp(metric), 3),
        "threshold": _format_threshold(pass_min, warn_min, metric_label),
        "severity": _severity_from_status(status),
        "recommendation": recommendation,
    }


def _finalize_checks(checks: List[Dict[str, Any]], *, sampled_note: str | None = None) -> Dict[str, Any]:
    weighted = 0.0
    for check in checks:
        weight = RAG_CHECK_WEIGHTS.get(check["id"], 0.0)
        check["weight"] = weight
        weighted += STATUS_POINTS.get(check["status"], 0.2) * weight

    score = int(round(_clamp(weighted) * 100))
    band = _readiness_band(score)
    fail_count = sum(1 for check in checks if check["status"] == "fail")
    warn_count = sum(1 for check in checks if check["status"] == "warn")

    summary = (
        f"{fail_count} failed checks, {warn_count} warning checks. "
        "Address failed checks first to improve retrieval quality."
    )
    result = {"score": score, "band": band, "checks": checks, "summary": summary}
    if sampled_note:
        result["sampled_note"] = sampled_note
    return result


def _has_token_match(items: Iterable[str], tokens: Iterable[str]) -> bool:
    lowered = [item.lower() for item in items]
    for token in tokens:
        if any(token in value for value in lowered):
            return True
    return False


def _key_field_metric_profile(profile: Dict[str, Any]) -> float:
    columns = profile.get("columns", []) or []
    clean_names = [str(col.get("clean_name") or "") for col in columns]
    semantics = [str(col.get("semantic_hint") or "") for col in columns]
    domain_tags = [
        tag
        for col in columns
        for tag in (col.get("domain_tags") or [])
    ]

    has_id = _has_token_match(semantics + clean_names, {"id", "claim_id", "member_id", "_id"})
    has_date = _has_token_match(
        semantics + clean_names,
        {"date", "dob", "admit_date", "discharge_date", "encounter_date"},
    )
    has_domain_anchor = bool(domain_tags) or _has_token_match(
        semantics + clean_names, {"code", "loinc", "icd", "cpt", "ndc", "rxnorm"}
    )

    return (int(has_id) + int(has_date) + int(has_domain_anchor)) / 3


def build_rag_readiness_from_profile(
    profile: Dict[str, Any],
    *,
    privacy_mode: str = "none",
) -> Dict[str, Any]:
    columns = profile.get("columns", []) or []
    summary = profile.get("summary", {}) or {}
    column_count = int(profile.get("column_count") or len(columns) or 0)
    sampled = bool(profile.get("sampled"))

    high_missing_count = len(summary.get("columns_high_missing", []) or [])
    low_variance_count = len(summary.get("low_variance_columns", []) or [])
    pii_count = len(summary.get("columns_with_pii", []) or [])

    text_candidates = [
        col
        for col in columns
        if (col.get("primitive_type") == "string")
        and (col.get("semantic_hint") not in NON_TEXT_SEMANTICS)
    ]
    text_qualified = 0
    chunkable_qualified = 0
    for col in text_candidates:
        missing_pct = float(col.get("missing_pct") or 0.0)
        distinct_count = int(col.get("distinct_count") or 0)
        examples = [str(value) for value in (col.get("example_values") or [])]
        avg_len = sum(len(value) for value in examples) / len(examples) if examples else 0.0
        if missing_pct < 60 and distinct_count > 5 and avg_len >= 15:
            text_qualified += 1
        if 40 <= avg_len <= 450 and missing_pct < 65:
            chunkable_qualified += 1

    text_density_metric = text_qualified / len(text_candidates) if text_candidates else 0.0
    chunkable_metric = chunkable_qualified / len(text_candidates) if text_candidates else 0.0

    missingness_metric = 1.0 - (high_missing_count / max(1, column_count))
    dedup_metric = 1.0 - (low_variance_count / max(1, column_count))

    normalization_targets = [
        col
        for col in columns
        if (col.get("semantic_hint") in {"code", "phone", "postal_code", "gender", "date", "dob"})
        or bool(col.get("domain_tags"))
    ]
    normalization_good = [
        col for col in normalization_targets if (col.get("primitive_type") or "unknown") != "unknown"
    ]
    if normalization_targets:
        normalization_metric = len(normalization_good) / len(normalization_targets)
    else:
        normalization_metric = 1.0

    pii_ratio = pii_count / max(1, column_count)
    if privacy_mode == "safe_harbor":
        pii_metric = _clamp(1.0 - (pii_ratio * 0.25))
    else:
        pii_metric = _clamp(1.0 - (pii_ratio * 1.2))

    known_schema = sum(
        1 for col in columns if (col.get("primitive_type") or "unknown") != "unknown"
    )
    schema_metric = known_schema / max(1, column_count)

    checks = [
        _build_check(
            check_id="key_fields_presence",
            label="Key Fields Present",
            metric=_key_field_metric_profile(profile),
            pass_min=0.67,
            warn_min=0.34,
            recommendation="Include stable IDs, date fields, and at least one domain code anchor.",
        ),
        _build_check(
            check_id="text_density",
            label="Text Density",
            metric=text_density_metric,
            pass_min=0.45,
            warn_min=0.25,
            recommendation="Add/retain descriptive text columns with low missingness and enough variation.",
        ),
        _build_check(
            check_id="chunkable_text_quality",
            label="Chunkable Text Quality",
            metric=chunkable_metric,
            pass_min=0.35,
            warn_min=0.20,
            recommendation="Normalize text fields to sentence-length content suitable for chunking.",
        ),
        _build_check(
            check_id="missingness_health",
            label="Missingness Health",
            metric=missingness_metric,
            pass_min=0.80,
            warn_min=0.60,
            recommendation="Reduce high-missing columns or impute where clinically safe.",
        ),
        _build_check(
            check_id="dedup_health",
            label="Duplicate/Variance Health",
            metric=dedup_metric,
            pass_min=0.85,
            warn_min=0.70,
            recommendation="Remove duplicate rows and low-variance columns that add retrieval noise.",
        ),
        _build_check(
            check_id="normalization_health",
            label="Normalization Health",
            metric=normalization_metric,
            pass_min=0.75,
            warn_min=0.50,
            recommendation="Normalize code/date/phone/postal fields before embedding.",
        ),
        _build_check(
            check_id="pii_safety",
            label="PII Safety",
            metric=pii_metric,
            pass_min=0.80,
            warn_min=0.60,
            recommendation="Use Safe Harbor for external sharing or model-training exports.",
        ),
        _build_check(
            check_id="schema_clarity",
            label="Schema Clarity",
            metric=schema_metric,
            pass_min=0.85,
            warn_min=0.65,
            recommendation="Resolve unknown column types and ambiguous semantic hints.",
        ),
    ]

    sampled_note = None
    if sampled:
        sampled_note = (
            f"RAG readiness is estimated from sampled rows ({int(profile.get('sampled_rows') or 0)})."
        )
    return _finalize_checks(checks, sampled_note=sampled_note)


def build_rag_readiness_comparison(
    before: Dict[str, Any] | None,
    after: Dict[str, Any] | None,
) -> Dict[str, Any] | None:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return None

    before_checks = {str(check.get("id")): check for check in (before.get("checks") or []) if check.get("id")}
    after_check_list = [check for check in (after.get("checks") or []) if check.get("id")]
    after_checks = {str(check.get("id")): check for check in after_check_list}

    ordered_ids: List[str] = [str(check.get("id")) for check in after_check_list]
    for check_id in before_checks:
        if check_id not in after_checks:
            ordered_ids.append(check_id)

    check_deltas: List[Dict[str, Any]] = []
    for check_id in ordered_ids:
        before_check = before_checks.get(check_id, {})
        after_check = after_checks.get(check_id, {})
        status_before = (before_check.get("status") or "fail").lower()
        status_after = (after_check.get("status") or "fail").lower()
        delta = _status_delta(status_before, status_after)

        metric_before = _to_float_or_none(before_check.get("metric"))
        metric_after = _to_float_or_none(after_check.get("metric"))
        metric_delta = None
        if metric_before is not None and metric_after is not None:
            metric_delta = round(metric_after - metric_before, 3)

        if delta == "regressed" or status_after == "fail":
            priority = "high"
        elif status_after == "warn":
            priority = "medium"
        else:
            priority = "low"

        check_deltas.append(
            {
                "id": check_id,
                "label": after_check.get("label") or before_check.get("label") or check_id,
                "status_before": status_before,
                "status_after": status_after,
                "status_delta": delta,
                "metric_before": round(metric_before, 3) if metric_before is not None else None,
                "metric_after": round(metric_after, 3) if metric_after is not None else None,
                "metric_delta": metric_delta,
                "priority": priority,
                "recommended_action": after_check.get("recommendation")
                or before_check.get("recommendation")
                or "",
            }
        )

    priority_rank = {"high": 0, "medium": 1, "low": 2}

    def _action_reason(item: Dict[str, Any]) -> str:
        if item.get("status_delta") == "regressed":
            return "Regressed after cleaning"
        if item.get("status_after") == "fail":
            return "Still fail after cleaning"
        return "Still warn after cleaning"

    priority_actions = []
    for item in sorted(
        (delta for delta in check_deltas if delta.get("priority") in {"high", "medium"}),
        key=lambda delta: (
            priority_rank.get(str(delta.get("priority")), 2),
            0 if delta.get("status_delta") == "regressed" else 1,
            _status_rank(str(delta.get("status_after"))),
            float(delta.get("metric_after")) if isinstance(delta.get("metric_after"), (int, float)) else 1.0,
        ),
    ):
        priority_actions.append(
            {
                "check_id": item["id"],
                "label": item["label"],
                "priority": item["priority"],
                "reason": _action_reason(item),
                "action": item.get("recommended_action") or "",
            }
        )

    improved = sum(1 for item in check_deltas if item["status_delta"] == "improved")
    regressed = sum(1 for item in check_deltas if item["status_delta"] == "regressed")
    unchanged = sum(1 for item in check_deltas if item["status_delta"] == "unchanged")
    high_priority_count = sum(1 for item in priority_actions if item["priority"] == "high")

    score_before = int(before.get("score") or 0)
    score_after = int(after.get("score") or 0)
    score_delta = int(score_after - score_before)

    return {
        "score_before": score_before,
        "score_after": score_after,
        "score_delta": score_delta,
        "band_before": before.get("band") or _readiness_band(score_before),
        "band_after": after.get("band") or _readiness_band(score_after),
        "check_deltas": check_deltas,
        "priority_actions": priority_actions,
        "summary": (
            f"{improved}/{max(1, len(check_deltas))} checks improved, "
            f"{regressed} regressed, {unchanged} unchanged. "
            f"Address {high_priority_count} high-priority actions first."
        ),
    }


def _is_string_series(series: pd.Series) -> bool:
    return pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series)


def _estimate_pii_leak_ratio(df: pd.DataFrame) -> float:
    if df.empty or df.shape[1] == 0:
        return 0.0

    row_count = max(1, len(df))
    pii_hits = 0

    for column in df.columns:
        col_name = str(column).lower()
        if any(token in col_name for token in PII_NAME_HINTS):
            pii_hits += row_count
            continue

        series = df[column]
        if not _is_string_series(series):
            continue
        sample = series.dropna().astype(str).head(500)
        if sample.empty:
            continue
        pii_hits += int(sample.map(lambda v: bool(EMAIL_RE.search(v))).sum())
        pii_hits += int(sample.map(lambda v: bool(PHONE_RE.search(v))).sum())
        pii_hits += int(sample.map(lambda v: bool(SSN_RE.search(v))).sum())

    max_possible = row_count * max(1, len(df.columns))
    return _clamp(pii_hits / max_possible)


def _key_field_metric_dataframe(df: pd.DataFrame) -> float:
    names = [str(col).lower() for col in df.columns]
    has_id = any(name.endswith("_id") or "member_id" in name or "patient_id" in name for name in names)
    has_date = any("date" in name or name == "dob" or "dob" in name for name in names)
    has_domain_anchor = any(
        token in name for name in names for token in {"icd", "cpt", "loinc", "ndc", "rxnorm", "code"}
    )
    return (int(has_id) + int(has_date) + int(has_domain_anchor)) / 3


def build_rag_readiness_from_dataframe(
    cleaned_df: pd.DataFrame,
    qc_report: Dict[str, Any],
    *,
    privacy_mode: str = "none",
    sampled: bool = False,
    baseline_score: int | None = None,
) -> Dict[str, Any]:
    row_count = int(len(cleaned_df))
    column_count = int(len(cleaned_df.columns))
    column_names = [str(col).lower() for col in cleaned_df.columns]

    text_columns = [
        col
        for col in cleaned_df.columns
        if _is_string_series(cleaned_df[col])
        and not any(
            token in str(col).lower()
            for token in {"_id", "id", "date", "dob", "icd", "cpt", "loinc", "ndc", "rxnorm", "zip", "phone"}
        )
    ]

    text_density_hits = 0
    chunkable_hits = 0
    for col in text_columns:
        series = cleaned_df[col]
        non_null = series.dropna().astype(str)
        if non_null.empty:
            continue
        non_empty_ratio = float((non_null.str.strip() != "").mean())
        avg_len = float(non_null.str.len().mean())
        if non_empty_ratio >= 0.70:
            text_density_hits += 1
        if 40 <= avg_len <= 500 and non_empty_ratio >= 0.50:
            chunkable_hits += 1

    text_density_metric = text_density_hits / len(text_columns) if text_columns else 0.0
    chunkable_metric = chunkable_hits / len(text_columns) if text_columns else 0.0

    missing_pct_map = qc_report.get("missing_pct_cleaned", {}) or {}
    avg_missing = (
        sum(float(value) for value in missing_pct_map.values()) / len(missing_pct_map)
        if missing_pct_map
        else 0.0
    )
    missingness_metric = _clamp(1.0 - (avg_missing / 100.0))

    duplicate_removed = int(qc_report.get("duplicate_rows_removed") or 0)
    raw_rows = int(qc_report.get("row_count_raw") or row_count)
    dedup_metric = _clamp(1.0 - (duplicate_removed / max(1, raw_rows)))

    invalid_values = qc_report.get("invalid_values", {}) or {}
    invalid_total = sum(int(value) for value in invalid_values.values())
    invalid_rate = invalid_total / max(1, row_count * max(1, len(invalid_values)))
    normalization_metric = _clamp(1.0 - invalid_rate)

    pii_leak_ratio = _estimate_pii_leak_ratio(cleaned_df)
    if privacy_mode == "safe_harbor":
        pii_metric = _clamp(1.0 - (pii_leak_ratio * 0.5))
    else:
        pii_metric = _clamp(1.0 - (pii_leak_ratio * 1.4))

    snake_case_count = sum(
        1 for name in column_names if re.match(r"^[a-z0-9_]+$", name)
    )
    schema_metric = snake_case_count / max(1, column_count)

    checks = [
        _build_check(
            check_id="key_fields_presence",
            label="Key Fields Present",
            metric=_key_field_metric_dataframe(cleaned_df),
            pass_min=0.67,
            warn_min=0.34,
            recommendation="Preserve stable IDs, date anchors, and domain-code columns in cleaned output.",
        ),
        _build_check(
            check_id="text_density",
            label="Text Density",
            metric=text_density_metric,
            pass_min=0.45,
            warn_min=0.25,
            recommendation="Retain meaningful narrative/text fields for retrieval context.",
        ),
        _build_check(
            check_id="chunkable_text_quality",
            label="Chunkable Text Quality",
            metric=chunkable_metric,
            pass_min=0.35,
            warn_min=0.20,
            recommendation="Increase sentence-level content and reduce terse/free-form noise fields.",
        ),
        _build_check(
            check_id="missingness_health",
            label="Missingness Health",
            metric=missingness_metric,
            pass_min=0.80,
            warn_min=0.60,
            recommendation="Lower missingness in key retrieval columns before embedding.",
        ),
        _build_check(
            check_id="dedup_health",
            label="Duplicate/Variance Health",
            metric=dedup_metric,
            pass_min=0.90,
            warn_min=0.75,
            recommendation="Reduce duplicate records and repeated low-information rows.",
        ),
        _build_check(
            check_id="normalization_health",
            label="Normalization Health",
            metric=normalization_metric,
            pass_min=0.90,
            warn_min=0.75,
            recommendation="Fix remaining invalid code/date/typed values flagged in QC.",
        ),
        _build_check(
            check_id="pii_safety",
            label="PII Safety",
            metric=pii_metric,
            pass_min=0.85,
            warn_min=0.65,
            recommendation="Mask direct identifiers before using data in external LLM/RAG systems.",
        ),
        _build_check(
            check_id="schema_clarity",
            label="Schema Clarity",
            metric=schema_metric,
            pass_min=0.90,
            warn_min=0.75,
            recommendation="Use stable snake_case schema names for reproducible chunk pipelines.",
        ),
    ]

    sampled_note = None
    if sampled:
        sampled_note = f"RAG readiness estimated from sampled cleaned rows ({row_count})."
    readiness = _finalize_checks(checks, sampled_note=sampled_note)
    if baseline_score is not None:
        readiness["delta_from_profile_score"] = int(readiness["score"] - baseline_score)
    return readiness
