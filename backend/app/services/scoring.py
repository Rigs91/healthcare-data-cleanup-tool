from __future__ import annotations

from typing import Any, Dict, List


ASSESSMENT_WEIGHTS = {
    "high_missing_columns_pct": 0.40,
    "low_variance_columns_pct": 0.20,
    "pii_suspected_columns_pct": 0.25,
    "schema_uncertainty_pct": 0.15,
}


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return max(0.0, min(1.0, numerator / denominator))


def _score_band(score: int) -> str:
    if score >= 85:
        return "excellent"
    if score >= 70:
        return "good"
    if score >= 50:
        return "fair"
    return "poor"


def build_assessment_breakdown(profile: Dict[str, Any]) -> Dict[str, Any]:
    columns = profile.get("columns", []) or []
    summary = profile.get("summary", {}) or {}
    column_count = int(profile.get("column_count") or len(columns) or 0)

    high_missing_count = len(summary.get("columns_high_missing", []) or [])
    low_variance_count = len(summary.get("low_variance_columns", []) or [])
    pii_count = len(summary.get("columns_with_pii", []) or [])
    unknown_schema_count = sum(
        1 for col in columns if (col.get("primitive_type") or "unknown") == "unknown"
    )

    ratio_map = {
        "high_missing_columns_pct": _safe_ratio(high_missing_count, column_count),
        "low_variance_columns_pct": _safe_ratio(low_variance_count, column_count),
        "pii_suspected_columns_pct": _safe_ratio(pii_count, column_count),
        "schema_uncertainty_pct": _safe_ratio(unknown_schema_count, column_count),
    }

    factor_labels = {
        "high_missing_columns_pct": "High missing columns",
        "low_variance_columns_pct": "Low variance columns",
        "pii_suspected_columns_pct": "Potential PII columns",
        "schema_uncertainty_pct": "Schema uncertainty",
    }

    factors: List[Dict[str, Any]] = []
    total_penalty = 0.0
    for factor_name, ratio in ratio_map.items():
        weight = ASSESSMENT_WEIGHTS[factor_name]
        penalty = ratio * 100 * weight
        total_penalty += penalty
        factors.append(
            {
                "id": factor_name,
                "name": factor_labels[factor_name],
                "value_pct": round(ratio * 100, 2),
                "weight": weight,
                "penalty": round(penalty, 2),
            }
        )

    score = int(round(max(0.0, min(100.0, 100.0 - total_penalty))))
    assessment = {
        "score": score,
        "band": _score_band(score),
        "factors": factors,
        "definitions": {
            "high_missing_columns_pct": "Columns with >40% missing values.",
            "low_variance_columns_pct": "Columns with <=1 distinct non-null value.",
            "pii_suspected_columns_pct": "Columns flagged by PII name/value heuristics.",
            "schema_uncertainty_pct": "Columns inferred as unknown primitive type.",
            "bands": {
                "excellent": "85-100",
                "good": "70-84",
                "fair": "50-69",
                "poor": "0-49",
            },
        },
    }

    if profile.get("sampled"):
        sampled_rows = int(profile.get("sampled_rows") or 0)
        assessment["sampled_note"] = (
            f"Assessment is based on a sampled subset ({sampled_rows} rows)."
        )

    return assessment
