from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd

ICD10_RE = re.compile(r"^[A-TV-Z][0-9][A-Z0-9](?:\.?[A-Z0-9]{0,4})?$")
CPT_RE = re.compile(r"^[0-9]{5}$")
HCPCS_RE = re.compile(r"^[A-Z][0-9]{4}$")
LOINC_RE = re.compile(r"^\d{1,5}-\d$")
NDC_RE = re.compile(r"^\d{10,11}$")
RXNORM_RE = re.compile(r"^\d{1,8}$")
NPI_RE = re.compile(r"^\d{10}$")

ISSUE_IMPACT_WEIGHTS = {
    "dob_future": 3.0,
    "date_future": 2.0,
    "invalid_icd10": 2.0,
    "invalid_cpt": 2.0,
    "invalid_hcpcs": 2.0,
    "invalid_loinc": 2.0,
    "invalid_ndc": 2.0,
    "invalid_rxnorm": 2.0,
    "invalid_npi": 2.0,
    "negative_amount": 1.0,
}

HARD_HIGH_ISSUES = {"dob_future"}


def _count_invalid(series: pd.Series, pattern: re.Pattern) -> int:
    values = series.dropna().astype(str)
    if values.empty:
        return 0
    return int((~values.str.match(pattern)).sum())


def _date_out_of_range(series: pd.Series, *, max_future_days: int = 1) -> int:
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    if parsed.empty:
        return 0
    today = pd.Timestamp.now(tz="UTC").normalize()
    return int((parsed > today + pd.Timedelta(days=max_future_days)).sum())


def _dob_in_future(series: pd.Series) -> int:
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    if parsed.empty:
        return 0
    today = pd.Timestamp.now(tz="UTC").normalize()
    return int((parsed > today).sum())


def _negative_values(series: pd.Series) -> int:
    numeric = pd.to_numeric(series, errors="coerce")
    return int((numeric < 0).sum())


def _severity_for(issue_type: str, count: int, total_rows: int) -> tuple[str, float, float, str]:
    impact_weight = ISSUE_IMPACT_WEIGHTS.get(issue_type, 1.0)
    rate_pct = (count / max(1, total_rows)) * 100.0
    severity_score = rate_pct * impact_weight

    if issue_type in HARD_HIGH_ISSUES and count > 0:
        return (
            "high",
            round(rate_pct, 3),
            round(severity_score, 3),
            "Critical rule triggered for this issue type.",
        )
    if severity_score >= 15:
        return (
            "high",
            round(rate_pct, 3),
            round(severity_score, 3),
            "High severity because (rate_pct * impact_weight) >= 15.",
        )
    if severity_score >= 5:
        return (
            "medium",
            round(rate_pct, 3),
            round(severity_score, 3),
            "Medium severity because 5 <= (rate_pct * impact_weight) < 15.",
        )
    return (
        "low",
        round(rate_pct, 3),
        round(severity_score, 3),
        "Low severity because (rate_pct * impact_weight) < 5.",
    )


def _build_issue(
    *,
    issue_type: str,
    column: str,
    message: str,
    count: int,
    total_rows: int,
) -> Dict[str, Any]:
    severity, rate_pct, severity_score, severity_reason = _severity_for(
        issue_type, count, total_rows
    )
    return {
        "issue_type": issue_type,
        "severity": severity,
        "column": column,
        "message": message,
        "count": count,
        "affected_rows": count,
        "rate_pct": rate_pct,
        "impact_weight": ISSUE_IMPACT_WEIGHTS.get(issue_type, 1.0),
        "severity_score": severity_score,
        "severity_reason": severity_reason,
    }


def build_validation_issues(
    df: pd.DataFrame,
    column_metadata: Dict[str, Dict[str, str]],
    *,
    total_rows: int | None = None,
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    dataset_rows = int(total_rows if total_rows is not None else len(df))

    for col in df.columns:
        meta = column_metadata.get(col, {})
        semantic = meta.get("semantic_hint")
        lower = col.lower()

        if semantic in {"dob"} or "dob" in lower or "birth" in lower:
            count = _dob_in_future(df[col])
            if count:
                issues.append(
                    _build_issue(
                        issue_type="dob_future",
                        column=col,
                        message="DOB values in the future",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if semantic in {"encounter_date", "admit_date", "discharge_date", "date"} or "date" in lower:
            count = _date_out_of_range(df[col])
            if count:
                issues.append(
                    _build_issue(
                        issue_type="date_future",
                        column=col,
                        message="Dates in the future",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if "icd" in lower:
            count = _count_invalid(df[col], ICD10_RE)
            if count:
                issues.append(
                    _build_issue(
                        issue_type="invalid_icd10",
                        column=col,
                        message="Invalid ICD-10 format",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if "cpt" in lower:
            count = _count_invalid(df[col], CPT_RE)
            if count:
                issues.append(
                    _build_issue(
                        issue_type="invalid_cpt",
                        column=col,
                        message="Invalid CPT format",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if "hcpcs" in lower:
            count = _count_invalid(df[col], HCPCS_RE)
            if count:
                issues.append(
                    _build_issue(
                        issue_type="invalid_hcpcs",
                        column=col,
                        message="Invalid HCPCS format",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if "loinc" in lower:
            count = _count_invalid(df[col], LOINC_RE)
            if count:
                issues.append(
                    _build_issue(
                        issue_type="invalid_loinc",
                        column=col,
                        message="Invalid LOINC format",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if "ndc" in lower:
            count = _count_invalid(df[col], NDC_RE)
            if count:
                issues.append(
                    _build_issue(
                        issue_type="invalid_ndc",
                        column=col,
                        message="Invalid NDC format",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if "rxnorm" in lower:
            count = _count_invalid(df[col], RXNORM_RE)
            if count:
                issues.append(
                    _build_issue(
                        issue_type="invalid_rxnorm",
                        column=col,
                        message="Invalid RxNorm format",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if "npi" in lower:
            count = _count_invalid(df[col], NPI_RE)
            if count:
                issues.append(
                    _build_issue(
                        issue_type="invalid_npi",
                        column=col,
                        message="Invalid NPI format",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

        if any(token in lower for token in ["charge", "amount", "paid", "allowed", "billed"]):
            count = _negative_values(df[col])
            if count:
                issues.append(
                    _build_issue(
                        issue_type="negative_amount",
                        column=col,
                        message="Negative monetary values",
                        count=count,
                        total_rows=dataset_rows,
                    )
                )

    return issues
