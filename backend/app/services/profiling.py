from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pandas as pd

from app.services.rag_readiness import build_rag_readiness_from_profile
from app.services.scoring import build_assessment_breakdown
from app.utils.text import collapse_whitespace


SEMANTIC_HINTS = {
    "dob": ["dob", "birth"],
    "admit_date": ["admit", "admission"],
    "discharge_date": ["discharge"],
    "encounter_date": ["encounter", "visit_date", "service_date"],
    "date": ["date", "time", "timestamp"],
    "id": ["id", "uuid", "mrn", "patient", "encounter", "visit", "claim", "member"],
    "claim_id": ["claim_id", "claim number", "claim no"],
    "member_id": ["member_id", "subscriber", "policy"],
    "code": ["code", "icd", "cpt", "ndc", "loinc", "drg", "hcpcs", "rxnorm"],
    "email": ["email", "e-mail"],
    "phone": ["phone", "mobile", "tel", "cell"],
    "ssn": ["ssn", "social"],
    "postal_code": ["zip", "postal"],
    "state": ["state", "province"],
    "city": ["city", "town"],
    "address": ["address", "street", "addr"],
    "gender": ["gender", "sex"],
    "race": ["race", "ethnicity"],
    "language": ["language", "lang"],
    "name": ["name", "first", "last", "middle"],
    "provider": ["provider", "npi", "physician", "doctor"],
    "facility": ["facility", "hospital", "clinic", "site"],
    "payer": ["payer", "plan", "insur"],
    "medication": ["drug", "med", "medication", "ndc", "rxnorm"],
    "lab": ["lab", "loinc", "test", "result"],
    "vital": ["bp", "heart", "pulse", "weight", "height", "temp"],
    "boolean": ["flag", "is_", "has_"],
}

DOMAIN_PATTERNS = {
    "ehr": [
        "mrn",
        "patient",
        "encounter",
        "visit",
        "vital",
        "bp",
        "height",
        "weight",
        "diagnosis",
        "problem",
        "allergy",
        "provider",
        "admit",
        "discharge",
    ],
    "claims": [
        "claim",
        "payer",
        "plan",
        "subscriber",
        "member",
        "cpt",
        "hcpcs",
        "drg",
        "allowed",
        "paid",
        "billed",
        "service_date",
    ],
    "labs": ["loinc", "lab", "specimen", "result", "unit", "reference"],
    "pharmacy": ["ndc", "rx", "drug", "medication", "days_supply", "quantity", "refill"],
}

MISSING_VALUES = {
    "",
    "na",
    "n/a",
    "null",
    "none",
    "unknown",
    "unk",
    "?",
    "-",
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b")
SSN_RE = re.compile(r"\b\d{3}-?\d{2}-?\d{4}\b")
ZIP_RE = re.compile(r"^\d{5}(-\d{4})?$")


def _name_to_hint(column_name: str) -> Optional[str]:
    lowered = column_name.lower()
    for hint, tokens in SEMANTIC_HINTS.items():
        if any(token in lowered for token in tokens):
            return hint
    return None


def _domain_scores(column_names: List[str]) -> Dict[str, int]:
    scores: Dict[str, int] = {domain: 0 for domain in DOMAIN_PATTERNS}
    lowered = [name.lower() for name in column_names]
    for domain, tokens in DOMAIN_PATTERNS.items():
        for token in tokens:
            scores[domain] += sum(1 for name in lowered if token in name)
    return scores


def _detected_domains(column_names: List[str]) -> List[Dict[str, Any]]:
    lowered = [name.lower() for name in column_names]
    total_columns = max(1, len(column_names))
    detected: List[Dict[str, Any]] = []

    for domain, tokens in DOMAIN_PATTERNS.items():
        score = 0
        matched_tokens: set[str] = set()
        evidence_columns: List[str] = []

        for idx, name in enumerate(lowered):
            token_hits = [token for token in tokens if token in name]
            if not token_hits:
                continue
            score += len(token_hits)
            matched_tokens.update(token_hits)
            evidence_name = column_names[idx]
            if evidence_name not in evidence_columns:
                evidence_columns.append(evidence_name)

        if score <= 0:
            continue

        token_coverage = len(matched_tokens) / max(1, len(tokens))
        column_coverage = len(evidence_columns) / total_columns
        confidence = max(0.0, min(1.0, (token_coverage * 0.7) + (column_coverage * 0.3)))

        detected.append(
            {
                "domain": domain,
                "score": int(score),
                "confidence": round(confidence, 3),
                "evidence_columns": evidence_columns[:12],
            }
        )

    return sorted(
        detected,
        key=lambda item: (
            int(item.get("score") or 0),
            float(item.get("confidence") or 0.0),
        ),
        reverse=True,
    )


def _domain_suggestions(column_names: List[str]) -> List[Dict[str, Any]]:
    return [
        {"domain": item["domain"], "score": item["score"]}
        for item in _detected_domains(column_names)
    ]


def _sample_values(series: pd.Series, limit: int = 5) -> List[str]:
    values = []
    for value in series.dropna().head(50).tolist():
        if value is None:
            continue
        text = collapse_whitespace(str(value))
        if text and text not in values:
            values.append(text)
        if len(values) >= limit:
            break
    return values


def _normalize_missing(series: pd.Series) -> pd.Series:
    def _clean(value: Any) -> Any:
        if value is None:
            return pd.NA
        text = str(value).strip()
        if text.lower() in MISSING_VALUES:
            return pd.NA
        return value

    return series.map(_clean)


def _infer_primitive_type(series: pd.Series) -> str:
    sample = series.dropna().head(50)
    if sample.empty:
        return "unknown"

    numeric_count = 0
    date_count = 0
    bool_count = 0

    for value in sample.tolist():
        text = str(value).strip()
        if text.lower() in {"true", "false", "yes", "no", "y", "n", "0", "1"}:
            bool_count += 1
        if any(char.isdigit() for char in text):
            try:
                float(text.replace(",", "").replace("$", ""))
                numeric_count += 1
            except ValueError:
                pass
        try:
            parsed = pd.to_datetime(text, errors="raise")
            if parsed is not pd.NaT:
                date_count += 1
        except Exception:
            pass

    if date_count / len(sample) > 0.6:
        return "date"
    if numeric_count / len(sample) > 0.6:
        return "number"
    if bool_count / len(sample) > 0.6:
        return "boolean"
    return "string"


def _infer_semantic(series: pd.Series, clean_name: str) -> Optional[str]:
    name_hint = _name_to_hint(clean_name)
    if name_hint:
        return name_hint

    sample = series.dropna().head(50).astype(str)
    if sample.empty:
        return None

    if sample.map(lambda v: bool(EMAIL_RE.search(v))).mean() > 0.4:
        return "email"
    if sample.map(lambda v: bool(PHONE_RE.search(v))).mean() > 0.4:
        return "phone"
    if sample.map(lambda v: bool(SSN_RE.search(v))).mean() > 0.4:
        return "ssn"
    if sample.map(lambda v: bool(ZIP_RE.match(v.strip()))).mean() > 0.5:
        return "postal_code"

    return None


def _pii_signal_counts(series: pd.Series) -> Dict[str, int]:
    sample = series.dropna().astype(str)
    return {
        "email": int(sample.map(lambda v: bool(EMAIL_RE.search(v))).sum()),
        "phone": int(sample.map(lambda v: bool(PHONE_RE.search(v))).sum()),
        "ssn": int(sample.map(lambda v: bool(SSN_RE.search(v))).sum()),
    }


def _top_values(series: pd.Series, limit: int = 3) -> Dict[str, int]:
    counts = series.dropna().astype(str).value_counts().head(limit)
    return {value: int(count) for value, count in counts.items()}


def _numeric_stats(series: pd.Series) -> Optional[Dict[str, float]]:
    numeric = pd.to_numeric(series, errors="coerce")
    numeric = numeric.dropna()
    if numeric.empty:
        return None
    return {
        "min": float(numeric.min()),
        "max": float(numeric.max()),
        "mean": float(numeric.mean()),
    }


def _date_stats(series: pd.Series) -> Optional[Dict[str, str]]:
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    parsed = parsed.dropna()
    if parsed.empty:
        return None
    return {
        "min": parsed.min().date().isoformat(),
        "max": parsed.max().date().isoformat(),
    }


def _domain_tags(clean_name: str) -> List[str]:
    tags = []
    lower = clean_name.lower()
    for tag in ["loinc", "icd", "cpt", "hcpcs", "ndc", "rxnorm", "drg"]:
        if tag in lower:
            tags.append(tag)
    return tags


def _preclean_decision(profile_payload: Dict[str, Any], *, privacy_mode: str) -> Dict[str, Any]:
    assessment = profile_payload.get("assessment") or {}
    rag = profile_payload.get("rag_readiness") or {}
    summary = profile_payload.get("summary") or {}
    column_count = int(profile_payload.get("column_count") or 0)

    assessment_score = int(assessment.get("score") or 0)
    rag_score = int(rag.get("score") or 0)
    high_missing_count = len(summary.get("columns_high_missing", []) or [])
    pii_count = len(summary.get("columns_with_pii", []) or [])
    high_missing_ratio = high_missing_count / max(1, column_count)
    pii_ratio = pii_count / max(1, column_count)

    schema_uncertainty_pct = 0.0
    for factor in assessment.get("factors", []) or []:
        if factor.get("id") == "schema_uncertainty_pct":
            schema_uncertainty_pct = float(factor.get("value_pct") or 0.0)
            break

    severity = 0
    reasons: List[str] = []
    actions: List[str] = []

    def _escalate(level: int, reason: str, action: str) -> None:
        nonlocal severity
        severity = max(severity, level)
        reasons.append(reason)
        actions.append(action)

    if assessment_score < 50:
        _escalate(
            2,
            f"Assessment score is poor ({assessment_score}).",
            "Resolve high-penalty assessment factors before running production cleaning.",
        )
    elif assessment_score < 70:
        _escalate(
            1,
            f"Assessment score is below preferred threshold ({assessment_score}).",
            "Review missingness, variance, and schema factors before cleaning.",
        )

    if high_missing_ratio >= 0.35:
        _escalate(
            2,
            f"High missingness affects {high_missing_count}/{max(1, column_count)} columns.",
            "Add source-side completeness checks or targeted imputation before cleaning.",
        )
    elif high_missing_ratio >= 0.15:
        _escalate(
            1,
            f"Elevated missingness detected in {high_missing_count} columns.",
            "Plan imputation or column-level handling for high-missing fields.",
        )

    if pii_count > 0 and privacy_mode != "safe_harbor" and pii_ratio >= 0.3:
        _escalate(
            2,
            f"Potential PII appears in {pii_count} columns without Safe Harbor mode.",
            "Switch to safe_harbor privacy mode before external sharing or model training.",
        )
    elif pii_count > 0:
        _escalate(
            1,
            f"Potential PII detected in {pii_count} columns.",
            "Confirm de-identification settings and validate privacy transformations.",
        )

    if schema_uncertainty_pct >= 35:
        _escalate(
            2,
            f"Schema uncertainty is high ({round(schema_uncertainty_pct, 2)}%).",
            "Resolve unknown column types and ambiguous semantic mappings.",
        )
    elif schema_uncertainty_pct >= 20:
        _escalate(
            1,
            f"Schema uncertainty is moderate ({round(schema_uncertainty_pct, 2)}%).",
            "Review uncertain columns and update semantic/type hints.",
        )

    if rag_score < 55:
        _escalate(
            2,
            f"RAG readiness score is low ({rag_score}).",
            "Improve key-field coverage, text density, and normalization before embedding use.",
        )
    elif rag_score < 70:
        _escalate(
            1,
            f"RAG readiness score is below target ({rag_score}).",
            "Address failed/warn RAG checks prior to downstream retrieval workloads.",
        )

    dedup_actions: List[str] = []
    for action in actions:
        if action not in dedup_actions:
            dedup_actions.append(action)
    dedup_reasons: List[str] = []
    for reason in reasons:
        if reason not in dedup_reasons:
            dedup_reasons.append(reason)

    if severity == 0:
        return {
            "status": "ready",
            "reasons": ["Assessment and readiness signals are within acceptable thresholds."],
            "actions": ["Proceed with standard cleaning settings."],
        }
    if severity == 1:
        return {
            "status": "needs_review",
            "reasons": dedup_reasons,
            "actions": dedup_actions,
        }
    return {
        "status": "blocked",
        "reasons": dedup_reasons,
        "actions": dedup_actions,
    }


def build_profile(
    df: pd.DataFrame,
    column_map: Dict[str, str],
    *,
    total_rows: Optional[int] = None,
    sampled: bool = False,
    privacy_mode: str = "none",
) -> Dict[str, Any]:
    columns: List[Dict[str, Any]] = []

    normalized_df = df.copy()
    for col in normalized_df.columns:
        normalized_df[col] = _normalize_missing(normalized_df[col])

    for original_name, clean_name in column_map.items():
        series = normalized_df[original_name] if original_name in normalized_df.columns else pd.Series([], dtype=str)
        missing_pct = float(series.isna().mean() * 100) if len(series) else 100.0
        primitive = _infer_primitive_type(series)
        semantic = _infer_semantic(series, clean_name)
        pii_counts = _pii_signal_counts(series)
        tags = _domain_tags(clean_name)

        notes = []
        if missing_pct > 40:
            notes.append("High missing rate")
        if semantic:
            notes.append(f"Semantic hint: {semantic}")
        if sum(pii_counts.values()) > 0:
            notes.append("Potential PII detected")

        column_stats: Dict[str, Any] = {}
        if primitive == "number":
            column_stats = _numeric_stats(series) or {}
        elif primitive == "date":
            column_stats = _date_stats(series) or {}

        columns.append(
            {
                "original_name": original_name,
                "clean_name": clean_name,
                "primitive_type": primitive,
                "semantic_hint": semantic,
                "domain_tags": tags,
                "missing_pct": round(missing_pct, 2),
                "distinct_count": int(series.nunique(dropna=True)) if len(series) else 0,
                "example_values": _sample_values(series),
                "top_values": _top_values(series),
                "pii_signals": pii_counts,
                "stats": column_stats,
                "notes": "; ".join(notes) if notes else None,
            }
        )

    total = total_rows if total_rows is not None else int(len(df))
    high_missing = [col["clean_name"] for col in columns if col["missing_pct"] > 40]
    pii_columns = [col["clean_name"] for col in columns if sum(col["pii_signals"].values()) > 0]
    low_variance = [col["clean_name"] for col in columns if col["distinct_count"] <= 1]

    summary = {
        "columns_high_missing": high_missing,
        "columns_with_pii": pii_columns,
        "low_variance_columns": low_variance,
    }

    detected_domains = _detected_domains(list(column_map.values()))
    domain_suggestions = _domain_suggestions(list(column_map.values()))
    primary_domain = detected_domains[0]["domain"] if detected_domains else None

    profile_payload = {
        "row_count": total,
        "sampled_rows": int(len(df)),
        "sampled": sampled,
        "column_count": int(len(df.columns)),
        "summary": summary,
        "domains": domain_suggestions,
        "primary_domain": primary_domain,
        "detected_domains": detected_domains,
        "columns": columns,
    }
    profile_payload["assessment"] = build_assessment_breakdown(profile_payload)
    profile_payload["rag_readiness"] = build_rag_readiness_from_profile(
        profile_payload, privacy_mode=privacy_mode
    )
    profile_payload["preclean_decision"] = _preclean_decision(profile_payload, privacy_mode=privacy_mode)
    return profile_payload


def infer_hints_from_profile(
    profile: Dict[str, Any],
    *,
    semantic_overrides: Dict[str, str] | None = None,
) -> Dict[str, Dict[str, str]]:
    hints: Dict[str, Dict[str, str]] = {}
    for column in profile.get("columns", []):
        clean_name = column["clean_name"]
        hints[clean_name] = {
            "primitive_type": column.get("primitive_type") or "string",
            "semantic_hint": column.get("semantic_hint"),
        }
    for clean_name, semantic_hint in (semantic_overrides or {}).items():
        if clean_name not in hints:
            continue
        hints[clean_name]["semantic_hint"] = semantic_hint
    return hints
