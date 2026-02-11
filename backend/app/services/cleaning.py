from __future__ import annotations

import re
from typing import Any, Dict, Tuple

import pandas as pd

from app.utils.text import to_snake_case
from app.services.privacy import apply_safe_harbor

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
MISSING_VALUES_LOWER = {value.lower() for value in MISSING_VALUES}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\d")
PHONE_DEID_RE = re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b")
SSN_RE = re.compile(r"\b\d{3}-?\d{2}-?\d{4}\b")
ZIP_RE = re.compile(r"\d")

MIN_PARSE_SUCCESS = 0.6
CRITICAL_SEMANTICS = {"dob", "encounter_date", "admit_date", "discharge_date"}


def standardize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    mapping: Dict[str, str] = {}
    used = set()

    for original in df.columns:
        base = to_snake_case(str(original))
        candidate = base
        index = 2
        while candidate in used:
            candidate = f"{base}_{index}"
            index += 1
        mapping[original] = candidate
        used.add(candidate)

    df = df.rename(columns=mapping)
    return df, mapping


def normalize_missing(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        series = df[col].astype("string")
        lowered = series.str.strip().str.lower()
        mask = lowered.isin(MISSING_VALUES_LOWER)
        df[col] = series.where(~mask, pd.NA)
    return df


def _coerce_numeric(series: pd.Series) -> Tuple[pd.Series, int]:
    original = series.copy()
    cleaned = series.astype("string")
    cleaned = cleaned.str.replace(",", "", regex=False)
    cleaned = cleaned.str.replace("$", "", regex=False)
    cleaned = cleaned.str.replace("%", "", regex=False)
    cleaned = cleaned.str.replace(r"\(([^)]+)\)", r"-\1", regex=True)

    numeric = pd.to_numeric(cleaned, errors="coerce")
    invalid = int((numeric.isna() & original.notna()).sum())
    return numeric, invalid


def _coerce_dates(series: pd.Series) -> Tuple[pd.Series, int]:
    original = series.copy()
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed", cache=True)
    invalid = int((parsed.isna() & original.notna()).sum())
    return parsed, invalid


def _coerce_bool(series: pd.Series) -> Tuple[pd.Series, int]:
    mapping = {
        "true": True,
        "false": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
        "1": True,
        "0": False,
    }
    original = series.copy()
    normalized = series.astype(str).str.strip().str.lower()
    bools = normalized.map(mapping)
    invalid = int((bools.isna() & original.notna()).sum())
    return bools, invalid


def _success_ratio(series: pd.Series, invalid_count: int) -> float:
    non_null = int(series.notna().sum())
    if non_null == 0:
        return 0.0
    return (non_null - invalid_count) / non_null


def _sanitize_strings(series: pd.Series) -> pd.Series:
    series = series.astype("string")
    series = series.str.replace(r"\s+", " ", regex=True)
    series = series.str.strip()
    return series


def _normalize_gender(series: pd.Series) -> pd.Series:
    mapping = {
        "m": "M",
        "male": "M",
        "f": "F",
        "female": "F",
        "woman": "F",
        "man": "M",
        "other": "O",
        "unknown": "U",
        "u": "U",
    }
    normalized = series.astype("string").str.strip().str.lower()
    mapped = normalized.map(mapping)
    return mapped.where(mapped.notna(), series)


def _normalize_phone(series: pd.Series) -> pd.Series:
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    digits = digits.where(digits != "", pd.NA)
    mask = digits.str.len() >= 10
    digits = digits.where(~mask, digits.str[-10:])
    return digits


def _normalize_zip(series: pd.Series) -> pd.Series:
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    digits = digits.where(digits != "", pd.NA)
    mask = digits.str.len() >= 5
    digits = digits.where(~mask, digits.str.slice(0, 5))
    return digits


def _normalize_ndc(series: pd.Series) -> pd.Series:
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    digits = digits.where(digits != "", pd.NA)
    length = digits.str.len()
    digits = digits.where(length != 10, "0" + digits)
    return digits


def _normalize_rxnorm(series: pd.Series) -> pd.Series:
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    digits = digits.where(digits != "", pd.NA)
    return digits


def _apply_text_case(series: pd.Series, text_case: str) -> pd.Series:
    if text_case == "lower":
        return series.astype("string").str.lower()
    if text_case == "upper":
        return series.astype("string").str.upper()
    if text_case == "title":
        return series.astype("string").str.title()
    return series


def _deidentify_value(value: str, name_hint: bool) -> str:
    if EMAIL_RE.search(value):
        return EMAIL_RE.sub("[redacted_email]", value)
    if PHONE_DEID_RE.search(value):
        return PHONE_DEID_RE.sub("[redacted_phone]", value)
    if SSN_RE.search(value):
        return SSN_RE.sub("***-**-****", value)
    if name_hint:
        return "[redacted_name]"
    return value


def deidentify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        name_hint = "name" in col and "username" not in col
        series = df[col]
        if name_hint:
            df[col] = series.where(series.isna(), "[redacted_name]")
            continue

        series = series.astype("string")
        series = series.str.replace(EMAIL_RE, "[redacted_email]", regex=True)
        series = series.str.replace(PHONE_DEID_RE, "[redacted_phone]", regex=True)
        series = series.str.replace(SSN_RE, "***-**-****", regex=True)
        df[col] = series
    return df


def clean_dataframe(
    raw_df: pd.DataFrame,
    column_metadata: Dict[str, Dict[str, str]],
    *,
    column_map: Dict[str, str] | None = None,
    already_standardized: bool = False,
    normalize_missing_values: bool = True,
    sanitize_strings: bool = True,
    remove_duplicates: bool = True,
    drop_empty_columns: bool = True,
    deidentify: bool = False,
    normalize_phone: bool = True,
    normalize_zip: bool = True,
    normalize_gender: bool = True,
    text_case: str = "none",
    coercion_mode: str = "safe",
    privacy_mode: str = "none",
    performance_mode: str = "balanced",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = raw_df.copy()

    if performance_mode not in {"balanced", "fast", "ultra_fast"}:
        raise ValueError("Unsupported performance mode.")

    if not already_standardized:
        df, column_map = standardize_columns(df)
    else:
        column_map = column_map or {col: col for col in df.columns}
    if normalize_missing_values:
        df = normalize_missing(df)

    conversion_report: Dict[str, Dict[str, Any]] = {}

    for col in df.columns:
        series = df[col]
        sanitized: pd.Series | None = None

        def _get_sanitized() -> pd.Series:
            nonlocal sanitized
            if sanitized is None:
                if sanitize_strings:
                    sanitized = _sanitize_strings(series)
                else:
                    sanitized = series
            return sanitized
        meta = column_metadata.get(col, {})
        primitive = meta.get("primitive_type", "string")
        semantic = meta.get("semantic_hint")
        if performance_mode == "ultra_fast":
            primitive = "string"
        if semantic is None:
            lowered = col.lower()
            if "phone" in lowered or "mobile" in lowered:
                semantic = "phone"
            elif "zip" in lowered or "postal" in lowered:
                semantic = "postal_code"
            elif "gender" in lowered or "sex" in lowered:
                semantic = "gender"
            elif "code" in lowered or "icd" in lowered or "cpt" in lowered or "loinc" in lowered:
                semantic = "code"

        effective_coercion = "safe" if semantic in CRITICAL_SEMANTICS else coercion_mode
        if semantic in CRITICAL_SEMANTICS and primitive == "string":
            primitive = "date"

        if primitive in {"number", "integer", "float"}:
            numeric, invalid = _coerce_numeric(series)
            success_ratio = _success_ratio(series, invalid)
            if success_ratio < MIN_PARSE_SUCCESS:
                df[col] = _get_sanitized()
                conversion_report[col] = {
                    "type": "string",
                    "invalid": invalid,
                    "coercion_skipped": True,
                    "success_ratio": round(success_ratio, 3),
                }
            else:
                if effective_coercion == "safe" and invalid > 0:
                    original = _get_sanitized()
                    cleaned = numeric.astype(object)
                    mask = numeric.isna() & series.notna()
                    cleaned[mask] = original[mask]
                    df[col] = cleaned
                else:
                    df[col] = numeric
                conversion_report[col] = {
                    "type": "number",
                    "invalid": invalid,
                    "coercion_mode": effective_coercion,
                    "success_ratio": round(success_ratio, 3),
                }
        elif primitive in {"date", "datetime"}:
            parsed, invalid = _coerce_dates(series)
            success_ratio = _success_ratio(series, invalid)
            if success_ratio < MIN_PARSE_SUCCESS:
                df[col] = _get_sanitized()
                conversion_report[col] = {
                    "type": "string",
                    "invalid": invalid,
                    "coercion_skipped": True,
                    "success_ratio": round(success_ratio, 3),
                }
            else:
                formatted = parsed.dt.strftime("%Y-%m-%d")
                if effective_coercion == "safe" and invalid > 0:
                    original = _get_sanitized()
                    cleaned = formatted.astype(object)
                    mask = parsed.isna() & series.notna()
                    cleaned[mask] = original[mask]
                    df[col] = cleaned
                else:
                    df[col] = formatted
                conversion_report[col] = {
                    "type": "date",
                    "invalid": invalid,
                    "coercion_mode": effective_coercion,
                    "success_ratio": round(success_ratio, 3),
                }
        elif primitive in {"boolean", "bool"}:
            bools, invalid = _coerce_bool(series)
            success_ratio = _success_ratio(series, invalid)
            if success_ratio < MIN_PARSE_SUCCESS:
                df[col] = _get_sanitized()
                conversion_report[col] = {
                    "type": "string",
                    "invalid": invalid,
                    "coercion_skipped": True,
                    "success_ratio": round(success_ratio, 3),
                }
            else:
                if effective_coercion == "safe" and invalid > 0:
                    original = _get_sanitized()
                    cleaned = bools.astype(object)
                    mask = bools.isna() & series.notna()
                    cleaned[mask] = original[mask]
                    df[col] = cleaned
                else:
                    df[col] = bools
                conversion_report[col] = {
                    "type": "boolean",
                    "invalid": invalid,
                    "coercion_mode": effective_coercion,
                    "success_ratio": round(success_ratio, 3),
                }
        else:
            df[col] = _get_sanitized()
            conversion_report[col] = {"type": "string", "invalid": 0}

        if semantic in {"code", "medication", "lab"}:
            df[col] = df[col].astype("string").str.replace(" ", "", regex=False).str.upper()
        if "loinc" in col.lower():
            df[col] = df[col].astype("string").str.replace(" ", "", regex=False).str.upper()
        if "ndc" in col.lower():
            df[col] = _normalize_ndc(df[col])
        if "rxnorm" in col.lower():
            df[col] = _normalize_rxnorm(df[col])
        if semantic == "phone" and normalize_phone:
            df[col] = _normalize_phone(df[col])
        if semantic == "postal_code" and normalize_zip:
            df[col] = _normalize_zip(df[col])
        if semantic == "gender" and normalize_gender:
            df[col] = _normalize_gender(df[col])

        if text_case != "none" and primitive == "string" and semantic not in {"code"}:
            df[col] = _apply_text_case(df[col], text_case)

    if drop_empty_columns:
        empty_cols = [col for col in df.columns if df[col].isna().all()]
        df = df.drop(columns=empty_cols)
    else:
        empty_cols = []

    duplicate_rows_removed = 0
    if remove_duplicates:
        before = len(df)
        df = df.drop_duplicates()
        duplicate_rows_removed = before - len(df)

    if deidentify:
        df = deidentify_dataframe(df)

    privacy_report: Dict[str, Any] | None = None
    if privacy_mode == "safe_harbor":
        df, privacy_report = apply_safe_harbor(df, column_metadata)

    report = {
        "column_map": column_map,
        "conversion": conversion_report,
        "empty_columns_removed": empty_cols,
        "duplicate_rows_removed": duplicate_rows_removed,
        "privacy_mode": privacy_mode,
        "privacy_report": privacy_report,
    }

    return df, report
