from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Tuple

import pandas as pd

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b")
SSN_RE = re.compile(r"\b\d{3}-?\d{2}-?\d{4}\b")
URL_RE = re.compile(r"https?://|www\.")
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")

SAFE_HARBOR_LABELS = {
    "name": "[redacted_name]",
    "email": "[redacted_email]",
    "phone": "[redacted_phone]",
    "ssn": "***-**-****",
    "address": "[redacted_address]",
    "city": "[redacted_city]",
    "url": "[redacted_url]",
    "ip": "[redacted_ip]",
    "id": "[redacted_id]",
    "zip": "[redacted_zip]",
}

SAFE_HARBOR_TOKENS = {
    "name": ["name", "first", "last", "middle"],
    "email": ["email", "e-mail"],
    "phone": ["phone", "mobile", "tel", "cell"],
    "ssn": ["ssn", "social"],
    "address": ["address", "street", "addr"],
    "city": ["city", "town"],
    "zip": ["zip", "postal"],
    "id": ["mrn", "medical_record", "record_number", "member_id", "patient_id", "account", "claim_id"],
    "url": ["url", "website", "web"],
    "ip": ["ip", "ipv4", "ipv6"],
}

DATE_HINTS = {"date", "dob", "birth", "admit", "discharge", "encounter"}


def _normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _matches_token(column: str, token_list: list[str]) -> bool:
    lowered = _normalize_token(column)
    for token in token_list:
        token_norm = _normalize_token(token)
        if not token_norm:
            continue
        pattern = rf"(^|_){re.escape(token_norm)}(_|$)"
        if re.search(pattern, lowered):
            return True
    return False


def _generalize_date(value: Any, *, is_dob: bool) -> Any:
    if pd.isna(value):
        return pd.NA
    text = str(value).strip()
    if not text:
        return pd.NA
    try:
        parsed = pd.to_datetime(text, errors="raise")
    except Exception:
        return "[redacted_date]"

    year = parsed.year
    if is_dob:
        age = datetime.utcnow().year - year
        if age > 89:
            return "90+"
    return str(year)


def apply_safe_harbor(
    df: pd.DataFrame,
    column_metadata: Dict[str, Dict[str, str]],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    report: Dict[str, Any] = {
        "columns_masked": [],
        "dates_generalized": [],
        "zip_truncated": [],
    }

    for col in df.columns:
        meta = column_metadata.get(col, {})
        semantic = meta.get("semantic_hint") or ""
        lowered = col.lower()

        if _matches_token(col, SAFE_HARBOR_TOKENS["name"]):
            df[col] = SAFE_HARBOR_LABELS["name"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["email"]):
            df[col] = SAFE_HARBOR_LABELS["email"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["phone"]):
            df[col] = SAFE_HARBOR_LABELS["phone"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["ssn"]):
            df[col] = SAFE_HARBOR_LABELS["ssn"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["url"]):
            df[col] = SAFE_HARBOR_LABELS["url"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["ip"]):
            df[col] = SAFE_HARBOR_LABELS["ip"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["address"]):
            df[col] = SAFE_HARBOR_LABELS["address"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["city"]):
            df[col] = SAFE_HARBOR_LABELS["city"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["id"]):
            df[col] = SAFE_HARBOR_LABELS["id"]
            report["columns_masked"].append(col)
            continue

        if _matches_token(col, SAFE_HARBOR_TOKENS["zip"]):
            series = df[col].astype("string")
            digits = series.str.replace(r"\D", "", regex=True)
            masked = digits.where(digits.str.len() >= 3, pd.NA)
            masked = masked.str.slice(0, 3) + "00"
            df[col] = masked
            report["zip_truncated"].append(col)
            continue

        if semantic in {"dob", "admit_date", "discharge_date", "encounter_date", "date"} or any(
            token in lowered for token in DATE_HINTS
        ):
            is_dob = semantic == "dob" or "dob" in lowered or "birth" in lowered
            series = df[col].astype("string")
            parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
            year = parsed.dt.year.astype("Int64")
            if is_dob:
                age = datetime.utcnow().year - year
                dob_mask = age > 89
                generalized = year.astype("string")
                generalized = generalized.where(~dob_mask, "90+")
            else:
                generalized = year.astype("string")
            generalized = generalized.where(parsed.notna(), "[redacted_date]")
            generalized = generalized.where(series.notna(), pd.NA)
            df[col] = generalized
            report["dates_generalized"].append(col)
            continue

    return df, report
