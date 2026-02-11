from __future__ import annotations

import re


def to_snake_case(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip())
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    return cleaned.lower() or "column"


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()
