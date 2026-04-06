from __future__ import annotations

import json
import copy
from pathlib import Path
from typing import Any

from app.config import BASE_DIR


FEATURE_REGISTRY_PATH = Path(BASE_DIR) / "backend" / "app" / "assets" / "feature_registry.json"
_FEATURE_REGISTRY_CACHE: dict[str, Any] | None = None
_FEATURE_REGISTRY_MTIME: int | None = None


def _build_empty_registry() -> dict[str, Any]:
    return {
        "version": 1,
        "generated_for": "50-feature-rollout",
        "features": [],
        "summary": {"total": 0, "by_status": {}, "by_wave": {}},
    }


def load_feature_registry() -> dict[str, Any]:
    global _FEATURE_REGISTRY_CACHE, _FEATURE_REGISTRY_MTIME
    if not FEATURE_REGISTRY_PATH.exists():
        return copy.deepcopy(_build_empty_registry())

    mtime = int(FEATURE_REGISTRY_PATH.stat().st_mtime_ns)
    if _FEATURE_REGISTRY_CACHE is not None and _FEATURE_REGISTRY_MTIME == mtime:
        return copy.deepcopy(_FEATURE_REGISTRY_CACHE)

    try:
        payload = json.loads(FEATURE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception:
        payload = _build_empty_registry()
    if not isinstance(payload, dict):
        payload = _build_empty_registry()

    if "features" not in payload or not isinstance(payload.get("features"), list):
        payload["features"] = []

    features = payload.get("features") or []

    by_status: dict[str, int] = {}
    by_wave: dict[str, int] = {}
    for item in features:
        status = str(item.get("status") or "planned")
        wave = str(item.get("wave") or "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        by_wave[wave] = by_wave.get(wave, 0) + 1

    payload["summary"] = {
        "total": len(features),
        "by_status": by_status,
        "by_wave": by_wave,
    }
    _FEATURE_REGISTRY_CACHE = payload
    _FEATURE_REGISTRY_MTIME = mtime
    return payload
