from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.config import BASE_DIR


FEATURE_REGISTRY_PATH = Path(BASE_DIR) / "docs" / "rollout" / "features.json"


def load_feature_registry() -> dict[str, Any]:
    if not FEATURE_REGISTRY_PATH.exists():
        return {
            "version": 1,
            "generated_for": "50-feature-rollout",
            "features": [],
            "summary": {"total": 0, "by_status": {}, "by_wave": {}},
        }

    payload = json.loads(FEATURE_REGISTRY_PATH.read_text(encoding="utf-8"))
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
    return payload

