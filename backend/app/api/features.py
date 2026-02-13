from __future__ import annotations

from fastapi import APIRouter, Query

from app.services.feature_registry import load_feature_registry

router = APIRouter(prefix="/api", tags=["features"])


@router.get("/features")
async def get_feature_registry(
    wave: int | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict:
    registry = load_feature_registry()
    features = list(registry.get("features") or [])

    if wave is not None:
        features = [item for item in features if _safe_int(item.get("wave")) == wave]
    if status:
        needle = status.strip().lower()
        features = [item for item in features if str(item.get("status") or "").lower() == needle]

    registry["features"] = features
    registry["summary"]["filtered_total"] = len(features)
    return registry


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except Exception:
        return None
