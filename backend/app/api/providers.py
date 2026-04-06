from __future__ import annotations

from fastapi import APIRouter, Query

from app.services.llm import get_ollama_provider_status

router = APIRouter(prefix="/api/providers", tags=["providers"])


@router.get("/ollama/models")
async def list_ollama_models(
    requested_model: str | None = Query(default=None),
) -> dict:
    return get_ollama_provider_status(requested_model=requested_model)
