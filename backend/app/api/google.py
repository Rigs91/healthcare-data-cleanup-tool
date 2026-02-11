from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/integrations/google", tags=["google"])

FEATURE_DISABLED_CODE = "FEATURE_DISABLED"
FEATURE_DISABLED_MESSAGE = "Google Drive integration is a future enhancement and is currently disabled."


def _feature_disabled_error() -> HTTPException:
    return HTTPException(
        status_code=501,
        detail={
            "code": FEATURE_DISABLED_CODE,
            "message": FEATURE_DISABLED_MESSAGE,
        },
    )


@router.get("/auth/status")
async def google_auth_status() -> dict:
    return {
        "configured": False,
        "authenticated": False,
        "feature_enabled": False,
        "supported_mime_types": [],
        "message": FEATURE_DISABLED_MESSAGE,
    }


@router.get("/auth/start")
async def google_auth_start() -> dict:
    raise _feature_disabled_error()


@router.get("/auth/callback")
async def google_auth_callback() -> dict:
    raise _feature_disabled_error()


@router.post("/auth/logout")
async def google_auth_logout() -> dict:
    raise _feature_disabled_error()


@router.get("/drive/files")
async def google_drive_files() -> dict:
    raise _feature_disabled_error()
