from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

router = APIRouter(tags=["compat"])


def _redirect(path: str) -> RedirectResponse:
    return RedirectResponse(url=f"/api{path}", status_code=307)


@router.api_route("/health", methods=["GET"])
async def compat_health():
    return _redirect("/health")


@router.api_route("/uploads/start", methods=["POST", "OPTIONS"])
async def compat_upload_start():
    return _redirect("/uploads/start")


@router.api_route("/uploads/{upload_id}/chunk", methods=["POST", "OPTIONS"])
async def compat_upload_chunk(upload_id: str):
    return _redirect(f"/uploads/{upload_id}/chunk")


@router.api_route("/uploads/{upload_id}/complete", methods=["POST", "OPTIONS"])
async def compat_upload_complete(upload_id: str):
    return _redirect(f"/uploads/{upload_id}/complete")


@router.api_route("/uploads/{upload_id}", methods=["GET", "DELETE", "OPTIONS"])
async def compat_upload_session(upload_id: str):
    return _redirect(f"/uploads/{upload_id}")


@router.api_route("/datasets", methods=["GET", "POST", "OPTIONS"])
async def compat_datasets():
    return _redirect("/datasets")


@router.api_route("/datasets/from-google-drive", methods=["POST", "OPTIONS"])
async def compat_dataset_from_google_drive():
    return _redirect("/datasets/from-google-drive")


@router.api_route("/datasets/{dataset_id}/clean", methods=["POST", "OPTIONS"])
async def compat_clean(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}/clean")


@router.api_route("/datasets/{dataset_id}/cleanup/autopilot", methods=["POST", "OPTIONS"])
async def compat_autopilot_dataset(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}/cleanup/autopilot")


@router.api_route("/cleanup/autopilot", methods=["POST", "OPTIONS"])
async def compat_autopilot_cleanup():
    return _redirect("/cleanup/autopilot")


@router.api_route("/datasets/{dataset_id}/assessment:recompute", methods=["POST", "OPTIONS"])
async def compat_recompute_assessment(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}/assessment:recompute")


@router.api_route("/datasets/{dataset_id}/clean-jobs", methods=["POST", "OPTIONS"])
async def compat_clean_jobs(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}/clean-jobs")


@router.api_route("/clean-jobs/{job_id}", methods=["GET", "OPTIONS"])
async def compat_clean_job(job_id: str):
    return _redirect(f"/clean-jobs/{job_id}")


@router.api_route("/clean-jobs/{job_id}/cancel", methods=["POST", "OPTIONS"])
async def compat_clean_job_cancel(job_id: str):
    return _redirect(f"/clean-jobs/{job_id}/cancel")


@router.api_route("/datasets/{dataset_id}", methods=["GET", "OPTIONS"])
async def compat_dataset(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}")


@router.api_route("/datasets/{dataset_id}/preview", methods=["GET", "OPTIONS"])
async def compat_preview(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}/preview")


@router.api_route("/datasets/{dataset_id}/download", methods=["GET", "OPTIONS"])
async def compat_download(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}/download")


@router.api_route("/datasets/{dataset_id}/runs", methods=["GET", "OPTIONS"])
async def compat_dataset_runs(dataset_id: str):
    return _redirect(f"/datasets/{dataset_id}/runs")


@router.api_route("/features", methods=["GET"])
async def compat_features():
    return _redirect("/features")


@router.api_route("/runs/{run_id}", methods=["GET", "OPTIONS"])
async def compat_run(run_id: str):
    return _redirect(f"/runs/{run_id}")


@router.api_route("/runs/{run_id}/outcomes", methods=["GET", "OPTIONS"])
async def compat_run_outcomes(run_id: str):
    return _redirect(f"/runs/{run_id}/outcomes")


@router.api_route("/integrations/google/auth/status", methods=["GET", "OPTIONS"])
async def compat_google_auth_status():
    return _redirect("/integrations/google/auth/status")


@router.api_route("/integrations/google/auth/start", methods=["GET", "OPTIONS"])
async def compat_google_auth_start():
    return _redirect("/integrations/google/auth/start")


@router.api_route("/integrations/google/auth/callback", methods=["GET", "OPTIONS"])
async def compat_google_auth_callback(request: Request):
    suffix = f"?{request.url.query}" if request.url.query else ""
    return RedirectResponse(url=f"/api/integrations/google/auth/callback{suffix}", status_code=307)


@router.api_route("/integrations/google/auth/logout", methods=["POST", "OPTIONS"])
async def compat_google_auth_logout():
    return _redirect("/integrations/google/auth/logout")


@router.api_route("/integrations/google/drive/files", methods=["GET", "OPTIONS"])
async def compat_google_drive_files():
    return _redirect("/integrations/google/drive/files")
