from __future__ import annotations

import io
import re
import time
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlencode
from uuid import uuid4

import httpx
import pandas as pd
from fastapi import Request

from app.config import (
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_OAUTH_AUTH_URL,
    GOOGLE_OAUTH_SCOPE,
    GOOGLE_OAUTH_TOKEN_URL,
    GOOGLE_REDIRECT_URI,
    RAW_DIR,
)

GOOGLE_SESSION_COOKIE = "google_demo_sid"
GOOGLE_SHEETS_MIME = "application/vnd.google-apps.spreadsheet"
GOOGLE_CSV_MIME = "text/csv"
GOOGLE_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

SUPPORTED_DRIVE_MIME_TYPES = {
    GOOGLE_SHEETS_MIME,
    GOOGLE_CSV_MIME,
    GOOGLE_XLSX_MIME,
}

DRIVE_FILES_LIST_URL = "https://www.googleapis.com/drive/v3/files"
DRIVE_FILE_EXPORT_URL = "https://www.googleapis.com/drive/v3/files/{file_id}/export"
DRIVE_FILE_MEDIA_URL = "https://www.googleapis.com/drive/v3/files/{file_id}"

_SESSION_STORE: Dict[str, Dict[str, Any]] = {}
_OAUTH_STATE_INDEX: Dict[str, str] = {}


class GoogleIntegrationError(Exception):
    def __init__(self, message: str, *, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def missing_google_config_keys() -> list[str]:
    missing: list[str] = []
    if not GOOGLE_CLIENT_ID:
        missing.append("GOOGLE_CLIENT_ID")
    if not GOOGLE_CLIENT_SECRET:
        missing.append("GOOGLE_CLIENT_SECRET")
    if not GOOGLE_REDIRECT_URI:
        missing.append("GOOGLE_REDIRECT_URI")
    return missing


def google_configured() -> bool:
    return not missing_google_config_keys()


def _ensure_configured() -> None:
    if google_configured():
        return
    missing = missing_google_config_keys()
    missing_text = ", ".join(missing) if missing else "GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REDIRECT_URI"
    raise GoogleIntegrationError(
        f"Google integration is not configured. Missing: {missing_text}.",
        status_code=503,
    )


def _ensure_session(session_id: str) -> Dict[str, Any]:
    if session_id not in _SESSION_STORE:
        _SESSION_STORE[session_id] = {"oauth_state": None, "tokens": None}
    return _SESSION_STORE[session_id]


def get_or_create_session_id(request: Request) -> tuple[str, bool]:
    session_id = request.cookies.get(GOOGLE_SESSION_COOKIE)
    if session_id:
        _ensure_session(session_id)
        return session_id, False
    new_id = uuid4().hex
    _ensure_session(new_id)
    return new_id, True


def _get_session_id_from_request(request: Request) -> str | None:
    session_id = request.cookies.get(GOOGLE_SESSION_COOKIE)
    if not session_id:
        return None
    _ensure_session(session_id)
    return session_id


def _get_tokens_by_session_id(session_id: str | None) -> Dict[str, Any] | None:
    if not session_id:
        return None
    session = _SESSION_STORE.get(session_id)
    if not session:
        return None
    tokens = session.get("tokens")
    return tokens if isinstance(tokens, dict) else None


def _set_tokens(session_id: str, tokens: Dict[str, Any]) -> None:
    session = _ensure_session(session_id)
    session["tokens"] = tokens


def clear_google_session(request: Request) -> None:
    session_id = _get_session_id_from_request(request)
    if session_id:
        session = _SESSION_STORE.pop(session_id, None)
        if isinstance(session, dict):
            oauth_state = session.get("oauth_state")
            if oauth_state:
                _OAUTH_STATE_INDEX.pop(str(oauth_state), None)


def build_google_auth_url(request: Request) -> tuple[str, str]:
    _ensure_configured()
    session_id, _created = get_or_create_session_id(request)
    state = uuid4().hex
    session = _ensure_session(session_id)
    session["oauth_state"] = state
    _OAUTH_STATE_INDEX[state] = session_id

    query = urlencode(
        {
            "client_id": GOOGLE_CLIENT_ID,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "response_type": "code",
            "scope": GOOGLE_OAUTH_SCOPE,
            "access_type": "offline",
            "include_granted_scopes": "true",
            "prompt": "consent",
            "state": state,
        }
    )
    return f"{GOOGLE_OAUTH_AUTH_URL}?{query}", session_id


def _build_token_payload(token_data: Dict[str, Any], *, existing_refresh: str | None = None) -> Dict[str, Any]:
    access_token = str(token_data.get("access_token") or "").strip()
    if not access_token:
        raise GoogleIntegrationError("Google token exchange failed: missing access token.", status_code=502)

    refresh_token = token_data.get("refresh_token") or existing_refresh
    expires_raw = token_data.get("expires_in")
    try:
        expires_in = int(expires_raw) if expires_raw is not None else 3600
    except (TypeError, ValueError):
        expires_in = 3600
    expires_at = int(time.time()) + max(60, expires_in - 30)
    scope = str(token_data.get("scope") or "").strip()
    token_type = str(token_data.get("token_type") or "Bearer").strip()
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at,
        "scope": scope,
        "token_type": token_type,
    }


def _parse_json_response(response: httpx.Response, *, context: str) -> Dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        detail = response.text[:300]
        detail_text = detail or "non-JSON response body"
        raise GoogleIntegrationError(f"{context}: {detail_text}", status_code=502) from exc
    if not isinstance(payload, dict):
        raise GoogleIntegrationError(f"{context}: unexpected response payload.", status_code=502)
    return payload


def _exchange_code_for_token(code: str) -> Dict[str, Any]:
    payload = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    with httpx.Client(timeout=30.0) as client:
        response = client.post(GOOGLE_OAUTH_TOKEN_URL, data=payload)
    if response.status_code >= 400:
        detail = response.text[:300]
        raise GoogleIntegrationError(f"Google token exchange failed: {detail}", status_code=502)
    return _parse_json_response(response, context="Google token exchange failed")


def _refresh_access_token(tokens: Dict[str, Any]) -> Dict[str, Any]:
    refresh_token = str(tokens.get("refresh_token") or "").strip()
    if not refresh_token:
        raise GoogleIntegrationError("Google session expired. Reconnect Google Drive.", status_code=401)

    payload = {
        "refresh_token": refresh_token,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }
    with httpx.Client(timeout=30.0) as client:
        response = client.post(GOOGLE_OAUTH_TOKEN_URL, data=payload)
    if response.status_code >= 400:
        raise GoogleIntegrationError("Google session expired. Reconnect Google Drive.", status_code=401)
    token_data = _parse_json_response(response, context="Google token refresh failed")
    refreshed = _build_token_payload(token_data, existing_refresh=refresh_token)
    if not refreshed.get("scope"):
        refreshed["scope"] = tokens.get("scope") or ""
    return refreshed


def complete_google_oauth_callback(request: Request, *, code: str, state: str) -> str:
    _ensure_configured()
    session_id = _get_session_id_from_request(request) or _OAUTH_STATE_INDEX.get(state)
    if not session_id:
        raise GoogleIntegrationError("Google OAuth session was not found. Start authentication again.", status_code=400)

    session = _ensure_session(session_id)
    expected_state = str(session.get("oauth_state") or "")
    if not expected_state or state != expected_state:
        raise GoogleIntegrationError("Invalid Google OAuth state.", status_code=400)

    token_data = _exchange_code_for_token(code)
    existing_refresh = (_get_tokens_by_session_id(session_id) or {}).get("refresh_token")
    payload = _build_token_payload(token_data, existing_refresh=existing_refresh)
    _set_tokens(session_id, payload)
    session["oauth_state"] = None
    _OAUTH_STATE_INDEX.pop(state, None)
    return session_id


def get_google_auth_status(request: Request) -> Dict[str, Any]:
    configured = google_configured()
    missing = missing_google_config_keys()
    session_id = _get_session_id_from_request(request)
    tokens = _get_tokens_by_session_id(session_id)
    authenticated = bool(tokens and tokens.get("access_token"))
    expires_at = int(tokens.get("expires_at") or 0) if tokens else 0
    return {
        "configured": configured,
        "missing_config": missing,
        "authenticated": authenticated,
        "expires_at": expires_at if authenticated else None,
        "supported_mime_types": sorted(SUPPORTED_DRIVE_MIME_TYPES),
    }


def _get_valid_access_token(request: Request) -> str:
    _ensure_configured()
    session_id = _get_session_id_from_request(request)
    tokens = _get_tokens_by_session_id(session_id)
    if not session_id or not tokens:
        raise GoogleIntegrationError("Google Drive is not connected.", status_code=401)

    now = int(time.time())
    expires_at = int(tokens.get("expires_at") or 0)
    if expires_at <= now:
        refreshed = _refresh_access_token(tokens)
        _set_tokens(session_id, refreshed)
        tokens = refreshed
    access_token = str(tokens.get("access_token") or "").strip()
    if not access_token:
        raise GoogleIntegrationError("Google session is invalid. Reconnect Google Drive.", status_code=401)
    return access_token


def _auth_headers(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def _drive_query(search: str | None = None) -> str:
    mime_filters = " or ".join(f"mimeType='{mime}'" for mime in sorted(SUPPORTED_DRIVE_MIME_TYPES))
    query_parts = ["trashed = false", f"({mime_filters})"]
    if search:
        escaped = search.replace("\\", "\\\\").replace("'", "\\'")
        query_parts.append(f"name contains '{escaped}'")
    return " and ".join(query_parts)


def list_drive_files(
    request: Request,
    *,
    search: str | None = None,
    page_token: str | None = None,
    page_size: int = 25,
) -> Dict[str, Any]:
    access_token = _get_valid_access_token(request)
    safe_size = max(1, min(100, int(page_size)))
    params: Dict[str, Any] = {
        "q": _drive_query(search),
        "pageSize": safe_size,
        "orderBy": "modifiedTime desc",
        "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,size,owners(displayName,emailAddress))",
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    }
    if page_token:
        params["pageToken"] = page_token

    with httpx.Client(timeout=30.0) as client:
        response = client.get(DRIVE_FILES_LIST_URL, params=params, headers=_auth_headers(access_token))
    if response.status_code == 401:
        clear_google_session(request)
        raise GoogleIntegrationError("Google Drive session expired. Reconnect and try again.", status_code=401)
    if response.status_code >= 400:
        detail = response.text[:300]
        raise GoogleIntegrationError(f"Failed to list Drive files: {detail}", status_code=502)

    payload = _parse_json_response(response, context="Failed to list Drive files")
    files = payload.get("files") or []
    items: list[Dict[str, Any]] = []
    for file_item in files:
        mime_type = str(file_item.get("mimeType") or "")
        if mime_type not in SUPPORTED_DRIVE_MIME_TYPES:
            continue
        size_raw = file_item.get("size")
        size_bytes = None
        if isinstance(size_raw, str) and size_raw.isdigit():
            size_bytes = int(size_raw)
        elif isinstance(size_raw, int):
            size_bytes = size_raw
        owners = []
        for owner in file_item.get("owners") or []:
            if not isinstance(owner, dict):
                continue
            display = owner.get("displayName") or owner.get("emailAddress")
            if display:
                owners.append(str(display))
        items.append(
            {
                "id": file_item.get("id"),
                "name": file_item.get("name"),
                "mime_type": mime_type,
                "modified_time": file_item.get("modifiedTime"),
                "size_bytes": size_bytes,
                "owners": owners,
            }
        )
    return {"items": items, "next_page_token": payload.get("nextPageToken"), "page_size": safe_size}


def _download_drive_file(access_token: str, *, file_id: str, mime_type: str) -> bytes:
    if mime_type == GOOGLE_SHEETS_MIME:
        url = DRIVE_FILE_EXPORT_URL.format(file_id=file_id)
        params = {"mimeType": "text/csv", "supportsAllDrives": "true"}
    else:
        url = DRIVE_FILE_MEDIA_URL.format(file_id=file_id)
        params = {"alt": "media", "supportsAllDrives": "true"}
    with httpx.Client(timeout=60.0) as client:
        response = client.get(url, params=params, headers=_auth_headers(access_token))
    if response.status_code == 404:
        raise GoogleIntegrationError("Drive file not found.", status_code=404)
    if response.status_code == 403:
        raise GoogleIntegrationError("Permission denied for selected Drive file.", status_code=403)
    if response.status_code >= 400:
        detail = response.text[:300]
        raise GoogleIntegrationError(f"Failed to download Drive file: {detail}", status_code=502)
    return response.content


def _sanitize_filename(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return "google_drive_file.csv"
    text = text.replace("..", ".")
    text = re.sub(r"[<>:\"/\\|?*]", "_", text)
    text = text.rstrip(" .")
    if not text:
        return "google_drive_file.csv"
    return text


def _to_csv_filename(name: str) -> str:
    p = Path(name)
    stem = p.stem if p.suffix else str(p)
    if not stem:
        stem = "google_drive_file"
    return f"{stem}.csv"


def materialize_drive_file_for_ingest(
    request: Request,
    *,
    file_id: str,
    file_name: str,
    mime_type: str,
    dataset_id: str,
) -> tuple[Path, str, int]:
    if mime_type not in SUPPORTED_DRIVE_MIME_TYPES:
        raise GoogleIntegrationError("Unsupported Drive file type. Use Google Sheets, CSV, or XLSX.", status_code=400)

    access_token = _get_valid_access_token(request)
    content = _download_drive_file(access_token, file_id=file_id, mime_type=mime_type)
    safe_name = _sanitize_filename(file_name)
    output_name = _to_csv_filename(safe_name)
    output_path = RAW_DIR / f"{dataset_id}__{output_name}"

    if mime_type in {GOOGLE_SHEETS_MIME, GOOGLE_CSV_MIME}:
        output_path.write_bytes(content)
        return output_path, output_name, int(output_path.stat().st_size)

    if mime_type == GOOGLE_XLSX_MIME:
        try:
            df = pd.read_excel(io.BytesIO(content), dtype=str)
        except ImportError as exc:
            raise GoogleIntegrationError("XLSX import requires openpyxl dependency.", status_code=400) from exc
        except Exception as exc:
            raise GoogleIntegrationError(f"Failed to parse XLSX file: {exc}", status_code=400) from exc
        df.to_csv(output_path, index=False)
        return output_path, output_name, int(output_path.stat().st_size)

    raise GoogleIntegrationError("Unsupported Drive file type.", status_code=400)
