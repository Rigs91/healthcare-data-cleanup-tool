from fastapi.testclient import TestClient

from app.main import app


def test_google_auth_status_reports_feature_disabled():
    client = TestClient(app)
    response = client.get("/api/integrations/google/auth/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["configured"] is False
    assert payload["authenticated"] is False
    assert payload["feature_enabled"] is False
    assert payload["supported_mime_types"] == []
    assert "future enhancement" in payload["message"].lower()


def test_google_auth_start_returns_not_implemented():
    client = TestClient(app)
    response = client.get("/api/integrations/google/auth/start")
    assert response.status_code == 501
    payload = response.json()
    detail = payload.get("detail") or {}
    assert detail.get("code") == "FEATURE_DISABLED"
    assert "future enhancement" in (detail.get("message") or "").lower()

def test_google_drive_files_returns_not_implemented():
    client = TestClient(app)
    response = client.get("/api/integrations/google/drive/files?q=claims&page_size=20")
    assert response.status_code == 501
    payload = response.json()
    detail = payload.get("detail") or {}
    assert detail.get("code") == "FEATURE_DISABLED"
    assert "future enhancement" in (detail.get("message") or "").lower()

def test_from_google_drive_returns_not_implemented():
    client = TestClient(app)
    response = client.post(
        "/api/datasets/from-google-drive",
        json={
            "file_id": "drive-file-1",
            "file_name": "claims_sheet",
            "mime_type": "application/vnd.google-apps.spreadsheet",
            "name": "drive_dataset",
            "usage_intent": "training",
            "output_format": "csv",
            "privacy_mode": "safe_harbor",
        },
    )
    assert response.status_code == 501
    payload = response.json()
    detail = payload.get("detail") or {}
    assert detail.get("code") == "FEATURE_DISABLED"
    assert "future enhancement" in (detail.get("message") or "").lower()
