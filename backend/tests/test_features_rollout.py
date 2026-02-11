import io
from typing import Any

from fastapi.testclient import TestClient

from app.main import app


def _create_dataset(client: TestClient, csv_payload: str, *, name: str) -> dict[str, Any]:
    files = {"file": ("rollout_test.csv", io.BytesIO(csv_payload.encode("utf-8")), "text/csv")}
    data = {
        "name": name,
        "usage_intent": "training",
        "output_format": "csv",
        "privacy_mode": "safe_harbor",
    }
    create_resp = client.post("/api/datasets", files=files, data=data)
    assert create_resp.status_code == 200
    return create_resp.json()


def test_feature_registry_endpoint_returns_rollout_catalog():
    client = TestClient(app)
    response = client.get("/api/features")
    assert response.status_code == 200
    payload = response.json()
    summary = payload.get("summary") or {}
    assert summary.get("total") == 50
    assert summary.get("filtered_total") == 50
    assert str(summary.get("by_wave", {}).get("1")) == "5"


def test_feature_registry_filtering_by_wave_and_status():
    client = TestClient(app)
    response = client.get("/api/features?wave=1&status=in_progress")
    assert response.status_code == 200
    payload = response.json()
    features = payload.get("features") or []
    assert len(features) == 5
    assert all(int(item["wave"]) == 1 for item in features)
    assert all(str(item["status"]) == "in_progress" for item in features)


def test_autopilot_cleanup_endpoint_runs_cleaning_pipeline():
    client = TestClient(app)
    dataset = _create_dataset(
        client,
        "patient_id,dob,paid_amount,note\n1,1980-01-01,100,Needs follow up\n2,1981-01-01,-50,Call member\n",
        name="autopilot_test",
    )

    response = client.post(
        f"/api/datasets/{dataset['id']}/cleanup/autopilot",
        json={"target_score": 95},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload.get("run", {}).get("status") == "completed"
    assert isinstance(payload.get("autopilot"), dict)
    autopilot = payload["autopilot"]
    assert autopilot.get("target_score") == 95
    assert autopilot.get("status") in {"on_track", "needs_attention"}
    assert isinstance(autopilot.get("resolved_options"), dict)
    assert isinstance(autopilot.get("preclean_top_blockers"), list)
    assert isinstance(autopilot.get("postclean_top_blockers"), list)

