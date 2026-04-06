import io
from typing import Any

from fastapi.testclient import TestClient

from app.main import app


def _create_dataset(client: TestClient, csv_payload: str, *, name: str) -> dict[str, Any]:
    files = {"file": ("run_test.csv", io.BytesIO(csv_payload.encode("utf-8")), "text/csv")}
    data = {
        "name": name,
        "usage_intent": "training",
        "output_format": "csv",
        "privacy_mode": "safe_harbor",
    }
    create_resp = client.post("/api/datasets", files=files, data=data)
    assert create_resp.status_code == 200
    return create_resp.json()


def _extract_profile(payload: dict[str, Any]) -> dict[str, Any] | None:
    profile = payload.get("profile")
    if isinstance(profile, dict):
        return profile

    dataset = payload.get("dataset")
    if isinstance(dataset, dict) and isinstance(dataset.get("profile"), dict):
        return dataset["profile"]

    return None


def _post_recompute_assessment(client: TestClient, dataset_id: str):
    candidate_paths = [
        f"/api/datasets/{dataset_id}/assessment/recompute",
        f"/api/datasets/{dataset_id}/assessment:recompute",
        f"/api/datasets/{dataset_id}/recompute-assessment",
        f"/api/datasets/{dataset_id}/recompute_assessment",
    ]

    tried: list[str] = []
    for path in candidate_paths:
        resp = client.post(path, json={})
        if resp.status_code == 422:
            resp = client.post(path)
        if resp.status_code not in {404, 405}:
            return resp
        tried.append(f"{path} -> {resp.status_code}")

    raise AssertionError(
        "Recompute assessment endpoint not found. Tried: " + ", ".join(tried)
    )


def _assert_detected_domains_shape(value: Any) -> None:
    assert isinstance(value, (list, dict))

    if isinstance(value, list):
        assert value
        first = value[0]
        assert isinstance(first, dict)
        assert "domain" in first
        assert "score" in first
        assert "confidence" in first
        assert "evidence_columns" in first
        assert 0.0 <= float(first["confidence"]) <= 1.0
        assert isinstance(first["evidence_columns"], list)
        return

    assert any(key in value for key in {"primary", "domains", "scores"})


def test_recompute_assessment_updates_profile_and_returns_decision_fields():
    client = TestClient(app)

    csv_payload = (
        "claim_id,member_id,service_date,paid_amount,diagnosis_code\n"
        "c1,m1,2025-01-01,100,A10\n"
        "c2,m2,2025-01-02,200,B20\n"
    )
    dataset = _create_dataset(client, csv_payload, name="recompute_assessment_test")
    dataset_id = dataset["id"]

    recompute_resp = _post_recompute_assessment(client, dataset_id)
    assert recompute_resp.status_code == 200
    recompute_payload = recompute_resp.json()

    recomputed_profile = _extract_profile(recompute_payload)
    assert isinstance(recomputed_profile, dict)
    assert isinstance(recomputed_profile.get("preclean_decision"), dict)
    decision = recomputed_profile["preclean_decision"]
    assert decision["status"] in {"ready", "needs_review", "blocked"}
    assert isinstance(decision.get("reasons"), list)
    assert isinstance(decision.get("actions"), list)
    assert recomputed_profile.get("primary_domain")
    _assert_detected_domains_shape(recomputed_profile.get("detected_domains"))

    assert "assessment" in recomputed_profile
    assert "domains" in recomputed_profile

    get_resp = client.get(f"/api/datasets/{dataset_id}")
    assert get_resp.status_code == 200
    persisted_profile = (get_resp.json().get("profile") or {})

    assert isinstance(persisted_profile.get("preclean_decision"), dict)
    persisted_decision = persisted_profile["preclean_decision"]
    assert persisted_decision["status"] in {"ready", "needs_review", "blocked"}
    assert isinstance(persisted_decision.get("reasons"), list)
    assert isinstance(persisted_decision.get("actions"), list)
    assert persisted_profile.get("primary_domain")
    _assert_detected_domains_shape(persisted_profile.get("detected_domains"))
    assert "assessment" in persisted_profile
    assert "domains" in persisted_profile


def test_clean_creates_persisted_run_with_outcomes():
    client = TestClient(app)

    csv_payload = "patient_id,dob,paid_amount\n1,1980-01-01,100\n2,1981-01-01,-50\n"
    dataset = _create_dataset(client, csv_payload, name="run_test")
    dataset_id = dataset["id"]

    clean_options = {
        "remove_duplicates": True,
        "drop_empty_columns": True,
        "privacy_mode": "safe_harbor",
        "normalize_phone": True,
        "normalize_zip": True,
        "normalize_gender": True,
        "text_case": "none",
        "output_format": "csv",
        "coercion_mode": "safe",
        "performance_mode": "balanced",
    }
    clean_resp = client.post(f"/api/datasets/{dataset_id}/clean", json=clean_options)
    assert clean_resp.status_code == 200
    payload = clean_resp.json()
    assert "run" in payload
    assert payload["run"]["status"] == "completed"
    assert "outcomes" in payload["qc"]
    assert "quality_gate" in payload["qc"]
    assert "rag_readiness_comparison" in payload["qc"]
    assert isinstance(payload["qc"].get("postclean_decision"), dict)
    assert isinstance(payload["qc"].get("change_summary"), dict)
    postclean = payload["qc"]["postclean_decision"]
    assert postclean["status"] in {"pass", "warn", "fail"}
    assert isinstance(postclean.get("release_recommendation"), str)
    assert isinstance(postclean.get("blockers"), list)
    assert isinstance(postclean.get("actions"), list)
    change_summary = payload["qc"]["change_summary"]
    assert isinstance(change_summary.get("column_renames"), dict)
    assert isinstance(change_summary.get("type_conversions"), dict)
    assert isinstance(change_summary.get("normalizations"), dict)
    assert isinstance(change_summary.get("changed_fields_count"), int)
    assert isinstance(change_summary.get("columns_removed"), list)
    assert isinstance(change_summary.get("cols_removed"), int)
    assert isinstance(change_summary.get("row_level_diffs"), list)
    assert len(change_summary.get("row_level_diffs", [])) >= 5
    assert "row_count_raw" in payload["qc"]
    assert "row_count_cleaned" in payload["qc"]
    run_id = payload["run"]["id"]

    runs_resp = client.get(f"/api/datasets/{dataset_id}/runs")
    assert runs_resp.status_code == 200
    runs_payload = runs_resp.json()
    assert runs_payload["items"]
    assert any(item["id"] == run_id for item in runs_payload["items"])

    outcome_resp = client.get(f"/api/runs/{run_id}/outcomes")
    assert outcome_resp.status_code == 200
    outcome_payload = outcome_resp.json()
    assert outcome_payload["run_id"] == run_id
    assert outcome_payload["dataset_id"] == dataset_id
    assert "status" in outcome_payload
    assert "quality_gate" in outcome_payload
    assert isinstance(outcome_payload["outcomes"], list)

    run_resp = client.get(f"/api/runs/{run_id}")
    assert run_resp.status_code == 200
    run_payload = run_resp.json()
    assert "outcomes" in run_payload
    assert "quality_gate" in run_payload
    assert "rag_readiness" in run_payload
    assert "qc" in run_payload
    assert "rag_readiness_comparison" in (run_payload["qc"] or {})
    assert isinstance((run_payload["qc"] or {}).get("postclean_decision"), dict)
    assert isinstance((run_payload["qc"] or {}).get("change_summary"), dict)


def test_chunked_complete_upload_accepts_mixed_timezone_dates():
    client = TestClient(app)

    csv_payload = (
        "encounter_date,dob,paid_amount\n"
        "2026-01-01T12:00:00Z,1980-01-01,100\n"
        "2026-01-02 08:30:00,2099-01-01,-50\n"
        "2026-01-03T04:00:00-0500,1975-07-12,70\n"
    )
    csv_bytes = csv_payload.encode("utf-8")

    start_payload = {
        "filename": "mixed_timezone_upload.csv",
        "name": "mixed_timezone_upload",
        "usage_intent": "training",
        "output_format": "csv",
        "privacy_mode": "safe_harbor",
        "fileSize": len(csv_bytes),
        "totalChunks": 1,
    }
    start_resp = client.post("/api/uploads/start", json=start_payload)
    assert start_resp.status_code == 200
    upload_id = start_resp.json()["upload_id"]

    chunk_resp = client.post(
        f"/api/uploads/{upload_id}/chunk",
        data={"index": "0"},
        files={"chunk": ("chunk_0", io.BytesIO(csv_bytes), "application/octet-stream")},
    )
    assert chunk_resp.status_code == 200

    complete_resp = client.post(f"/api/uploads/{upload_id}/complete")
    assert complete_resp.status_code == 200
    dataset = complete_resp.json()
    assert dataset["id"]
    assert dataset["status"] == "ingested"
    assert (dataset.get("profile") or {}).get("row_count", 0) >= 3


def test_legacy_clean_supports_ollama_assisted_mode(monkeypatch):
    from app.api import datasets as datasets_api

    monkeypatch.setattr(
        datasets_api,
        "plan_cleanup_with_ollama",
        lambda **kwargs: {
            "provider": "ollama",
            "model": kwargs.get("requested_model") or "llama3.1:8b",
            "status": "validated",
            "prompt_version": "test_prompt_v1",
            "summary": "Prefer Safe Harbor.",
            "explanation": "Synthetic planner result for legacy test.",
            "top_blockers": ["PII present"],
            "semantic_overrides": {"phone": "phone"},
            "drop_candidates": [],
            "keep_priority_columns": ["notes"],
            "recommended_options": {
                "privacy_mode": "safe_harbor",
                "performance_mode": "balanced",
                "output_format": "csv",
            },
            "validation_notes": [],
        },
    )

    client = TestClient(app)
    files = {"file": ("run_assisted.csv", io.BytesIO(b"member_id,notes,phone\nm1,Needs review,312-555-1111\n"), "text/csv")}
    create_resp = client.post(
        "/api/datasets",
        files=files,
        data={
            "name": "legacy_assisted",
            "usage_intent": "training",
            "cleanup_mode": "ollama_assisted",
            "llm_model": "llama3.1:8b",
        },
    )
    assert create_resp.status_code == 200
    dataset = create_resp.json()
    assert dataset["cleanup_mode"] == "ollama_assisted"
    assert dataset["llm_model"] == "llama3.1:8b"

    clean_resp = client.post(
        f"/api/datasets/{dataset['id']}/clean",
        json={"cleanup_mode": "ollama_assisted", "llm_model": "llama3.1:8b"},
    )
    assert clean_resp.status_code == 200
    payload = clean_resp.json()
    assert payload["run"]["cleanup_mode"] == "ollama_assisted"
    assert payload["run"]["llm_provider"] == "ollama"
    assert payload["run"]["llm_model"] == "llama3.1:8b"
    assert isinstance(payload["run"]["llm_plan"], dict)


def test_legacy_create_dataset_returns_503_when_ollama_is_unavailable(monkeypatch):
    from app.api import datasets as datasets_api
    from app.services.llm.ollama_client import OllamaClientError

    def _raise_unavailable(**kwargs):
        raise OllamaClientError("Ollama is down.")

    monkeypatch.setattr(datasets_api, "plan_cleanup_with_ollama", _raise_unavailable)

    client = TestClient(app)
    files = {"file": ("run_assisted.csv", io.BytesIO(b"member_id,notes\nm1,Needs review\n"), "text/csv")}
    create_resp = client.post(
        "/api/datasets",
        files=files,
        data={
            "name": "legacy_assisted_unavailable",
            "usage_intent": "training",
            "cleanup_mode": "ollama_assisted",
            "llm_model": "llama3.1:8b",
        },
    )
    assert create_resp.status_code == 503
    detail = create_resp.json().get("detail") or {}
    assert detail.get("code") == "OLLAMA_UNAVAILABLE"


def test_legacy_create_dataset_returns_400_when_requested_model_is_filtered(monkeypatch):
    from app.api import datasets as datasets_api
    from app.services.llm.ollama_client import OllamaModelSelectionError

    def _raise_unsupported(**kwargs):
        raise OllamaModelSelectionError(
            "Requested Ollama model 'qwen2.5:32b' is installed locally but not eligible for planner-safe cleanup: Model is 32B and exceeds the 14B planner-safe limit."
        )

    monkeypatch.setattr(datasets_api, "plan_cleanup_with_ollama", _raise_unsupported)

    client = TestClient(app)
    files = {"file": ("run_assisted.csv", io.BytesIO(b"member_id,notes\nm1,Needs review\n"), "text/csv")}
    create_resp = client.post(
        "/api/datasets",
        files=files,
        data={
            "name": "legacy_assisted_filtered",
            "usage_intent": "training",
            "cleanup_mode": "ollama_assisted",
            "llm_model": "qwen2.5:32b",
        },
    )
    assert create_resp.status_code == 400
    detail = create_resp.json().get("detail") or {}
    assert detail.get("code") == "OLLAMA_MODEL_UNSUPPORTED"
    assert "not eligible for planner-safe cleanup" in str(detail.get("message", "")).lower()
