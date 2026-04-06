import io

from fastapi.testclient import TestClient

from app.main import app


def _upload_workflow(client: TestClient, csv_payload: str, *, name: str) -> dict:
    files = {"file": ("workflow_test.csv", io.BytesIO(csv_payload.encode("utf-8")), "text/csv")}
    data = {
        "name": name,
        "usage_intent": "training",
    }
    response = client.post("/api/v2/workflows/upload", files=files, data=data)
    assert response.status_code == 200
    return response.json()


def _fake_plan(model: str = "llama3.1:8b") -> dict:
    return {
        "provider": "ollama",
        "model": model,
        "status": "validated",
        "prompt_version": "test_prompt_v1",
        "summary": "Use Safe Harbor and preserve notes.",
        "explanation": "Synthetic planner result for tests.",
        "top_blockers": ["PII present"],
        "semantic_overrides": {"phone": "phone"},
        "drop_candidates": ["scratch_text"],
        "keep_priority_columns": ["notes"],
        "recommended_options": {
            "privacy_mode": "safe_harbor",
            "performance_mode": "balanced",
            "output_format": "csv",
        },
        "validation_notes": [],
    }


def test_health_reports_workflow_metadata_and_capabilities(monkeypatch):
    from app.api import datasets as datasets_api

    monkeypatch.setattr(datasets_api, "get_ollama_provider_status", lambda requested_model=None: {
        "provider_contract_version": 2,
        "enabled": True,
        "reachable": True,
        "provider": "ollama",
        "base_url": "http://127.0.0.1:11434",
        "selected_model": requested_model or "llama3.1:8b",
        "requested_model": requested_model,
        "requested_model_available": True if requested_model else None,
        "requested_model_installed": True if requested_model else None,
        "requested_model_selectable": True if requested_model else None,
        "models": ["llama3.1:8b"],
        "installed_models": ["llama3.1:8b"],
        "filtered_models": [],
        "error": None,
    })
    client = TestClient(app)
    response = client.get("/api/health")
    assert response.status_code == 200

    payload = response.json()
    assert payload.get("status") == "ok"
    assert payload.get("service") == "hc-data-cleanup-ai"
    assert payload.get("ui_workflow_version") in {"v2_legacy", "v3_guided"}

    capabilities = payload.get("capabilities") or {}
    assert capabilities.get("workflow_v2_routes") is True
    assert capabilities.get("workflow_v2_enabled") is True
    assert capabilities.get("workflow_v2_active") in {True, False}
    assert capabilities.get("upload_cancellation") is True
    assert capabilities.get("clean_job_cancellation") is True
    assert capabilities.get("supported_sources") == ["file_upload"]
    assert capabilities.get("cleanup_modes") == ["deterministic", "ollama_assisted"]
    ollama_provider = (payload.get("providers") or {}).get("ollama", {})
    assert ollama_provider.get("provider_contract_version") == 2
    assert ollama_provider.get("reachable") is True
    assert ollama_provider.get("installed_models") == ["llama3.1:8b"]
    assert ollama_provider.get("filtered_models") == []


def test_provider_discovery_route_returns_ollama_metadata(monkeypatch):
    from app.api import providers as providers_api

    monkeypatch.setattr(providers_api, "get_ollama_provider_status", lambda requested_model=None: {
        "provider_contract_version": 2,
        "enabled": True,
        "reachable": True,
        "provider": "ollama",
        "base_url": "http://127.0.0.1:11434",
        "selected_model": requested_model or "llama3.1:8b",
        "requested_model": requested_model,
        "requested_model_available": True if requested_model else None,
        "requested_model_installed": True if requested_model else None,
        "requested_model_selectable": True if requested_model else None,
        "models": ["llama3.1:8b", "mistral:latest"],
        "installed_models": ["llama3.1:8b", "mistral:latest"],
        "filtered_models": [],
        "error": None,
    })

    client = TestClient(app)
    response = client.get("/api/providers/ollama/models", params={"requested_model": "llama3.1:8b"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["provider_contract_version"] == 2
    assert payload["provider"] == "ollama"
    assert payload["reachable"] is True
    assert payload["selected_model"] == "llama3.1:8b"
    assert payload["requested_model_available"] is True
    assert payload["requested_model_installed"] is True
    assert payload["requested_model_selectable"] is True
    assert payload["models"] == ["llama3.1:8b", "mistral:latest"]
    assert payload["installed_models"] == ["llama3.1:8b", "mistral:latest"]
    assert payload["filtered_models"] == []


def test_v2_workflow_list_route_accepts_with_and_without_trailing_slash():
    client = TestClient(app)

    res_no_slash = client.get("/api/v2/workflows")
    assert res_no_slash.status_code == 200
    payload_no_slash = res_no_slash.json()
    assert isinstance(payload_no_slash.get("items"), list)

    res_with_slash = client.get("/api/v2/workflows/")
    assert res_with_slash.status_code == 200
    payload_with_slash = res_with_slash.json()
    assert isinstance(payload_with_slash.get("items"), list)


def test_v2_workflow_upload_returns_precheck_state_and_next_actions():
    client = TestClient(app)
    workflow = _upload_workflow(
        client,
        (
            "claim_id,member_id,service_date,paid_amount,diagnosis_code\n"
            "c1,m1,2025-01-01,100,A10\n"
            "c2,m2,2025-01-02,200,B20\n"
        ),
        name="workflow_precheck",
    )

    assert workflow.get("workflow_id")
    assert workflow.get("dataset_id") == workflow.get("workflow_id")
    assert workflow.get("stage") in {"uploaded", "prechecked"}
    assert isinstance(workflow.get("next_actions"), list)

    precheck = workflow.get("precheck_summary") or {}
    assert isinstance(precheck, dict)
    assert "decision_status" in precheck
    assert "readiness_score" in precheck


def test_v2_workflow_autopilot_result_and_export_paths():
    client = TestClient(app)
    workflow = _upload_workflow(
        client,
        "patient_id,dob,paid_amount\n1,1980-01-01,120\n2,1981-01-01,-20\n",
        name="workflow_autopilot",
    )

    workflow_id = workflow["workflow_id"]
    run_response = client.post(
        f"/api/v2/workflows/{workflow_id}/autopilot-run",
        json={"target_score": 95, "output_format": "csv", "privacy_mode": "safe_harbor", "performance_mode": "balanced"},
    )
    assert run_response.status_code == 200

    run_payload = run_response.json()
    assert run_payload.get("stage") == "completed"
    assert isinstance(run_payload.get("result_summary"), dict)

    result_response = client.get(f"/api/v2/workflows/{workflow_id}/result")
    assert result_response.status_code == 200
    result_payload = result_response.json()
    assert result_payload.get("workflow_id") == workflow_id
    assert isinstance(result_payload.get("result_summary"), dict)
    assert isinstance(result_payload.get("qc"), dict)

    export_response = client.get(f"/api/v2/workflows/{workflow_id}/export")
    assert export_response.status_code == 200
    content_type = export_response.headers.get("content-type", "")
    assert any(token in content_type for token in ["text/csv", "application/octet-stream", "application/vnd.ms-excel"])


def test_v2_workflow_upload_and_run_support_ollama_assisted_mode(monkeypatch):
    from app.api import datasets as datasets_api

    monkeypatch.setattr(datasets_api, "plan_cleanup_with_ollama", lambda **kwargs: _fake_plan(kwargs.get("requested_model") or "llama3.1:8b"))

    client = TestClient(app)
    files = {"file": ("workflow_assisted.csv", io.BytesIO(b"member_id,notes,phone\nm1,Needs review,312-555-1111\n"), "text/csv")}
    data = {
        "name": "workflow_assisted",
        "usage_intent": "training",
        "cleanup_mode": "ollama_assisted",
        "llm_model": "llama3.1:8b",
    }
    upload_response = client.post("/api/v2/workflows/upload", files=files, data=data)
    assert upload_response.status_code == 200
    workflow = upload_response.json()
    assert workflow.get("execution", {}).get("cleanup_mode") == "ollama_assisted"
    assert workflow.get("execution", {}).get("llm_model") == "llama3.1:8b"
    assert (workflow.get("dataset") or {}).get("cleanup_mode") == "ollama_assisted"
    assert ((workflow.get("dataset") or {}).get("profile") or {}).get("llm_assist", {}).get("summary")

    workflow_id = workflow["workflow_id"]
    run_response = client.post(
        f"/api/v2/workflows/{workflow_id}/autopilot-run",
        json={
            "target_score": 95,
            "cleanup_mode": "ollama_assisted",
            "llm_model": "llama3.1:8b",
        },
    )
    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload.get("execution", {}).get("cleanup_mode") == "ollama_assisted"
    assert run_payload.get("execution", {}).get("llm_model") == "llama3.1:8b"
    latest_run = (run_payload.get("dataset") or {}).get("latest_run") or {}
    assert latest_run.get("cleanup_mode") == "ollama_assisted"
    assert latest_run.get("llm_model") == "llama3.1:8b"
    assert isinstance(latest_run.get("llm_plan"), dict)


def test_v2_workflow_upload_rejects_filtered_requested_model(monkeypatch):
    from app.api import datasets as datasets_api
    from app.services.llm.ollama_client import OllamaModelSelectionError

    def _raise_filtered(**kwargs):
        raise OllamaModelSelectionError(
            "Installed Ollama model 'qwen2.5:32b' is not supported for assisted cleanup in this app: larger than 14B. "
            "Choose a locally installed text-generation model up to 14B."
        )

    monkeypatch.setattr(datasets_api, "plan_cleanup_with_ollama", _raise_filtered)

    client = TestClient(app)
    files = {"file": ("workflow_filtered.csv", io.BytesIO(b"member_id,notes\nm1,Needs review\n"), "text/csv")}
    data = {
        "name": "workflow_filtered",
        "usage_intent": "training",
        "cleanup_mode": "ollama_assisted",
        "llm_model": "qwen2.5:32b",
    }
    response = client.post("/api/v2/workflows/upload", files=files, data=data)
    assert response.status_code == 400
    detail = response.json().get("detail") or {}
    assert detail.get("code") == "OLLAMA_MODEL_UNSUPPORTED"
    assert "up to 14b" in str(detail.get("message", "")).lower()
