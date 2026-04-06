from __future__ import annotations

import httpx

from app.services.llm.ollama_client import OllamaClient, OllamaClientError, OllamaModelSelectionError
from app.services.llm.planner import (
    _build_model_catalog,
    get_ollama_provider_status,
    merge_plan_recommendations,
    normalize_cleanup_mode,
    plan_cleanup_with_ollama,
    profile_with_llm_assist,
    validate_planner_output,
)


class _DummyResponse:
    def __init__(self, payload, *, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError("http error")

    def json(self):
        return self._payload


def test_ollama_client_lists_models_and_generates_json(monkeypatch):
    def fake_get(_url, timeout):
        assert timeout == 5
        return _DummyResponse(
            {
                "models": [
                    {"name": "llama3.1:8b", "details": {"parameter_size": "8B"}},
                    {"name": "mistral:latest", "details": {"parameter_size": "7B"}},
                ]
            }
        )

    def fake_post(_url, json, timeout):
        assert json["format"] == "json"
        assert timeout == 5
        return _DummyResponse({"model": json["model"], "response": '{"summary":"ok","top_blockers":[]}'})

    monkeypatch.setattr("app.services.llm.ollama_client.httpx.get", fake_get)
    monkeypatch.setattr("app.services.llm.ollama_client.httpx.post", fake_post)

    client = OllamaClient(base_url="http://127.0.0.1:11434", timeout_seconds=5)
    assert client.list_model_metadata()[0]["parameter_size"] == "8B"
    assert client.list_models() == ["llama3.1:8b", "mistral:latest"]
    payload = client.generate_json(model="llama3.1:8b", prompt="hello", system_prompt="system")
    assert payload["model"] == "llama3.1:8b"
    assert payload["response"]["summary"] == "ok"


def test_ollama_client_raises_for_malformed_json(monkeypatch):
    def fake_post(_url, json, timeout):
        return _DummyResponse({"model": json["model"], "response": "{not-json}"})

    monkeypatch.setattr("app.services.llm.ollama_client.httpx.post", fake_post)
    client = OllamaClient(base_url="http://127.0.0.1:11434", timeout_seconds=5)
    try:
        client.generate_json(model="llama3.1:8b", prompt="hello")
    except OllamaClientError as exc:
        assert "malformed" in str(exc).lower()
    else:
        raise AssertionError("Expected OllamaClientError for malformed planner JSON.")


def test_ollama_client_raises_for_timeout(monkeypatch):
    def fake_post(_url, json, timeout):
        raise httpx.ReadTimeout("timeout")

    monkeypatch.setattr("app.services.llm.ollama_client.httpx.post", fake_post)
    client = OllamaClient(base_url="http://127.0.0.1:11434", timeout_seconds=5)
    try:
        client.generate_json(model="llama3.1:8b", prompt="hello")
    except OllamaClientError as exc:
        message = str(exc).lower()
        assert "timed out" in message
        assert "ollama_timeout_seconds" in message
    else:
        raise AssertionError("Expected OllamaClientError for timeout.")


def test_ollama_client_raises_for_unavailable_service(monkeypatch):
    def fake_get(_url, timeout):
        raise httpx.ConnectError("connect failed")

    monkeypatch.setattr("app.services.llm.ollama_client.httpx.get", fake_get)
    client = OllamaClient(base_url="http://127.0.0.1:11434", timeout_seconds=5)
    try:
        client.list_models()
    except OllamaClientError as exc:
        assert "unable to reach ollama" in str(exc).lower()
    else:
        raise AssertionError("Expected OllamaClientError when Ollama is unavailable.")


def test_build_model_catalog_filters_unsafe_models_and_ranks_safe_subset():
    catalog = _build_model_catalog(
        [
            {"name": "qwen2.5:32b", "details": {"parameter_size": "32B"}},
            {"name": "nomic-embed-text:latest"},
            {"name": "llama3.2-vision:11b"},
            {"name": "tinyllama:1.1b"},
            {"name": "qwen2.5:7b-instruct"},
        ]
    )

    assert catalog["installed_models"] == [
        "qwen2.5:32b",
        "nomic-embed-text:latest",
        "llama3.2-vision:11b",
        "tinyllama:1.1b",
        "qwen2.5:7b-instruct",
    ]
    assert catalog["models"][0] == "qwen2.5:7b-instruct"
    assert "tinyllama:1.1b" in catalog["models"]
    assert {item["name"] for item in catalog["filtered_models"]} == {
        "qwen2.5:32b",
        "nomic-embed-text:latest",
        "llama3.2-vision:11b",
    }
    assert any("14b" in item["reason"].lower() for item in catalog["filtered_models"] if item["name"] == "qwen2.5:32b")
    assert any("embedding" in item["reason"].lower() for item in catalog["filtered_models"] if item["name"] == "nomic-embed-text:latest")
    assert any("vision" in item["reason"].lower() or "multimodal" in item["reason"].lower() for item in catalog["filtered_models"] if item["name"] == "llama3.2-vision:11b")


def test_validate_planner_output_filters_unknown_columns_and_unsupported_values():
    payload = validate_planner_output(
        {
            "summary": "Use Safe Harbor",
            "top_blockers": ["PII present"],
            "semantic_overrides": {
                "phone": "phone",
                "unknown_col": "email",
                "member_id": "unsupported_hint",
            },
            "recommended_cleanup_options": {
                "privacy_mode": "safe_harbor",
                "performance_mode": "balanced",
                "output_format": "csv",
                "text_case": "weird_case",
            },
            "drop_candidates": ["notes", "missing_col"],
            "keep_priority_columns": ["member_id", "notes"],
        },
        available_columns=["member_id", "phone", "notes"],
    )

    assert payload["semantic_overrides"] == {"phone": "phone"}
    assert payload["recommended_options"]["privacy_mode"] == "safe_harbor"
    assert payload["recommended_options"]["performance_mode"] == "balanced"
    assert payload["recommended_options"]["output_format"] == "csv"
    assert payload["keep_priority_columns"] == ["member_id", "notes"]
    assert payload["drop_candidates"] == []
    assert any("unsupported" in note.lower() for note in payload["validation_notes"])


def test_validate_planner_output_ignores_invalid_nested_cleanup_values():
    payload = validate_planner_output(
        {
            "summary": "Keep stable fields",
            "recommended_cleanup_options": {
                "text_case": {"bad": "shape"},
                "performance_mode": False,
                "privacy_mode": "safe_harbor",
            },
        },
        available_columns=["member_id", "phone"],
    )

    assert payload["recommended_options"] == {"privacy_mode": "safe_harbor"}
    assert payload["acceptance_status"] == "partially_accepted"
    assert any("non-string cleanup option 'text_case'" in note for note in payload["validation_notes"])


def test_merge_plan_recommendations_respects_explicit_fields():
    merged = merge_plan_recommendations(
        {"privacy_mode": "none", "performance_mode": None},
        plan={"recommended_options": {"privacy_mode": "safe_harbor", "performance_mode": "fast"}},
        explicit_fields={"privacy_mode"},
    )
    assert merged["privacy_mode"] == "none"
    assert merged["performance_mode"] == "fast"


def test_get_ollama_provider_status_handles_unreachable(monkeypatch):
    class _BrokenClient:
        def list_model_metadata(self):
            raise OllamaClientError("boom")

    monkeypatch.setattr("app.services.llm.planner._client", lambda: _BrokenClient())
    status = get_ollama_provider_status("llama3.1:8b")
    assert status["provider_contract_version"] == 2
    assert status["enabled"] is True
    assert status["reachable"] is False
    assert status["selected_model"] == "llama3.1:8b"


def test_get_ollama_provider_status_marks_missing_requested_model(monkeypatch):
    class _ClientWithDifferentModels:
        def list_model_metadata(self):
            return [{"name": "mistral:latest", "parameter_size": "7B"}]

    monkeypatch.setattr("app.services.llm.planner._client", lambda: _ClientWithDifferentModels())
    status = get_ollama_provider_status("llama3.1:8b")
    assert status["provider_contract_version"] == 2
    assert status["reachable"] is True
    assert status["selected_model"] == "mistral:latest"
    assert status["requested_model"] == "llama3.1:8b"
    assert status["requested_model_available"] is False
    assert status["requested_model_installed"] is False
    assert status["requested_model_selectable"] is False
    assert "not installed" in str(status["error"]).lower()


def test_get_ollama_provider_status_prefers_reasonable_default_model(monkeypatch):
    class _MixedModelClient:
        def list_model_metadata(self):
            return [
                {"name": "qwen2.5:32b", "parameter_size": "32B"},
                {"name": "nomic-embed-text:latest", "parameter_size": "274M"},
                {"name": "llama3.2-vision:11b", "parameter_size": "11B", "family": "mllama"},
                {"name": "qwen2.5:7b-instruct", "parameter_size": "7B"},
                {"name": "tinyllama:1.1b", "parameter_size": "1.1B"},
            ]

    monkeypatch.setattr("app.services.llm.planner._client", lambda: _MixedModelClient())
    status = get_ollama_provider_status()
    assert status["provider_contract_version"] == 2
    assert status["selected_model"] == "qwen2.5:7b-instruct"
    assert status["models"][0] == "qwen2.5:7b-instruct"
    assert status["installed_models"][0] == "qwen2.5:32b"
    assert any(item["name"] == "qwen2.5:32b" for item in status["filtered_models"])
    assert any(item["name"] == "nomic-embed-text:latest" for item in status["filtered_models"])


def test_plan_cleanup_with_ollama_rejects_missing_requested_model(monkeypatch):
    monkeypatch.setattr(
        "app.services.llm.planner.get_ollama_provider_status",
        lambda requested_model=None: {
            "enabled": True,
            "reachable": True,
            "provider": "ollama",
            "base_url": "http://127.0.0.1:11434",
            "selected_model": None,
            "requested_model": requested_model,
            "requested_model_available": False,
            "models": ["mistral:latest"],
            "error": f"Requested Ollama model '{requested_model}' is not installed locally.",
        },
    )

    try:
        plan_cleanup_with_ollama(
            profile={"columns": [{"clean_name": "member_id"}]},
            sample_rows=[],
            usage_intent="training",
            output_format="csv",
            privacy_mode="safe_harbor",
            requested_model="llama3.1:8b",
        )
    except OllamaModelSelectionError as exc:
        assert "not installed locally" in str(exc).lower()
    else:
        raise AssertionError("Expected OllamaModelSelectionError for missing requested model.")


def test_plan_cleanup_with_ollama_rejects_installed_but_filtered_model(monkeypatch):
    monkeypatch.setattr(
        "app.services.llm.planner.get_ollama_provider_status",
        lambda requested_model=None: {
            "enabled": True,
            "reachable": True,
            "provider": "ollama",
            "base_url": "http://127.0.0.1:11434",
            "selected_model": None,
            "requested_model": requested_model,
            "requested_model_installed": True,
            "requested_model_selectable": False,
            "requested_model_available": False,
            "installed_models": ["qwen2.5:32b"],
            "models": [],
            "filtered_models": [
                {
                    "name": "qwen2.5:32b",
                    "reason": "Model is 32B and exceeds the 14B planner-safe limit.",
                }
            ],
            "error": "Requested Ollama model 'qwen2.5:32b' is installed locally but not eligible for planner-safe cleanup: Model is 32B and exceeds the 14B planner-safe limit.",
        },
    )

    try:
        plan_cleanup_with_ollama(
            profile={"columns": [{"clean_name": "member_id"}]},
            sample_rows=[],
            usage_intent="training",
            output_format="csv",
            privacy_mode="safe_harbor",
            requested_model="qwen2.5:32b",
        )
    except OllamaModelSelectionError as exc:
        message = str(exc).lower()
        assert "not eligible for planner-safe cleanup" in message or "not supported for assisted cleanup" in message
    else:
        raise AssertionError("Expected OllamaModelSelectionError for filtered requested model.")


def test_profile_with_llm_assist_attaches_summary():
    payload = profile_with_llm_assist(
        {"columns": [], "row_count": 10},
        {"provider": "ollama", "model": "llama3.1:8b", "status": "validated", "summary": "Keep notes"},
    )
    assert payload["llm_assist"]["provider"] == "ollama"
    assert payload["llm_assist"]["model"] == "llama3.1:8b"
    assert payload["llm_assist"]["summary"] == "Keep notes"


def test_normalize_cleanup_mode_defaults_to_deterministic():
    assert normalize_cleanup_mode("ollama_assisted") == "ollama_assisted"
    assert normalize_cleanup_mode("deterministic") == "deterministic"
    assert normalize_cleanup_mode("unexpected") == "deterministic"
