from __future__ import annotations

import json
from typing import Any

import httpx


class OllamaClientError(RuntimeError):
    """Raised when the local Ollama API cannot satisfy a request."""


class OllamaModelSelectionError(OllamaClientError):
    """Raised when a requested local model is installed but unsupported for this planner."""


class OllamaClient:
    def __init__(self, *, base_url: str, timeout_seconds: float) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_seconds = max(1.0, float(timeout_seconds or 20.0))

    def _url(self, path: str) -> str:
        suffix = path if path.startswith("/") else f"/{path}"
        return f"{self.base_url}{suffix}"

    def list_model_metadata(self) -> list[dict[str, Any]]:
        try:
            response = httpx.get(self._url("/api/tags"), timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as exc:
            raise OllamaClientError(f"Unable to reach Ollama at {self.base_url}.") from exc
        except ValueError as exc:
            raise OllamaClientError("Ollama returned invalid JSON for model listing.") from exc

        models = payload.get("models") if isinstance(payload, dict) else []
        if not isinstance(models, list):
            return []

        items: list[dict[str, Any]] = []
        for item in models:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            details = item.get("details") if isinstance(item.get("details"), dict) else {}
            families = details.get("families") if isinstance(details.get("families"), list) else []
            items.append(
                {
                    "name": name,
                    "model": str(item.get("model") or name).strip(),
                    "family": str(details.get("family") or "").strip() or None,
                    "families": [str(value).strip() for value in families if str(value).strip()],
                    "parameter_size": str(details.get("parameter_size") or "").strip() or None,
                    "quantization_level": str(details.get("quantization_level") or "").strip() or None,
                }
            )
        return items

    def list_models(self) -> list[str]:
        return [item.get("name", "") for item in self.list_model_metadata() if item.get("name")]

    def generate_json(
        self,
        *,
        model: str,
        prompt: str,
        system_prompt: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.1,
                "num_predict": 400,
            },
        }
        if system_prompt:
            payload["system"] = system_prompt

        try:
            response = httpx.post(
                self._url("/api/generate"),
                json=payload,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
        except httpx.ReadTimeout as exc:
            raise OllamaClientError(
                f"Ollama timed out after {self.timeout_seconds:.0f}s while loading or generating with model "
                f"'{model}'. Retry after the model warms up, choose a smaller model, or increase "
                "OLLAMA_TIMEOUT_SECONDS."
            ) from exc
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            raise OllamaClientError(
                f"Ollama generation request failed for model '{model}' with HTTP {status_code}."
            ) from exc
        except httpx.HTTPError as exc:
            raise OllamaClientError(
                f"Ollama generation request failed for model '{model}'."
            ) from exc
        except ValueError as exc:
            raise OllamaClientError("Ollama returned invalid JSON for generation.") from exc

        text = data.get("response") if isinstance(data, dict) else None
        if not isinstance(text, str) or not text.strip():
            raise OllamaClientError("Ollama returned an empty planner response.")

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise OllamaClientError("Ollama returned malformed planner JSON.") from exc

        if not isinstance(parsed, dict):
            raise OllamaClientError("Ollama planner response must be a JSON object.")

        return {
            "model": str(data.get("model") or model),
            "response": parsed,
            "response_text": text,
        }
