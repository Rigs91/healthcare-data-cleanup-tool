from __future__ import annotations

import json
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.config import (
    OLLAMA_BASE_URL,
    OLLAMA_ENABLED,
    OLLAMA_MODEL,
    OLLAMA_TIMEOUT_SECONDS,
)
from app.services.llm.ollama_client import (
    OllamaClient,
    OllamaClientError,
    OllamaModelSelectionError,
)
from app.services.profiling import SEMANTIC_HINTS

OLLAMA_PROVIDER_NAME = "ollama"
OLLAMA_PROVIDER_CONTRACT_VERSION = 2
PLANNER_PROMPT_VERSION = "ollama_cleanup_planner_v1"
SUPPORTED_CLEANUP_MODES = {"deterministic", "ollama_assisted"}
SUPPORTED_OUTPUT_FORMATS = {"csv", "jsonl", "parquet"}
SUPPORTED_PRIVACY_MODES = {"none", "safe_harbor"}
SUPPORTED_PERFORMANCE_MODES = {"balanced", "fast", "ultra_fast"}
SUPPORTED_TEXT_CASES = {"none", "lower", "upper", "title"}
SUPPORTED_SEMANTIC_HINTS = set(SEMANTIC_HINTS)
MODEL_SIZE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*b")
NON_TEXT_MODEL_TOKENS = ("embed", "embedding", "vision", "-vl", ":vl")
MAX_PLANNER_SAFE_SIZE_B = 14.0


class PlannerCleanupOptions(BaseModel):
    model_config = ConfigDict(extra="ignore")

    remove_duplicates: bool | None = None
    drop_empty_columns: bool | None = None
    normalize_phone: bool | None = None
    normalize_zip: bool | None = None
    normalize_gender: bool | None = None
    text_case: str | None = None
    output_format: str | None = None
    privacy_mode: str | None = None
    performance_mode: str | None = None


class PlannerResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    summary: str | None = None
    explanation: str | None = None
    top_blockers: list[str] = Field(default_factory=list)
    semantic_overrides: dict[str, str] = Field(default_factory=dict)
    drop_candidates: list[str] = Field(default_factory=list)
    keep_priority_columns: list[str] = Field(default_factory=list)
    recommended_cleanup_options: PlannerCleanupOptions = Field(default_factory=PlannerCleanupOptions)


def normalize_cleanup_mode(value: str | None, *, default: str = "deterministic") -> str:
    candidate = str(value or "").strip().lower()
    if candidate in SUPPORTED_CLEANUP_MODES:
        return candidate
    return default


def _client() -> OllamaClient:
    return OllamaClient(base_url=OLLAMA_BASE_URL, timeout_seconds=OLLAMA_TIMEOUT_SECONDS)


def _parse_parameter_size_b(value: Any) -> float | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    match = MODEL_SIZE_PATTERN.search(text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _entry_name(entry: dict[str, Any]) -> str:
    return str(entry.get("name") or "").strip()


def _entry_name_lower(entry: dict[str, Any]) -> str:
    return _entry_name(entry).lower()


def _entry_family_tokens(entry: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    family = str(entry.get("family") or "").strip().lower()
    if family:
        tokens.add(family)
    for item in entry.get("families") or []:
        token = str(item or "").strip().lower()
        if token:
            tokens.add(token)
    return tokens


def _entry_size_b(entry: dict[str, Any]) -> float | None:
    size_value = _parse_parameter_size_b(entry.get("parameter_size"))
    if size_value is not None:
        return size_value
    return _parse_parameter_size_b(_entry_name(entry))


def _model_selection_score(entry: dict[str, Any]) -> tuple[int, float, str]:
    normalized = _entry_name_lower(entry)
    score = 0
    size_bias = 0.0

    if any(token in normalized for token in NON_TEXT_MODEL_TOKENS):
        score -= 100
    if "instruct" in normalized:
        score += 40
    if "latest" in normalized:
        score += 10
    if "coder" in normalized:
        score += 8

    size_value = _entry_size_b(entry)
    if size_value is not None:
        if 6 <= size_value <= 10:
            score += 30
            size_bias = 8
        elif 10 < size_value <= MAX_PLANNER_SAFE_SIZE_B:
            score += 24
            size_bias = 6
        elif 3 <= size_value < 6:
            score += 18
            size_bias = 5
        elif 1 <= size_value < 3:
            score += 10
            size_bias = 3
        elif size_value > MAX_PLANNER_SAFE_SIZE_B:
            score -= 20
            size_bias = -2

    return score, size_bias, normalized


def _model_filter_reason(entry: dict[str, Any]) -> str | None:
    normalized_name = _entry_name_lower(entry)
    family_tokens = _entry_family_tokens(entry)

    if any(token in normalized_name for token in ("embed", "embedding")):
        return "embedding model"

    if any(token in normalized_name for token in ("vision", "-vl", ":vl")):
        return "vision or multimodal model"

    if any(
        token == "mllama"
        or "vision" in token
        or token.endswith("vl")
        or "vlmoe" in token
        for token in family_tokens
    ):
        return "vision or multimodal model"

    size_value = _entry_size_b(entry)
    if size_value is not None and size_value > MAX_PLANNER_SAFE_SIZE_B:
        return f"larger than {int(MAX_PLANNER_SAFE_SIZE_B)}B"

    return None


def _classify_models(
    model_entries: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, str]]]:
    installed_models: list[str] = []
    selectable_entries: list[dict[str, Any]] = []
    filtered_models: list[dict[str, str]] = []
    seen: set[str] = set()

    for entry in model_entries:
        clean_name = _entry_name(entry)
        lowered = clean_name.lower()
        if not clean_name or lowered in seen:
            continue
        seen.add(lowered)
        installed_models.append(clean_name)
        reason = _model_filter_reason(entry)
        if reason:
            filtered_models.append({"name": clean_name, "reason": reason})
            continue
        selectable_entries.append(entry)

    return installed_models, selectable_entries, filtered_models


def _rank_models(model_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        model_entries,
        key=_model_selection_score,
        reverse=True,
    )


def _build_model_catalog(model_entries: list[dict[str, Any]]) -> dict[str, Any]:
    installed_models, selectable_entries, filtered_models = _classify_models(model_entries)
    ranked_entries = _rank_models(selectable_entries)
    models = [_entry_name(entry) for entry in ranked_entries]
    return {
        "installed_models": installed_models,
        "models": models,
        "filtered_models": filtered_models,
        "selected_model": models[0] if models else None,
    }


def _compact_notes(value: Any) -> list[str]:
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
    elif value is None:
        items = []
    else:
        items = [str(value).strip()]
    return [item[:120] for item in items[:2]]


def _sample_rows_payload(sample_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not isinstance(sample_rows, list):
        return []
    trimmed: list[dict[str, Any]] = []
    for row in sample_rows[:5]:
        if not isinstance(row, dict):
            continue
        trimmed.append({str(key): value for key, value in list(row.items())[:10]})
    return trimmed


def _planner_prompts(context: dict[str, Any]) -> tuple[str, str]:
    context_json = json.dumps(context, ensure_ascii=True, separators=(",", ":"), default=str)
    system_prompt = (
        "You are a cautious healthcare dataset cleanup planner. "
        "Return one JSON object only. Do not invent columns or unsupported options. "
        "If unsure, leave a field empty instead of guessing."
    )
    user_prompt = (
        "Return a JSON object with keys summary, explanation, top_blockers, semantic_overrides, "
        "drop_candidates, keep_priority_columns, and recommended_cleanup_options. "
        "recommended_cleanup_options may only use remove_duplicates, drop_empty_columns, normalize_phone, "
        "normalize_zip, normalize_gender, text_case, output_format, privacy_mode, and performance_mode. "
        "Keep summary and explanation concise. "
        f"Allowed semantic hints: {sorted(SUPPORTED_SEMANTIC_HINTS)}. "
        f"Context JSON: {context_json}"
    )
    return system_prompt, user_prompt


def get_ollama_provider_status(requested_model: str | None = None) -> dict[str, Any]:
    requested = str(requested_model or "").strip()
    configured_default = str(OLLAMA_MODEL or "").strip() or None
    selected = requested or configured_default

    if not OLLAMA_ENABLED:
        return {
            "provider_contract_version": OLLAMA_PROVIDER_CONTRACT_VERSION,
            "enabled": False,
            "reachable": False,
            "provider": OLLAMA_PROVIDER_NAME,
            "base_url": OLLAMA_BASE_URL,
            "selected_model": selected,
            "requested_model": requested or None,
            "requested_model_available": False if requested else None,
            "requested_model_installed": False if requested else None,
            "requested_model_selectable": False if requested else None,
            "models": [],
            "installed_models": [],
            "filtered_models": [],
            "error": "Ollama integration is disabled.",
        }

    try:
        catalog = _build_model_catalog(_client().list_model_metadata())
        installed_models = catalog["installed_models"]
        models = catalog["models"]
        filtered_models = catalog["filtered_models"]
        reachable = True
        error = None
    except OllamaClientError as exc:
        installed_models = []
        models = []
        filtered_models = []
        reachable = False
        error = str(exc)

    installed_lookup = {model.lower(): model for model in installed_models}
    selectable_lookup = {model.lower(): model for model in models}
    filtered_lookup = {item["name"].lower(): item["reason"] for item in filtered_models}
    requested_model_available = None
    requested_model_installed = None
    requested_model_selectable = None

    if requested and reachable:
        requested_lower = requested.lower()
        requested_model_installed = requested_lower in installed_lookup
        requested_model_selectable = requested_lower in selectable_lookup
        requested_model_available = requested_model_selectable
        if requested_model_selectable:
            selected = selectable_lookup[requested_lower]
        elif requested_model_installed:
            error = (
                f"Installed Ollama model '{installed_lookup[requested_lower]}' is not supported for assisted cleanup "
                f"in this app: {filtered_lookup.get(requested_lower) or 'not supported for this cleanup planner'}. "
                f"Choose a locally installed text-generation model up to {int(MAX_PLANNER_SAFE_SIZE_B)}B."
            )
            selected = models[0] if models else None
        else:
            error = f"Requested Ollama model '{requested}' is not installed locally."
            selected = models[0] if models else None
    elif configured_default and reachable:
        configured_lower = configured_default.lower()
        if configured_lower in selectable_lookup:
            selected = selectable_lookup[configured_lower]
        elif models:
            selected = models[0]
        else:
            selected = None
    elif reachable and models:
        selected = models[0]

    if reachable and not models and not error:
        error = (
            f"Ollama is reachable, but no planner-safe local models are installed. "
            f"Install a text-generation model up to {int(MAX_PLANNER_SAFE_SIZE_B)}B or use deterministic cleanup."
        )

    return {
        "provider_contract_version": OLLAMA_PROVIDER_CONTRACT_VERSION,
        "enabled": True,
        "reachable": reachable,
        "provider": OLLAMA_PROVIDER_NAME,
        "base_url": OLLAMA_BASE_URL,
        "selected_model": selected,
        "requested_model": requested or None,
        "requested_model_available": requested_model_available,
        "requested_model_installed": requested_model_installed,
        "requested_model_selectable": requested_model_selectable,
        "models": models,
        "installed_models": installed_models,
        "filtered_models": filtered_models,
        "error": error,
    }


def validate_planner_output(
    raw_payload: dict[str, Any],
    *,
    available_columns: list[str],
) -> dict[str, Any]:
    payload = raw_payload if isinstance(raw_payload, dict) else {}
    notes: list[str] = []

    try:
        parsed = PlannerResponse.model_validate(payload)
        summary = str(parsed.summary or "").strip()
        explanation = str(parsed.explanation or "").strip()
        top_blockers_raw = parsed.top_blockers
        semantic_raw = parsed.semantic_overrides
        drop_raw = parsed.drop_candidates
        keep_raw = parsed.keep_priority_columns
        recommended_raw = parsed.recommended_cleanup_options.model_dump(exclude_none=True)
    except ValidationError:
        notes.append("Planner output used unexpected value types; unsupported fields were ignored.")
        summary = str(payload.get("summary") or "").strip() if isinstance(payload.get("summary"), str) else ""
        explanation = str(payload.get("explanation") or "").strip() if isinstance(payload.get("explanation"), str) else ""
        top_blockers_raw = payload.get("top_blockers") if isinstance(payload.get("top_blockers"), list) else []
        semantic_raw = payload.get("semantic_overrides") if isinstance(payload.get("semantic_overrides"), dict) else {}
        drop_raw = payload.get("drop_candidates") if isinstance(payload.get("drop_candidates"), list) else []
        keep_raw = (
            payload.get("keep_priority_columns")
            if isinstance(payload.get("keep_priority_columns"), list)
            else []
        )
        recommended_raw = (
            payload.get("recommended_cleanup_options")
            if isinstance(payload.get("recommended_cleanup_options"), dict)
            else {}
        )

    available = {str(column) for column in available_columns}

    semantic_overrides: dict[str, str] = {}
    for column, semantic_hint in semantic_raw.items():
        clean_name = str(column or "").strip()
        if not isinstance(semantic_hint, str):
            notes.append(f"Ignored non-string semantic override for '{clean_name}'.")
            continue
        hint = str(semantic_hint or "").strip()
        if clean_name not in available:
            notes.append(f"Ignored semantic override for unknown column '{clean_name}'.")
            continue
        if hint not in SUPPORTED_SEMANTIC_HINTS:
            notes.append(f"Ignored unsupported semantic override '{hint}' for '{clean_name}'.")
            continue
        semantic_overrides[clean_name] = hint

    sanitized_options: dict[str, Any] = {}
    boolean_option_keys = {
        "remove_duplicates",
        "drop_empty_columns",
        "normalize_phone",
        "normalize_zip",
        "normalize_gender",
    }
    string_option_keys = {
        "text_case",
        "output_format",
        "privacy_mode",
        "performance_mode",
    }

    for key, value in recommended_raw.items():
        if key in boolean_option_keys:
            if not isinstance(value, bool):
                notes.append(f"Ignored non-boolean cleanup option '{key}'.")
                continue
        elif key in string_option_keys:
            if not isinstance(value, str):
                notes.append(f"Ignored non-string cleanup option '{key}'.")
                continue
            value = value.strip()
            if not value:
                continue
        else:
            notes.append(f"Ignored unsupported cleanup option '{key}'.")
            continue

        if key == "output_format" and value not in SUPPORTED_OUTPUT_FORMATS:
            notes.append(f"Ignored unsupported output format '{value}'.")
            continue
        if key == "privacy_mode" and value not in SUPPORTED_PRIVACY_MODES:
            notes.append(f"Ignored unsupported privacy mode '{value}'.")
            continue
        if key == "performance_mode" and value not in SUPPORTED_PERFORMANCE_MODES:
            notes.append(f"Ignored unsupported performance mode '{value}'.")
            continue
        if key == "text_case" and value not in SUPPORTED_TEXT_CASES:
            notes.append(f"Ignored unsupported text_case '{value}'.")
            continue
        sanitized_options[key] = value

    keep_priority_columns = [
        column for column in [str(item).strip() for item in keep_raw]
        if column in available
    ][:8]
    drop_candidates = [
        column for column in [str(item).strip() for item in drop_raw]
        if column in available and column not in keep_priority_columns
    ][:8]
    top_blockers = [str(item).strip() for item in top_blockers_raw if str(item).strip()][:5]
    if not any([summary, explanation, top_blockers, semantic_overrides, drop_candidates, keep_priority_columns, sanitized_options]):
        notes.append("Planner response did not contain usable cleanup guidance.")
    acceptance_status = "fully_accepted" if not notes else "partially_accepted"

    return {
        "provider": OLLAMA_PROVIDER_NAME,
        "status": "validated",
        "prompt_version": PLANNER_PROMPT_VERSION,
        "acceptance_status": acceptance_status,
        "summary": summary,
        "explanation": explanation,
        "top_blockers": top_blockers,
        "semantic_overrides": semantic_overrides,
        "drop_candidates": drop_candidates,
        "keep_priority_columns": keep_priority_columns,
        "recommended_options": sanitized_options,
        "validation_notes": notes,
    }


def plan_cleanup_with_ollama(
    *,
    profile: dict[str, Any],
    sample_rows: list[dict[str, Any]] | None,
    usage_intent: str | None,
    output_format: str | None,
    privacy_mode: str | None,
    requested_model: str | None = None,
) -> dict[str, Any]:
    provider_status = get_ollama_provider_status(requested_model)
    if not provider_status.get("enabled"):
        raise OllamaClientError("Ollama integration is disabled.")
    if not provider_status.get("reachable"):
        raise OllamaClientError(str(provider_status.get("error") or "Ollama is unavailable."))
    if provider_status.get("requested_model") and provider_status.get("requested_model_available") is False:
        raise OllamaModelSelectionError(
            str(provider_status.get("error") or "Requested Ollama model is unavailable.")
        )

    selected_model = str(provider_status.get("selected_model") or "").strip()
    if not selected_model:
        raise OllamaModelSelectionError(
            str(provider_status.get("error") or "No planner-safe Ollama model is available for assisted cleanup.")
        )

    detected_domains: list[str] = []
    for item in profile.get("detected_domains") or []:
        if not isinstance(item, dict):
            continue
        domain = str(item.get("domain") or "").strip()
        if domain:
            detected_domains.append(domain)

    context = {
        "usage_intent": usage_intent or "training",
        "output_format": output_format or "csv",
        "privacy_mode": privacy_mode or "safe_harbor",
        "row_count": profile.get("row_count"),
        "sampled_rows": profile.get("sampled_rows"),
        "sampled": bool(profile.get("sampled")),
        "primary_domain": profile.get("primary_domain"),
        "detected_domains": detected_domains[:4],
        "preclean_decision": {
            "status": (profile.get("preclean_decision") or {}).get("status"),
            "reasons": ((profile.get("preclean_decision") or {}).get("reasons") or [])[:3],
        },
        "rag_readiness": {
            "score": (profile.get("rag_readiness") or {}).get("score"),
            "band": (profile.get("rag_readiness") or {}).get("band"),
        },
        "columns": [
            {
                "name": column.get("clean_name"),
                "type": column.get("primitive_type"),
                "hint": column.get("semantic_hint"),
                "missing_pct": column.get("missing_pct"),
                "distinct_count": column.get("distinct_count"),
                "notes": _compact_notes(column.get("notes")),
            }
            for column in (profile.get("columns") or [])[:24]
            if isinstance(column, dict)
        ],
        "sample_rows": _sample_rows_payload(sample_rows),
    }

    system_prompt, prompt = _planner_prompts(context)
    response = _client().generate_json(
        model=selected_model,
        prompt=prompt,
        system_prompt=system_prompt,
    )
    plan = validate_planner_output(
        response.get("response") if isinstance(response, dict) else {},
        available_columns=[
            str(column.get("clean_name"))
            for column in (profile.get("columns") or [])
            if isinstance(column, dict)
        ],
    )
    plan["provider"] = OLLAMA_PROVIDER_NAME
    plan["model"] = selected_model
    plan["availability"] = {
        "reachable": True,
        "models": provider_status.get("models") or [],
        "installed_models": provider_status.get("installed_models") or [],
        "filtered_models": provider_status.get("filtered_models") or [],
    }
    return plan


def merge_plan_recommendations(
    base_values: dict[str, Any],
    *,
    plan: dict[str, Any] | None,
    explicit_fields: set[str] | None = None,
) -> dict[str, Any]:
    merged = dict(base_values or {})
    explicit = explicit_fields or set()
    if not isinstance(plan, dict):
        return merged
    recommended = plan.get("recommended_options") or {}
    if not isinstance(recommended, dict):
        return merged
    for key, value in recommended.items():
        if value is None or key in explicit:
            continue
        merged[key] = value
    return merged


def semantic_overrides_from_plan(plan: dict[str, Any] | None) -> dict[str, str]:
    overrides = plan.get("semantic_overrides") if isinstance(plan, dict) else {}
    if not isinstance(overrides, dict):
        return {}
    return {
        str(column): str(semantic_hint)
        for column, semantic_hint in overrides.items()
        if str(column).strip() and str(semantic_hint).strip()
    }


def profile_with_llm_assist(profile: dict[str, Any], plan: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(profile or {})
    if not isinstance(plan, dict):
        payload.pop("llm_assist", None)
        return payload

    payload["llm_assist"] = {
        "provider": plan.get("provider"),
        "model": plan.get("model"),
        "status": plan.get("status"),
        "prompt_version": plan.get("prompt_version"),
        "acceptance_status": plan.get("acceptance_status"),
        "summary": plan.get("summary"),
        "explanation": plan.get("explanation"),
        "top_blockers": plan.get("top_blockers") or [],
        "semantic_overrides": semantic_overrides_from_plan(plan),
        "keep_priority_columns": plan.get("keep_priority_columns") or [],
        "drop_candidates": plan.get("drop_candidates") or [],
        "recommended_options": plan.get("recommended_options") or {},
        "validation_notes": plan.get("validation_notes") or [],
    }
    return payload
