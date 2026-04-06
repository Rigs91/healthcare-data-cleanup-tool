import { ApiClient } from "./api/client.js";
import { registerExportFeature } from "./features/export.js";
import { registerHistoryFeature } from "./features/history.js";
import { registerPrecheckFeature } from "./features/precheck.js";
import { registerRunFeature } from "./features/run.js";
import { registerUploadFeature } from "./features/upload.js";
import { createStore, initialState } from "./state/store.js";
import { renderApp } from "./ui/render.js";

const CLEANUP_MODE_STORAGE_KEY = "hcdata.cleanupMode";
const LLM_MODEL_STORAGE_KEY = "hcdata.ollamaModel";

function readEnginePreferences() {
  try {
    const cleanupMode = localStorage.getItem(CLEANUP_MODE_STORAGE_KEY) || "deterministic";
    const llmModel = localStorage.getItem(LLM_MODEL_STORAGE_KEY) || "";
    return {
      cleanupMode: cleanupMode === "ollama_assisted" ? "ollama_assisted" : "deterministic",
      llmModel,
    };
  } catch (_error) {
    return {
      cleanupMode: "deterministic",
      llmModel: "",
    };
  }
}

function writeEnginePreferences(engine) {
  try {
    localStorage.setItem(CLEANUP_MODE_STORAGE_KEY, engine.cleanupMode || "deterministic");
    if (engine.llmModel) {
      localStorage.setItem(LLM_MODEL_STORAGE_KEY, engine.llmModel);
    } else {
      localStorage.removeItem(LLM_MODEL_STORAGE_KEY);
    }
  } catch (_error) {
    // Ignore storage failures.
  }
}

function normalizeProviderStatus(provider, requestedModel) {
  const source = provider && typeof provider === "object" ? provider : {};
  const models = Array.isArray(source.models)
    ? source.models
        .map((item) => String(item || "").trim())
        .filter((item) => item.length > 0)
    : [];
  const installedSource = Array.isArray(source.installed_models)
    ? source.installed_models
    : Array.isArray(source.installedModels)
      ? source.installedModels
      : [];
  const installedModels = Array.isArray(installedSource)
    ? installedSource
        .map((item) => String(item || "").trim())
        .filter((item) => item.length > 0)
    : [];
  const filteredSource = Array.isArray(source.filtered_models)
    ? source.filtered_models
    : Array.isArray(source.filteredModels)
      ? source.filteredModels
      : [];
  const filteredModels = Array.isArray(filteredSource)
    ? filteredSource
        .map((item) => ({
          name: String(item?.name || "").trim(),
          reason: String(item?.reason || "").trim(),
        }))
        .filter((item) => item.name.length > 0)
    : [];
  const requested = String(requestedModel || "").trim();
  const safeSelected = models.includes(requested)
    ? requested
    : models.includes(String(source.selected_model || source.selectedModel || "").trim())
      ? String(source.selected_model || source.selectedModel || "").trim()
      : models[0] || "";

  return {
    name: "ollama",
    enabled: source.enabled !== false,
    reachable: Boolean(source.reachable),
    loading: false,
    selectedModel: safeSelected,
    models,
    installedModels,
    filteredModels,
    requestedModel: String(source.requested_model || source.requestedModel || requested || "").trim(),
    requestedModelAvailable:
      typeof source.requested_model_available === "boolean"
        ? source.requested_model_available
        : typeof source.requestedModelAvailable === "boolean"
          ? source.requestedModelAvailable
          : null,
    requestedModelInstalled:
      typeof source.requested_model_installed === "boolean"
        ? source.requested_model_installed
        : typeof source.requestedModelInstalled === "boolean"
          ? source.requestedModelInstalled
          : null,
    requestedModelSelectable:
      typeof source.requested_model_selectable === "boolean"
        ? source.requested_model_selectable
        : typeof source.requestedModelSelectable === "boolean"
          ? source.requestedModelSelectable
          : null,
    hiddenModelCount: installedModels.length > models.length
      ? installedModels.length - models.length
      : filteredModels.length,
    error: String(source.error || "").trim(),
    baseUrl: String(source.base_url || source.baseUrl || "").trim(),
    lastCheckedAt: new Date().toISOString(),
  };
}

function initialEngineState() {
  const prefs = readEnginePreferences();
  return {
    cleanupMode: prefs.cleanupMode,
    llmModel: prefs.llmModel,
    provider: {
      ...initialState.engine.provider,
      selectedModel: prefs.llmModel,
    },
  };
}

const elements = {
  apiHealthBadge: document.getElementById("api-health-badge"),
  workflowVersionChip: document.getElementById("workflow-version-chip"),
  apiHealthDetail: document.getElementById("api-health-detail"),

  navWizard: document.getElementById("nav-wizard"),
  navHistory: document.getElementById("nav-history"),

  wizardView: document.getElementById("wizard-view"),
  historyView: document.getElementById("history-view"),

  stepIndicator: document.getElementById("step-indicator"),
  stepTitle: document.getElementById("wizard-step-title"),
  stepHint: document.getElementById("wizard-step-hint"),

  wizardStatus: document.getElementById("wizard-status"),
  wizardError: document.getElementById("wizard-error"),

  stepUpload: document.getElementById("step-upload"),
  stepPrecheck: document.getElementById("step-precheck"),
  stepRun: document.getElementById("step-run"),
  stepResult: document.getElementById("step-result"),

  uploadForm: document.getElementById("upload-form"),
  uploadSubmit: document.getElementById("upload-submit"),
  datasetName: document.getElementById("dataset-name"),
  datasetFile: document.getElementById("dataset-file"),
  usageIntent: document.getElementById("usage-intent"),
  cleanupModeDeterministic: document.getElementById("cleanup-mode-deterministic"),
  cleanupModeOllama: document.getElementById("cleanup-mode-ollama"),
  modeCardDeterministic: document.getElementById("mode-card-deterministic"),
  modeCardOllama: document.getElementById("mode-card-ollama"),
  ollamaPanel: document.getElementById("ollama-panel"),
  ollamaStatusBadge: document.getElementById("ollama-status-badge"),
  ollamaStatusText: document.getElementById("ollama-status-text"),
  ollamaModelSelect: document.getElementById("ollama-model-select"),
  ollamaFilteredNote: document.getElementById("ollama-filtered-note"),
  ollamaRefresh: document.getElementById("ollama-refresh"),

  precheckSummary: document.getElementById("precheck-summary"),
  profileSummary: document.getElementById("profile-summary"),
  profileColumns: document.getElementById("profile-columns"),
  precheckBlockers: document.getElementById("precheck-blockers"),
  precheckActions: document.getElementById("precheck-actions"),
  goToRun: document.getElementById("go-to-run"),

  runSummary: document.getElementById("run-summary"),
  runTargetScore: document.getElementById("run-target-score"),
  runOutputFormat: document.getElementById("run-output-format"),
  runPrivacyMode: document.getElementById("run-privacy-mode"),
  runPerformanceMode: document.getElementById("run-performance-mode"),
  runAutopilot: document.getElementById("run-autopilot"),

  resultSummary: document.getElementById("result-summary"),
  resultWarnings: document.getElementById("result-warnings"),
  downloadResult: document.getElementById("download-result"),
  copyDownloadUrl: document.getElementById("copy-download-url"),
  startOver: document.getElementById("start-over"),

  historyStatus: document.getElementById("history-status"),
  historyRefresh: document.getElementById("history-refresh"),
  historyDatasets: document.getElementById("history-datasets"),
  historyRuns: document.getElementById("history-runs"),
  historyRunMeta: document.getElementById("history-run-meta"),
  historyDiagnostics: document.getElementById("history-diagnostics"),
};

const store = createStore({
  ...initialState,
  engine: initialEngineState(),
});
const api = new ApiClient();

store.subscribe((state) => {
  writeEnginePreferences(state.engine || initialState.engine);
  renderApp(elements, state);
});
renderApp(elements, store.getState());

registerUploadFeature({ elements, store, api });
registerPrecheckFeature({ elements, store });
registerRunFeature({ elements, store, api });
registerExportFeature({ elements, store, api });
const historyFeature = registerHistoryFeature({ elements, store, api });

async function refreshOllamaStatus(options = {}) {
  const state = store.getState();
  const requestedModel = options.requestedModel ?? state.engine.llmModel ?? "";

  store.update((current) => ({
    ...current,
    engine: {
      ...current.engine,
      provider: {
        ...current.engine.provider,
        loading: true,
        error: "",
      },
    },
  }));

  try {
    const provider = await api.listOllamaModels(requestedModel);
    const normalized = normalizeProviderStatus(provider, requestedModel);
    store.update((current) => {
      const currentModel = String(current.engine.llmModel || "").trim();
      const nextModel = normalized.models.includes(currentModel)
        ? currentModel
        : normalized.selectedModel || normalized.models[0] || "";
      return {
        ...current,
        engine: {
          ...current.engine,
          llmModel: nextModel,
          provider: {
            ...normalized,
            selectedModel: nextModel,
          },
        },
      };
      });
  } catch (error) {
    store.update((current) => ({
      ...current,
      engine: {
        ...current.engine,
        provider: {
          ...current.engine.provider,
          loading: false,
          reachable: false,
          error: error?.message || "Unable to refresh Ollama provider status.",
          lastCheckedAt: new Date().toISOString(),
        },
      },
    }));
  }
}

function setCleanupMode(cleanupMode) {
  store.update((state) => ({
    ...state,
    engine: {
      ...state.engine,
      cleanupMode: cleanupMode === "ollama_assisted" ? "ollama_assisted" : "deterministic",
    },
  }));
}

function setLlmModel(llmModel) {
  store.update((state) => ({
    ...state,
    engine: {
      ...state.engine,
      llmModel,
      provider: {
        ...state.engine.provider,
        selectedModel: llmModel,
      },
    },
  }));
}

async function bootstrap() {
  store.setState({ info: "Checking API health..." });
  try {
    const health = await api.health();
    const mode = health?.ui_workflow_version === "v3_guided" ? "v3_guided" : "v2_legacy";
    api.setWorkflowMode(mode);
    const currentState = store.getState();
    const provider = normalizeProviderStatus(health?.providers?.ollama, currentState.engine.llmModel);
    const defaultModel = currentState.engine.llmModel || provider.selectedModel || provider.models[0] || "";

    store.setState({
      health,
      engine: {
        ...currentState.engine,
        llmModel: defaultModel,
        provider: {
          ...provider,
          selectedModel: defaultModel,
        },
      },
      info:
        mode === "v3_guided"
          ? "Guided v3 workflow is active."
          : "Guided UI is active with legacy API compatibility mode.",
      error: "",
    });

    await refreshOllamaStatus({ requestedModel: defaultModel });
  } catch (error) {
    store.setState({
      health: {
        status: "unavailable",
        service: "hc-data-cleanup-ai",
        version: "unknown",
        ui_workflow_version: "unknown",
        capabilities: {},
        providers: {},
      },
      error: error?.message || "Unable to reach API.",
      info: "",
    });
  }
}

bootstrap().catch((error) => {
  store.setState({ error: error?.message || "Failed to initialize app.", info: "" });
});

if (elements.cleanupModeDeterministic) {
  elements.cleanupModeDeterministic.addEventListener("change", () => {
    setCleanupMode("deterministic");
  });
}

if (elements.cleanupModeOllama) {
  elements.cleanupModeOllama.addEventListener("change", async () => {
    setCleanupMode("ollama_assisted");
    const state = store.getState();
    if (!state.engine.provider.models.length || !state.engine.provider.reachable) {
      await refreshOllamaStatus({ requestedModel: state.engine.llmModel });
    }
  });
}

if (elements.ollamaModelSelect) {
  elements.ollamaModelSelect.addEventListener("change", () => {
    setLlmModel(elements.ollamaModelSelect.value || "");
  });
}

if (elements.ollamaRefresh) {
  elements.ollamaRefresh.addEventListener("click", async () => {
    await refreshOllamaStatus();
  });
}

window.addEventListener("keydown", async (event) => {
  if (event.key === "F5") {
    return;
  }

  if (event.altKey && event.key.toLowerCase() === "h") {
    event.preventDefault();
    store.setState({ view: "history" });
    await historyFeature.refreshHistory();
  }

  if (event.altKey && event.key.toLowerCase() === "g") {
    event.preventDefault();
    store.setState({ view: "wizard" });
  }
});
