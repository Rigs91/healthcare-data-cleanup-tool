const defaultHealth = {
  status: "checking",
  service: "hc-data-cleanup-ai",
  version: "unknown",
  ui_workflow_version: "unknown",
  capabilities: {},
  providers: {},
};

export const initialState = {
  view: "wizard",
  step: 1,
  busy: false,
  info: "",
  error: "",
  health: defaultHealth,
  engine: {
    cleanupMode: "deterministic",
    llmModel: "",
    provider: {
      name: "ollama",
      enabled: true,
      reachable: false,
      loading: false,
      selectedModel: "",
      models: [],
      installedModels: [],
      filteredModels: [],
      requestedModel: "",
      requestedModelAvailable: null,
      requestedModelInstalled: null,
      requestedModelSelectable: null,
      hiddenModelCount: 0,
      error: "",
      baseUrl: "",
      lastCheckedAt: null,
    },
  },
  workflow: null,
  history: {
    loading: false,
    datasets: [],
    runs: [],
    selectedDatasetId: null,
    selectedRunId: null,
    selectedRun: null,
    runMeta: null,
    diagnostics: "Select a run to inspect diagnostics.",
  },
};

function cloneState(value) {
  if (typeof structuredClone === "function") {
    return structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value));
}

export function createStore(seed = initialState) {
  let state = cloneState(seed);
  const listeners = new Set();

  function notify() {
    for (const listener of listeners) {
      listener(state);
    }
  }

  return {
    getState() {
      return state;
    },

    setState(patch) {
      state = { ...state, ...patch };
      notify();
    },

    update(mutator) {
      state = mutator(state);
      notify();
    },

    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}
