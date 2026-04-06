function prettyJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch (_error) {
    return String(value || "");
  }
}

function runMetaFromRun(run) {
  if (!run || typeof run !== "object") return null;
  const plan = run.llm_plan && typeof run.llm_plan === "object" ? run.llm_plan : {};
  return {
    cleanupMode: run.cleanup_mode || "deterministic",
    llmProvider: run.llm_provider || null,
    llmModel: run.llm_model || null,
    llmPlanStatus: plan.status || plan.validation_status || plan.plan_status || null,
    llmSummary: plan.summary || null,
  };
}

async function fetchRunsForDataset({ datasetId, store, api }) {
  const runs = await api.listRuns(datasetId);
  store.update((state) => ({
    ...state,
      history: {
        ...state.history,
        runs,
        selectedDatasetId: datasetId,
        selectedRunId: null,
        selectedRun: null,
        runMeta: null,
        diagnostics: "Select a run to inspect diagnostics.",
      },
    }));
}

async function refreshHistory({ store, api }) {
  store.update((state) => ({
    ...state,
    history: {
      ...state.history,
      loading: true,
    },
  }));

  try {
    const datasets = await api.listDatasets();
    store.update((state) => ({
      ...state,
      history: {
        ...state.history,
        datasets,
        loading: false,
      },
      error: "",
    }));
  } catch (error) {
    store.update((state) => ({
      ...state,
      history: {
        ...state.history,
        loading: false,
      },
      error: error?.message || "Unable to load history.",
    }));
  }
}

export function registerHistoryFeature({ elements, store, api }) {
  if (elements.navWizard) {
    elements.navWizard.addEventListener("click", () => {
      store.setState({ view: "wizard" });
    });
  }

  if (elements.navHistory) {
    elements.navHistory.addEventListener("click", async () => {
      store.setState({ view: "history", info: "", error: "" });
      const state = store.getState();
      if (!state.history.datasets.length) {
        await refreshHistory({ store, api });
      }
    });
  }

  if (elements.historyRefresh) {
    elements.historyRefresh.addEventListener("click", async () => {
      await refreshHistory({ store, api });
    });
  }

  if (elements.historyDatasets) {
    elements.historyDatasets.addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-dataset-id]");
      if (!button) return;
      const datasetId = button.getAttribute("data-dataset-id") || "";
      if (!datasetId) return;

      store.update((state) => ({
        ...state,
        history: {
          ...state.history,
          selectedDatasetId: datasetId,
          selectedRunId: null,
          selectedRun: null,
          runMeta: null,
          diagnostics: "Loading runs...",
        },
      }));

      try {
        await fetchRunsForDataset({ datasetId, store, api });
      } catch (error) {
        store.setState({ error: error?.message || "Unable to load runs." });
      }
    });
  }

  if (elements.historyRuns) {
    elements.historyRuns.addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-run-id]");
      if (!button) return;
      const runId = button.getAttribute("data-run-id") || "";
      if (!runId) return;

      store.update((state) => ({
        ...state,
        history: {
          ...state.history,
          selectedRunId: runId,
          diagnostics: "Loading run diagnostics...",
        },
      }));

      try {
        const run = await api.getRun(runId);
        store.update((state) => ({
          ...state,
          history: {
            ...state.history,
            selectedRunId: runId,
            selectedRun: run,
            runMeta: runMetaFromRun(run),
            diagnostics: prettyJson(run),
          },
        }));
      } catch (error) {
        store.setState({ error: error?.message || "Unable to load run diagnostics." });
      }
    });
  }

  return {
    refreshHistory: () => refreshHistory({ store, api }),
  };
}
