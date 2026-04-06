import { stageToStep } from "../ui/wizard.js";

function buildAutopilotPayload(elements, engine) {
  const targetScore = Number(elements.runTargetScore?.value || 95);
  const safeTarget = Number.isFinite(targetScore) ? Math.max(70, Math.min(100, Math.round(targetScore))) : 95;
  const cleanupMode = engine?.cleanupMode || "deterministic";

  return {
    target_score: safeTarget,
    output_format: elements.runOutputFormat?.value || "csv",
    privacy_mode: elements.runPrivacyMode?.value || "safe_harbor",
    performance_mode: elements.runPerformanceMode?.value || "balanced",
    cleanup_mode: cleanupMode,
    llm_model: cleanupMode === "ollama_assisted" ? engine?.llmModel || "" : "",
  };
}

export function registerRunFeature({ elements, store, api }) {
  if (!elements.runAutopilot) return;

  elements.runAutopilot.addEventListener("click", async () => {
    const state = store.getState();
    const workflowId = state.workflow?.workflow_id;
    if (!workflowId) {
      store.setState({ error: "Upload and review a dataset before running cleanup." });
      return;
    }

    const provider = state.engine?.provider || {};
    const cleanupMode = state.engine?.cleanupMode || "deterministic";
    const selectableModels = Array.isArray(provider.models) ? provider.models : [];
    if (cleanupMode === "ollama_assisted" && (!provider.reachable || !state.engine?.llmModel || !selectableModels.includes(state.engine.llmModel))) {
      store.setState({
        error: provider.reachable
          ? "Select one of the planner-safe Ollama models before running assisted cleanup."
          : "Ollama is unavailable. Start the local Ollama service or switch back to deterministic mode.",
        info: "",
      });
      return;
    }

    const payload = buildAutopilotPayload(elements, state.engine);

    store.setState({
      busy: true,
      error: "",
      info:
        payload.cleanup_mode === "ollama_assisted"
          ? "Running Ollama-assisted guided cleanup..."
          : "Running deterministic guided cleanup...",
    });

    try {
      const workflow = await api.runAutopilot(workflowId, payload);
      const nextStep = Math.max(4, stageToStep(workflow.stage));
      store.setState({
        workflow,
        step: nextStep,
        info:
          payload.cleanup_mode === "ollama_assisted"
            ? "Ollama-assisted cleanup completed. Review outcome and export your file."
            : "Cleanup completed. Review outcome and export your file.",
      });
    } catch (error) {
      store.setState({
        error: error?.message || "Autopilot run failed.",
        info: "",
      });
    } finally {
      store.setState({ busy: false });
    }
  });
}
