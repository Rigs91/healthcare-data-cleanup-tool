import { stageToStep } from "../ui/wizard.js";

export function registerUploadFeature({ elements, store, api }) {
  if (!elements.uploadForm) return;

  elements.uploadForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const file = elements.datasetFile?.files?.[0] || null;
    if (!file) {
      store.setState({ error: "Choose a file to upload.", info: "" });
      return;
    }

    const usageIntent = elements.usageIntent?.value || "training";
    const name = (elements.datasetName?.value || "").trim() || file.name.replace(/\.[^.]+$/, "");
    const engine = store.getState().engine || {};
    const provider = engine.provider || {};
    const cleanupMode = engine.cleanupMode || "deterministic";
    const llmModel = cleanupMode === "ollama_assisted" ? engine.llmModel || "" : "";
    const selectableModels = Array.isArray(provider.models) ? provider.models : [];

    if (cleanupMode === "ollama_assisted" && (!provider.reachable || !llmModel || !selectableModels.includes(llmModel))) {
      store.setState({
        error: provider.reachable
          ? "Select one of the planner-safe Ollama models before uploading in assisted mode."
          : "Ollama is unavailable. Start the local Ollama service or switch back to deterministic mode.",
        info: "",
      });
      return;
    }

    store.setState({
      busy: true,
      error: "",
      info: "Uploading and profiling dataset...",
      view: "wizard",
    });

    try {
      const workflow = await api.uploadWorkflow({ file, name, usageIntent, cleanupMode, llmModel });
      const nextStep = Math.max(2, stageToStep(workflow.stage));
      store.setState({
        workflow,
        step: nextStep,
        info: `Dataset "${workflow.dataset?.name || name}" is ready for pre-check review with ${cleanupMode === "ollama_assisted" ? "Ollama-assisted" : "deterministic"} mode.`,
      });
    } catch (error) {
      store.setState({
        error: error?.message || "Upload failed.",
        info: "",
      });
    } finally {
      store.setState({ busy: false });
    }
  });
}
