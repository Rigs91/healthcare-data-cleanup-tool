export function registerPrecheckFeature({ elements, store }) {
  if (!elements.goToRun) return;

  elements.goToRun.addEventListener("click", () => {
    const state = store.getState();
    if (!state.workflow) {
      store.setState({ error: "Upload a dataset before running cleanup." });
      return;
    }
    store.setState({
      step: 3,
      error: "",
      info: "Cleanup is ready. Keep defaults for the fastest path, or customize the run options.",
    });
  });
}
