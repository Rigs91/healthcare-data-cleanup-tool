export function registerExportFeature({ elements, store, api }) {
  if (elements.startOver) {
    elements.startOver.addEventListener("click", () => {
      if (elements.uploadForm) {
        elements.uploadForm.reset();
      }
      store.setState({
        workflow: null,
        step: 1,
        error: "",
        info: "Ready for a new dataset.",
      });
    });
  }

  if (elements.copyDownloadUrl) {
    elements.copyDownloadUrl.addEventListener("click", async () => {
      const workflow = store.getState().workflow;
      const relative = workflow?.result_summary?.download_url || "";
      if (!relative) {
        store.setState({ error: "No download URL available yet." });
        return;
      }
      const absolute = api.absoluteUrl(relative);
      try {
        await navigator.clipboard.writeText(absolute);
        store.setState({ info: "Download URL copied.", error: "" });
      } catch (_error) {
        store.setState({ error: "Could not copy URL. Copy it manually from the download button.", info: "" });
      }
    });
  }
}