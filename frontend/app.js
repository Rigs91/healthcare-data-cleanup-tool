const uploadForm = document.getElementById("upload-form");
const uploadButton = document.getElementById("upload-button");
const uploadProgressBar = document.getElementById("upload-progress-bar");
const uploadProgressText = document.getElementById("upload-progress-text");
const statusEl = document.getElementById("status");
const cleanStatusEl = document.getElementById("clean-status");
const cleanProgressBar = document.getElementById("clean-progress-bar");
const cleanProgressText = document.getElementById("clean-progress-text");
const profileSummaryEl = document.getElementById("profile-summary");
const assessmentPrimaryEl = document.getElementById("assessment-primary");
const assessmentBlockersEl = document.getElementById("assessment-blockers");
const assessmentExplainabilityEl = document.getElementById("assessment-explainability");
const profileFlagsEl = document.getElementById("profile-flags");
const domainSuggestionsEl = document.getElementById("domain-suggestions");
const assessmentOverviewEl = document.getElementById("assessment-overview");
const assessmentLegendEl = document.getElementById("assessment-legend");
const assessmentFactorsEl = document.getElementById("assessment-factors");
const assessmentAdvancedEl = document.getElementById("assessment-advanced");
const assessmentRagEl = document.getElementById("assessment-rag");
const profileTable = document.getElementById("profile-table");
const rawPreviewTable = document.getElementById("raw-preview-table");
const previewRawTable = document.getElementById("preview-raw-table");
const previewCleanTable = document.getElementById("preview-clean-table");
const whatChangedSummaryEl = document.getElementById("what-changed-summary");
const whatChangedMetricsEl = document.getElementById("what-changed-metrics");
const whatChangedExamplesEl = document.getElementById("what-changed-examples");
const qcDecisionEl = document.getElementById("qc-decision");
const qcPrimaryResultEl = document.getElementById("qc-primary-result");
const qcDownloadLink = document.getElementById("qc-download-link");
const qcSummaryEl = document.getElementById("qc-summary");
const qcOutcomesEl = document.getElementById("qc-outcomes");
const qcSeveritySummaryEl = document.getElementById("qc-severity-summary");
const qcSeverityLegendEl = document.getElementById("qc-severity-legend");
const qcRagEl = document.getElementById("qc-rag");
const qcRagPlanEl = document.getElementById("qc-rag-plan");
const runHistoryEl = document.getElementById("run-history");
const qcDetailsEl = document.getElementById("qc-details");
const qcIssuesEl = document.getElementById("qc-issues");
const runCleaningBtn = document.getElementById("run-cleaning");
const runAutopilotBtn = document.getElementById("run-autopilot");
const downloadLink = document.getElementById("download-link");
const refreshProfileBtn = document.getElementById("refresh-profile");
const metaGrid = document.getElementById("meta-grid");
const viewModeSelect = document.getElementById("view-mode");
const guidedModeToggle = document.getElementById("guided-mode");
const wizardStepsEl = document.getElementById("wizard-steps");
const wizardNextActionEl = document.getElementById("wizard-next-action");
const featureCatalogStatusEl = document.getElementById("feature-catalog-status");

const usageIntentSelect = document.getElementById("usage-intent");
const outputFormatSelect = document.getElementById("output-format");
const optOutputFormat = document.getElementById("opt-output-format");
const optCoercionMode = document.getElementById("opt-coercion-mode");
const optPrivacyMode = document.getElementById("opt-privacy-mode");
const optNormalizePhone = document.getElementById("opt-normalize-phone");
const optNormalizeZip = document.getElementById("opt-normalize-zip");
const optNormalizeGender = document.getElementById("opt-normalize-gender");
const optTextCase = document.getElementById("opt-text-case");
const optPerformanceMode = document.getElementById("opt-performance-mode");
const usageIntentNote = document.getElementById("usage-intent-note");
const datasetNameInput = document.getElementById("dataset-name");
const datasetFileField = document.getElementById("dataset-file-field");
const datasetFileInput = document.getElementById("dataset-file");
const sourceOptions = document.querySelectorAll(".source-option[data-source]");
const googleDrivePanel = document.getElementById("google-drive-panel");
const googleDriveStatusEl = document.getElementById("google-drive-status");
const googleConnectBtn = document.getElementById("google-connect");
const googleDisconnectBtn = document.getElementById("google-disconnect");
const googleRefreshFilesBtn = document.getElementById("google-refresh-files");
const googleSearchInput = document.getElementById("google-search");
const googleFilesEl = document.getElementById("google-drive-files");
const googleImportBtn = document.getElementById("google-import");

const previewCard = document.getElementById("preview-card");
const previewFullscreenBtn = document.getElementById("preview-fullscreen");
const previewCloseBtn = document.getElementById("preview-close");
const fullscreenBackdrop = document.getElementById("fullscreen-backdrop");
const apiStatusEl = document.getElementById("api-status");
const apiStatusDetailEl = document.getElementById("api-status-detail");

let currentDataset = null;
let apiHealthy = false;
let apiReachable = false;
let apiService = "unknown";
let googleDriveConnected = false;
let selectedGoogleFile = null;
let googleSearchDebounce = null;
let googleAuthInProgress = false;
let wizardStep = 1;

const CHUNK_THRESHOLD = 50 * 1024 * 1024;
const CHUNK_SIZE = 10 * 1024 * 1024;
const STREAMING_THRESHOLD_MB = 200;

const DEFAULT_API_BASE = "http://localhost:8000";
const origin = window.location.origin;
const isValidOrigin = origin && origin !== "null" && origin.startsWith("http");
const API_BASE = isValidOrigin ? origin : DEFAULT_API_BASE;
const API_PREFIX = `${API_BASE}/api`;

function apiUrl(path) {
  if (path.startsWith("/")) {
    return `${API_PREFIX}${path}`;
  }
  return `${API_PREFIX}/${path}`;
}

function setViewMode(mode) {
  const resolved = mode === "advanced" ? "advanced" : "simple";
  document.body.classList.toggle("view-simple", resolved === "simple");
  if (viewModeSelect && viewModeSelect.value !== resolved) {
    viewModeSelect.value = resolved;
  }
}

function setWizardStep(step) {
  const safeStep = Math.max(1, Math.min(4, Number(step) || 1));
  wizardStep = safeStep;
  if (!wizardStepsEl) return;

  const guided = guidedModeToggle ? guidedModeToggle.checked : true;
  wizardStepsEl.classList.toggle("hidden", !guided);
  if (wizardNextActionEl) {
    wizardNextActionEl.classList.toggle("hidden", !guided);
  }

  const labels = {
    1: "Next: Upload and analyze a dataset.",
    2: "Next: Review blockers and explainability before cleanup.",
    3: "Next: Run 95% autopilot or custom cleaning.",
    4: "Next: Download cleaned dataset and review QC.",
  };
  if (wizardNextActionEl) {
    wizardNextActionEl.textContent = labels[safeStep] || "";
  }

  Array.from(wizardStepsEl.querySelectorAll(".wizard-step")).forEach((node) => {
    const stepValue = Number(node.getAttribute("data-step") || "0");
    node.classList.toggle("active", stepValue === safeStep);
    node.classList.toggle("done", stepValue > 0 && stepValue < safeStep);
  });
}

async function fetchFeatureCatalogStatus() {
  if (!featureCatalogStatusEl) return;
  try {
    const response = await fetch(apiUrl("/features"));
    if (!response.ok) {
      featureCatalogStatusEl.textContent = "Roadmap catalog unavailable.";
      return;
    }
    const payload = await response.json();
    const summary = payload.summary || {};
    const total = summary.total || 0;
    const inProgress = Number((summary.by_status || {}).in_progress || 0);
    const planned = Number((summary.by_status || {}).planned || 0);
    featureCatalogStatusEl.textContent =
      `Roadmap: ${total} tracked features | In progress: ${inProgress} | Planned: ${planned}.`;
  } catch (error) {
    featureCatalogStatusEl.textContent = "Roadmap catalog unavailable.";
  }
}

async function checkApiHealth() {
  if (!apiStatusEl) return;
  apiStatusEl.textContent = `API: checking (${API_BASE})`;
  apiStatusEl.classList.remove("ok", "warn");
  apiHealthy = false;
  apiReachable = false;
  apiService = "unknown";
  if (apiStatusDetailEl) {
    apiStatusDetailEl.textContent = "";
  }

  try {
    const response = await fetch(apiUrl("/health"));
    if (!response.ok) {
      apiStatusEl.textContent = `API: error (${response.status})`;
      apiStatusEl.classList.add("warn");
      return;
    }
    apiReachable = true;
    const data = await response.json();
    const service = data.service || "unknown";
    apiService = service;
    const version = data.version || "unknown";
    apiStatusEl.textContent = `API: ${service} (v${version})`;
    apiHealthy = service === "hc-data-cleanup-ai";
    apiStatusEl.classList.add(apiHealthy ? "ok" : "warn");
    if (!apiHealthy && apiStatusDetailEl) {
      apiStatusDetailEl.textContent =
        `You are not connected to the HcDataCleanUpAi API. Open the app at ${DEFAULT_API_BASE} (or the port shown in your server logs) and refresh.`;
    }
  } catch (error) {
    apiReachable = false;
    apiStatusEl.textContent = "API: unavailable";
    apiStatusEl.classList.add("warn");
    if (apiStatusDetailEl) {
      apiStatusDetailEl.textContent = `Unable to reach ${DEFAULT_API_BASE}. Start the server and refresh.`;
    }
  }
}

function setGoogleStatus(message) {
  if (googleDriveStatusEl) {
    googleDriveStatusEl.textContent = message;
  }
}

function updateGoogleControls() {
  if (!googleConnectBtn || !googleDisconnectBtn || !googleRefreshFilesBtn || !googleImportBtn) return;
  googleConnectBtn.classList.toggle("hidden", googleDriveConnected);
  googleConnectBtn.disabled = googleAuthInProgress;
  googleDisconnectBtn.classList.toggle("hidden", !googleDriveConnected);
  googleRefreshFilesBtn.disabled = !googleDriveConnected;
  googleImportBtn.disabled = !googleDriveConnected || !selectedGoogleFile;
  if (googleSearchInput) {
    googleSearchInput.disabled = !googleDriveConnected;
  }
}

function applySourceSelection(source) {
  const selectedSource = source === "google_drive" ? "google_drive" : "file";
  sourceOptions.forEach((option) => {
    option.classList.toggle("active", option.dataset.source === selectedSource);
  });
  const googleSelected = selectedSource === "google_drive";
  if (googleDrivePanel) {
    googleDrivePanel.classList.toggle("hidden", !googleSelected);
  }
  if (datasetFileField) {
    datasetFileField.classList.toggle("hidden", googleSelected);
  }
  if (uploadButton) {
    uploadButton.classList.toggle("hidden", googleSelected);
  }
  if (datasetFileInput) {
    datasetFileInput.required = !googleSelected;
    datasetFileInput.disabled = googleSelected;
  }
  if (googleSelected && googleDrivePanel) {
    googleDrivePanel.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }
}

function getGoogleAuthMessageFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const status = params.get("google_auth");
  const message = params.get("google_msg");
  if (!status) return null;
  const safeMessage = message || (status === "success" ? "Google Drive connected." : "Google authentication failed.");

  params.delete("google_auth");
  params.delete("google_msg");
  const newQuery = params.toString();
  const newUrl = `${window.location.pathname}${newQuery ? `?${newQuery}` : ""}${window.location.hash || ""}`;
  window.history.replaceState({}, document.title, newUrl);

  return { status, message: safeMessage };
}

function relayGoogleAuthResultToOpener() {
  const result = getGoogleAuthMessageFromUrl();
  if (!result) return null;
  if (window.opener && window.opener !== window) {
    try {
      window.opener.postMessage(
        {
          type: "google_auth_result",
          status: result.status,
          message: result.message,
        },
        window.location.origin
      );
    } catch (error) {
      // Ignore cross-window messaging failures.
    }
    window.close();
    return null;
  }
  return result;
}

function popupFeatures(width = 560, height = 760) {
  const dualScreenLeft = window.screenLeft !== undefined ? window.screenLeft : window.screenX;
  const dualScreenTop = window.screenTop !== undefined ? window.screenTop : window.screenY;
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth || screen.width;
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight || screen.height;
  const left = Math.max(0, Math.floor(dualScreenLeft + (viewportWidth - width) / 2));
  const top = Math.max(0, Math.floor(dualScreenTop + (viewportHeight - height) / 2));
  return `popup=yes,width=${width},height=${height},left=${left},top=${top},resizable=yes,scrollbars=yes`;
}

function waitForGoogleAuthPopupResult(popupWindow) {
  return new Promise((resolve, reject) => {
    const expectedOrigin = window.location.origin;
    let settled = false;
    let closePoll = null;
    let timeoutHandle = null;

    function cleanup() {
      window.removeEventListener("message", onMessage);
      if (closePoll) {
        clearInterval(closePoll);
      }
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    }

    function finishSuccess(result) {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(result);
    }

    function finishError(message) {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(message));
    }

    function onMessage(event) {
      if (event.origin !== expectedOrigin) return;
      const payload = event.data || {};
      if (payload.type !== "google_auth_result") return;
      if (payload.status === "success") {
        finishSuccess(payload);
        return;
      }
      finishError(payload.message || "Google authentication failed.");
    }

    window.addEventListener("message", onMessage);

    closePoll = setInterval(() => {
      if (!popupWindow || popupWindow.closed) {
        finishError("Google authentication window closed before completion.");
      }
    }, 400);

    timeoutHandle = setTimeout(() => {
      finishError("Google authentication timed out. Please try again.");
    }, 5 * 60 * 1000);
  });
}

async function fetchGoogleAuthStatus() {
  if (!googleDriveStatusEl) return;
  try {
    const response = await fetch(apiUrl("/integrations/google/auth/status"));
    if (!response.ok) {
      googleDriveConnected = false;
      updateGoogleControls();
      setGoogleStatus(`Google status unavailable (${response.status}).`);
      return;
    }
    const payload = await response.json();
    const configured = Boolean(payload.configured);
    googleDriveConnected = configured && Boolean(payload.authenticated);
    const missingConfig = Array.isArray(payload.missing_config) ? payload.missing_config : [];
    if (!configured) {
      const suffix = missingConfig.length ? ` Missing: ${missingConfig.join(", ")}.` : "";
      setGoogleStatus(`Google integration not configured on backend.${suffix}`);
    } else if (googleDriveConnected) {
      setGoogleStatus("Google Drive connected. Select a file below and import.");
    } else {
      setGoogleStatus("Connect Google Drive to import Sheets/CSV/XLSX.");
    }
    updateGoogleControls();
    if (googleDriveConnected) {
      await loadGoogleFiles(googleSearchInput?.value || "");
    } else if (googleFilesEl) {
      googleFilesEl.innerHTML = "";
    }
  } catch (error) {
    googleDriveConnected = false;
    updateGoogleControls();
    setGoogleStatus("Unable to reach Google integration endpoints.");
  }
}

async function connectGoogleDrive() {
  if (!apiReachable) {
    setGoogleStatus(`API not ready (${apiService}).`);
    return;
  }
  if (googleAuthInProgress) {
    setGoogleStatus("Google authentication is already in progress.");
    return;
  }
  try {
    const response = await fetch(apiUrl("/integrations/google/auth/start"));
    if (!response.ok) {
      let message = `Failed to start Google auth (${response.status}).`;
      try {
        const payload = await response.json();
        message = payload.detail || message;
      } catch (error) {
        // ignore parse errors
      }
      throw new Error(message);
    }
    const payload = await response.json();
    if (!payload.auth_url) {
      throw new Error("Missing Google auth URL.");
    }

    const popupWindow = window.open(payload.auth_url, "googleDriveOAuth", popupFeatures());
    if (!popupWindow) {
      setGoogleStatus("Popup blocked. Allow popups for this site, then try again.");
      return;
    }
    popupWindow.focus();
    googleAuthInProgress = true;
    updateGoogleControls();
    setGoogleStatus("Complete Google login in the popup window.");

    await waitForGoogleAuthPopupResult(popupWindow);
    await fetchGoogleAuthStatus();
  } catch (error) {
    setGoogleStatus(error.message || "Could not start Google authentication.");
  } finally {
    googleAuthInProgress = false;
    updateGoogleControls();
  }
}

async function disconnectGoogleDrive() {
  try {
    const response = await fetch(apiUrl("/integrations/google/auth/logout"), { method: "POST" });
    if (!response.ok) {
      throw new Error(`Failed to disconnect Google Drive (${response.status}).`);
    }
    googleDriveConnected = false;
    selectedGoogleFile = null;
    if (googleFilesEl) {
      googleFilesEl.innerHTML = "";
    }
    if (googleSearchInput) {
      googleSearchInput.value = "";
    }
    updateGoogleControls();
    setGoogleStatus("Google Drive disconnected.");
  } catch (error) {
    setGoogleStatus(error.message || "Could not disconnect Google Drive.");
  }
}

function renderGoogleFiles(items) {
  if (!googleFilesEl) return;
  googleFilesEl.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "google-file google-file--empty";
    empty.innerHTML = "<strong>No matching Drive files</strong><small>Try a different search term.</small>";
    googleFilesEl.appendChild(empty);
    return;
  }

  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "google-file";
    row.dataset.fileId = item.id;
    row.innerHTML = `
      <strong>${item.name || "Untitled file"}</strong>
      <small>${item.mime_type || "unknown"} | ${item.modified_time || "unknown date"}</small>
    `;
      row.addEventListener("click", () => {
        selectedGoogleFile = item;
        Array.from(googleFilesEl.querySelectorAll(".google-file")).forEach((node) => {
          node.classList.toggle("selected", node.dataset.fileId === item.id);
        });
        updateGoogleControls();
      });
      row.addEventListener("dblclick", async () => {
        selectedGoogleFile = item;
        Array.from(googleFilesEl.querySelectorAll(".google-file")).forEach((node) => {
          node.classList.toggle("selected", node.dataset.fileId === item.id);
        });
        updateGoogleControls();
        await runGoogleImportFlow();
      });
      googleFilesEl.appendChild(row);
    });
  }

async function loadGoogleFiles(search = "") {
  if (!googleDriveConnected) return;
  try {
    const params = new URLSearchParams();
    if (search) params.set("q", search);
    params.set("page_size", "50");
    const response = await fetch(apiUrl(`/integrations/google/drive/files?${params.toString()}`));
    if (!response.ok) {
      let message = `Failed to load Google Drive files (${response.status}).`;
      try {
        const payload = await response.json();
        message = payload.detail || message;
      } catch (error) {
        // ignore parse errors
      }
      throw new Error(message);
    }
    const payload = await response.json();
    const items = payload.items || [];
    selectedGoogleFile = null;
    renderGoogleFiles(items);
    updateGoogleControls();
  } catch (error) {
    setGoogleStatus(error.message || "Could not load Drive files.");
  }
}

async function importSelectedGoogleFile() {
  if (!selectedGoogleFile) {
    setGoogleStatus("Choose a Google Drive file first.");
    return null;
  }
  const requestPayload = {
    file_id: selectedGoogleFile.id,
    file_name: selectedGoogleFile.name,
    mime_type: selectedGoogleFile.mime_type,
    name: datasetNameInput.value || null,
    usage_intent: usageIntentSelect.value,
    output_format: outputFormatSelect.value,
    privacy_mode: optPrivacyMode.value,
  };
  const response = await fetch(apiUrl("/datasets/from-google-drive"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestPayload),
  });
  if (!response.ok) {
    let message = `Google Drive import failed (${response.status}) at ${response.url}.`;
    try {
      const payload = await response.json();
      message = payload.detail || message;
    } catch (error) {
      // ignore parse errors
    }
    throw new Error(message);
  }
  return response.json();
}

async function runGoogleImportFlow() {
  if (!apiReachable) {
    setGoogleStatus(`API not ready (${apiService}).`);
    return;
  }
  if (!googleDriveConnected) {
    setGoogleStatus("Connect Google Drive first.");
    return;
  }
  if (!selectedGoogleFile) {
    setGoogleStatus("Select a Google Drive file to import.");
    return;
  }

  setStatus(statusEl, "Importing from Google Drive... analyzing dataset.");
  resetTables();
  uploadButton.disabled = true;
  runCleaningBtn.disabled = true;
  refreshProfileBtn.disabled = true;
  previewFullscreenBtn.disabled = true;
  googleImportBtn.disabled = true;
  googleRefreshFilesBtn.disabled = true;

  try {
    const dataset = await importSelectedGoogleFile();
    await handleDatasetReady(dataset, `Analysis complete for ${dataset.name}.`);
    setGoogleStatus(`Imported ${selectedGoogleFile.name}.`);
  } catch (error) {
    setStatus(statusEl, error.message || "Google Drive import failed.");
    setGoogleStatus(error.message || "Google Drive import failed.");
    uploadButton.disabled = false;
    runCleaningBtn.disabled = true;
    refreshProfileBtn.disabled = true;
    previewFullscreenBtn.disabled = true;
  } finally {
    googleRefreshFilesBtn.disabled = !googleDriveConnected;
    googleImportBtn.disabled = !googleDriveConnected || !selectedGoogleFile;
  }
}

function updateUsageIntentNote() {
  const intent = usageIntentSelect.value;
  if (intent === "training") {
    usageIntentNote.textContent = "Default cleanup: HIPAA Safe Harbor on. Intended usage only sets privacy defaults; it does not delete data.";
    optPrivacyMode.value = "safe_harbor";
  } else if (intent === "external_share") {
    usageIntentNote.textContent = "Default cleanup: HIPAA Safe Harbor on. Intended usage only sets privacy defaults; it does not delete data.";
    optPrivacyMode.value = "safe_harbor";
  } else if (intent === "inference") {
    usageIntentNote.textContent = "Default cleanup: HIPAA Safe Harbor off. Intended usage only sets privacy defaults; it does not delete data.";
    optPrivacyMode.value = "none";
  } else {
    usageIntentNote.textContent = "Default cleanup: HIPAA Safe Harbor off. Intended usage only sets privacy defaults; it does not delete data.";
    optPrivacyMode.value = "none";
  }
}

function setStatus(el, message) {
  el.textContent = message;
}

function setUploadProgress(percent) {
  const safePercent = Math.max(0, Math.min(100, percent));
  uploadProgressBar.style.width = `${safePercent}%`;
  uploadProgressText.textContent = `Upload progress: ${safePercent}%`;
}

function setCleanProgress(percent, message) {
  const safePercent = Math.max(0, Math.min(100, percent));
  cleanProgressBar.style.width = `${safePercent}%`;
  cleanProgressText.textContent = message ? `${message} (${safePercent}%)` : `Cleaning progress: ${safePercent}%`;
}


function resetTables() {
  profileTable.innerHTML = "";
  rawPreviewTable.innerHTML = "";
  previewRawTable.innerHTML = "";
  previewCleanTable.innerHTML = "";
  whatChangedSummaryEl.textContent = "";
  whatChangedMetricsEl.innerHTML = "";
  whatChangedExamplesEl.innerHTML = "";
  qcDecisionEl.textContent = "";
  qcPrimaryResultEl.textContent = "";
  if (qcDownloadLink) {
    qcDownloadLink.classList.add("hidden");
    qcDownloadLink.removeAttribute("href");
  }
  qcDetailsEl.textContent = "";
  qcSummaryEl.textContent = "";
  qcOutcomesEl.innerHTML = "";
  qcSeveritySummaryEl.innerHTML = "";
  qcSeverityLegendEl.innerHTML = "";
  qcRagEl.innerHTML = "";
  qcRagPlanEl.innerHTML = "";
  runHistoryEl.innerHTML = "";
  qcIssuesEl.innerHTML = "";
  profileSummaryEl.textContent = "";
  assessmentPrimaryEl.textContent = "";
  assessmentBlockersEl.innerHTML = "";
  assessmentExplainabilityEl.innerHTML = "";
  profileFlagsEl.innerHTML = "";
  domainSuggestionsEl.innerHTML = "";
  assessmentOverviewEl.innerHTML = "";
  assessmentLegendEl.textContent = "";
  assessmentFactorsEl.innerHTML = "";
  assessmentAdvancedEl.textContent = "";
  assessmentRagEl.innerHTML = "";
  metaGrid.innerHTML = "";
  uploadProgressBar.style.width = "0%";
  uploadProgressText.textContent = "";
  cleanProgressBar.style.width = "0%";
  cleanProgressText.textContent = "";
  setWizardStep(1);
}

function formatCell(value) {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return value;
}

function createTable(columns, rows) {
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      const value = row[col];
      td.textContent = formatCell(value);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  return table;
}

function formatBytes(bytes) {
  if (!bytes && bytes !== 0) return "n/a";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(1)} ${units[unitIndex]}`;
}

function formatDateTime(value) {
  if (!value) return "n/a";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

function metricText(value) {
  if (typeof value === "number") {
    return value.toFixed(3);
  }
  return value || "n/a";
}

function signedMetricText(value) {
  if (typeof value === "number") {
    if (value > 0) return `+${value}`;
    return String(value);
  }
  return "n/a";
}

function bandClass(value) {
  const safe = String(value || "unknown").toLowerCase().replace(/[^a-z0-9_]/g, "_");
  return `band-pill band-pill--${safe}`;
}

function severityClass(value) {
  const safe = String(value || "low").toLowerCase().replace(/[^a-z0-9_]/g, "_");
  return `severity-chip severity-chip--${safe}`;
}

function normalizeSeverity(value) {
  const safe = String(value || "low").toLowerCase();
  if (safe === "high") return "high";
  if (safe === "medium") return "medium";
  return "low";
}

function severityLabel(value) {
  const normalized = normalizeSeverity(value);
  if (normalized === "high") return "Critical";
  if (normalized === "medium") return "Attention";
  return "Info";
}

function readinessTargetStatus(score) {
  const numericScore = Number(score);
  if (!Number.isFinite(numericScore)) {
    return { label: "Needs Attention", severity: "medium" };
  }
  if (numericScore >= 95) {
    return { label: "On Track", severity: "low" };
  }
  if (numericScore >= 80) {
    return { label: "Close", severity: "medium" };
  }
  return { label: "Needs Attention", severity: "high" };
}

function ragDeltaClass(value) {
  if (value === "improved") return "severity-chip severity-chip--low";
  if (value === "regressed") return "severity-chip severity-chip--high";
  return "severity-chip severity-chip--medium";
}

function toLabel(value) {
  if (value === null || value === undefined) return "n/a";
  const text = String(value).trim();
  if (!text) return "n/a";
  return text
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function toNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function normalizeDomainSignals(profile) {
  const source = profile?.detected_domains ?? profile?.domains ?? [];
  if (Array.isArray(source)) {
    return source
      .map((entry) => {
        if (typeof entry === "string") {
          return { domain: entry, score: null };
        }
        if (entry && typeof entry === "object") {
          const domain = entry.domain || entry.name || entry.label || entry.id;
          if (!domain) return null;
          const score = entry.score ?? entry.confidence ?? entry.weight ?? null;
          return { domain, score };
        }
        return null;
      })
      .filter(Boolean);
  }
  if (source && typeof source === "object") {
    return Object.entries(source)
      .map(([domain, score]) => ({ domain, score }))
      .filter((entry) => entry.domain);
  }
  return [];
}

function resolvePrecleanDecision(profile) {
  const decision = profile?.preclean_decision;
  if (decision && typeof decision === "object") {
    return toLabel(decision.status || decision.decision || decision.recommendation || "needs_review");
  }
  if (typeof decision === "string") {
    return toLabel(decision);
  }
  const band = String(profile?.assessment?.band || "").toLowerCase();
  if (band === "excellent" || band === "good") {
    return "Ready For Cleaning Run";
  }
  if (band === "fair") {
    return "Review Risks Before Cleaning";
  }
  if (band === "poor") {
    return "Needs Remediation Before Cleaning";
  }
  return "Awaiting Analysis";
}

function resolvePostcleanDecision(qc) {
  const decision = qc?.postclean_decision;
  if (decision && typeof decision === "object") {
    return toLabel(decision.status || decision.decision || decision.recommendation || "warn");
  }
  if (typeof decision === "string") {
    return toLabel(decision);
  }
  const gateStatus = String(qc?.quality_gate?.status || "").toLowerCase();
  if (gateStatus === "pass") {
    return "Approved For Downstream Use";
  }
  if (gateStatus === "warn") {
    return "Use With Caution";
  }
  if (gateStatus === "fail") {
    return "Hold For Remediation";
  }
  return qc ? "Review Outcomes Before Release" : "Run Cleaning To Generate Decision";
}

function factorActionText(factor) {
  const factorId = String(factor?.id || "").toLowerCase();
  const value = typeof factor?.value_pct === "number" ? `${factor.value_pct}%` : "n/a";

  if (factorId === "high_missing_columns_pct") {
    return {
      happened: `${value} of columns are heavily missing.`,
      why: "Missing-heavy columns reduce usable signal and can destabilize model behavior.",
      action: "Review high-missing columns and decide drop, impute, or retain before running cleanup.",
    };
  }

  if (factorId === "low_variance_columns_pct") {
    return {
      happened: `${value} of columns have low variance.`,
      why: "Low-variance fields often add little predictive value and can bloat embeddings.",
      action: "Drop or deprioritize low-variance fields unless required for joins or compliance.",
    };
  }

  if (factorId === "pii_suspected_columns_pct") {
    return {
      happened: `${value} of columns may contain PII.`,
      why: "Potential PII increases compliance risk and affects how data can be shared or trained on.",
      action: "Validate de-identification settings and confirm privacy mode before cleaning.",
    };
  }

  if (factorId === "schema_uncertainty_pct") {
    return {
      happened: `${value} of columns have uncertain schema inference.`,
      why: "Schema ambiguity can cause normalization drift and downstream parse failures.",
      action: "Review semantic hints and coercion strategy for uncertain columns.",
    };
  }

  return {
    happened: `${value} observed for this readiness factor.`,
    why: "This factor contributes directly to readiness score penalties.",
    action: "Review this factor and adjust cleaning settings based on risk tolerance.",
  };
}

function renderAssessmentBlockers(profile) {
  assessmentBlockersEl.innerHTML = "";
  if (!profile) return;

  const checks = (profile.rag_readiness && profile.rag_readiness.checks) || [];
  const blockers = checks
    .filter((check) => {
      const status = String(check.status || "").toLowerCase();
      return status === "fail" || status === "warn";
    })
    .sort((a, b) => {
      const rank = (value) => (value === "fail" ? 0 : value === "warn" ? 1 : 2);
      return rank(String(a.status || "").toLowerCase()) - rank(String(b.status || "").toLowerCase());
    })
    .slice(0, 5);

  const card = document.createElement("div");
  card.className = "detail-card";
  const title = document.createElement("h4");
  title.textContent = "Top blockers to 95%";
  card.appendChild(title);

  if (!blockers.length) {
    const empty = document.createElement("p");
    empty.textContent = "No major blockers detected in pre-clean checks.";
    card.appendChild(empty);
  } else {
    blockers.forEach((item, idx) => {
      const row = document.createElement("p");
      const severity = String(item.status || "").toLowerCase() === "fail" ? "high" : "medium";
      row.innerHTML = `${idx + 1}. <span class="${severityClass(severity)}">${severityLabel(severity)}</span> ${item.label || item.id}: ${item.recommendation || "Review this check."}`;
      card.appendChild(row);
    });
  }
  assessmentBlockersEl.appendChild(card);
}

function renderAssessmentExplainability(assessment) {
  assessmentExplainabilityEl.innerHTML = "";
  if (!assessment || !Array.isArray(assessment.factors)) return;

  const factors = assessment.factors
    .slice()
    .sort((a, b) => toNumber(b.penalty, 0) - toNumber(a.penalty, 0))
    .slice(0, 3);
  if (!factors.length) return;

  factors.forEach((factor, idx) => {
    const actionText = factorActionText(factor);
    const card = document.createElement("div");
    card.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = `Explainability #${idx + 1}: ${factor.name || factor.id}`;
    const happened = document.createElement("p");
    happened.textContent = `What happened: ${actionText.happened}`;
    const why = document.createElement("p");
    why.textContent = `Why it matters: ${actionText.why}`;
    const action = document.createElement("p");
    action.textContent = `Action: ${actionText.action}`;
    card.appendChild(title);
    card.appendChild(happened);
    card.appendChild(why);
    card.appendChild(action);
    assessmentExplainabilityEl.appendChild(card);
  });
}

function changedFieldsCountFallback(qc) {
  const changedColumns = new Set();
  const invalidValues = qc?.invalid_values || {};
  Object.entries(invalidValues).forEach(([column, count]) => {
    if (toNumber(count, 0) > 0) {
      changedColumns.add(column);
    }
  });

  const missingRaw = qc?.missing_pct_raw || {};
  const missingCleaned = qc?.missing_pct_cleaned || {};
  Object.keys(missingRaw).forEach((column) => {
    const rawValue = toNumber(missingRaw[column], 0);
    const cleanValue = toNumber(missingCleaned[column], rawValue);
    if (Math.abs(rawValue - cleanValue) > 0.01) {
      changedColumns.add(column);
    }
  });

  (qc?.empty_columns_removed || []).forEach((column) => changedColumns.add(column));
  return changedColumns.size;
}

function formatChangeExample(example) {
  if (typeof example === "string") {
    return example;
  }
  if (!example || typeof example !== "object") {
    return "";
  }

  const column = example.column || example.field || example.name || "field";
  const before = example.before ?? example.raw ?? example.previous;
  const after = example.after ?? example.cleaned ?? example.current;
  if (before !== undefined || after !== undefined) {
    const beforeText = before === undefined ? "n/a" : formatCell(before);
    const afterText = after === undefined ? "n/a" : formatCell(after);
    return `${column}: ${beforeText} -> ${afterText}`;
  }

  if (example.description) {
    return String(example.description);
  }
  return JSON.stringify(example);
}

function buildChangeSummary(qc) {
  const summary = qc?.change_summary || {};
  const removedColumnsList = summary.removed_columns || qc?.empty_columns_removed || [];
  const changedFieldsCount =
    summary.changed_fields_count ??
    summary.changed_fields ??
    summary.fields_changed_count ??
    changedFieldsCountFallback(qc);

  const rowsRemoved = summary.rows_removed ?? summary.removed_rows ?? qc?.rows_removed ?? 0;
  const colsRemoved = summary.cols_removed ?? summary.columns_removed ?? removedColumnsList.length;

  const rawExamples =
    summary.examples ??
    summary.sample_examples ??
    summary.samples ??
    [];
  const formattedExamples = Array.isArray(rawExamples)
    ? rawExamples.map(formatChangeExample).filter(Boolean)
    : [];

  if (!formattedExamples.length) {
    const fallback = (qc?.issues || [])
      .slice(0, 3)
      .map((issue) => `${issue.column || "dataset"}: ${issue.message || "Quality issue detected"}`);
    formattedExamples.push(...fallback);
  }

  return {
    changed_fields_count: toNumber(changedFieldsCount, 0),
    rows_removed: toNumber(rowsRemoved, 0),
    cols_removed: toNumber(colsRemoved, 0),
    removed_columns: Array.isArray(removedColumnsList) ? removedColumnsList : [],
    examples: formattedExamples.slice(0, 6),
    note: summary.note || summary.summary || "",
  };
}

function renderWhatChanged(qc) {
  whatChangedSummaryEl.textContent = "";
  whatChangedMetricsEl.innerHTML = "";
  whatChangedExamplesEl.innerHTML = "";

  if (!qc) {
    whatChangedSummaryEl.textContent = "Run cleaning to generate a change summary and before/after deltas.";
    return;
  }

  const summary = buildChangeSummary(qc);
  const noteSuffix = summary.note ? ` ${summary.note}` : "";
  whatChangedSummaryEl.textContent =
    `Changed fields: ${summary.changed_fields_count} | Rows removed: ${summary.rows_removed} | Columns removed: ${summary.cols_removed}.${noteSuffix}`;

  const metricCards = [
    { label: "Changed fields", value: summary.changed_fields_count },
    { label: "Rows removed", value: summary.rows_removed },
    { label: "Columns removed", value: summary.cols_removed },
  ];
  metricCards.forEach((item) => {
    const card = document.createElement("div");
    card.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = item.label;
    const value = document.createElement("p");
    value.textContent = String(item.value);
    card.appendChild(title);
    card.appendChild(value);
    whatChangedMetricsEl.appendChild(card);
  });

  if (summary.removed_columns.length) {
    const removedCard = document.createElement("div");
    removedCard.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = "Removed columns";
    const value = document.createElement("p");
    value.textContent = summary.removed_columns.join(", ");
    removedCard.appendChild(title);
    removedCard.appendChild(value);
    whatChangedMetricsEl.appendChild(removedCard);
  }

  if (summary.examples.length) {
    summary.examples.forEach((example) => {
      const row = document.createElement("div");
      row.className = "issue issue--low";
      row.textContent = example;
      whatChangedExamplesEl.appendChild(row);
    });
  } else {
    const row = document.createElement("div");
    row.className = "issue issue--low";
    row.textContent = "No sample change examples were provided.";
    whatChangedExamplesEl.appendChild(row);
  }
}

function renderOutcomes(qc) {
  qcOutcomesEl.innerHTML = "";
  if (!qc) return;
  const outcomes = qc.outcomes || [];
  const qualityGate = qc.quality_gate || null;
  if (!outcomes.length && !qualityGate) return;

  if (qualityGate) {
    const gateCard = document.createElement("div");
    gateCard.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = "Quality gate";
    const status = document.createElement("p");
    status.innerHTML = `<span class="${severityClass(qualityGate.status === "fail" ? "high" : qualityGate.status === "warn" ? "medium" : "low")}">${qualityGate.status || "warn"}</span> | mode=${qualityGate.mode || "warn"}`;
    const summary = document.createElement("p");
    summary.textContent = qualityGate.summary || "";
    gateCard.appendChild(title);
    gateCard.appendChild(status);
    gateCard.appendChild(summary);
    qcOutcomesEl.appendChild(gateCard);
  }

  outcomes.forEach((outcome) => {
    const card = document.createElement("div");
    card.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = outcome.label || outcome.id;
    const meta = document.createElement("p");
    const outcomeStatus = String(outcome.status || "warn").toLowerCase();
    const outcomeSeverity = outcomeStatus === "fail" ? "high" : outcomeStatus === "warn" ? "medium" : "low";
    meta.innerHTML = `<span class="${severityClass(outcomeSeverity)}">${toLabel(outcomeStatus)}</span> | Target: ${outcome.target || "n/a"} | Observed: ${outcome.observed_value} ${outcome.unit || ""}`;
    const evidence = document.createElement("p");
    evidence.textContent = `Evidence: ${outcome.evidence || "n/a"}`;
    const action = document.createElement("p");
    action.textContent = `Action: ${outcome.recommended_action || "n/a"}`;
    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(evidence);
    card.appendChild(action);
    qcOutcomesEl.appendChild(card);
  });
}

function renderRunHistory(runs) {
  runHistoryEl.innerHTML = "";
  if (!runs || !runs.length) {
    const empty = document.createElement("div");
    empty.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = "Run history";
    const text = document.createElement("p");
    text.textContent = "No cleaning runs yet for this dataset.";
    empty.appendChild(title);
    empty.appendChild(text);
    runHistoryEl.appendChild(empty);
    return;
  }

  const card = document.createElement("div");
  card.className = "detail-card";
  const title = document.createElement("h4");
  title.textContent = "Recent runs";
  card.appendChild(title);

  const list = document.createElement("div");
  list.className = "run-list";
  runs.slice(0, 8).forEach((run) => {
    const item = document.createElement("div");
    item.className = "run-item";
    const heading = document.createElement("strong");
    heading.innerHTML = `${run.id} <span class="${severityClass(run.status === "failed" ? "high" : run.status === "completed" ? "low" : "medium")}">${run.status}</span>`;
    const info = document.createElement("p");
    info.textContent = `Started: ${formatDateTime(run.started_at)} | Duration: ${run.duration_ms || 0} ms | Mode: ${run.performance_mode || "balanced"}`;
    item.appendChild(heading);
    item.appendChild(info);
    if (run.quality_gate && run.quality_gate.summary) {
      const gate = document.createElement("p");
      gate.textContent = `Gate: ${run.quality_gate.summary}`;
      item.appendChild(gate);
    }
    const ragComparison = run.qc && run.qc.rag_readiness_comparison;
    if (ragComparison) {
      const ragSummary = document.createElement("p");
      const scoreDelta = typeof ragComparison.score_delta === "number"
        ? `${ragComparison.score_delta >= 0 ? "+" : ""}${ragComparison.score_delta}`
        : "n/a";
      ragSummary.textContent = `RAG delta: ${scoreDelta} (${ragComparison.band_before || "n/a"} -> ${ragComparison.band_after || "n/a"})`;
      item.appendChild(ragSummary);
    }
    list.appendChild(item);
  });
  card.appendChild(list);
  runHistoryEl.appendChild(card);
}

function renderRagReadiness(container, readiness, title) {
  container.innerHTML = "";
  if (!readiness) return;

  const summaryCard = document.createElement("div");
  summaryCard.className = "detail-card";
  const titleEl = document.createElement("h4");
  titleEl.textContent = title;
  const scoreEl = document.createElement("p");
  const targetStatus = readinessTargetStatus(readiness.score);
  scoreEl.innerHTML = `<strong>${readiness.score}/100</strong> <span class="${bandClass(readiness.band)}">${readiness.band}</span> | Target: <strong>95</strong> <span class="${severityClass(targetStatus.severity)}">${targetStatus.label}</span>`;
  const summaryEl = document.createElement("p");
  summaryEl.textContent = readiness.summary || "";
  summaryCard.appendChild(titleEl);
  summaryCard.appendChild(scoreEl);
  summaryCard.appendChild(summaryEl);
  if (readiness.sampled_note) {
    const sampled = document.createElement("p");
    sampled.textContent = readiness.sampled_note;
    summaryCard.appendChild(sampled);
  }
  container.appendChild(summaryCard);

  const checks = readiness.checks || [];
  const actionableChecks = checks.filter((check) => {
    const status = String(check.status || "warn").toLowerCase();
    return status === "fail" || status === "warn";
  });
  const uniqueActions = Array.from(
    new Set(
      actionableChecks
        .map((check) => String(check.recommendation || "").trim())
        .filter(Boolean)
    )
  ).slice(0, 4);

  const actionCard = document.createElement("div");
  actionCard.className = "detail-card";
  const actionTitle = document.createElement("h4");
  actionTitle.textContent = "What to fix next";
  actionCard.appendChild(actionTitle);
  if (!uniqueActions.length) {
    const doneText = document.createElement("p");
    doneText.textContent = "No urgent RAG fixes detected.";
    actionCard.appendChild(doneText);
  } else {
    uniqueActions.forEach((item, index) => {
      const p = document.createElement("p");
      p.textContent = `${index + 1}. ${item}`;
      actionCard.appendChild(p);
    });
  }
  container.appendChild(actionCard);

  const advanced = document.createElement("details");
  advanced.className = "advanced-panel";
  const advancedSummary = document.createElement("summary");
  advancedSummary.textContent = `Advanced: RAG checks (${checks.length})`;
  advanced.appendChild(advancedSummary);

  const checksWrap = document.createElement("div");
  checksWrap.className = "rag-check-list";
  checks.forEach((check) => {
    const checkEl = document.createElement("div");
    checkEl.className = "rag-check";

    const header = document.createElement("div");
    header.className = "rag-check-header";
    const label = document.createElement("strong");
    label.textContent = check.label || check.id;
    const sev = document.createElement("span");
    sev.className = severityClass(check.severity);
    sev.textContent = severityLabel(check.severity);
    header.appendChild(label);
    header.appendChild(sev);

    const meta = document.createElement("div");
    meta.className = "rag-check-meta";
    meta.textContent = `Status: ${toLabel(check.status)} | Metric: ${metricText(check.metric)} | ${check.threshold || "n/a"} | ${check.recommendation || ""}`;

    checkEl.appendChild(header);
    checkEl.appendChild(meta);
    checksWrap.appendChild(checkEl);
  });
  advanced.appendChild(checksWrap);
  container.appendChild(advanced);
}

function renderRagImprovementPlan(container, comparison) {
  container.innerHTML = "";
  if (!comparison) return;

  const summaryCard = document.createElement("div");
  summaryCard.className = "detail-card";
  const titleEl = document.createElement("h4");
  titleEl.textContent = "RAG Progress";
  const scoreEl = document.createElement("p");
  const scoreDelta = typeof comparison.score_delta === "number"
    ? `${comparison.score_delta >= 0 ? "+" : ""}${comparison.score_delta}`
    : "n/a";
  scoreEl.innerHTML = `Before: <strong>${comparison.score_before ?? "n/a"}</strong> <span class="${bandClass(comparison.band_before)}">${comparison.band_before || "n/a"}</span> | After: <strong>${comparison.score_after ?? "n/a"}</strong> <span class="${bandClass(comparison.band_after)}">${comparison.band_after || "n/a"}</span> | Change: <strong>${scoreDelta}</strong>`;
  const summaryEl = document.createElement("p");
  summaryEl.textContent = comparison.summary || "";
  summaryCard.appendChild(titleEl);
  summaryCard.appendChild(scoreEl);
  summaryCard.appendChild(summaryEl);
  container.appendChild(summaryCard);

  const actionsCard = document.createElement("div");
  actionsCard.className = "detail-card";
  const actionsTitle = document.createElement("h4");
  actionsTitle.textContent = "Priority actions";
  actionsCard.appendChild(actionsTitle);

  const actions = comparison.priority_actions || [];
  if (!actions.length) {
    const empty = document.createElement("p");
    empty.textContent = "No high-priority actions detected.";
    actionsCard.appendChild(empty);
  } else {
    actions.slice(0, 5).forEach((action, index) => {
      const prioritySeverity = action.priority === "high" ? "high" : "medium";
      const row = document.createElement("p");
      const chip = document.createElement("span");
      chip.className = severityClass(prioritySeverity);
      chip.textContent = severityLabel(prioritySeverity);
      row.append(`${index + 1}. `);
      row.appendChild(chip);
      row.append(` ${action.label || action.check_id}: ${action.action || action.reason || "Review this check."}`);
      actionsCard.appendChild(row);
    });
  }
  container.appendChild(actionsCard);

  const advanced = document.createElement("details");
  advanced.className = "advanced-panel";
  const advancedSummary = document.createElement("summary");
  advancedSummary.textContent = "Advanced: check-level deltas";
  advanced.appendChild(advancedSummary);

  const deltaCard = document.createElement("div");
  deltaCard.className = "detail-card";
  const deltaTitle = document.createElement("h4");
  deltaTitle.textContent = "Check deltas";
  deltaCard.appendChild(deltaTitle);

  const deltas = comparison.check_deltas || [];
  deltas.forEach((delta) => {
    const row = document.createElement("div");
    row.className = "rag-delta-row";

    const head = document.createElement("div");
    head.className = "rag-delta-head";
    const label = document.createElement("strong");
    label.textContent = delta.label || delta.id;
    const chip = document.createElement("span");
    chip.className = ragDeltaClass(delta.status_delta);
    chip.textContent = delta.status_delta || "unchanged";
    head.appendChild(label);
    head.appendChild(chip);

    const meta = document.createElement("p");
    const statusBefore = delta.status_before || "n/a";
    const statusAfter = delta.status_after || "n/a";
    const metricBefore = metricText(delta.metric_before);
    const metricAfter = metricText(delta.metric_after);
    meta.textContent =
      `${statusBefore} -> ${statusAfter} | Metric: ${metricBefore} -> ${metricAfter} | Delta: ${signedMetricText(delta.metric_delta)} | Priority: ${delta.priority || "low"}`;

    row.appendChild(head);
    row.appendChild(meta);
    deltaCard.appendChild(row);
  });
  advanced.appendChild(deltaCard);
  container.appendChild(advanced);
}

function renderAssessmentOverview(profile) {
  if (!profile) return;
  const summary = profile.summary || {};
  const highMissing = summary.columns_high_missing || [];
  const lowVariance = summary.low_variance_columns || [];
  const piiColumns = summary.columns_with_pii || [];
  const columnCount = profile.column_count || 0;
  const precleanDecision = resolvePrecleanDecision(profile);
  const domainSignals = normalizeDomainSignals(profile);
  const primaryDomain = profile.primary_domain || (domainSignals[0] && domainSignals[0].domain) || "n/a";
  const assessment = profile.assessment || {
    score: 0,
    band: "fair",
    factors: [],
    definitions: {},
  };

  const precheckStatus = readinessTargetStatus(assessment.score);
  assessmentPrimaryEl.textContent =
    `Pre-clean readiness: ${assessment.score}/100 (${toLabel(assessment.band)}). Status: ${precheckStatus.label}. Goal is to reach 95 after cleanup.`;
  renderAssessmentBlockers(profile);
  renderAssessmentExplainability(assessment);

  const cards = [
    { label: "Readiness score", value: `${assessment.score}/100`, band: assessment.band },
    { label: "Pre-clean decision", value: precleanDecision },
    { label: "Primary domain", value: toLabel(primaryDomain) },
    { label: "Columns needing missingness attention", value: `${highMissing.length}` },
    { label: "Columns with low information value", value: `${lowVariance.length}` },
    { label: "Columns needing privacy review", value: `${piiColumns.length}` },
    { label: "Total columns reviewed", value: `${columnCount}` },
  ];

  assessmentOverviewEl.innerHTML = "";
  cards.forEach((card) => {
    const div = document.createElement("div");
    div.className = "assessment-card";
    const label = document.createElement("span");
    label.textContent = card.label;
    const strong = document.createElement("strong");
    strong.textContent = card.value;
    div.appendChild(label);
    div.appendChild(strong);
    if (card.band) {
      const band = document.createElement("span");
      band.className = bandClass(card.band);
      band.textContent = card.band;
      div.appendChild(band);
    }
    assessmentOverviewEl.appendChild(div);
  });

  assessmentLegendEl.textContent =
    "Advanced diagnostics below include scoring formulas, factor weights, and raw ratios used for the readiness calculation.";

  assessmentFactorsEl.innerHTML = "";
  (assessment.factors || []).forEach((factor) => {
    const actionText = factorActionText(factor);
    const valuePct = toNumber(factor.value_pct, 0);
    const weight = toNumber(factor.weight, 0);
    const computedPenalty = Math.round(valuePct * weight * 100) / 100;
    const reportedPenalty = toNumber(factor.penalty, 0);

    const card = document.createElement("div");
    card.className = "detail-card";
    const h4 = document.createElement("h4");
    h4.textContent = factor.name || factor.id;
    const happened = document.createElement("p");
    happened.className = "action-line";
    happened.textContent = `What happened: ${actionText.happened}`;
    const why = document.createElement("p");
    why.className = "action-line";
    why.textContent = `Why it matters: ${actionText.why}`;
    const action = document.createElement("p");
    action.className = "action-line";
    action.textContent = `Action: ${actionText.action}`;

    const advanced = document.createElement("details");
    advanced.className = "advanced-details";
    const advancedSummary = document.createElement("summary");
    advancedSummary.textContent = "Advanced details";
    const advancedMeta = document.createElement("div");
    advancedMeta.className = "advanced-meta";
    const valueLine = document.createElement("p");
    valueLine.textContent = `Value: ${valuePct}%`;
    const weightLine = document.createElement("p");
    weightLine.textContent = `Weight: ${weight}`;
    const penaltyLine = document.createElement("p");
    penaltyLine.textContent = `Penalty: ${reportedPenalty}`;
    const formulaLine = document.createElement("p");
    formulaLine.textContent = `Formula: value_pct * weight = ${valuePct} * ${weight} = ${computedPenalty}`;
    const idLine = document.createElement("p");
    idLine.textContent = `Factor id: ${factor.id || "n/a"}`;
    advancedMeta.appendChild(valueLine);
    advancedMeta.appendChild(weightLine);
    advancedMeta.appendChild(penaltyLine);
    advancedMeta.appendChild(formulaLine);
    advancedMeta.appendChild(idLine);
    advanced.appendChild(advancedSummary);
    advanced.appendChild(advancedMeta);

    card.appendChild(h4);
    card.appendChild(happened);
    card.appendChild(why);
    card.appendChild(action);
    card.appendChild(advanced);
    assessmentFactorsEl.appendChild(card);
  });

  if (assessment.sampled_note) {
    const sampledCard = document.createElement("div");
    sampledCard.className = "detail-card";
    const sampledTitle = document.createElement("h4");
    sampledTitle.textContent = "Sampling note";
    const sampledText = document.createElement("p");
    sampledText.textContent = assessment.sampled_note;
    sampledCard.appendChild(sampledTitle);
    sampledCard.appendChild(sampledText);
    assessmentFactorsEl.appendChild(sampledCard);
  }

  assessmentAdvancedEl.textContent = JSON.stringify(
    {
      preclean_decision: profile.preclean_decision || null,
      primary_domain: profile.primary_domain || null,
      detected_domains: profile.detected_domains || profile.domains || [],
      assessment,
    },
    null,
    2
  );

  renderRagReadiness(assessmentRagEl, profile.rag_readiness, "RAG readiness (pre-clean)");
}

function renderMetadata(dataset, profile) {
  if (!dataset) return;
  const items = [
    { label: "File type", value: dataset.file_type || "csv" },
    { label: "File size", value: formatBytes(dataset.file_size_bytes) },
    { label: "Row estimate", value: dataset.row_count_estimate || profile?.row_count || "n/a" },
    { label: "Usage intent", value: dataset.usage_intent || "training" },
    { label: "Output format", value: dataset.output_format || "csv" },
    { label: "Privacy mode", value: dataset.privacy_mode || "safe_harbor" },
    {
      label: "Profile", 
      value: profile?.sampled ? `Sampled (${profile.sampled_rows} rows)` : "Full scan",
    },
  ];

  metaGrid.innerHTML = "";
  items.forEach((item) => {
    const card = document.createElement("div");
    card.className = "meta-card";
    card.innerHTML = `<span>${item.label}</span><strong>${item.value}</strong>`;
    metaGrid.appendChild(card);
  });
}

function renderProfile(profile) {
  if (!profile) {
    profileSummaryEl.textContent = "";
    assessmentPrimaryEl.textContent = "";
    assessmentBlockersEl.innerHTML = "";
    assessmentExplainabilityEl.innerHTML = "";
    assessmentOverviewEl.innerHTML = "";
    assessmentLegendEl.textContent = "";
    assessmentFactorsEl.innerHTML = "";
    assessmentRagEl.innerHTML = "";
    profileTable.innerHTML = "";
    assessmentAdvancedEl.textContent = "";
    return;
  }

  const summary = profile.summary || {};
  const highMissing = summary.columns_high_missing || [];
  const piiColumns = summary.columns_with_pii || [];
  const lowVariance = summary.low_variance_columns || [];
  const domainSignals = normalizeDomainSignals(profile);
  const primaryDomain = profile.primary_domain || (domainSignals[0] && domainSignals[0].domain) || "n/a";
  const precleanDecision = resolvePrecleanDecision(profile);
  const precleanPayload = profile.preclean_decision && typeof profile.preclean_decision === "object"
    ? profile.preclean_decision
    : null;

  profileSummaryEl.textContent =
    `Rows: ${profile.row_count} | Columns: ${profile.column_count} | ` +
    `Pre-clean decision: ${precleanDecision}`;

  renderAssessmentOverview(profile);

  const flags = [
    { label: "Primary domain", value: toLabel(primaryDomain) },
    {
      label: "Pre-clean reasons",
      value: precleanPayload?.reasons?.length ? precleanPayload.reasons.slice(0, 2).join(" | ") : "None",
    },
    {
      label: "Pre-clean actions",
      value: precleanPayload?.actions?.length ? precleanPayload.actions.slice(0, 2).join(" | ") : "Proceed with standard cleaning settings.",
    },
    {
      label: "Missingness attention",
      value: highMissing.length ? `${highMissing.length} columns (examples: ${highMissing.slice(0, 3).join(", ")})` : "None",
    },
    {
      label: "Privacy review",
      value: piiColumns.length ? `${piiColumns.length} columns (examples: ${piiColumns.slice(0, 3).join(", ")})` : "None",
    },
    {
      label: "Low-information fields",
      value: lowVariance.length ? `${lowVariance.length} columns (examples: ${lowVariance.slice(0, 3).join(", ")})` : "None",
    },
  ];

  profileFlagsEl.innerHTML = "";
  flags.forEach((flag) => {
    const div = document.createElement("div");
    div.className = "flag";
    div.textContent = `${flag.label}: ${flag.value}`;
    profileFlagsEl.appendChild(div);
  });

  domainSuggestionsEl.innerHTML = "";
  if (domainSignals.length) {
    domainSignals.slice(0, 5).forEach((domain) => {
      const div = document.createElement("div");
      div.className = "flag";
      const scoreText = domain.score === null || domain.score === undefined ? "n/a" : domain.score;
      div.textContent = `Detected domain: ${toLabel(domain.domain)} (score ${scoreText})`;
      domainSuggestionsEl.appendChild(div);
    });
  } else {
    const div = document.createElement("div");
    div.className = "flag";
    div.textContent = "Detected domains: none";
    domainSuggestionsEl.appendChild(div);
  }

  const columns = [
    "clean_name",
    "semantic_hint",
    "domain_tags",
    "primitive_type",
    "missing_pct",
    "distinct_count",
    "example_values",
    "notes",
  ];
  const rows = profile.columns || [];
  profileTable.innerHTML = "";
  profileTable.appendChild(createTable(columns, rows));
}

function renderPreview(targetTable, preview) {
  if (!preview) {
    targetTable.innerHTML = "";
    return;
  }
  targetTable.innerHTML = "";
  targetTable.appendChild(createTable(preview.columns, preview.rows));
}

function renderQc(qc) {
  renderWhatChanged(qc);
  if (!qc) {
    qcDecisionEl.textContent = "";
    qcPrimaryResultEl.textContent = "";
    if (qcDownloadLink) {
      qcDownloadLink.classList.add("hidden");
      qcDownloadLink.removeAttribute("href");
    }
    qcSummaryEl.textContent = "";
    qcOutcomesEl.innerHTML = "";
    qcSeveritySummaryEl.innerHTML = "";
    qcSeverityLegendEl.innerHTML = "";
    qcRagEl.innerHTML = "";
    qcRagPlanEl.innerHTML = "";
    qcDetailsEl.textContent = "";
    qcIssuesEl.innerHTML = "";
    return;
  }

  const postcleanDecision = resolvePostcleanDecision(qc);
  const gateLabel = qc.quality_gate?.status ? toLabel(qc.quality_gate.status) : "n/a";
  qcDecisionEl.textContent = `Post-clean decision: ${postcleanDecision} | Quality gate: ${gateLabel}`;

  const ragAfter = qc.rag_readiness || {};
  const ragScore = Number(ragAfter.score);
  const ragStatus = readinessTargetStatus(ragScore);
  qcPrimaryResultEl.innerHTML =
    `RAG readiness after cleanup: <strong>${Number.isFinite(ragScore) ? ragScore : "n/a"}/100</strong> | Target: <strong>95</strong> <span class="${severityClass(ragStatus.severity)}">${ragStatus.label}</span>`;

  if (qcDownloadLink && currentDataset?.id) {
    qcDownloadLink.href = apiUrl(`/datasets/${currentDataset.id}/download?kind=cleaned`);
    qcDownloadLink.classList.remove("hidden");
  }

  const severityCounts = (qc.severity_summary && qc.severity_summary.counts) || {};
  const warningText = qc.warnings && qc.warnings.length ? ` | Warnings: ${qc.warnings.length}` : "";
  qcSummaryEl.textContent =
    `Raw rows: ${qc.row_count_raw} | Cleaned rows: ${qc.row_count_cleaned} | Removed rows: ${qc.rows_removed}${warningText} | ` +
    `Critical issues: ${severityCounts.high || 0}, Attention issues: ${severityCounts.medium || 0}, Info issues: ${severityCounts.low || 0}`;
  renderOutcomes(qc);

  const decisionPayload = qc.postclean_decision || {};
  if (decisionPayload && typeof decisionPayload === "object") {
    const releaseText = decisionPayload.release_recommendation || "";
    if (releaseText) {
      const releaseCard = document.createElement("div");
      releaseCard.className = "detail-card";
      const title = document.createElement("h4");
      title.textContent = "Release recommendation";
      const text = document.createElement("p");
      text.textContent = releaseText;
      releaseCard.appendChild(title);
      releaseCard.appendChild(text);

      const blockers = Array.isArray(decisionPayload.blockers) ? decisionPayload.blockers : [];
      if (blockers.length) {
        const blockerText = document.createElement("p");
        blockerText.textContent = `Blockers: ${blockers.join(" | ")}`;
        releaseCard.appendChild(blockerText);
      }
      const actions = Array.isArray(decisionPayload.actions) ? decisionPayload.actions : [];
      if (actions.length) {
        const actionText = document.createElement("p");
        actionText.textContent = `Actions: ${actions.join(" | ")}`;
        releaseCard.appendChild(actionText);
      }
      qcOutcomesEl.prepend(releaseCard);
    }
  }

  qcSeveritySummaryEl.innerHTML = "";
  const summaryItems = [
    { label: "Critical", value: severityCounts.high || 0, severity: "high" },
    { label: "Attention", value: severityCounts.medium || 0, severity: "medium" },
    { label: "Info", value: severityCounts.low || 0, severity: "low" },
  ];
  summaryItems.forEach((item) => {
    const card = document.createElement("div");
    card.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = item.label;
    const row = document.createElement("div");
    row.className = "severity-row";
    const value = document.createElement("strong");
    value.textContent = String(item.value);
    const chip = document.createElement("span");
    chip.className = severityClass(item.severity);
    chip.textContent = severityLabel(item.severity);
    row.appendChild(value);
    row.appendChild(chip);
    card.appendChild(title);
    card.appendChild(row);
    qcSeveritySummaryEl.appendChild(card);
  });

  qcSeverityLegendEl.innerHTML = "";
  const legend = qc.severity_legend || {};
  ["high", "medium", "low"].forEach((severity) => {
    const card = document.createElement("div");
    card.className = "detail-card";
    const title = document.createElement("h4");
    title.textContent = severityLabel(severity);
    const chip = document.createElement("span");
    chip.className = severityClass(severity);
    chip.textContent = severityLabel(severity);
    const text = document.createElement("p");
    text.textContent = legend[severity] || "";
    card.appendChild(title);
    card.appendChild(chip);
    card.appendChild(text);
    qcSeverityLegendEl.appendChild(card);
  });

  renderRagReadiness(qcRagEl, qc.rag_readiness, "RAG readiness (post-clean)");
  renderRagImprovementPlan(qcRagPlanEl, qc.rag_readiness_comparison);

  qcDetailsEl.textContent = JSON.stringify(qc, null, 2);

  qcIssuesEl.innerHTML = "";
  const issues = qc.issues || [];
  if (issues.length) {
    issues
      .slice()
      .sort((a, b) => (b.severity_score || 0) - (a.severity_score || 0))
      .slice(0, 12)
      .forEach((issue) => {
      const div = document.createElement("div");
      const severity = normalizeSeverity(issue.severity || "low");
      div.className = `issue issue--${severity}`;
      const reason = issue.severity_reason ? ` | ${issue.severity_reason}` : "";
      div.textContent =
        `${severityLabel(severity)}: ${issue.message} (${issue.column}) - ${issue.count}${reason}`;
      qcIssuesEl.appendChild(div);
      });
  } else {
    const empty = document.createElement("div");
    empty.className = "issue issue--low";
    empty.textContent = "No validation issues were reported for this run.";
    qcIssuesEl.appendChild(empty);
  }
}

async function fetchPreview(kind) {
  if (!currentDataset) return null;
  const response = await fetch(apiUrl(`/datasets/${currentDataset.id}/preview?kind=${kind}&limit=50`));
  if (!response.ok) {
    throw new Error("Failed to load preview.");
  }
  return response.json();
}

function uploadDataset(formData) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", apiUrl("/datasets"), true);

    xhr.upload.addEventListener("progress", (event) => {
      if (event.lengthComputable) {
        const percent = Math.round((event.loaded / event.total) * 100);
        setUploadProgress(percent);
      }
    });

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          resolve(JSON.parse(xhr.responseText));
        } catch (error) {
          reject(new Error("Upload succeeded but response parsing failed."));
        }
      } else {
        let message = `Upload failed (${xhr.status}) at ${xhr.responseURL || "unknown URL"}.`;
        try {
          const error = JSON.parse(xhr.responseText);
          message = error.detail || message;
        } catch (error) {
          // ignore parse errors
        }
        reject(new Error(message));
      }
    };

    xhr.onerror = () => reject(new Error("Upload failed."));
    xhr.send(formData);
  });
}

function uploadChunk(uploadId, index, chunk, uploadedSoFar, totalSize) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append("index", index);
    formData.append("chunk", chunk, `chunk_${index}`);
    xhr.open("POST", apiUrl(`/uploads/${uploadId}/chunk`), true);
    xhr.upload.addEventListener("progress", (event) => {
      if (event.lengthComputable) {
        const percent = Math.round(((uploadedSoFar + event.loaded) / totalSize) * 100);
        setUploadProgress(percent);
      }
    });
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve();
      } else {
        let message = `Chunk ${index} upload failed (${xhr.status}) at ${xhr.responseURL || "unknown URL"}.`;
        try {
          const error = JSON.parse(xhr.responseText);
          message = error.detail || message;
        } catch (err) {
          // ignore parse errors
        }
        reject(new Error(message));
      }
    };
    xhr.onerror = () => reject(new Error("Chunk upload failed."));
    xhr.send(formData);
  });
}

async function uploadDatasetWithChunks(file, meta) {
  const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
  const startPayload = {
    filename: file.name,
    name: meta.name,
    usage_intent: meta.usage_intent,
    output_format: meta.output_format,
    privacy_mode: meta.privacy_mode,
    file_size: file.size,
    total_chunks: totalChunks,
  };
  const startRes = await fetch(apiUrl("/uploads/start"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(startPayload),
  });
  if (!startRes.ok) {
    let message = `Failed to start chunked upload (${startRes.status}) at ${startRes.url}.`;
    try {
      const error = await startRes.json();
      message = error.detail || message;
    } catch (err) {
      // ignore parse errors
    }
    const err = new Error(message);
    err.status = startRes.status;
    throw err;
  }
  const startData = await startRes.json();
  const uploadId = startData.upload_id;

  let uploadedSoFar = 0;
  for (let index = 0; index < totalChunks; index += 1) {
    const start = index * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, file.size);
    const chunk = file.slice(start, end);
    await uploadChunk(uploadId, index, chunk, uploadedSoFar, file.size);
    uploadedSoFar += chunk.size;
    setUploadProgress(Math.round((uploadedSoFar / file.size) * 100));
  }

  const completeRes = await fetch(apiUrl(`/uploads/${uploadId}/complete`), { method: "POST" });
  if (!completeRes.ok) {
    let message = `Failed to finalize upload (${completeRes.status}) at ${completeRes.url}.`;
    try {
      const error = await completeRes.json();
      message = error.detail || message;
    } catch (err) {
      // ignore parse errors
    }
    throw new Error(message);
  }
  return completeRes.json();
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function startCleanJob(datasetId, options) {
  const response = await fetch(apiUrl(`/datasets/${datasetId}/clean-jobs`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(options),
  });
  if (!response.ok) {
    let message = `Failed to start cleaning job (${response.status}) at ${response.url}.`;
    try {
      const error = await response.json();
      message = error.detail || message;
    } catch (err) {
      // ignore parse errors
    }
    const error = new Error(message);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

async function fetchRunHistory() {
  if (!currentDataset) return [];
  const response = await fetch(apiUrl(`/datasets/${currentDataset.id}/runs?limit=20`));
  if (!response.ok) {
    return [];
  }
  const payload = await response.json();
  return payload.items || [];
}

async function refreshRunHistory() {
  const runs = await fetchRunHistory();
  renderRunHistory(runs);
}

async function runAutopilotClean(datasetId, payload = { target_score: 95 }) {
  setCleanProgress(20, "Autopilot analysis");
  const response = await fetch(apiUrl(`/datasets/${datasetId}/cleanup/autopilot`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    let message = `Autopilot cleanup failed (${response.status}) at ${response.url}.`;
    try {
      const error = await response.json();
      message = error.detail || error.message || message;
      if (typeof message === "object") {
        message = message.message || JSON.stringify(message);
      }
    } catch (err) {
      // ignore parse errors
    }
    throw new Error(message);
  }
  setCleanProgress(85, "Autopilot QC and scoring");
  return response.json();
}

async function runSyncClean(datasetId, options) {
  setCleanProgress(20, "Cleaning (sync)");
  const response = await fetch(apiUrl(`/datasets/${datasetId}/clean`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(options),
  });
  if (!response.ok) {
    let message = `Cleaning failed (${response.status}) at ${response.url}.`;
    try {
      const error = await response.json();
      message = error.detail || message;
    } catch (err) {
      // ignore parse errors
    }
    throw new Error(message);
  }
  setCleanProgress(85, "Building QC");
  return response.json();
}

async function pollCleanJob(jobId) {
  while (true) {
    const response = await fetch(apiUrl(`/clean-jobs/${jobId}`));
    if (!response.ok) {
      throw new Error(
        `Failed to fetch job status (${response.status}) at ${response.url}.`
      );
    }
    const job = await response.json();
    if (typeof job.progress === "number") {
      setCleanProgress(job.progress, job.message);
    }
    if (job.status === "completed") {
      return job.result;
    }
    if (job.status === "failed") {
      throw new Error(job.error || "Cleaning failed.");
    }
    await wait(1000);
  }
}

function setFullscreen(enable) {
  if (enable) {
    previewCard.classList.add("fullscreen");
    previewCloseBtn.classList.remove("hidden");
    fullscreenBackdrop.classList.add("active");
    document.body.classList.add("no-scroll");
  } else {
    previewCard.classList.remove("fullscreen");
    previewCloseBtn.classList.add("hidden");
    fullscreenBackdrop.classList.remove("active");
    document.body.classList.remove("no-scroll");
  }
}

previewFullscreenBtn.addEventListener("click", () => {
  setFullscreen(true);
});

previewCloseBtn.addEventListener("click", () => {
  setFullscreen(false);
});

fullscreenBackdrop.addEventListener("click", () => {
  setFullscreen(false);
});

async function handleDatasetReady(dataset, statusMessage) {
  currentDataset = dataset;
  setStatus(statusEl, statusMessage || `Analysis complete for ${currentDataset.name}.`);
  renderProfile(currentDataset.profile);
  renderMetadata(currentDataset, currentDataset.profile);
  renderQc(currentDataset.qc);

  try {
    const preview = await fetchPreview("raw");
    renderPreview(rawPreviewTable, preview);
    renderPreview(previewRawTable, preview);
    renderPreview(previewCleanTable, null);
    await refreshRunHistory();
  } catch (error) {
    setStatus(statusEl, "Analysis complete, but preview failed to load.");
    await refreshRunHistory();
  }

  runCleaningBtn.disabled = false;
  if (runAutopilotBtn) runAutopilotBtn.disabled = false;
  refreshProfileBtn.disabled = false;
  previewFullscreenBtn.disabled = false;
  uploadButton.disabled = false;
  downloadLink.href = "#";
  downloadLink.textContent = "Download cleaned file";
  optOutputFormat.value = currentDataset.output_format || "csv";
  optCoercionMode.value = "safe";
  optPrivacyMode.value = currentDataset.privacy_mode || "safe_harbor";
  if (currentDataset.qc) {
    setWizardStep(4);
  } else {
    setWizardStep(3);
  }
}

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const fileInput = document.getElementById("dataset-file");
  const nameInput = datasetNameInput;

  if (!fileInput.files.length) {
    setStatus(statusEl, "Choose a file to upload.");
    return;
  }
  if (!apiHealthy) {
    setStatus(
      statusEl,
      `API not ready (${apiService}). Open the app at ${DEFAULT_API_BASE} and refresh.`
    );
    return;
  }

  const file = fileInput.files[0];
  const meta = {
    name: nameInput.value || null,
    usage_intent: usageIntentSelect.value,
    output_format: outputFormatSelect.value,
    privacy_mode: optPrivacyMode.value,
  };

  setStatus(statusEl, "Analyzing your dataset...");
  resetTables();
  setUploadProgress(0);
  uploadButton.disabled = true;
  runCleaningBtn.disabled = true;
  refreshProfileBtn.disabled = true;
  previewFullscreenBtn.disabled = true;

  const formData = new FormData();
  formData.append("file", file);
  if (nameInput.value) {
    formData.append("name", nameInput.value);
  }
  formData.append("usage_intent", usageIntentSelect.value);
  formData.append("output_format", outputFormatSelect.value);
  formData.append("privacy_mode", optPrivacyMode.value);

  try {
    if (file.size >= CHUNK_THRESHOLD) {
      try {
        currentDataset = await uploadDatasetWithChunks(file, meta);
      } catch (error) {
        if (error.status === 404 || error.status === 405) {
          if (file.size > STREAMING_THRESHOLD_MB * 1024 * 1024) {
            throw new Error(
              `Chunked upload endpoint unavailable at ${API_BASE}. For large files, open the app at ${DEFAULT_API_BASE} and refresh.`
            );
          }
          currentDataset = await uploadDataset(formData);
        } else {
          throw error;
        }
      }
    } else {
      currentDataset = await uploadDataset(formData);
    }
    setUploadProgress(100);
  } catch (error) {
    setStatus(statusEl, error.message || "Upload failed.");
    uploadButton.disabled = false;
    return;
  }
  await handleDatasetReady(currentDataset, `Analysis complete for ${currentDataset.name}.`);
});

if (runAutopilotBtn) {
  runAutopilotBtn.addEventListener("click", async () => {
    if (!currentDataset) return;
    if (!apiHealthy) {
      setStatus(
        cleanStatusEl,
        `API not ready (${apiService}). Open the app at ${DEFAULT_API_BASE} and refresh.`
      );
      return;
    }

    setStatus(cleanStatusEl, "Running 95% autopilot cleanup...");
    runAutopilotBtn.disabled = true;
    runCleaningBtn.disabled = true;
    uploadButton.disabled = true;

    try {
      setCleanProgress(0, "Starting autopilot");
      const result = await runAutopilotClean(currentDataset.id, {
        target_score: 95,
        output_format: optOutputFormat.value,
        privacy_mode: optPrivacyMode.value,
        performance_mode: optPerformanceMode ? optPerformanceMode.value : "balanced",
      });

      currentDataset = result.dataset;
      renderQc(result.qc);
      renderPreview(previewCleanTable, result.preview);
      await refreshRunHistory();
      setCleanProgress(100, "Completed");

      const autopilot = result.autopilot || {};
      const achieved = autopilot.achieved_score;
      const statusText = autopilot.status === "on_track" ? "On track for target." : "Needs attention to hit target.";
      const optimization = autopilot.optimization || {};
      const actions = Array.isArray(optimization.actions) ? optimization.actions : [];
      const actionText = actions.length
        ? ` Applied ${actions.length} optimization action(s).`
        : optimization.note
          ? ` ${optimization.note}`
          : "";
      const blockers = Array.isArray(autopilot.postclean_top_blockers) ? autopilot.postclean_top_blockers : [];
      const blockerText = blockers.length
        ? ` Next priority: ${(blockers[0].label || blockers[0].id || "RAG check")} -> ${blockers[0].recommendation || "review check details."}`
        : "";
      setStatus(
        cleanStatusEl,
        `Autopilot complete. Readiness: ${achieved ?? "n/a"}/100. ${statusText}${actionText}${blockerText}`
      );
      setWizardStep(4);

      downloadLink.href = apiUrl(`/datasets/${currentDataset.id}/download?kind=cleaned`);
      downloadLink.textContent = `Download cleaned ${currentDataset.output_format || "file"}`;
    } catch (error) {
      setStatus(cleanStatusEl, error.message || "Autopilot cleanup failed.");
      setCleanProgress(0, "Failed");
    } finally {
      runAutopilotBtn.disabled = false;
      runCleaningBtn.disabled = false;
      uploadButton.disabled = false;
    }
  });
}

runCleaningBtn.addEventListener("click", async () => {
  if (!currentDataset) return;
  if (!apiHealthy) {
    setStatus(
      cleanStatusEl,
      `API not ready (${apiService}). Open the app at ${DEFAULT_API_BASE} and refresh.`
    );
    return;
  }

  setStatus(cleanStatusEl, "Running cleanup... this may take a moment.");
  runCleaningBtn.disabled = true;
  if (runAutopilotBtn) runAutopilotBtn.disabled = true;
  uploadButton.disabled = true;

  const options = {
    remove_duplicates: document.getElementById("opt-dedup").checked,
    drop_empty_columns: document.getElementById("opt-drop-empty").checked,
    privacy_mode: optPrivacyMode.value,
    normalize_phone: optNormalizePhone.checked,
    normalize_zip: optNormalizeZip.checked,
    normalize_gender: optNormalizeGender.checked,
    text_case: optTextCase.value,
    output_format: optOutputFormat.value,
    coercion_mode: optCoercionMode.value,
    performance_mode: optPerformanceMode ? optPerformanceMode.value : "balanced",
  };

  try {
    setCleanProgress(0, "Starting");
    let result = null;
    try {
      const job = await startCleanJob(currentDataset.id, options);
      result = await pollCleanJob(job.id);
    } catch (error) {
      if (error.status === 404 || error.status === 405) {
        const sizeBytes = currentDataset.file_size_bytes || 0;
        if (sizeBytes > STREAMING_THRESHOLD_MB * 1024 * 1024) {
          throw new Error(
            `Cleaning job endpoint unavailable at ${API_BASE}. For large files, open the app at ${DEFAULT_API_BASE} and refresh.`
          );
        }
        result = await runSyncClean(currentDataset.id, options);
      } else {
        throw error;
      }
    }

    currentDataset = result.dataset;
    renderQc(result.qc);
    renderPreview(previewCleanTable, result.preview);
    await refreshRunHistory();
    setCleanProgress(100, "Completed");
    setStatus(cleanStatusEl, "Cleanup complete. Your dataset is ready to download.");
    setWizardStep(4);
    downloadLink.href = apiUrl(`/datasets/${currentDataset.id}/download?kind=cleaned`);
    downloadLink.textContent = `Download cleaned ${currentDataset.output_format || "file"}`;
    runCleaningBtn.disabled = false;
    if (runAutopilotBtn) runAutopilotBtn.disabled = false;
    uploadButton.disabled = false;
    return;
  } catch (error) {
    setStatus(cleanStatusEl, error.message || "Cleaning failed.");
    setCleanProgress(0, "Failed");
    runCleaningBtn.disabled = false;
    if (runAutopilotBtn) runAutopilotBtn.disabled = false;
    uploadButton.disabled = false;
    return;
  }
});

refreshProfileBtn.addEventListener("click", async () => {
  if (!currentDataset) return;
  setStatus(statusEl, "Refreshing view...");
  const response = await fetch(apiUrl(`/datasets/${currentDataset.id}`));
  if (response.ok) {
    const dataset = await response.json();
    currentDataset = dataset;
    renderProfile(dataset.profile);
    renderMetadata(dataset, dataset.profile);
    renderQc(dataset.qc);

    try {
      const rawPreview = await fetchPreview("raw");
      renderPreview(rawPreviewTable, rawPreview);
      renderPreview(previewRawTable, rawPreview);
    } catch (error) {
      // Keep current tables if preview refresh fails.
    }

    if (dataset.qc) {
      try {
        const cleanPreview = await fetchPreview("cleaned");
        renderPreview(previewCleanTable, cleanPreview);
      } catch (error) {
        renderPreview(previewCleanTable, null);
      }
    } else {
      renderPreview(previewCleanTable, null);
    }

    await refreshRunHistory();
    setStatus(statusEl, `View refreshed for ${dataset.name}.`);
    setWizardStep(dataset.qc ? 4 : 3);
  } else {
    setStatus(statusEl, "Failed to refresh view.");
  }
});

sourceOptions.forEach((option) => {
  option.addEventListener("click", () => {
    const source = option.dataset.source;
    if (!source) return;
    applySourceSelection(source);
  });
});

if (googleConnectBtn) {
  googleConnectBtn.addEventListener("click", async () => {
    applySourceSelection("google_drive");
    await connectGoogleDrive();
  });
}

if (googleDisconnectBtn) {
  googleDisconnectBtn.addEventListener("click", async () => {
    await disconnectGoogleDrive();
  });
}

if (googleRefreshFilesBtn) {
  googleRefreshFilesBtn.addEventListener("click", async () => {
    await loadGoogleFiles(googleSearchInput ? googleSearchInput.value.trim() : "");
  });
}

if (googleSearchInput) {
  googleSearchInput.addEventListener("input", () => {
    if (!googleDriveConnected) return;
    if (googleSearchDebounce) {
      clearTimeout(googleSearchDebounce);
    }
    googleSearchDebounce = setTimeout(() => {
      loadGoogleFiles(googleSearchInput.value.trim());
    }, 350);
  });
}

if (googleImportBtn) {
  googleImportBtn.addEventListener("click", async () => {
    await runGoogleImportFlow();
  });
}

usageIntentSelect.addEventListener("change", updateUsageIntentNote);
if (viewModeSelect) {
  viewModeSelect.addEventListener("change", () => {
    setViewMode(viewModeSelect.value);
  });
}
if (guidedModeToggle) {
  guidedModeToggle.addEventListener("change", () => {
    setWizardStep(wizardStep);
  });
}
updateUsageIntentNote();
renderWhatChanged(null);
renderRunHistory([]);
applySourceSelection("file");
setViewMode(viewModeSelect ? viewModeSelect.value : "simple");
setWizardStep(1);
const googleAuthResult = relayGoogleAuthResultToOpener();
if (googleAuthResult) {
  setGoogleStatus(googleAuthResult.message);
}
checkApiHealth();
fetchGoogleAuthStatus();
fetchFeatureCatalogStatus();
