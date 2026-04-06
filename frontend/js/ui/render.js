import { normalizeStep, stepMeta } from "./wizard.js";

function setText(node, text) {
  if (!node) return;
  node.textContent = text || "";
}

function setHtml(node, html) {
  if (!node) return;
  node.innerHTML = html;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function listMarkup(items, emptyLabel) {
  if (!Array.isArray(items) || items.length === 0) {
    return `<li class="empty">${escapeHtml(emptyLabel)}</li>`;
  }
  return items.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
}

function formatInteger(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "n/a";
  return numeric.toLocaleString();
}

function formatPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "n/a";
  return `${numeric.toFixed(numeric >= 10 ? 0 : 1)}%`;
}

function humanizeMode(mode) {
  return mode === "ollama_assisted" ? "Ollama assisted" : "Deterministic";
}

function humanizePlanStatus(status) {
  if (!status) return "not available";
  return String(status).replaceAll("_", " ");
}

function pillClass(kind) {
  const normalized = String(kind || "").toLowerCase();
  if (["validated", "accepted", "ready", "connected", "reachable", "fully_accepted"].includes(normalized)) {
    return "ok";
  }
  if (["warn", "warning", "partial", "unavailable", "disabled", "partially_accepted"].includes(normalized)) {
    return "warn";
  }
  return "info";
}

function summarizeFilteredModels(provider) {
  const filtered = Array.isArray(provider?.filteredModels)
    ? provider.filteredModels
    : [];
  if (filtered.length === 0) {
    return "";
  }

  const preview = filtered
    .slice(0, 3)
    .map((item) => `${item.name}${item.reason ? ` (${item.reason})` : ""}`)
    .join(", ");
  const suffix = filtered.length > 3 ? `, and ${filtered.length - 3} more` : "";
  return `Hidden from the planner-safe picker: ${preview}${suffix}.`;
}

function summarizeValidationNotes(notes, limit = 2) {
  const items = Array.isArray(notes)
    ? notes.map((item) => String(item || "").trim()).filter((item) => item.length > 0)
    : [];
  if (items.length === 0) {
    return {
      items: [],
      remaining: 0,
    };
  }

  return {
    items: items.slice(0, limit),
    remaining: Math.max(0, items.length - limit),
  };
}

function providerStatusDetail(provider, models, installedModels, filteredModels) {
  if (!provider.enabled) {
    return "Backend support for Ollama is disabled. Deterministic cleanup is still available.";
  }

  if (!provider.reachable) {
    return provider.error || "Ollama was not reachable. Start the local daemon or switch to deterministic cleanup.";
  }

  const hiddenCount = filteredModels.length || Math.max(0, installedModels.length - models.length);

  if (!models.length) {
    const base = `Local provider reachable at ${provider.baseUrl || "configured endpoint"}, but no planner-safe local models are available.`;
    const hiddenText = hiddenCount > 0
      ? ` ${hiddenCount} installed local model${hiddenCount === 1 ? "" : "s"} were hidden from the picker.`
      : "";
    return `${base}${hiddenText}${provider.error ? ` ${provider.error}` : ""}`;
  }

  const requestedIssue = provider.error
    ? ` ${provider.error}`
    : "";
  const hiddenText = hiddenCount > 0
    ? ` ${hiddenCount} installed local model${hiddenCount === 1 ? "" : "s"} are hidden from this picker.`
    : "";
  return `Local provider reachable at ${provider.baseUrl || "configured endpoint"}. ${models.length} planner-safe local model${models.length === 1 ? "" : "s"} available.${hiddenText}${requestedIssue}`;
}

function workflowExecution(workflow) {
  const execution = workflow?.execution && typeof workflow.execution === "object"
    ? workflow.execution
    : {};
  const dataset = workflow?.dataset && typeof workflow.dataset === "object"
    ? workflow.dataset
    : {};
  const latestRun = dataset?.latest_run && typeof dataset.latest_run === "object"
    ? dataset.latest_run
    : {};
  const datasetPlan = dataset?.llm_plan && typeof dataset.llm_plan === "object"
    ? dataset.llm_plan
    : {};
  const latestPlan = latestRun?.llm_plan && typeof latestRun.llm_plan === "object"
    ? latestRun.llm_plan
    : {};
  const profileAssist = dataset?.profile?.llm_assist && typeof dataset.profile.llm_assist === "object"
    ? dataset.profile.llm_assist
    : {};
  const activePlan = Object.keys(latestPlan).length ? latestPlan : datasetPlan;

  return {
    cleanupMode:
      execution.cleanup_mode
      || latestRun.cleanup_mode
      || dataset.cleanup_mode
      || "deterministic",
    llmProvider:
      execution.llm_provider
      || latestRun.llm_provider
      || dataset.llm_provider
      || null,
    llmModel:
      execution.llm_model
      || latestRun.llm_model
      || dataset.llm_model
      || null,
    llmPlanStatus:
      execution.llm_plan_status
      || activePlan.status
      || activePlan.validation_status
      || profileAssist.status
      || null,
    llmAcceptanceStatus:
      execution.llm_acceptance_status
      || activePlan.acceptance_status
      || profileAssist.acceptance_status
      || null,
    llmSummary:
      execution.llm_summary
      || activePlan.summary
      || profileAssist.summary
      || null,
    llmValidationNotes:
      execution.llm_validation_notes
      || activePlan.validation_notes
      || profileAssist.validation_notes
      || [],
  };
}

function executionMarkup(execution) {
  if (!execution || typeof execution !== "object") {
    return "";
  }

  const pills = [
    `<span class="pill info">Mode: ${escapeHtml(humanizeMode(execution.cleanupMode))}</span>`,
  ];

  if (execution.llmProvider || execution.llmModel) {
    pills.push(
      `<span class="pill ${pillClass(execution.llmPlanStatus)}">LLM: ${escapeHtml(execution.llmModel || execution.llmProvider || "configured")}</span>`
    );
  }

  if (execution.llmPlanStatus) {
    pills.push(
      `<span class="pill ${pillClass(execution.llmPlanStatus)}">Plan: ${escapeHtml(humanizePlanStatus(execution.llmPlanStatus))}</span>`
    );
  }

  if (execution.llmAcceptanceStatus) {
    pills.push(
      `<span class="pill ${pillClass(execution.llmAcceptanceStatus)}">Acceptance: ${escapeHtml(humanizePlanStatus(execution.llmAcceptanceStatus))}</span>`
    );
  }

  const summary = execution.llmSummary
    ? `<div><strong>Planner:</strong> ${escapeHtml(execution.llmSummary)}</div>`
    : "";
  const validationSummary = summarizeValidationNotes(execution.llmValidationNotes, 2);
  const notes = validationSummary.items.length
    ? `<div><strong>Validation notes:</strong> ${validationSummary.items.map((note) => escapeHtml(note)).join(" | ")}${validationSummary.remaining > 0 ? ` | +${validationSummary.remaining} more in diagnostics` : ""}</div>`
    : "";

  return `
    <div class="summary-pills">${pills.join("")}</div>
    ${summary}
    ${notes}
  `;
}

function profileSummaryMarkup(profile) {
  if (!profile || typeof profile !== "object") {
    return "Upload a dataset to inspect schema and quality signals.";
  }

  const summary = profile.summary && typeof profile.summary === "object"
    ? profile.summary
    : {};
  const domains = Array.isArray(profile.detected_domains)
    ? profile.detected_domains
    : [];
  const strongestDomain = domains[0] || null;
  const notes = [];
  if (profile.sampled) {
    notes.push(`Profile built from ${formatInteger(profile.sampled_rows)} sampled rows out of roughly ${formatInteger(profile.row_count)}.`);
  }
  if (strongestDomain?.domain) {
    notes.push(`Strongest domain signal: ${strongestDomain.domain} (${formatPercent(Number(strongestDomain.confidence || 0) * 100)}).`);
  }

  return `
    <div class="profile-stats">
      <div class="profile-stat">
        <span class="profile-stat-label">Rows</span>
        <span class="profile-stat-value">${escapeHtml(formatInteger(profile.row_count))}</span>
      </div>
      <div class="profile-stat">
        <span class="profile-stat-label">Columns</span>
        <span class="profile-stat-value">${escapeHtml(formatInteger(profile.column_count))}</span>
      </div>
      <div class="profile-stat">
        <span class="profile-stat-label">PII Signals</span>
        <span class="profile-stat-value">${escapeHtml(formatInteger((summary.columns_with_pii || []).length))}</span>
      </div>
      <div class="profile-stat">
        <span class="profile-stat-label">High Missing</span>
        <span class="profile-stat-value">${escapeHtml(formatInteger((summary.columns_high_missing || []).length))}</span>
      </div>
    </div>
    <div class="profile-notes">${escapeHtml(notes.join(" ")) || "No schema notes available yet."}</div>
  `;
}

function profileColumnsMarkup(profile) {
  const columns = Array.isArray(profile?.columns)
    ? profile.columns.slice(0, 6)
    : [];
  if (columns.length === 0) {
    return '<div class="schema-empty">Upload a dataset to inspect key columns, inferred types, and missingness.</div>';
  }

  const rows = columns.map((column) => {
    const semantic = column.semantic_hint || "unclassified";
    const examples = Array.isArray(column.example_values) && column.example_values.length
      ? column.example_values.slice(0, 2).join(" | ")
      : "No sample values";
    return `
      <tr>
        <td>
          <strong>${escapeHtml(column.clean_name || column.original_name || "Column")}</strong>
          <small>${escapeHtml(column.original_name || "")}</small>
        </td>
        <td>${escapeHtml(column.primitive_type || "unknown")}</td>
        <td>${escapeHtml(semantic)}</td>
        <td>${escapeHtml(formatPercent(column.missing_pct))}</td>
        <td>${escapeHtml(examples)}</td>
      </tr>
    `;
  }).join("");

  return `
    <table class="schema-table">
      <thead>
        <tr>
          <th>Column</th>
          <th>Type</th>
          <th>Hint</th>
          <th>Missing</th>
          <th>Examples</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderPrecheck(elements, workflow) {
  const summary = workflow?.precheck_summary || null;
  const profile = workflow?.dataset?.profile || null;
  if (!summary) {
    setHtml(elements.precheckSummary, "Upload a dataset to generate pre-check results.");
    setHtml(elements.profileSummary, "Upload a dataset to inspect schema and quality signals.");
    setHtml(elements.profileColumns, '<div class="schema-empty">Upload a dataset to inspect key columns, inferred types, and missingness.</div>');
    setHtml(elements.precheckBlockers, listMarkup([], "No blockers to show yet."));
    setHtml(elements.precheckActions, listMarkup([], "No recommendations yet."));
    return;
  }

  const execution = workflowExecution(workflow);
  const decision = String(summary.decision_status || "needs_review").replaceAll("_", " ");
  const score = summary.readiness_score ?? "n/a";
  const band = summary.readiness_band || "unknown";
  const domain = summary.primary_domain || "unknown";
  const planner = summary.llm_summary
    ? `<div><strong>Planner summary:</strong> ${escapeHtml(summary.llm_summary)}</div>`
    : "";
  const planStatus = summary.llm_plan_status
    ? `<div><strong>LLM plan:</strong> ${escapeHtml(humanizePlanStatus(summary.llm_plan_status))}</div>`
    : "";
  const acceptance = summary.llm_acceptance_status
    ? `<div><strong>Plan acceptance:</strong> ${escapeHtml(humanizePlanStatus(summary.llm_acceptance_status))}</div>`
    : "";

  setHtml(
    elements.precheckSummary,
    `
      <div class="summary-grid">
        <div><strong>Decision:</strong> ${escapeHtml(decision)}</div>
        <div><strong>Readiness:</strong> ${escapeHtml(score)} (${escapeHtml(band)})</div>
        <div><strong>Primary domain:</strong> ${escapeHtml(domain)}</div>
        ${planStatus}
        ${acceptance}
        ${planner}
      </div>
      ${executionMarkup(execution)}
    `
  );
  setHtml(elements.profileSummary, profileSummaryMarkup(profile));
  setHtml(elements.profileColumns, profileColumnsMarkup(profile));

  setHtml(
    elements.precheckBlockers,
    listMarkup((summary.decision_reasons || []).concat(summary.llm_top_blockers || []), "No critical blockers were identified.")
  );
  setHtml(
    elements.precheckActions,
    listMarkup((summary.recommended_actions || []).concat(summary.llm_validation_notes || []), "No specific action required before cleanup.")
  );
}

function renderRunSummary(elements, workflow) {
  if (!workflow) {
    setHtml(elements.runSummary, "Upload and review a dataset before running cleanup.");
    return;
  }

  const summary = workflow.precheck_summary || {};
  const score = summary.readiness_score ?? "n/a";
  const blockers = Array.isArray(summary.decision_reasons) ? summary.decision_reasons.length : 0;
  const status = summary.decision_status || "needs_review";
  const execution = workflowExecution(workflow);

  setHtml(
    elements.runSummary,
    `
      <div class="summary-grid">
        <div><strong>Current readiness:</strong> ${escapeHtml(score)}</div>
        <div><strong>Pre-check status:</strong> ${escapeHtml(status)}</div>
        <div><strong>Blockers:</strong> ${escapeHtml(blockers)}</div>
      </div>
      ${executionMarkup(execution)}
    `
  );
}

function renderResult(elements, workflow) {
  if (!workflow || !workflow.result_summary) {
    setHtml(elements.resultSummary, "Run cleanup to produce a cleaned dataset and quality summary.");
    setHtml(elements.resultWarnings, "");
    elements.downloadResult.classList.add("hidden");
    elements.downloadResult.setAttribute("href", "#");
    elements.copyDownloadUrl.disabled = true;
    return;
  }

  const summary = workflow.result_summary;
  const ragScore = summary.rag_score ?? "n/a";
  const ragDelta = summary.rag_score_delta ?? "n/a";
  const gate = summary.quality_gate_status || "warn";
  const execution = workflowExecution(workflow);

  setHtml(
    elements.resultSummary,
    `
      <div class="summary-grid">
        <div><strong>Decision:</strong> ${escapeHtml(summary.decision_status || "warn")}</div>
        <div><strong>Quality gate:</strong> ${escapeHtml(gate)}</div>
        <div><strong>RAG score:</strong> ${escapeHtml(ragScore)} (delta ${escapeHtml(ragDelta)})</div>
        <div><strong>Recommendation:</strong> ${escapeHtml(summary.release_recommendation || "Review diagnostics.")}</div>
      </div>
      ${executionMarkup(execution)}
    `
  );

  const warningItems = Array.isArray(summary.warnings) ? summary.warnings : [];
  const validationSummary = summarizeValidationNotes(execution.llmValidationNotes, 3);
  const warnings = warningItems.concat(validationSummary.items);
  if (validationSummary.remaining > 0) {
    warnings.push(`${validationSummary.remaining} additional validation note${validationSummary.remaining === 1 ? "" : "s"} are available in diagnostics.`);
  }
  if (warnings.length > 0) {
    setHtml(
      elements.resultWarnings,
      warnings.map((warning) => `<div class="note">${escapeHtml(warning)}</div>`).join("")
    );
  } else {
    setHtml(elements.resultWarnings, "");
  }

  const downloadUrl = summary.download_url || "";
  elements.downloadResult.classList.toggle("hidden", !downloadUrl);
  elements.downloadResult.setAttribute("href", downloadUrl || "#");
  elements.copyDownloadUrl.disabled = !downloadUrl;
}

function renderHistoryDatasets(elements, history) {
  if (!elements.historyDatasets) return;
  const items = history.datasets || [];
  if (!Array.isArray(items) || items.length === 0) {
    setHtml(elements.historyDatasets, '<li class="empty">No datasets yet.</li>');
    return;
  }

  const selectedId = history.selectedDatasetId || "";
  const rows = items
    .map((item) => {
      const isActive = item.id === selectedId;
      const status = item.status || "unknown";
      const name = item.name || item.original_filename || item.id;
      const created = item.created_at || "";
      const mode = humanizeMode(item.cleanup_mode);
      const model = item.llm_model || item.llm_provider || "n/a";
      return `
        <li>
          <button class="select-item ${isActive ? "active" : ""}" type="button" data-dataset-id="${escapeHtml(item.id)}">
            <strong>${escapeHtml(name)}</strong>
            <small>${escapeHtml(status)} | ${escapeHtml(created)}</small>
            <div class="select-item-meta">Engine ${escapeHtml(mode)} | Model ${escapeHtml(model)}</div>
          </button>
        </li>
      `;
    })
    .join("");
  setHtml(elements.historyDatasets, rows);
}

function renderHistoryRuns(elements, history) {
  if (!elements.historyRuns) return;
  const items = history.runs || [];
  if (!Array.isArray(items) || items.length === 0) {
    setHtml(elements.historyRuns, '<li class="empty">Select a dataset to load runs.</li>');
    return;
  }

  const selectedId = history.selectedRunId || "";
  const rows = items
    .map((item) => {
      const isActive = item.id === selectedId;
      const status = item.status || "unknown";
      const score = item.rag_readiness?.score ?? "n/a";
      const created = item.created_at || "";
      const mode = humanizeMode(item.cleanup_mode);
      const model = item.llm_model || item.llm_provider || "n/a";
      const plan = item.llm_plan?.status || "n/a";
      return `
        <li>
          <button class="select-item ${isActive ? "active" : ""}" type="button" data-run-id="${escapeHtml(item.id)}">
            <strong>${escapeHtml(item.id)}</strong>
            <small>${escapeHtml(status)} | RAG ${escapeHtml(score)} | ${escapeHtml(created)}</small>
            <div class="select-item-meta">Engine ${escapeHtml(mode)} | Model ${escapeHtml(model)} | Plan ${escapeHtml(humanizePlanStatus(plan))}</div>
          </button>
        </li>
      `;
    })
    .join("");
  setHtml(elements.historyRuns, rows);
}

function renderHistoryRunMeta(elements, runMeta) {
  if (!elements.historyRunMeta) return;
  if (!runMeta) {
    setHtml(elements.historyRunMeta, "Select a run to inspect execution mode, model, and plan status.");
    return;
  }

  setHtml(
    elements.historyRunMeta,
    `
      <div class="summary-grid">
        <div><strong>Cleanup mode:</strong> ${escapeHtml(humanizeMode(runMeta.cleanupMode))}</div>
        <div><strong>Provider:</strong> ${escapeHtml(runMeta.llmProvider || "deterministic only")}</div>
        <div><strong>Model:</strong> ${escapeHtml(runMeta.llmModel || "n/a")}</div>
        <div><strong>Plan status:</strong> ${escapeHtml(humanizePlanStatus(runMeta.llmPlanStatus || "not available"))}</div>
        ${runMeta.llmSummary ? `<div><strong>Planner summary:</strong> ${escapeHtml(runMeta.llmSummary)}</div>` : ""}
      </div>
    `
  );
}

function renderProviderControls(elements, state) {
  const engine = state.engine || {};
  const provider = engine.provider || {};
  const useOllama = engine.cleanupMode === "ollama_assisted";
  const modelSelect = elements.ollamaModelSelect;

  if (elements.cleanupModeDeterministic) {
    elements.cleanupModeDeterministic.checked = !useOllama;
  }
  if (elements.cleanupModeOllama) {
    elements.cleanupModeOllama.checked = useOllama;
  }
  if (elements.modeCardDeterministic) {
    elements.modeCardDeterministic.classList.toggle("active", !useOllama);
  }
  if (elements.modeCardOllama) {
    elements.modeCardOllama.classList.toggle("active", useOllama);
  }
  if (elements.ollamaPanel) {
    elements.ollamaPanel.classList.toggle("hidden", !useOllama);
  }

  const models = Array.isArray(provider.models) ? provider.models : [];
  const installedModels = Array.isArray(provider.installedModels) ? provider.installedModels : [];
  const filteredModels = Array.isArray(provider.filteredModels) ? provider.filteredModels : [];
  if (modelSelect) {
    const targetValue = engine.llmModel || provider.selectedModel || "";
    const options = [];

    if (provider.loading) {
      options.push('<option value="">Refreshing local models...</option>');
    } else if (!provider.enabled) {
      options.push('<option value="">Ollama support disabled in backend config</option>');
    } else if (!provider.reachable) {
      options.push('<option value="">Ollama unavailable - start local service</option>');
    } else if (!models.length) {
      options.push('<option value="">No planner-safe local models found</option>');
    } else {
      for (const model of models) {
        const safeModel = escapeHtml(model);
        const selected = model === targetValue ? ' selected="selected"' : "";
        options.push(`<option value="${safeModel}"${selected}>${safeModel}</option>`);
      }
    }

    const markup = options.join("");
    if (modelSelect.innerHTML !== markup) {
      modelSelect.innerHTML = markup;
    }
    modelSelect.value = models.includes(targetValue) ? targetValue : (models[0] || "");
    modelSelect.disabled = !useOllama || provider.loading || !provider.enabled || !provider.reachable || models.length === 0;
  }

  if (elements.ollamaRefresh) {
    elements.ollamaRefresh.disabled = Boolean(provider.loading);
  }

  if (elements.ollamaStatusBadge) {
    elements.ollamaStatusBadge.classList.remove("ok", "warn", "info");
    if (provider.loading) {
      elements.ollamaStatusBadge.classList.add("info");
      setText(elements.ollamaStatusBadge, "Ollama: checking");
    } else if (provider.enabled && provider.reachable && models.length > 0) {
      elements.ollamaStatusBadge.classList.add("ok");
      setText(
        elements.ollamaStatusBadge,
        `Ollama: ready (${models.length} selectable${models.length === 1 ? "" : "s"})`
      );
    } else if (provider.enabled && provider.reachable) {
      elements.ollamaStatusBadge.classList.add("warn");
      setText(elements.ollamaStatusBadge, "Ollama: no safe models");
    } else {
      elements.ollamaStatusBadge.classList.add("warn");
      setText(elements.ollamaStatusBadge, "Ollama: unavailable");
    }
  }

  if (elements.ollamaStatusText) {
    setText(elements.ollamaStatusText, providerStatusDetail(provider, models, installedModels, filteredModels));
  }

  if (elements.ollamaFilteredNote) {
    const hiddenSummary = summarizeFilteredModels(provider);
    elements.ollamaFilteredNote.classList.toggle("hidden", !hiddenSummary);
    setText(elements.ollamaFilteredNote, hiddenSummary);
  }
}

export function renderApp(elements, state) {
  const meta = stepMeta(state.step);
  const step = normalizeStep(state.step);

  const showWizard = state.view === "wizard";
  elements.wizardView.classList.toggle("hidden", !showWizard);
  elements.historyView.classList.toggle("hidden", showWizard);

  elements.navWizard.classList.toggle("active", showWizard);
  elements.navHistory.classList.toggle("active", !showWizard);

  setText(elements.stepIndicator, `Step ${step} of 4`);
  setText(elements.stepTitle, meta.title);
  setText(elements.stepHint, meta.hint);

  [elements.stepUpload, elements.stepPrecheck, elements.stepRun, elements.stepResult].forEach((node, index) => {
    const shouldShow = step === index + 1;
    node.classList.toggle("hidden", !shouldShow);
  });

  setText(elements.wizardStatus, state.info || "");
  const hasError = Boolean(state.error);
  elements.wizardError.classList.toggle("hidden", !hasError);
  setText(elements.wizardError, hasError ? state.error : "");

  const health = state.health || {};
  const isHealthy = String(health.status || "").toLowerCase() === "ok";
  const workflowVersion = health.ui_workflow_version || "unknown";
  const ollama = health.providers?.ollama || {};

  elements.apiHealthBadge.classList.remove("ok", "warn");
  elements.apiHealthBadge.classList.add(isHealthy ? "ok" : "warn");
  setText(elements.apiHealthBadge, isHealthy ? "API: connected" : "API: unavailable");
  setText(elements.workflowVersionChip, `Workflow: ${workflowVersion}`);

  const sourceSummary = Array.isArray(health.capabilities?.supported_sources)
    ? health.capabilities.supported_sources.join(", ")
    : "unknown";
  setText(
    elements.apiHealthDetail,
    `Service ${health.service || "hc-data-cleanup-ai"} v${health.version || "unknown"} | Supported sources: ${sourceSummary} | Ollama ${ollama.reachable ? "ready" : "not ready"}`
  );

  const workflow = state.workflow;
  renderPrecheck(elements, workflow || null);
  renderRunSummary(elements, workflow || null);
  renderResult(elements, workflow || null);
  renderProviderControls(elements, state);

  const locked = Boolean(state.busy);
  elements.uploadSubmit.disabled = locked;
  elements.goToRun.disabled = locked || !workflow;
  elements.runAutopilot.disabled = locked || !workflow;
  elements.startOver.disabled = locked;
  elements.historyRefresh.disabled = locked || state.history.loading;

  renderHistoryDatasets(elements, state.history);
  renderHistoryRuns(elements, state.history);
  renderHistoryRunMeta(elements, state.history.runMeta);
  setText(elements.historyStatus, state.history.loading ? "Refreshing history..." : "");
  setText(elements.historyDiagnostics, state.history.diagnostics || "Select a run to inspect diagnostics.");
}
