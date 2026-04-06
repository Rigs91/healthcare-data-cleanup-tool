const DEFAULT_BASE =
  window.location.origin && window.location.origin.startsWith("http")
    ? window.location.origin
    : "http://127.0.0.1:8000";

function asStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => String(entry || "").trim())
    .filter((entry) => entry.length > 0);
}

function deriveLegacyStage(dataset) {
  const qc = dataset?.qc || null;
  if (dataset?.cleaned_path && qc) return "completed";
  if (dataset?.status && String(dataset.status).toLowerCase() === "running") return "running";
  if (dataset?.status && ["failed", "error"].includes(String(dataset.status).toLowerCase())) return "failed";
  if (dataset?.profile) return "prechecked";
  return "uploaded";
}

function nextActionsForStage(stage) {
  const mapping = {
    uploaded: ["review_precheck", "refresh_workflow"],
    prechecked: ["run_autopilot", "refresh_workflow", "view_history"],
    running: ["refresh_workflow"],
    completed: ["download_result", "start_new_workflow", "view_history"],
    failed: ["run_autopilot", "refresh_workflow", "view_history"],
  };
  return mapping[stage] || ["refresh_workflow"];
}

function executionFromDataset(dataset) {
  const latestRun = dataset?.latest_run && typeof dataset.latest_run === "object"
    ? dataset.latest_run
    : {};
  const datasetPlan = dataset?.llm_plan && typeof dataset.llm_plan === "object"
    ? dataset.llm_plan
    : {};
  const latestPlan = latestRun?.llm_plan && typeof latestRun.llm_plan === "object"
    ? latestRun.llm_plan
    : {};
  const activePlan = Object.keys(latestPlan).length ? latestPlan : datasetPlan;

  return {
    cleanup_mode: latestRun.cleanup_mode || dataset?.cleanup_mode || "deterministic",
    llm_provider: latestRun.llm_provider || dataset?.llm_provider || null,
    llm_model: latestRun.llm_model || dataset?.llm_model || null,
    llm_plan_status:
      activePlan.status || activePlan.validation_status || activePlan.plan_status || null,
    llm_acceptance_status: activePlan.acceptance_status || null,
    llm_summary:
      activePlan.summary
      || dataset?.profile?.llm_assist?.summary
      || null,
    llm_validation_notes: asStringArray(activePlan.validation_notes),
  };
}

function precheckFromProfile(profile) {
  if (!profile || typeof profile !== "object") return null;
  const decision = profile.preclean_decision && typeof profile.preclean_decision === "object"
    ? profile.preclean_decision
    : {};
  const rag = profile.rag_readiness && typeof profile.rag_readiness === "object"
    ? profile.rag_readiness
    : {};
  const llmAssist = profile.llm_assist && typeof profile.llm_assist === "object"
    ? profile.llm_assist
    : {};

  let blockers = asStringArray(decision.reasons);
  if (blockers.length === 0 && Array.isArray(rag.checks)) {
    blockers = rag.checks
      .filter((check) => check && typeof check === "object")
      .filter((check) => {
        const status = String(check.status || "").toLowerCase();
        return ["partial", "not_ready", "blocked", "needs_attention"].includes(status);
      })
      .map((check) => String(check.summary || check.name || "Needs attention"));
  }

  return {
    decision_status: decision.status || "needs_review",
    decision_reasons: blockers.slice(0, 5),
    recommended_actions: asStringArray(decision.actions).slice(0, 5),
    readiness_score: rag.score,
    readiness_band: rag.band || rag.label,
    primary_domain: profile.primary_domain || null,
    is_sampled: Boolean(profile.sampled),
    llm_summary: llmAssist.summary || null,
    llm_plan_status: llmAssist.status || null,
    llm_acceptance_status: llmAssist.acceptance_status || null,
    llm_validation_notes: asStringArray(llmAssist.validation_notes).slice(0, 5),
    llm_top_blockers: asStringArray(llmAssist.top_blockers).slice(0, 5),
  };
}

function resultFromQc(workflowId, qc) {
  if (!qc || typeof qc !== "object") return null;
  const decision = qc.postclean_decision && typeof qc.postclean_decision === "object"
    ? qc.postclean_decision
    : {};
  const rag = qc.rag_readiness && typeof qc.rag_readiness === "object"
    ? qc.rag_readiness
    : {};
  const comparison = qc.rag_readiness_comparison && typeof qc.rag_readiness_comparison === "object"
    ? qc.rag_readiness_comparison
    : {};
  const gate = qc.quality_gate && typeof qc.quality_gate === "object"
    ? qc.quality_gate
    : {};

  return {
    decision_status: decision.status || "warn",
    release_recommendation: decision.release_recommendation || "Review diagnostics before sharing.",
    quality_gate_status: gate.status || "warn",
    rag_score: rag.score,
    rag_score_delta: comparison.score_delta,
    warnings: asStringArray(qc.warnings),
    blockers: asStringArray(decision.blockers),
    actions: asStringArray(decision.actions),
    download_url: `/api/datasets/${workflowId}/download?kind=cleaned`,
  };
}

function normalizeLegacyWorkflow(dataset) {
  const workflowId = dataset.id;
  const stage = deriveLegacyStage(dataset);
  return {
    workflow_id: workflowId,
    dataset_id: workflowId,
    stage,
    next_actions: nextActionsForStage(stage),
    dataset,
    precheck_summary: precheckFromProfile(dataset.profile || {}),
    result_summary: resultFromQc(workflowId, dataset.qc || {}),
    execution: executionFromDataset(dataset),
  };
}

function toErrorMessage(path, status, payload) {
  const detail = payload && typeof payload === "object" ? payload.detail : null;
  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }
  if (detail && typeof detail === "object") {
    if (typeof detail.message === "string" && detail.message.trim()) {
      return detail.message;
    }
    try {
      return JSON.stringify(detail);
    } catch (_error) {
      // ignore serialization issue
    }
  }
  return `Request failed (${status}) for ${path}`;
}

export class ApiClient {
  constructor(base = DEFAULT_BASE) {
    this.base = String(base || DEFAULT_BASE).replace(/\/+$/, "");
    this.workflowMode = "v3_guided";
  }

  setWorkflowMode(mode) {
    this.workflowMode = mode === "v3_guided" ? "v3_guided" : "v2_legacy";
  }

  absoluteUrl(path) {
    if (!path) return "";
    if (/^https?:\/\//i.test(path)) return path;
    const suffix = path.startsWith("/") ? path : `/${path}`;
    return `${this.base}${suffix}`;
  }

  async _request(path, options = {}) {
    const response = await fetch(this.absoluteUrl(path), options);
    const contentType = String(response.headers.get("content-type") || "").toLowerCase();
    let payload = null;
    if (contentType.includes("application/json")) {
      try {
        payload = await response.json();
      } catch (_error) {
        payload = null;
      }
    }

    if (!response.ok) {
      const error = new Error(toErrorMessage(path, response.status, payload));
      error.status = response.status;
      error.payload = payload;
      throw error;
    }

    return payload;
  }

  async health() {
    return this._request("/api/health");
  }

  _buildUploadFormData({ file, name, usageIntent, cleanupMode, llmModel }) {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("name", name || file.name.replace(/\.[^.]+$/, ""));
    formData.append("usage_intent", usageIntent || "training");
    formData.append("cleanup_mode", cleanupMode || "deterministic");
    if ((cleanupMode || "deterministic") === "ollama_assisted" && llmModel) {
      formData.append("llm_model", llmModel);
    }
    return formData;
  }

  async uploadWorkflow(payload) {
    const workflowForm = this._buildUploadFormData(payload);
    if (this.workflowMode === "v3_guided") {
      try {
        return await this._request("/api/v2/workflows/upload", {
          method: "POST",
          body: workflowForm,
        });
      } catch (error) {
        if (![404, 405].includes(Number(error.status || 0))) {
          throw error;
        }
      }
    }

    const dataset = await this._request("/api/datasets", {
      method: "POST",
      body: this._buildUploadFormData(payload),
    });
    return normalizeLegacyWorkflow(dataset);
  }

  async getWorkflow(workflowId) {
    if (this.workflowMode === "v3_guided") {
      try {
        return await this._request(`/api/v2/workflows/${workflowId}`);
      } catch (error) {
        if (![404, 405].includes(Number(error.status || 0))) {
          throw error;
        }
      }
    }

    const dataset = await this._request(`/api/datasets/${workflowId}`);
    return normalizeLegacyWorkflow(dataset);
  }

  async runAutopilot(workflowId, payload) {
    const body = JSON.stringify(payload || {});
    if (this.workflowMode === "v3_guided") {
      try {
        return await this._request(`/api/v2/workflows/${workflowId}/autopilot-run`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
        });
      } catch (error) {
        if (![404, 405].includes(Number(error.status || 0))) {
          throw error;
        }
      }
    }

    const legacyResult = await this._request(`/api/datasets/${workflowId}/cleanup/autopilot`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });

    let dataset = legacyResult?.dataset;
    if (!dataset || typeof dataset !== "object") {
      dataset = await this._request(`/api/datasets/${workflowId}`);
    }
    if (legacyResult?.run && typeof legacyResult.run === "object") {
      dataset.latest_run = legacyResult.run;
    }

    const workflow = normalizeLegacyWorkflow(dataset);
    if (legacyResult?.qc && typeof legacyResult.qc === "object") {
      workflow.dataset.qc = legacyResult.qc;
      workflow.result_summary = resultFromQc(workflowId, legacyResult.qc);
      workflow.execution = executionFromDataset(workflow.dataset);
      workflow.stage = "completed";
      workflow.next_actions = nextActionsForStage("completed");
    }
    return workflow;
  }

  async getWorkflowResult(workflowId) {
    if (this.workflowMode === "v3_guided") {
      try {
        return await this._request(`/api/v2/workflows/${workflowId}/result`);
      } catch (error) {
        if (![404, 405].includes(Number(error.status || 0))) {
          throw error;
        }
      }
    }

    const workflow = await this.getWorkflow(workflowId);
    if (workflow.stage !== "completed") {
      const error = new Error("Workflow has not completed cleaning yet.");
      error.status = 409;
      throw error;
    }
    return {
      workflow_id: workflowId,
      stage: workflow.stage,
      result_summary: workflow.result_summary,
      qc: workflow.dataset?.qc || null,
      latest_run: workflow.dataset?.latest_run || null,
    };
  }

  getWorkflowExportUrl(workflowId) {
    if (this.workflowMode === "v3_guided") {
      return this.absoluteUrl(`/api/v2/workflows/${workflowId}/export`);
    }
    return this.absoluteUrl(`/api/datasets/${workflowId}/download?kind=cleaned`);
  }

  async listOllamaModels(requestedModel) {
    const query = requestedModel
      ? `?requested_model=${encodeURIComponent(requestedModel)}`
      : "";

    try {
      return await this._request(`/api/providers/ollama/models${query}`);
    } catch (error) {
      if (![404, 405].includes(Number(error.status || 0))) {
        throw error;
      }
    }

    const health = await this.health();
    return health?.providers?.ollama || {
      provider: "ollama",
      enabled: false,
      reachable: false,
      models: [],
      installed_models: [],
      filtered_models: [],
      selected_model: requestedModel || "",
    };
  }

  async listDatasets() {
    const payload = await this._request("/api/datasets");
    return Array.isArray(payload?.items) ? payload.items : [];
  }

  async listRuns(datasetId) {
    const payload = await this._request(`/api/datasets/${datasetId}/runs`);
    return Array.isArray(payload?.items) ? payload.items : [];
  }

  async getRun(runId) {
    return this._request(`/api/runs/${runId}`);
  }
}
