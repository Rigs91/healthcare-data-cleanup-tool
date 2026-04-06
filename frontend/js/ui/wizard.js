export const STEP_META = {
  1: {
    title: "Upload dataset",
    hint: "Start with one file. The app profiles schema and quality automatically.",
  },
  2: {
    title: "Review pre-check",
    hint: "Review schema signals, blockers, and recommended actions before running cleanup.",
  },
  3: {
    title: "Run guided cleanup",
    hint: "Execute the cleanup plan. Use customization only if needed.",
  },
  4: {
    title: "Export cleaned data",
    hint: "Confirm the quality decision, then download the cleaned output.",
  },
};

export function normalizeStep(step) {
  const numeric = Number(step) || 1;
  return Math.max(1, Math.min(4, numeric));
}

export function stageToStep(stage) {
  const value = String(stage || "").toLowerCase();
  if (value === "completed") return 4;
  if (value === "running") return 3;
  if (value === "prechecked") return 2;
  if (value === "failed") return 3;
  return 1;
}

export function stepMeta(step) {
  const normalized = normalizeStep(step);
  return STEP_META[normalized] || STEP_META[1];
}
