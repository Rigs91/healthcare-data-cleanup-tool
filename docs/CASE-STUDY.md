# Case Study

## Problem

Healthcare teams often work from exports that are messy before they reach analytics or AI workflows. Dates drift across formats, contact fields are inconsistent, duplicate rows appear, and it is hard to know whether a dataset is safe to hand off without a slow manual review.

## Solution

This project turns that cleanup step into a guided product flow. A user uploads a healthcare-style dataset, reviews schema and quality signals, runs cleanup, and exports a cleaner output with a visible quality summary and run traceability.

## Product Decisions

- **Guided workflow over dashboard sprawl**: the product is optimized for one clear operator path, not endless exploration.
- **Trust before automation**: blockers, quality gates, and diagnostics are part of the main story instead of hidden behind admin tooling.
- **Local-first packaging**: the repo can run as a believable product demo without cloud setup or sensitive data.
- **Scoped AI usage**: Ollama helps interpret the dataset and suggest a cleanup strategy, but deterministic rules still execute the actual transforms.

## Trust Model

The product is designed to make cleanup decisions understandable. The user sees schema signals, readiness blockers, recommended actions, result summaries, and run history. That keeps the app credible in a healthcare data context where silent automation would be a weak product choice.

## Tradeoffs

- The app focuses on one-file-at-a-time guided cleanup rather than large-scale orchestration.
- Cloud connectors are not the main public story because local file upload is the clearest demo path.
- AI is intentionally secondary to avoid overclaiming and to preserve auditability.

## Technical Choices

- FastAPI serves both the API and the static frontend in one local runtime.
- Pandas handles profiling and deterministic cleanup logic.
- SQLite stores datasets, runs, and diagnostics for traceability.
- PowerShell scripts are part of the product experience because startup confidence matters in a portfolio repo.

## Why This Matters In PM Interviews

This repo shows workflow design, trust-oriented product judgment, practical AI scoping, and execution discipline. It reads as a product someone could actually demo and discuss, not just a technical prototype with a UI on top.
