# Case Study

## Problem

Healthcare data exports are often messy before they reach analytics or AI workflows. Column names drift, dates arrive in mixed formats, contact fields are inconsistent, duplicate rows slip through, and quality risks are easy to miss when teams work from raw CSV exports.

## User

The primary user is a technical operator or PM-adjacent builder who needs a fast, reviewable way to inspect and clean healthcare-style datasets without relying on a complex platform or cloud setup.

## Solution

This project packages a guided cleanup workflow that moves from upload to schema and quality profiling, then into deterministic cleanup, validation, and export. It also supports an optional local Ollama model that can help interpret fields and suggest a cleanup strategy while keeping final transforms deterministic and auditable.

## Product Strategy

The product intentionally favors a guided operator flow over a broad dashboard. That makes the main path easier to demo, easier to trust, and easier to explain in interviews. The local-first setup also keeps the repo practical for portfolio use because it can run without cloud dependencies or sensitive data.

## Technical Decisions

- FastAPI serves both the API and static frontend for a simple local runtime.
- Pandas powers profiling and deterministic cleanup logic.
- SQLite stores dataset and run metadata for history and diagnostics.
- Ollama is optional and scoped to planning rather than unrestricted rewriting.
- The launcher and smoke scripts are part of the product experience, not just developer tooling.

## Tradeoffs

- The app optimizes for one-file-at-a-time guided cleanup rather than large-scale orchestration.
- Cloud connectors are de-emphasized so the public demo path stays reliable.
- LLM usage is intentionally narrow to preserve trust, explainability, and public-safe positioning.

## Why This Matters In PM Interviews

This repo shows product judgment as much as implementation. It demonstrates workflow design, trust-oriented data handling, scoped AI usage, local-first packaging, and the ability to turn a technical prototype into a believable product artifact that a recruiter or hiring manager can understand quickly.
