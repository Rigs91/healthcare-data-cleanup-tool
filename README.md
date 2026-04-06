# Healthcare Data Cleanup Tool

Clean up messy healthcare datasets before they reach analytics, reporting, or downstream AI workflows.

Healthcare teams often inherit CSV exports that are inconsistent, hard to trust, and risky to hand off. This project turns that cleanup step into a guided workflow: upload a messy dataset, inspect schema and quality signals, run safer normalization, and export a cleaner result with traceability.

| Upload and setup | Quality outcome and export |
| --- | --- |
| ![Upload and setup](docs/screenshots/01-home-or-upload.png) | ![Quality outcome and export](docs/screenshots/03-cleanup-or-validation-results.png) |

## Why This Repo Is Strong Portfolio Material

- Shows product thinking through a guided workflow instead of a feature-heavy dashboard
- Makes trust visible with schema signals, blockers, quality summaries, and run history
- Uses AI honestly: Ollama can assist planning, but cleanup execution remains deterministic
- Ships as a real local product with a one-click launcher, health checks, and demo-safe sample data

## What The Product Does

This app helps a user move from a messy healthcare-style export to a cleaner, more reviewable dataset in four steps:

1. Upload a local CSV, TSV, JSONL, or Parquet file
2. Profile schema, missingness, likely field meaning, and data quality risks
3. Run guided cleanup with deterministic execution and optional local Ollama-assisted planning
4. Review the quality summary and export the cleaned file

The main demo path is local-first and public-safe. The bundled sample dataset is synthetic, and the product does not rely on cloud services to show its core value.

## The Problem It Solves

Messy healthcare data slows down analytics and weakens trust. Dates arrive in mixed formats, contact fields are inconsistent, duplicate rows slip in, and important cleanup decisions are often invisible. This product makes those problems visible early, guides the operator through cleanup, and keeps the output understandable enough to review before handoff.

## Who It Is For

- Product managers and PM leaders evaluating data workflow thinking
- Data platform and analytics teams preparing healthcare-style data for reporting
- Operators who need a simple, reviewable cleanup path without sending data to a cloud service
- Engineering leaders validating technical fluency and execution quality

## 60-Second Demo Path

1. Run `.\RUN_HCDATA_1_CLICK.cmd`
2. Keep `Deterministic` selected for the fastest path
3. Upload `data/sample_messy.csv`
4. Show the pre-check view and explain the blockers
5. Run guided cleanup
6. Show the final quality summary and export action
7. Optional: open `History & Diagnostics` to prove traceability

The exact talk track is in [docs/DEMO-SCRIPT.md](docs/DEMO-SCRIPT.md).

## Workflow At A Glance

| Profile and pre-check | History and diagnostics |
| --- | --- |
| ![Profile and pre-check](docs/screenshots/02-profile-or-schema-view.png) | ![History and diagnostics](docs/screenshots/04-export-or-quality-summary.png) |

## Why It Stands Out

- Guided flow over sprawl: one clear path from upload to export
- Trust-first design: blockers, quality gates, diagnostics, and run history are built into the story
- Honest AI scope: optional local Ollama planning supports interpretation and recommendations without pretending to automate everything
- Local-first packaging: simple startup, synthetic data, and no dependency on external infrastructure for the core demo

## Core Workflows

### Guided cleanup

- Upload a healthcare-style dataset
- Inspect readiness, blockers, and recommended actions
- Run cleanup with deterministic rules
- Export the cleaned result

### Optional Ollama assistance

- Use a local Ollama model to interpret columns and suggest a cleanup plan
- Keep the actual transforms deterministic and auditable
- Send only compact profile context and bounded sample rows to the model

### Diagnostics and traceability

- Review dataset and run history
- Inspect cleanup mode, model metadata, and plan status
- Open diagnostics JSON for a deeper run-level view

## Architecture And Data Handling

The frontend is a static guided UI served by FastAPI. The backend handles upload, profiling, cleanup orchestration, export, and run history. SQLite stores dataset metadata and run diagnostics locally, while cleaned outputs are written to disk.

The profiling layer infers primitive types, semantic hints, domain signals, missingness, and basic PII indicators. If Ollama-assisted mode is selected, the backend asks a local model for a structured cleanup plan, validates the response, and applies only supported directives through the deterministic cleanup path.

## Tech Stack

- **FastAPI**: serves the API, workflow routes, and static frontend from one local app
- **Pandas**: powers dataset profiling and cleanup logic on local files
- **SQLite + SQLAlchemy**: store datasets, runs, and diagnostics for traceability
- **Vanilla JavaScript + CSS**: keep the UI lightweight and easy to run without a frontend build step
- **Ollama**: adds optional local LLM planning without making cloud model access a requirement
- **PowerShell launch scripts**: provide one-click startup, health checks, and smoke-path validation on Windows

## Run Locally

### Fastest path

```powershell
.\RUN_HCDATA_1_CLICK.cmd
```

The launcher bootstraps `.venv`, installs backend dependencies if needed, starts the app on an available port, checks health, probes Ollama, and opens the browser.

### Developer commands

```powershell
.\scripts\dev.ps1
.\scripts\run_tests.ps1
.\scripts\test_tool.ps1 -BaseUrl http://127.0.0.1:8000
```

### Optional Ollama setup

```powershell
ollama serve
ollama list
```

If Ollama is not running, the app still works in deterministic mode.

## Demo Dataset

- Primary sample: `data/sample_messy.csv`
- The sample is synthetic and safe for screenshots, demos, and public repo artifacts
- The public demo path is intentionally built around local file upload rather than connectors

## Product Decisions And Tradeoffs

- **Deterministic cleanup is the primary product**: optional AI planning helps with interpretation, not unrestricted rewriting
- **File upload is the main demo path**: it keeps the repo easy to understand and reliable to run locally
- **Guided workflow beats a broad canvas**: the product optimizes for trust and clarity over feature sprawl
- **Local-first by default**: startup friction stays low, and the privacy story stays simple

## Supporting Docs

- [Case study](docs/CASE-STUDY.md)
- [60-second demo script](docs/DEMO-SCRIPT.md)
- [Resume and LinkedIn bullets](docs/RESUME-BULLETS.md)
- [Publish steps and GitHub metadata](docs/PUBLISH-STEPS.md)
- [Product history](docs/product-history/README.md)

## License

[MIT](LICENSE)
