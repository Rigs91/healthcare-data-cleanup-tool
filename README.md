# HcDataCleanUpAi MVP

A working MVP that ingests messy healthcare datasets, profiles the columns, runs a cleaning engine, and outputs AI- and RAG-ready cleaned data.

## Tech Stack
- Backend: `FastAPI` + `pandas` + `SQLAlchemy` (SQLite)
- Frontend: Static HTML/CSS/JS served by FastAPI
- Storage: Local `data/` folder for raw and cleaned files

## What the Engine Does
- Standardizes column names to `snake_case`
- Normalizes missing values (`"N/A"`, `"NULL"`, `"?"`, blanks)
- Infers semantic hints + primitive types from names and samples
- Cleans numeric/date/boolean columns with type coercion
- Upper-cases code columns (ICD/CPT/LOINC, etc.)
- Normalizes phone/ZIP/gender fields
- Optional de-identification for email/phone/SSN/name-like fields
- Drops fully empty columns and duplicates (configurable)
- Generates explainable QC metrics for missing rates and invalid conversions
- Classifies QC issues using `rate x impact` severity scoring with reasons
- Computes pre-clean and post-clean RAG readiness with actionable checks
- Persists cleaning run history with outcomes, quality gate status, and evidence
- Supports usage intent (training/inference/analytics/share) for default rules
- Output formats: CSV, JSONL, Parquet (requires `pyarrow`)
- Domain-aware assessment for EHR, Claims, Labs, and Pharmacy

### Usage Intent Defaults
- `training` / `external_share`: defaults to `HIPAA Safe Harbor`
- `inference` / `analytics`: defaults to `No de-identification` (can be overridden)
Note: intended usage only sets privacy defaults; it does not drop data by itself.

### Safe Type Coercion (Default)
- Date/number/boolean coercion only happens when parse success >= 60%.
- When coercion is applied, invalid values are preserved (safe mode) instead of dropped.

### HIPAA Safe Harbor (Default Privacy Mode)
- Masks direct identifiers (name, email, phone, SSN, IDs).
- Generalizes dates to year only; DOB ages > 89 become `90+`.
- Truncates ZIP to first 3 digits (with `00` suffix).

### Domain-Aware Validation (Highlights)
- ICD-10, CPT/HCPCS, LOINC, NDC, RxNorm format checks
- Date sanity checks (DOB future dates, encounter dates in future)
- Negative monetary value checks

### Outcome Traceability (Current)
- Every cleaning run is persisted as a `clean_run` record in SQLite.
- Each run stores:
  - QC payload
  - outcome metrics (`pass|warn|fail`)
  - quality gate summary (`warn` mode by default)
  - RAG readiness snapshot
- Run history is available via API and rendered in the UI.

## Folder Structure
- `backend/` FastAPI app, DB models, cleaning engine, tests
- `frontend/` Static UI served at `/`
- `data/` Raw + cleaned datasets and sample input
- `scripts/` Run, test, and demo scripts

## Source Coverage (MVP)
- File upload (CSV/TSV/JSONL/Parquet)
- Connector placeholders for Google Drive, AWS S3, Snowflake, Oracle, FHIR/HL7 (UI only)

## Quick Start
1. Install dependencies (first run will auto-install if you use `npm run dev`):
```
powershell
python -m pip install -r backend\requirements.txt
```

2. Start the backend (serves frontend too):
```
powershell
.\scripts\run_backend.ps1
```

Or use the one-command bootstrap:
```
powershell
npm run dev
```

3. Open the UI:
```
http://localhost:8000
```

## Demo Script
Run the scripted demo (assumes the server is already running):
```
powershell
.\scripts\test_tool.ps1
```
This will:
- Upload `data\sample_messy.csv`
- Run the cleaning pipeline
- Download the cleaned CSV to `data\cleaned\demo_cleaned.csv`

## Tests
```
powershell
.\scripts\run_tests.ps1
```

## API Endpoints
- `POST /api/datasets` (multipart) upload dataset
- `GET /api/datasets` list datasets
- `GET /api/datasets/{id}` dataset detail
- `POST /api/datasets/{id}/clean` run cleaning
- `GET /api/datasets/{id}/runs` list persisted cleaning runs
- `GET /api/runs/{run_id}` fetch run detail
- `GET /api/runs/{run_id}/outcomes` fetch run outcomes + quality gate
- `GET /api/datasets/{id}/preview?kind=raw|cleaned` data preview (limit/offset supported)
- `GET /api/datasets/{id}/download?kind=raw|cleaned` file download
- `GET /api/health` health check

## MVP Limitations
- CSV/TSV/JSONL/Parquet support (Parquet requires `pyarrow` or `fastparquet`)
- Large files use streaming mode for CSV/TSV (duplicate/empty-column removal disabled)
- Heuristic semantic inference
- De-identification is a basic mask (not HIPAA compliant)

If you want this pushed to production quality, we can add:
- Robust schema mapping + column dictionary editing
- Data lineage tracking
- Role-based access + audit logs
- Advanced de-identification and validation rules
- Streaming and large-file support
