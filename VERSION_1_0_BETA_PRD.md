# HcDataCleanUpAi Version 1.0 Beta - Product Requirements Document (PRD)
_Updated: February 8, 2026_

## 1. Purpose
Deliver a best-in-class, local-first healthcare data cleanup tool that ingests messy datasets (EHR, claims, labs, pharmacy), profiles quality, cleans with domain-aware rules, produces QC reports, and exports AI-ready outputs. The product must handle large files (1GB+) with responsive UX and transparent progress reporting.

## 2. Goals
- Ingest local files (CSV/TSV/JSONL/Parquet) up to 1GB reliably.
- Provide a clear, explainable assessment of data quality, schema, and domain signals.
- Offer safe, explainable cleaning with minimal data loss.
- Enforce HIPAA Safe Harbor when requested.
- Produce AI-ready outputs (CSV/JSONL/Parquet) with a transparent QC report.
- Provide a prioritized remediation plan from pre-clean to post-clean RAG readiness deltas.
- Provide before/after previews (first 50 rows) and a fullscreen compare.
- Provide RAG-readiness checks for LLM retrieval workflows before and after cleaning.
- Maintain a smooth, modern UI with clear progress feedback.

## 3. Non-Goals (Beta)
- Cloud connectors (S3, Snowflake, Oracle) - UI placeholders only.
- HL7/FHIR ingestion - UI placeholder only.
- Distributed processing beyond a single machine.
- Real-time collaboration/multi-user.

## 4. Personas
- **Data Scientist**: needs clean, AI-ready datasets quickly.
- **Data Engineer**: needs schema discovery, field meaning, QC.
- **Clinical Ops**: needs Safe Harbor and minimal data loss.
- **Startup Founder**: needs fast time-to-value with minimal setup.

## 5. Primary User Journey
1. **Select source** (file upload).
2. **Preview raw data** + metadata.
3. **Assessment**: missingness, low variance, PII flags, domain hints, and explainable score.
4. **Configure cleaning**: de-dup, missing columns, normalization, privacy.
5. **Run cleaning** with progress.
6. **QC report** with issue severity and RAG readiness.
7. **Download cleaned dataset**.

## 6. Requirements

### 6.1 Ingestion
- Accept file types: CSV, TSV, JSONL, Parquet.
- Handle files up to 1GB.
- Chunked upload for large files (>= 50MB).
- Integrity check after chunked upload (size match).
- Graceful fallback for smaller files.

### 6.2 Assessment
- Column profiling: type, missing %, distinct count, example values.
- Heuristic domain detection: EHR, claims, labs, pharmacy.
- Flags: high missing, low variance, PII-suspected.
- Assessment score is heuristic only and never deletes data.
- Assessment score must be explainable with explicit weighted factors:
  - High missing columns percent (weight 0.40)
  - Low variance columns percent (weight 0.20)
  - PII-suspected columns percent (weight 0.25)
  - Schema uncertainty percent (weight 0.15)
- Assessment score bands:
  - `excellent` (85-100)
  - `good` (70-84)
  - `fair` (50-69)
  - `poor` (0-49)
- Assessment response should include:
  - `assessment.score`
  - `assessment.band`
  - `assessment.factors` (value, weight, penalty per factor)
  - `assessment.definitions`
  - `assessment.sampled_note` when profile is sampled
- Metadata: file size, row estimate, usage intent, privacy mode.

### 6.3 Cleaning Engine
- Safe normalization (avoid deleting critical date fields).
- Type coercion modes: Safe (preserve invalids), Strict (null invalids).
- Domain-aware normalization (ICD/CPT/LOINC/NDC/RxNorm).
- Normalization for gender, phone, ZIP/postal.
- Duplicate row removal (optional).
- Drop fully empty columns (optional).
- Text case normalization.
- De-identification (Safe Harbor) toggle.
- Streaming mode for large CSV/TSV datasets.
- Performance modes:
  - **Balanced**: full validation and normalization.
  - **Fast**: larger chunks, parallel streaming, skips validation issues.
  - **Ultra Fast**: skips most type coercion/normalization for max throughput.

### 6.4 QC Report
- Raw vs cleaned row counts.
- Invalid values by column.
- Missing values by column.
- Missingness comparisons must treat known placeholder tokens (`N/A`, `NULL`, `?`, `unknown`, etc.) as missing in both raw and cleaned baselines.
- Removed duplicates and empty columns.
- Validation issues (ICD/CPT/LOINC/NDC/RxNorm/NPI/date anomalies).
- Warnings when using streaming (sample-based issues).
- Issue severity must be explainable using a rate-plus-impact model:
  - `severity_score = rate_pct * impact_weight`
  - Severity bands:
    - `high`: severity score >= 15, or critical hard-rule issue type
    - `medium`: 5 <= severity score < 15
    - `low`: severity score < 5
- Each QC issue should include:
  - `issue_type`
  - `count` / `affected_rows`
  - `rate_pct`
  - `impact_weight`
  - `severity_score`
  - `severity_reason`
- QC response should include:
  - `severity_legend`
  - `severity_summary` (counts + top issues)
  - `outcomes` (pass/warn/fail outcome metrics)
  - `quality_gate` (gate mode, failed outcomes, summary)
  - `rag_readiness_comparison` (before/after RAG score and check deltas with priority actions)

### 6.5 Output
- Export to CSV / JSONL / Parquet.
- Before/after preview (first 50 rows).
- Fullscreen preview (side-by-side).

### 6.6 UX Requirements
- Upload progress bar with %.
- Cleaning progress bar with %.
- Disable upload/run buttons while processing.
- API status indicator in UI.
- Clear error messages with endpoint info.
- Assessment UI must show score band and factor breakdown.
- QC UI must show severity legend and severity summary.

### 6.7 RAG Readiness for LLM Work
- Provide RAG-readiness checks in:
  - Assessment stage (pre-clean profile)
  - QC stage (post-clean data)
- RAG-readiness score bands:
  - `ready` (>= 80)
  - `partial` (60-79)
  - `not_ready` (< 60)
- Required check categories:
  - Key fields presence
  - Text density
  - Chunkable text quality
  - Missingness health
  - Duplicate/variance health
  - Normalization health
  - PII safety
  - Schema clarity
- Each check should include:
  - status (`pass`/`warn`/`fail`)
  - metric value
  - threshold
  - recommendation
- QC-stage RAG output should include delta from pre-clean score when available.
- QC-stage RAG output must also include:
  - per-check before/after status deltas (`improved`/`regressed`/`unchanged`)
  - per-check metric deltas when available
  - prioritized remediation actions for checks that regress or remain in `fail`/`warn`

### 6.8 Run Traceability
- Every cleaning execution should persist a run record in SQLite.
- Run record should capture:
  - start/end time and duration
  - processing mode and privacy mode
  - QC payload
  - outcome payload
  - quality gate payload
  - RAG readiness payload
  - RAG readiness comparison payload (score delta + check deltas + priority actions)
  - error detail for failed runs
- Run history should be queryable per dataset.

## 7. Functional Scope by Dataset Type

### 7.1 EHR / Clinical
- Detect: encounter_date, admit_date, discharge_date, DOB.
- Preserve critical dates in safe mode.
- Normalize diagnosis/procedure codes.

### 7.2 Claims
- Detect: claim_id, member_id, provider_id, NPI, ICD/CPT, paid_amount.
- Validate code formats and numeric ranges.
- Flag negative/invalid amounts.

### 7.3 Labs
- Detect: LOINC, result_value, units, reference ranges.
- Normalize code fields (strip spaces, uppercase).

### 7.4 Pharmacy
- Detect: NDC, RxNorm, dosage, fill date.
- Normalize NDC to 11-digit.

## 8. Technical Architecture

### 8.1 Backend (FastAPI + Pandas)
- **Endpoints**:
  - `/api/datasets` (upload + profile)
  - `/api/uploads/*` (chunked upload)
  - `/api/datasets/{id}/clean` (sync)
  - `/api/datasets/{id}/clean-jobs` (async)
  - `/api/clean-jobs/{job_id}` (polling)
  - `/api/datasets/{id}/runs`
  - `/api/runs/{run_id}`
  - `/api/runs/{run_id}/outcomes`
  - `/api/datasets/{id}/preview`
  - `/api/datasets/{id}/download`
  - `/api/health`
- **Storage**:
  - Raw in `data/raw`
  - Cleaned in `data/cleaned`
  - SQLite metadata in `data/app.db`
  - Run traceability in `clean_runs` table
- **Processing**:
  - Streaming chunk size: 200k rows (configurable)
  - Fast/Ultra modes use larger chunk sizes and parallel chunk processing
  - PyArrow CSV parsing for fast/ultra modes (fallback to pandas)
  - Vectorized normalization and coercion
  - Safe Harbor de-identification with report
  - Assessment scoring service with weighted factors and score bands
  - Validation severity model service (rate x impact)
  - RAG-readiness service for profile and post-clean QC
  - RAG comparison service that computes pre/post check deltas and prioritized actions
  - Outcome metric calibration for invalid-rate normalization (invalid counts over rows x tracked columns)

### 8.2 Frontend (Static HTML/CSS/JS)
- Served from FastAPI StaticFiles.
- API base pinned to server origin.
- Progress bars for upload and cleaning.
- Fullscreen preview.
- API status banner.
- Assessment panels for score, band, factor penalties, and definitions.
- QC panels for severity legend, severity summary, and issue explanations.
- RAG-readiness panels in both Assessment and QC sections.
- QC RAG panel includes an improvement plan with before/after score bands, check-level deltas, and prioritized actions.

## 9. Performance Targets
- 500MB CSV: ingest < 3 minutes (local disk) and cleaning < 5 minutes on a modern laptop.
- 1GB CSV: chunked upload succeeds reliably; streaming cleaning completes without memory errors.
- Preview + assessment for 1GB dataset uses sampling and finishes under 30s.

## 10. Security & Compliance
- HIPAA Safe Harbor mode available (default for training/external share).
- No cloud storage in beta.
- Data never leaves local machine.

## 11. QA & Testing
- Unit tests for cleaning, privacy logic, assessment scoring, QC severity model, and RAG-readiness scoring.
- Unit tests for RAG readiness comparison deltas and priority action ranking.
- Unit tests for token-aware missingness summaries and invalid-rate denominator normalization.
- Pipeline test script for chunked upload + cleaning job.
- Manual UX test checklist:
  - Upload small CSV -> clean -> download.
  - Upload 500MB CSV -> chunked upload -> async clean -> download.
  - Toggle Safe Harbor on/off and verify output.
  - Verify assessment score explanation and factor breakdown.
  - Verify QC severity legend and per-issue severity reason.
  - Verify pre-clean and post-clean RAG readiness displays.
  - Verify QC RAG improvement plan (before/after score delta, per-check deltas, priority actions).
  - Verify missingness delta reflects normalized placeholders (not raw string placeholders).
  - Verify run history and outcome/quality gate traceability in UI.
  - Preview fullscreen.

## 12. Risks & Mitigations
- **Risk**: wrong API origin -> 405 errors
  - Mitigation: API status banner + pinned API base
- **Risk**: large file memory use
  - Mitigation: streaming mode for large CSV/TSV
- **Risk**: overly aggressive cleaning
  - Mitigation: safe coercion, critical date protection
- **Risk**: opaque scoring can reduce user trust
  - Mitigation: explicit factors, thresholds, severity reasons, and legends

## 13. Milestones
1. **Beta Build** - local ingestion + streaming clean + QC + UI (Complete)
2. **Explainability Upgrade** - assessment factor scoring + QC severity model + RAG readiness (Complete)
3. **RAG Delta Remediation Upgrade** - persisted pre/post check deltas + priority action planning (Complete)
4. **Connector Phase** - S3/Snowflake/Oracle (Future)
5. **FHIR/HL7 Ingestion** (Future)

## 14. Definition of Done (Beta)
- Upload + clean works for small and 500MB+ files.
- Progress bars accurately update.
- QC report generated.
- Downloaded output is AI-ready.
- Assessment section shows score, score band, factor penalties, and definitions.
- QC section shows severity legend, severity summary, and per-issue reasons.
- RAG readiness is shown in both Assessment and QC (including score band and recommendations).
- QC includes an explicit RAG improvement plan with before/after check deltas and prioritized actions.
- Cleaning runs persist with outcomes and quality gate evidence, and are queryable via API.
- Tests pass and no blocking bugs.
