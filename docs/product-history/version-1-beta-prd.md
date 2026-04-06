# Version 1 Beta PRD Summary

## Intent

The first beta was designed as a local-first healthcare data cleanup tool for messy claims, EHR, lab, and pharmacy datasets.

## Core themes

- Upload local CSV, TSV, JSONL, and Parquet files.
- Profile schema, missingness, primitive types, and likely healthcare domains.
- Clean data with deterministic normalization and optional de-identification.
- Produce QC output and export cleaner datasets for analytics or AI-ready use cases.

## Notable constraints

- File upload was the main supported source.
- Cloud connectors and HL7/FHIR ingestion were explicitly out of scope.
- The product emphasized explainability and safe cleanup over aggressive automation.
