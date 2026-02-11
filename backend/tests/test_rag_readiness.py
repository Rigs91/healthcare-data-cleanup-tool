import pandas as pd

from app.services.rag_readiness import (
    build_rag_readiness_comparison,
    build_rag_readiness_from_dataframe,
    build_rag_readiness_from_profile,
)


def test_rag_readiness_profile_ready_shape():
    profile = {
        "column_count": 6,
        "sampled": False,
        "summary": {
            "columns_high_missing": [],
            "low_variance_columns": [],
            "columns_with_pii": [],
        },
        "columns": [
            {"clean_name": "patient_id", "primitive_type": "string", "semantic_hint": "id", "missing_pct": 0, "distinct_count": 100, "example_values": ["p1"], "domain_tags": []},
            {"clean_name": "encounter_date", "primitive_type": "date", "semantic_hint": "encounter_date", "missing_pct": 0, "distinct_count": 90, "example_values": ["2024-01-01"], "domain_tags": []},
            {"clean_name": "icd_code", "primitive_type": "string", "semantic_hint": "code", "missing_pct": 1, "distinct_count": 30, "example_values": ["A12.3"], "domain_tags": ["icd"]},
            {"clean_name": "clinical_note", "primitive_type": "string", "semantic_hint": None, "missing_pct": 5, "distinct_count": 80, "example_values": ["Patient presents with persistent cough and fatigue."], "domain_tags": []},
            {"clean_name": "plan_text", "primitive_type": "string", "semantic_hint": None, "missing_pct": 10, "distinct_count": 60, "example_values": ["Recommend hydration, follow-up in two weeks, and medication review."], "domain_tags": []},
            {"clean_name": "provider_name", "primitive_type": "string", "semantic_hint": "provider", "missing_pct": 2, "distinct_count": 30, "example_values": ["Dr Smith"], "domain_tags": []},
        ],
    }

    readiness = build_rag_readiness_from_profile(profile, privacy_mode="safe_harbor")

    assert readiness["score"] >= 60
    assert readiness["band"] in {"ready", "partial"}
    assert len(readiness["checks"]) == 8


def test_rag_readiness_dataframe_delta_present():
    cleaned_df = pd.DataFrame(
        {
            "patient_id": ["1", "2", "3"],
            "encounter_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "clinical_note": [
                "Persistent cough and fatigue, chest exam otherwise stable.",
                "Mild wheeze noted, advised follow-up and inhaler adherence.",
                "Symptoms improved after treatment; continue current plan.",
            ],
        }
    )
    qc = {
        "missing_pct_cleaned": {"patient_id": 0, "encounter_date": 0, "clinical_note": 0},
        "invalid_values": {"encounter_date": 0},
        "duplicate_rows_removed": 0,
        "row_count_raw": 3,
    }

    readiness = build_rag_readiness_from_dataframe(
        cleaned_df,
        qc,
        privacy_mode="safe_harbor",
        baseline_score=55,
    )

    assert "delta_from_profile_score" in readiness
    assert readiness["delta_from_profile_score"] == readiness["score"] - 55


def test_rag_readiness_comparison_marks_regressions_and_priorities():
    before = {
        "score": 72,
        "band": "partial",
        "checks": [
            {
                "id": "text_density",
                "label": "Text Density",
                "status": "warn",
                "metric": 0.31,
                "recommendation": "Improve text density.",
            },
            {
                "id": "pii_safety",
                "label": "PII Safety",
                "status": "warn",
                "metric": 0.72,
                "recommendation": "Strengthen privacy controls.",
            },
            {
                "id": "schema_clarity",
                "label": "Schema Clarity",
                "status": "pass",
                "metric": 0.9,
                "recommendation": "Keep stable schema names.",
            },
        ],
    }
    after = {
        "score": 78,
        "band": "partial",
        "checks": [
            {
                "id": "text_density",
                "label": "Text Density",
                "status": "pass",
                "metric": 0.52,
                "recommendation": "Improve text density.",
            },
            {
                "id": "pii_safety",
                "label": "PII Safety",
                "status": "fail",
                "metric": 0.48,
                "recommendation": "Strengthen privacy controls.",
            },
            {
                "id": "schema_clarity",
                "label": "Schema Clarity",
                "status": "pass",
                "metric": 0.95,
                "recommendation": "Keep stable schema names.",
            },
        ],
    }

    comparison = build_rag_readiness_comparison(before, after)

    assert comparison is not None
    assert comparison["score_before"] == 72
    assert comparison["score_after"] == 78
    assert comparison["score_delta"] == 6

    deltas = {item["id"]: item for item in comparison["check_deltas"]}
    assert deltas["text_density"]["status_delta"] == "improved"
    assert deltas["pii_safety"]["status_delta"] == "regressed"
    assert deltas["pii_safety"]["priority"] == "high"
    assert deltas["schema_clarity"]["status_delta"] == "unchanged"

    assert comparison["priority_actions"]
    assert comparison["priority_actions"][0]["check_id"] == "pii_safety"
    assert "Address 1 high-priority actions first." in comparison["summary"]
