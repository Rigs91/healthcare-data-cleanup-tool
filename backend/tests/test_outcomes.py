from app.services.outcomes import build_postclean_decision, evaluate_outcomes


def test_evaluate_outcomes_returns_quality_gate_and_items():
    qc = {
        "missing_pct_raw": {"a": 10.0, "b": 20.0},
        "missing_pct_cleaned": {"a": 8.0, "b": 18.0},
        "invalid_values": {"a": 2, "b": 1},
        "row_count_cleaned": 100,
        "duplicate_rows_removed": 3,
        "issues": [
            {"severity": "medium"},
            {"severity": "high"},
        ],
    }
    rag_after = {
        "score": 82,
        "checks": [{"id": "pii_safety", "status": "pass"}],
    }
    report = evaluate_outcomes(
        qc_report=qc,
        rag_before_score=75,
        rag_after=rag_after,
        duration_ms=80_000,
        performance_mode="balanced",
        remove_duplicates_enabled=True,
    )

    assert "items" in report
    assert "quality_gate" in report
    assert len(report["items"]) >= 5
    assert "mode" in report["quality_gate"]
    assert "status" in report["quality_gate"]


def test_evaluate_outcomes_warns_when_rag_drops():
    qc = {
        "missing_pct_raw": {"a": 0.0},
        "missing_pct_cleaned": {"a": 0.0},
        "invalid_values": {"a": 0},
        "row_count_cleaned": 50,
        "duplicate_rows_removed": 0,
        "issues": [],
    }
    rag_after = {
        "score": 50,
        "checks": [{"id": "pii_safety", "status": "warn"}],
    }
    report = evaluate_outcomes(
        qc_report=qc,
        rag_before_score=80,
        rag_after=rag_after,
        duration_ms=60_000,
        performance_mode="balanced",
        remove_duplicates_enabled=False,
    )

    rag_outcome = next(item for item in report["items"] if item["id"] == "rag_readiness_score")
    assert rag_outcome["status"] in {"warn", "fail"}


def test_quality_gate_fails_when_critical_outcomes_fail():
    qc = {
        "missing_pct_raw": {"a": 1.0},
        "missing_pct_cleaned": {"a": 25.0},
        "invalid_values": {"a": 20},
        "row_count_cleaned": 10,
        "duplicate_rows_removed": 0,
        "issues": [
            {"severity": "high"},
            {"severity": "high"},
        ],
    }
    rag_after = {
        "score": 40,
        "checks": [{"id": "pii_safety", "status": "fail"}],
    }

    report = evaluate_outcomes(
        qc_report=qc,
        rag_before_score=85,
        rag_after=rag_after,
        duration_ms=600_000,
        performance_mode="balanced",
        remove_duplicates_enabled=True,
    )

    failed_outcomes = set(report["quality_gate"]["failed_outcomes"])
    assert "critical_date_integrity" in failed_outcomes
    assert report["quality_gate"]["status"] == "fail"


def test_quality_gate_warns_when_only_non_critical_outcomes_fail():
    qc = {
        "missing_pct_raw": {"a": 0.0},
        "missing_pct_cleaned": {"a": 0.0},
        "invalid_values": {"a": 20},
        "row_count_cleaned": 10,
        "duplicate_rows_removed": 0,
        "issues": [],
    }
    rag_after = {
        "score": 90,
        "checks": [{"id": "pii_safety", "status": "pass"}],
    }

    report = evaluate_outcomes(
        qc_report=qc,
        rag_before_score=80,
        rag_after=rag_after,
        duration_ms=60_000,
        performance_mode="balanced",
        remove_duplicates_enabled=True,
    )

    failed_outcomes = set(report["quality_gate"]["failed_outcomes"])
    assert "invalid_value_reduction" in failed_outcomes
    assert "critical_date_integrity" not in failed_outcomes
    assert "pii_safety" not in failed_outcomes
    assert "rag_readiness_score" not in failed_outcomes
    assert report["quality_gate"]["status"] == "warn"


def test_postclean_decision_blocks_release_on_critical_failures():
    qc_report = {
        "outcomes": [
            {
                "id": "critical_date_integrity",
                "label": "Critical Integrity Issues",
                "status": "fail",
                "evidence": "high_severity_issues=2",
                "recommended_action": "Fix high severity data integrity issues.",
            },
            {
                "id": "invalid_value_reduction",
                "label": "Invalid Value Rate",
                "status": "warn",
                "evidence": "invalid_total=3",
                "recommended_action": "Investigate invalid values.",
            },
        ],
        "quality_gate": {
            "status": "fail",
            "failed_outcomes": ["critical_date_integrity"],
        },
    }

    decision = build_postclean_decision(qc_report=qc_report)

    assert decision["status"] == "fail"
    assert decision["blockers"]
    assert "Do not release" in decision["release_recommendation"]
    assert any("Fix high severity data integrity issues." == action for action in decision["actions"])


def test_invalid_value_rate_is_normalized_by_rows_and_columns():
    qc = {
        "missing_pct_raw": {"a": 0.0},
        "missing_pct_cleaned": {"a": 0.0},
        "invalid_values": {"c1": 1, "c2": 1, "c3": 1, "c4": 1, "c5": 1},
        "row_count_cleaned": 100,
        "duplicate_rows_removed": 0,
        "issues": [],
    }
    rag_after = {
        "score": 80,
        "checks": [{"id": "pii_safety", "status": "pass"}],
    }

    report = evaluate_outcomes(
        qc_report=qc,
        rag_before_score=75,
        rag_after=rag_after,
        duration_ms=30_000,
        performance_mode="balanced",
        remove_duplicates_enabled=True,
    )

    invalid_outcome = next(item for item in report["items"] if item["id"] == "invalid_value_reduction")
    assert invalid_outcome["observed_value"] == 1.0
    assert invalid_outcome["status"] == "pass"
