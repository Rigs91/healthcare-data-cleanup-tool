import pandas as pd

from app.services.qc import build_qc_report, build_streaming_qc_report
from app.services.validation import build_validation_issues


def test_qc_severity_uses_rate_and_impact():
    rows = 100
    df = pd.DataFrame(
        {
            "dob": ["2099-01-01"] + ["1980-01-01"] * (rows - 1),
            "paid_amount": (["100"] * 96) + (["-1"] * 4),
            "icd_code": (["A123"] * 96) + (["INVALID"] * 4),
        }
    )
    metadata = {
        "dob": {"semantic_hint": "dob"},
        "paid_amount": {"semantic_hint": None},
        "icd_code": {"semantic_hint": "code"},
    }

    issues = build_validation_issues(df, metadata, total_rows=rows)
    issue_by_type = {issue["issue_type"]: issue for issue in issues}

    assert issue_by_type["dob_future"]["severity"] == "high"
    assert issue_by_type["negative_amount"]["severity"] == "low"
    assert issue_by_type["invalid_icd10"]["severity"] == "medium"
    assert "severity_reason" in issue_by_type["invalid_icd10"]
    assert issue_by_type["invalid_icd10"]["rate_pct"] == 4.0


def test_qc_report_includes_severity_summary_and_legend():
    raw_df = pd.DataFrame({"a": ["1", "2", "3"]})
    cleaned_df = raw_df.copy()
    issues = [
        {"severity": "high", "message": "High issue", "column": "a", "count": 1, "severity_score": 20},
        {"severity": "medium", "message": "Medium issue", "column": "a", "count": 1, "severity_score": 8},
        {"severity": "low", "message": "Low issue", "column": "a", "count": 1, "severity_score": 2},
    ]
    report = build_qc_report(raw_df, cleaned_df, {"conversion": {}}, issues=issues)

    assert "severity_legend" in report
    assert report["severity_summary"]["counts"]["high"] == 1
    assert report["severity_summary"]["counts"]["medium"] == 1
    assert report["severity_summary"]["counts"]["low"] == 1


def test_qc_missing_summary_treats_known_tokens_as_missing():
    raw_df = pd.DataFrame({"notes": ["N/A", "?", "ok", "unknown"]})
    cleaned_df = pd.DataFrame({"notes": [None, None, "ok", None]})

    report = build_qc_report(raw_df, cleaned_df, {"conversion": {}})

    assert report["missing_pct_raw"]["notes"] == 75.0
    assert report["missing_pct_cleaned"]["notes"] == 75.0


def test_streaming_qc_change_summary_notes_row_diffs_unavailable():
    report = build_streaming_qc_report(
        row_count_raw=100,
        row_count_cleaned=95,
        missing_counts_raw={"claim_id": 0},
        missing_counts_cleaned={"claim_id": 0},
        invalid_values={"claim_id": 0},
        empty_columns_removed=[],
        duplicate_rows_removed=0,
        cleaning_report={
            "column_map": {"Claim ID": "claim_id"},
            "conversion": {"claim_id": {"type": "string", "invalid": 0}},
        },
        warnings=[],
        issues=[],
    )

    change_summary = report["change_summary"]
    assert change_summary["row_level_diffs"] == []
    assert "unavailable" in change_summary.get("row_level_diffs_note", "").lower()
    assert change_summary["column_renames"]["count"] == 1
