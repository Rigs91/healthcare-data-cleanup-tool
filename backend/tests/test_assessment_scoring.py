from app.services.scoring import build_assessment_breakdown


def _profile_template(column_count: int, high_missing: int, low_variance: int, pii: int, unknown: int):
    return {
        "column_count": column_count,
        "summary": {
            "columns_high_missing": [f"m_{idx}" for idx in range(high_missing)],
            "low_variance_columns": [f"v_{idx}" for idx in range(low_variance)],
            "columns_with_pii": [f"p_{idx}" for idx in range(pii)],
        },
        "columns": [
            {"primitive_type": "unknown" if idx < unknown else "string"}
            for idx in range(column_count)
        ],
    }


def test_assessment_breakdown_excellent_band():
    assessment = build_assessment_breakdown(
        _profile_template(column_count=10, high_missing=0, low_variance=0, pii=0, unknown=0)
    )

    assert assessment["score"] == 100
    assert assessment["band"] == "excellent"
    assert len(assessment["factors"]) == 4


def test_assessment_breakdown_good_band_boundary():
    # Penalty = 12 + 4 + 5 + 9 = 30 => score 70.
    assessment = build_assessment_breakdown(
        _profile_template(column_count=10, high_missing=3, low_variance=2, pii=2, unknown=6)
    )

    assert assessment["score"] == 70
    assert assessment["band"] == "good"


def test_assessment_breakdown_poor_band():
    assessment = build_assessment_breakdown(
        _profile_template(column_count=10, high_missing=8, low_variance=7, pii=6, unknown=8)
    )

    assert assessment["score"] < 50
    assert assessment["band"] == "poor"
