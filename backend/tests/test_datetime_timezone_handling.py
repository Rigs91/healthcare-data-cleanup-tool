import pandas as pd

from app.services.cleaning import clean_dataframe
from app.services.profiling import build_profile
from app.services.validation import build_validation_issues


def test_profile_handles_mixed_timezone_and_naive_dates():
    df = pd.DataFrame(
        {
            "Encounter Date": [
                "2026-01-01T12:00:00Z",
                "2026-01-02 08:30:00",
                "2026-01-03T04:00:00-0500",
                "bad-date",
            ]
        }
    )
    column_map = {"Encounter Date": "encounter_date"}

    profile = build_profile(df, column_map, total_rows=len(df), sampled=False, privacy_mode="none")
    col = profile["columns"][0]

    assert col["primitive_type"] == "date"
    assert col["stats"]["min"] == "2026-01-01"
    assert col["stats"]["max"] == "2026-01-03"


def test_validation_handles_mixed_timezone_and_naive_dates():
    now_utc = pd.Timestamp.now(tz="UTC")
    df = pd.DataFrame(
        {
            "encounter_date": [
                (now_utc + pd.Timedelta(days=5)).isoformat(),
                "2020-01-01 08:30:00",
            ],
            "dob": [
                "1980-01-01",
                (now_utc + pd.Timedelta(days=30)).isoformat(),
            ],
        }
    )
    metadata = {
        "encounter_date": {"semantic_hint": "encounter_date"},
        "dob": {"semantic_hint": "dob"},
    }

    issues = build_validation_issues(df, metadata, total_rows=len(df))
    issue_by_type = {issue["issue_type"]: issue for issue in issues}

    assert issue_by_type["date_future"]["count"] == 1
    assert issue_by_type["dob_future"]["count"] == 1


def test_clean_dataframe_handles_mixed_timezone_and_naive_dates():
    df = pd.DataFrame(
        {
            "encounter_date": [
                "2026-01-01T12:00:00Z",
                "2026-01-02 08:30:00",
                "bad-date",
            ]
        }
    )
    metadata = {"encounter_date": {"primitive_type": "date", "semantic_hint": "encounter_date"}}

    cleaned, report = clean_dataframe(
        df,
        metadata,
        already_standardized=True,
        column_map={"encounter_date": "encounter_date"},
        remove_duplicates=False,
        drop_empty_columns=False,
        privacy_mode="none",
    )

    assert cleaned.loc[0, "encounter_date"] == "2026-01-01"
    assert cleaned.loc[1, "encounter_date"] == "2026-01-02"
    assert cleaned.loc[2, "encounter_date"] == "bad-date"
    assert report["conversion"]["encounter_date"]["type"] == "date"
