import pandas as pd

from app.services.cleaning import clean_dataframe, standardize_columns


def test_clean_dataframe_basic():
    raw_df = pd.DataFrame(
        {
            "Patient ID": ["001", "001", "002"],
            "DOB": ["1/2/1980", "1/2/1980", "1985-03-04"],
            "Charge ($)": ["1,200.50", "1,200.50", "900"],
            "ICD Code": ["a10", "a10", "b20"],
            "Empty": ["", "", ""],
        }
    )

    standardized_df, column_map = standardize_columns(raw_df.copy())
    inferred_types = {
        column_map["Patient ID"]: {"primitive_type": "string", "semantic_hint": "id"},
        column_map["DOB"]: {"primitive_type": "date", "semantic_hint": "date"},
        column_map["Charge ($)"]: {"primitive_type": "number", "semantic_hint": None},
        column_map["ICD Code"]: {"primitive_type": "string", "semantic_hint": "code"},
    }

    cleaned_df, report = clean_dataframe(
        raw_df,
        inferred_types,
        remove_duplicates=True,
        drop_empty_columns=True,
        deidentify=False,
    )

    assert len(cleaned_df) == 2
    assert "empty" not in cleaned_df.columns
    assert report["duplicate_rows_removed"] == 1
    assert report["empty_columns_removed"] == ["empty"]

    cleaned_cols = set(cleaned_df.columns)
    assert "patient_id" in cleaned_cols
    assert "dob" in cleaned_cols
    assert "charge" in cleaned_cols
    assert "icd_code" in cleaned_cols
