import pandas as pd

from app.services.cleaning import clean_dataframe


def test_safe_harbor_generalizes_dates_and_masks_identifiers():
    df = pd.DataFrame(
        {
            "DOB": ["1980-01-15", "1910-05-02"],
            "Email": ["alice@example.com", "bob@example.com"],
            "Phone": ["312-555-1212", "773-555-9999"],
            "Zip": ["60601", "02139"],
            "Name": ["Alice Doe", "Bob Smith"],
        }
    )

    column_metadata = {
        "dob": {"primitive_type": "date", "semantic_hint": "dob"},
        "email": {"primitive_type": "string", "semantic_hint": "email"},
        "phone": {"primitive_type": "string", "semantic_hint": "phone"},
        "zip": {"primitive_type": "string", "semantic_hint": "postal_code"},
        "name": {"primitive_type": "string", "semantic_hint": "name"},
    }

    cleaned, report = clean_dataframe(
        df,
        column_metadata,
        privacy_mode="safe_harbor",
        remove_duplicates=False,
        drop_empty_columns=False,
        deidentify=False,
    )

    assert cleaned.loc[0, "dob"] == "1980"
    assert cleaned.loc[1, "dob"] == "90+"
    assert cleaned.loc[0, "email"] == "[redacted_email]"
    assert cleaned.loc[0, "phone"] == "[redacted_phone]"
    assert cleaned.loc[0, "zip"] == "60600"
    assert cleaned.loc[0, "name"] == "[redacted_name]"
    assert report["privacy_mode"] == "safe_harbor"
