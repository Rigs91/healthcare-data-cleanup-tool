import pandas as pd

from app.services.streaming import stream_clean_csv


def test_stream_clean_csv_supports_parquet_output(tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "cleaned.parquet"

    pd.DataFrame(
        {
            "Patient ID": ["001", "002", "003"],
            "NDC Code": ["12345-6789-0", "1234-5678-90", "0000-1111-22"],
            "Phone": ["312-555-1212", "(773) 555-9999", "847.555.1000"],
        }
    ).to_csv(input_path, index=False)

    _report, qc_payload, preview_df, _warnings = stream_clean_csv(
        input_path,
        file_type="csv",
        column_metadata={},
        output_path=output_path,
        output_format="parquet",
        privacy_mode="none",
        remove_duplicates=False,
        drop_empty_columns=False,
        deidentify=False,
        normalize_phone=True,
        normalize_zip=True,
        normalize_gender=True,
        text_case="none",
        coercion_mode="safe",
        performance_mode="balanced",
        chunksize=2,
        row_count_estimate=3,
    )

    assert output_path.exists()
    written = pd.read_parquet(output_path)
    assert len(written) == 3
    assert qc_payload["row_count_raw"] == 3
    assert qc_payload["row_count_cleaned"] == 3
    assert len(preview_df) == 3
