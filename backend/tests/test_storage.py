import json

import pandas as pd

from app.services.storage import estimate_row_count, read_dataframe


def test_estimate_row_count_parquet_uses_metadata(tmp_path):
    path = tmp_path / "sample.parquet"
    pd.DataFrame({"id": [1, 2, 3, 4]}).to_parquet(path, index=False)

    estimate = estimate_row_count(path, "parquet")

    assert estimate == 4


def test_read_dataframe_jsonl_respects_offset_and_limit(tmp_path):
    path = tmp_path / "sample.jsonl"
    rows = [{"id": idx, "value": f"row_{idx}"} for idx in range(10)]
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")

    df, delimiter = read_dataframe(path, file_type="jsonl", max_rows=3, offset=4)

    assert delimiter is None
    assert df["id"].tolist() == [4, 5, 6]
    assert df["value"].tolist() == ["row_4", "row_5", "row_6"]
