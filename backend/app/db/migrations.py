from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


DATASET_COLUMNS = {
    "source_type": "TEXT",
    "file_type": "TEXT",
    "usage_intent": "TEXT",
    "output_format": "TEXT",
    "privacy_mode": "TEXT",
    "file_size_bytes": "INTEGER",
    "row_count_estimate": "INTEGER",
    "latest_run_id": "TEXT",
}

CLEAN_RUN_COLUMNS = {
    "id": "TEXT PRIMARY KEY",
    "dataset_id": "TEXT NOT NULL",
    "status": "TEXT NOT NULL",
    "performance_mode": "TEXT",
    "privacy_mode": "TEXT",
    "output_format": "TEXT",
    "started_at": "DATETIME",
    "completed_at": "DATETIME",
    "duration_ms": "INTEGER",
    "profile_snapshot_json": "TEXT",
    "assessment_json": "TEXT",
    "qc_json": "TEXT",
    "outcomes_json": "TEXT",
    "rag_readiness_json": "TEXT",
    "quality_gate_json": "TEXT",
    "warnings_json": "TEXT",
    "error": "TEXT",
    "created_at": "DATETIME",
    "updated_at": "DATETIME",
}


def ensure_schema(engine: Engine) -> None:
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info(datasets)"))
        existing = {row[1] for row in result.fetchall()}

        for column, ddl_type in DATASET_COLUMNS.items():
            if column in existing:
                continue
            conn.execute(text(f"ALTER TABLE datasets ADD COLUMN {column} {ddl_type}"))

        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='clean_runs'")
        ).first()
        if not table_exists:
            column_ddl = ", ".join(f"{column} {ddl}" for column, ddl in CLEAN_RUN_COLUMNS.items())
            conn.execute(text(f"CREATE TABLE clean_runs ({column_ddl})"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_runs_dataset_id ON clean_runs(dataset_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_runs_created_at ON clean_runs(created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_runs_status ON clean_runs(status)"))
        else:
            run_info = conn.execute(text("PRAGMA table_info(clean_runs)"))
            run_existing = {row[1] for row in run_info.fetchall()}
            for column, ddl in CLEAN_RUN_COLUMNS.items():
                if column in run_existing or "PRIMARY KEY" in ddl:
                    continue
                base_ddl = ddl.replace("NOT NULL", "").strip()
                conn.execute(text(f"ALTER TABLE clean_runs ADD COLUMN {column} {base_ddl}"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_runs_dataset_id ON clean_runs(dataset_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_runs_created_at ON clean_runs(created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_runs_status ON clean_runs(status)"))
        conn.commit()
