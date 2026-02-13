from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"
DB_PATH = DATA_DIR / "app.db"
APP_VERSION = "0.3.0"
ALLOWED_ORIGINS = "*"


def _load_dotenv_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


for _env_path in (BASE_DIR / ".env", BASE_DIR / "backend" / ".env"):
    _load_dotenv_file(_env_path)

APP_VERSION = os.getenv("APP_VERSION", APP_VERSION)
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", ALLOWED_ORIGINS)

MAX_UPLOAD_MB = 2048
STREAMING_THRESHOLD_MB = 200
PROFILE_SAMPLE_ROWS = 5000
PREVIEW_ROWS = 50
CSV_CHUNK_SIZE = 200000
CSV_CHUNK_SIZE_FAST = 400000
CSV_CHUNK_SIZE_ULTRA = 600000

ARROW_BLOCK_SIZE = 64 * 1024 * 1024
STREAMING_MAX_WORKERS_FAST = 2
STREAMING_MAX_WORKERS_ULTRA = 3

SESSION_SECRET_KEY = os.getenv("SESSION_SECRET_KEY", "dev-session-secret-change-me")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "")
GOOGLE_OAUTH_SCOPE = os.getenv("GOOGLE_OAUTH_SCOPE", "https://www.googleapis.com/auth/drive.readonly")
GOOGLE_OAUTH_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"

for _path in (DATA_DIR, RAW_DIR, CLEANED_DIR):
    _path.mkdir(parents=True, exist_ok=True)
