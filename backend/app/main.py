from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.compat import router as compat_router
from app.api.datasets import router as datasets_router
from app.api.features import router as features_router
from app.api.google import router as google_router
from app.api.runs import router as runs_router
from app.config import BASE_DIR
from app.db.migrations import ensure_schema
from app.db.session import Base, engine


def create_app() -> FastAPI:
    Base.metadata.create_all(bind=engine)
    ensure_schema(engine)

    app = FastAPI(title="HcDataCleanUpAi MVP", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(datasets_router)
    app.include_router(features_router)
    app.include_router(google_router)
    app.include_router(runs_router)
    app.include_router(compat_router)

    frontend_dir = Path(BASE_DIR) / "frontend"
    if frontend_dir.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")

    return app


app = create_app()
