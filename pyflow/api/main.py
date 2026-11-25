"""FastAPI application. Phase 1 only exposes a health check; the real endpoints
(DAGs, runs, triggers, logs) arrive in Phase 7."""

from __future__ import annotations

from fastapi import FastAPI

from pyflow import __version__

app = FastAPI(title="PyFlow Orchestrator", version=__version__)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": __version__}
