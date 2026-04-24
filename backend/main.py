"""FastAPI entry point.

Phase 0 exposes only the health probe and the properties listing/markdown
routes. Later phases register additional routers from :mod:`backend.api`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

import structlog
from fastapi import FastAPI

from backend.api.properties import router as properties_router
from backend.config import get_settings
from backend.logging import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Configure logging on startup and emit a single boot log line."""
    configure_logging()
    log = structlog.get_logger("keystone")
    settings = get_settings()
    log.info("keystone.startup", env=settings.app_env)
    yield
    log.info("keystone.shutdown")


app = FastAPI(
    title="Keystone",
    version="0.1.0",
    description="The operational brain for property management.",
    lifespan=lifespan,
)

app.include_router(properties_router)


@app.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    """Liveness probe used by Docker / Railway and local smoke tests."""
    return {"status": "ok"}
