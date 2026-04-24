"""FastAPI entry point.

Phase 1 wires the event-triggering + SSE routers alongside the Phase 0
properties API, and starts the APScheduler worker/IMAP jobs during the
lifespan context so a single ``uvicorn backend.main:app`` boots the demo.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from backend.api.events import router as events_router
from backend.api.properties import router as properties_router
from backend.api.sse import router as sse_router
from backend.config import get_settings
from backend.logging import configure_logging
from backend.scheduler import build_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Configure logging, start the scheduler, and tear it down on shutdown."""
    configure_logging()
    log = structlog.get_logger("keystone")
    settings = get_settings()
    scheduler = build_scheduler()
    scheduler.start()
    log.info("keystone.startup", env=settings.app_env, jobs=len(scheduler.get_jobs()))
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        log.info("keystone.shutdown")


app = FastAPI(
    title="Keystone",
    version="0.1.0",
    description="The operational brain for property management.",
    lifespan=lifespan,
)

app.include_router(properties_router)
app.include_router(sse_router)
app.include_router(events_router)


@app.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    """Liveness probe used by Docker / Railway and local smoke tests."""
    return {"status": "ok"}
