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
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from backend.api.admin import router as admin_router
from backend.api.buildings import (
    building_router as buildings_router,
    liegenschaft_router as liegenschaften_router,
)
from backend.api.draft_reply import router as draft_reply_router
from backend.api.events import router as events_router
from backend.api.files import router as files_router
from backend.api.portfolio import router as portfolio_router
from backend.api.properties import router as properties_router
from backend.api.property_files import router as property_files_router
from backend.api.settings import router as settings_router
from backend.api.signals import router as signals_router
from backend.api.source_links import router as source_links_router
from backend.api.sse import (
    portfolio_router as sse_portfolio_router,
    router as sse_router,
)
from backend.api.uploads import router as uploads_router
from backend.api.webhooks import router as webhooks_router
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

_cors_origins = [
    origin.strip()
    for origin in get_settings().keystone_cors_origins.split(",")
    if origin.strip()
]

# Always allow Lovable origins
_cors_origins.extend([
    "https://keystone-insight.lovable.app",
    "https://preview--keystone-insight.lovable.app",
    "https://keystone-insight.lovable.app",
    "https://preview--keystone-insight.lovable.app",
    "https://e014e01e-3cc6-4aff-8939-c1bc4f248e09.lovableproject.com",
])

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_origin_regex=r"https://.*\.(lovable\.app|lovableproject\.com)",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(properties_router)
app.include_router(property_files_router)
app.include_router(buildings_router)
app.include_router(liegenschaften_router)
app.include_router(sse_router)
app.include_router(sse_portfolio_router)
app.include_router(events_router)
app.include_router(source_links_router)
app.include_router(files_router)
app.include_router(uploads_router)
app.include_router(webhooks_router)
app.include_router(signals_router)
app.include_router(portfolio_router)
app.include_router(settings_router)
app.include_router(admin_router)
app.include_router(draft_reply_router)


@app.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    """Liveness probe used by Docker / Railway and local smoke tests."""
    return {"status": "ok"}


@app.get("/aikido.txt", response_class=PlainTextResponse, tags=["system"])
async def aikido_verification() -> str:
    """Serve the Aikido domain verification string."""
    return "validation.aikido.e29d600660a740cc0f0e7cabb18d4b0a"
