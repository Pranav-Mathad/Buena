"""FastAPI entry point.

Mounts every router, configures CORS from ``KEYSTONE_CORS_ORIGINS``, and runs
the background scheduler (event worker, IMAP poller, signal evaluator, optional
ERP poller) inside the app's lifespan, so ``uvicorn backend.main:app`` is the
whole backend.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.admin import router as admin_router
from backend.api.buildings import (
    building_router as buildings_router,
)
from backend.api.buildings import (
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
)
from backend.api.sse import (
    router as sse_router,
)
from backend.api.uploads import router as uploads_router
from backend.api.webhooks import router as webhooks_router
from backend.config import get_settings
from backend.logging import configure_logging
from backend.scheduler import build_scheduler
from backend.services.llm import log_configuration as log_llm_configuration


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Configure logging, start the scheduler, and tear it down on shutdown."""
    configure_logging()
    log = structlog.get_logger("keystone")
    settings = get_settings()
    scheduler = build_scheduler()
    scheduler.start()
    log.info("keystone.startup", env=settings.app_env, jobs=len(scheduler.get_jobs()))
    log_llm_configuration()
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

# KEYSTONE_CORS_ORIGINS is the only source of allowed browser origins: no
# hardcoded hosts and no wildcard patterns, since credentials are allowed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origin_list(),
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
