"""APScheduler setup — worker ticks + IMAP poll.

Called from the FastAPI lifespan. The scheduler runs:

- worker_tick every 2s: drains the events queue
- imap_poll every 10s: fetches new mail
- erp_poll every 30s (only when MOCK_ERP_URL is set)
- signal_eval every 30s

All jobs are coalescing + no-overlap.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from backend.config import get_settings
from backend.pipeline.worker import process_batch
from backend.services.erp_poller import poll_once as erp_poll_once
from backend.services.imap_poller import poll_once as imap_poll_once
from backend.signals.evaluator import evaluate_all as evaluate_signals

log = structlog.get_logger(__name__)


def _now():
    """Return current UTC time for immediate scheduling."""
    return datetime.now(timezone.utc)


def build_scheduler() -> AsyncIOScheduler:
    """Build (but do not start) the app's background scheduler."""
    scheduler = AsyncIOScheduler(timezone="UTC")

    # Worker queue processor
    scheduler.add_job(
        process_batch,
        "interval",
        seconds=2,
        id="worker_tick",
        coalesce=True,
        max_instances=1,
        next_run_time=_now(),
    )

    # IMAP poller
    scheduler.add_job(
        imap_poll_once,
        "interval",
        seconds=10,
        id="imap_poll",
        coalesce=True,
        max_instances=1,
        next_run_time=_now(),
    )

    # ERP poller: only registered when a feed is configured, so an unset
    # MOCK_ERP_URL produces no job and no connection errors in the log.
    if get_settings().mock_erp_url:
        scheduler.add_job(
            erp_poll_once,
            "interval",
            seconds=30,
            id="erp_poll",
            coalesce=True,
            max_instances=1,
            next_run_time=_now(),
        )

    # Signal evaluator
    scheduler.add_job(
        evaluate_signals,
        "interval",
        seconds=30,
        id="signal_eval",
        coalesce=True,
        max_instances=1,
        next_run_time=_now(),
    )

    log.info("scheduler.built", jobs=[job.id for job in scheduler.get_jobs()])
    return scheduler