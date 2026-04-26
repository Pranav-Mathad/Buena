"""APScheduler setup — worker ticks + IMAP poll.

Called from the FastAPI lifespan. The scheduler runs:

- worker_tick every 2s: drains the events queue
- imap_poll every 10s: fetches new mail
- erp_poll every 30s
- signal_eval every 30s
- regulation_watch every 60m

All jobs are coalescing + no-overlap.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from backend.pipeline.worker import process_batch
from backend.services.erp_poller import poll_once as erp_poll_once
from backend.services.imap_poller import poll_once as imap_poll_once
from backend.services.tavily import watch_regulations
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

    # ERP poller
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

    # Regulation watcher
    scheduler.add_job(
        watch_regulations,
        "interval",
        minutes=60,
        id="regulation_watch",
        coalesce=True,
        max_instances=1,
        next_run_time=_now(),
    )

    log.info("scheduler.built", jobs=[job.id for job in scheduler.get_jobs()])
    return scheduler