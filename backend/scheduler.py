"""APScheduler setup — worker ticks + IMAP poll.

Called from the FastAPI lifespan. The scheduler runs two jobs in Phase 1:

- ``worker_tick`` every 2s: drains the ``events`` queue through the pipeline.
- ``imap_poll`` every 10s: fetches new mail and enqueues events.

Both jobs are coalescing + no-overlap so a long Gemini call can't pile up
duplicate runs. Phase 2+ will add Tavily regulation cron, signal evaluator,
etc. into the same scheduler.
"""

from __future__ import annotations

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from backend.pipeline.worker import process_batch
from backend.services.erp_poller import poll_once as erp_poll_once
from backend.services.imap_poller import poll_once as imap_poll_once
from backend.services.tavily import watch_regulations
from backend.signals.evaluator import evaluate_all as evaluate_signals

log = structlog.get_logger(__name__)


def build_scheduler() -> AsyncIOScheduler:
    """Build (but do not start) the app's background scheduler."""
    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(
        process_batch,
        "interval",
        seconds=2,
        id="worker_tick",
        coalesce=True,
        max_instances=1,
        next_run_time=None,
    )
    scheduler.add_job(
        imap_poll_once,
        "interval",
        seconds=10,
        id="imap_poll",
        coalesce=True,
        max_instances=1,
        next_run_time=None,
    )
    scheduler.add_job(
        erp_poll_once,
        "interval",
        seconds=30,
        id="erp_poll",
        coalesce=True,
        max_instances=1,
        next_run_time=None,
    )
    scheduler.add_job(
        evaluate_signals,
        "interval",
        seconds=30,
        id="signal_eval",
        coalesce=True,
        max_instances=1,
        next_run_time=None,
    )
    scheduler.add_job(
        watch_regulations,
        "interval",
        minutes=60,
        id="regulation_watch",
        coalesce=True,
        max_instances=1,
        next_run_time=None,
    )

    log.info("scheduler.built", jobs=[j.id for j in scheduler.get_jobs()])
    return scheduler
