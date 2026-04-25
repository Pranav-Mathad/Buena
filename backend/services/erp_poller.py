"""Poll the mock ERP and ingest new payments as events.

The poller runs on APScheduler every ~30s (Phase 2). State is kept entirely
in Postgres — each poll asks the mock ERP for ``payments`` and inserts each
new row as an ``erp`` event keyed on its ``payment_id``. The ``(source,
source_ref)`` unique constraint on the events table keeps the call idempotent.
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event

log = structlog.get_logger(__name__)


def _format_payment(row: dict[str, Any]) -> str:
    """Render a payment row in a shape the router and extractor can digest."""
    hint = row.get("property_hint", "")
    amount = row.get("amount_eur", 0)
    currency = row.get("currency", "EUR")
    kind = row.get("kind", "rent")
    period = row.get("period", "")
    late = row.get("late_days", 0)
    notes = row.get("notes", "")
    return (
        f"ERP payment notification\n"
        f"Account: {row.get('account', 'UNKNOWN')}\n"
        f"Property: {hint}\n"
        f"Period: {period}\n"
        f"Kind: {kind}\n"
        f"Amount: {amount} {currency}\n"
        f"Late by: {late} days\n"
        f"Notes: {notes}".strip()
    )


async def _fetch_payments(base_url: str) -> list[dict[str, Any]]:
    """Call ``GET {base_url}/payments`` with a short timeout."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(f"{base_url.rstrip('/')}/payments")
        response.raise_for_status()
        data = response.json()
    payments = data.get("payments", []) if isinstance(data, dict) else list(data)
    if not isinstance(payments, list):
        log.warning("erp.poll.bad_shape", payload_type=type(payments).__name__)
        return []
    return [row for row in payments if isinstance(row, dict)]


async def poll_once() -> int:
    """Single poll pass. Returns the count of newly inserted payment events."""
    settings = get_settings()
    base_url = settings.mock_erp_url
    if not base_url:
        log.debug("erp.poll.skip", reason="no_url")
        return 0

    try:
        payments = await _fetch_payments(base_url)
    except httpx.HTTPError as exc:
        log.warning("erp.poll.error", error=str(exc), url=base_url)
        return 0

    if not payments:
        return 0

    factory = get_sessionmaker()
    inserted = 0
    async with factory() as session:
        for row in payments:
            payment_id = str(row.get("payment_id") or "").strip()
            if not payment_id:
                continue
            raw = _format_payment(row)
            metadata = {"payment_id": payment_id, "account": row.get("account")}
            _, new = await insert_event(
                session,
                source="erp",
                source_ref=payment_id,
                raw_content=raw,
                metadata=metadata,
            )
            if new:
                inserted += 1
        await session.commit()

    if inserted:
        log.info("erp.poll.ingested", inserted=inserted, total=len(payments))
    else:
        log.debug("erp.poll.nothing_new", total=len(payments))
    return inserted
