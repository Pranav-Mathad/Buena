"""Tiny stand-in for an ERP rent-ledger API.

Runs as a standalone FastAPI service on port 8001 (docker-compose / `uvicorn
mock_erp.main:app --port 8001`). Reads its state from :file:`data.json` on
every request so you can edit the file live during the demo to simulate a
new payment arriving.

Endpoints:

- ``GET /payments``               → all payments, newest-first.
- ``GET /payments?account=X``     → filter by account.
- ``GET /payments?since=ISO8601`` → filter by ``posted_at`` timestamp.
- ``GET /healthz``                → liveness.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, Query

DATA_PATH = Path(__file__).resolve().parent / "data.json"
log = structlog.get_logger("mock_erp")

app = FastAPI(title="Mock ERP", version="0.1.0")


def _load_payments() -> list[dict[str, Any]]:
    """Read the payment ledger from :file:`data.json`.

    Returns an empty list if the file is missing or empty so an unedited
    clone still boots cleanly.
    """
    if not DATA_PATH.exists():
        return []
    raw = DATA_PATH.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    parsed = json.loads(raw)
    if isinstance(parsed, list):
        return parsed
    return list(parsed.get("payments", []))


def _parse_since(value: str | None) -> datetime | None:
    """Parse an ISO-8601 ``since`` query string, returning ``None`` on falsy input."""
    if not value:
        return None
    # ``fromisoformat`` handles both "2026-04-01" and "2026-04-01T10:00:00+00:00".
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok"}


@app.get("/payments")
async def payments(
    account: str | None = Query(default=None, description="Filter by account code"),
    since: str | None = Query(default=None, description="ISO-8601 floor on posted_at"),
) -> dict[str, Any]:
    """Return payments, optionally filtered by account + since."""
    items = _load_payments()
    since_dt = _parse_since(since)
    result: list[dict[str, Any]] = []
    for row in items:
        if account and row.get("account") != account:
            continue
        posted_at = row.get("posted_at")
        if since_dt is not None and posted_at:
            try:
                if datetime.fromisoformat(posted_at.replace("Z", "+00:00")) < since_dt:
                    continue
            except ValueError:
                continue
        result.append(row)
    result.sort(key=lambda r: r.get("posted_at", ""), reverse=True)
    log.info("mock_erp.payments", filter_account=account, since=since, returned=len(result))
    return {"payments": result}
