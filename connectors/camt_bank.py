"""Parse a Buena ``bank_index.csv`` (CAMT-derived) into events.

The Buena dataset ships ``Extracted/bank/bank_index.csv`` with one row
per transaction across 2024-2025 (~1,619 rows) plus daily delta files
under ``Extracted/incremental/day-NN/bank/bank_index.csv``. Both share
the same header:

``id, datum, typ, betrag, kategorie, gegen_name, verwendungszweck,
referenz_id, error_types``

This connector emits :class:`ConnectorEvent` rows the structured
extractor (Phase 8 Step 3) can convert directly into financial facts —
no Gemini needed.
"""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import structlog

from connectors import redact
from connectors.base import ConnectorEvent

log = structlog.get_logger(__name__)


_EH_RE = __import__("re").compile(r"\bEH-\d{3,}\b")
_MIE_RE = __import__("re").compile(r"\bMIE-\d{3,}\b")
_INV_RE = __import__("re").compile(r"\bINV-\d{3,}\b")


def _stable_source_ref(row: dict[str, str]) -> str:
    """Hash that survives re-runs even if Buena ever drops the ``id`` column."""
    canonical = (
        f"{row.get('datum', '')}|"
        f"{row.get('betrag', '')}|"
        f"{row.get('verwendungszweck', '')}|"
        f"{row.get('id', '')}"
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:12]


def _parse_amount(raw: str | None) -> Decimal | None:
    """Buena CSV stores positive amounts; ``typ`` carries CREDIT/DEBIT sign."""
    if not raw:
        return None
    cleaned = raw.replace(",", ".").strip()
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _parse_date(raw: str | None) -> datetime | None:
    if not raw or not raw.strip():
        return None
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_row(row: dict[str, str]) -> ConnectorEvent:
    """Convert one CSV row into a redacted :class:`ConnectorEvent`."""
    verwendungszweck = redact.scrub_text(row.get("verwendungszweck", "") or "")
    gegen_name = redact.scrub_text(row.get("gegen_name", "") or "")

    eh_match = _EH_RE.search(verwendungszweck)
    mie_match = _MIE_RE.search(verwendungszweck)
    inv_match = _INV_RE.search(verwendungszweck)

    amount = _parse_amount(row.get("betrag"))
    typ = (row.get("typ") or "").upper()
    signed_amount: Decimal | None = None
    if amount is not None:
        signed_amount = -amount if typ == "DEBIT" else amount

    received_at = _parse_date(row.get("datum"))
    received_at = received_at or datetime.now(timezone.utc)

    metadata: dict[str, Any] = {
        "valuta": row.get("datum"),
        "betrag": str(amount) if amount is not None else None,
        "betrag_signed": str(signed_amount) if signed_amount is not None else None,
        "kategorie": row.get("kategorie"),
        "typ": typ or None,
        "gegen_name": gegen_name,
        "verwendungszweck": verwendungszweck,
        "buena_tx_id": row.get("id"),
        "buena_referenz_id": row.get("referenz_id") or None,
        "eh_id": eh_match.group(0) if eh_match else None,
        "mie_id": mie_match.group(0) if mie_match else None,
        "invoice_ref": inv_match.group(0) if inv_match else None,
        "error_types": row.get("error_types") or None,
    }

    return ConnectorEvent(
        source="bank",
        source_ref=_stable_source_ref(row),
        raw_content=verwendungszweck or f"Bank tx {row.get('id', '?')}",
        metadata=metadata,
        received_at=received_at,
    )


def walk_csv(path: Path) -> Iterator[ConnectorEvent]:
    """Yield :class:`ConnectorEvent`s for every row in ``path``."""
    if not path.is_file():
        log.warning("camt_bank.missing", path=str(path))
        return
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        count = 0
        for row in reader:
            yield parse_row(row)
            count += 1
        log.info("camt_bank.read", path=str(path), rows=count)
