"""Synchronous fact writers for structured event sources.

Bank transactions and invoices arrive with enough metadata that no LLM
call is needed to derive the matching facts. This module owns those
deterministic conversions:

- ``bank`` events whose routed property is known and ``kategorie=miete``
  → write a ``financials.last_rent_payment`` fact superseding any
  prior one for that property.
- ``bank`` events with a clear refund / credit category that we can
  attribute → ``financials.last_credit``.
- ``invoice`` events whose ``metadata.dl_id`` resolves to a known
  contractor → write ``maintenance.last_contractor_invoice`` and
  ensure a ``relationships(serviced_by)`` edge exists between the
  contractor and the routed property (skipped when property is unknown).

All writes are idempotent: callers stamp ``events.processed_at`` after
this returns so the live worker leaves the row alone, and a re-run of
the backfill produces zero new fact rows because the differ-style
"identical value" guard short-circuits.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = structlog.get_logger(__name__)


# -----------------------------------------------------------------------------
# Bank
# -----------------------------------------------------------------------------


def _format_amount(amount_str: str | None) -> str:
    """``"1256.00"`` → ``"EUR 1,256.00"``. ``None`` passes through."""
    if not amount_str:
        return "EUR ?"
    try:
        amount = Decimal(amount_str)
    except Exception:  # noqa: BLE001
        return f"EUR {amount_str}"
    return f"EUR {amount:,.2f}"


async def _write_or_supersede_fact(
    session: AsyncSession,
    *,
    property_id: UUID | None = None,
    building_id: UUID | None = None,
    liegenschaft_id: UUID | None = None,
    section: str,
    field: str,
    value: str,
    confidence: float,
    source_event_id: UUID,
) -> bool:
    """Write a fact unless an identical scope+(section, field, value) is current.

    Exactly one of ``property_id`` / ``building_id`` / ``liegenschaft_id``
    must be set — the scope is the natural key for the
    ``superseded_by IS NULL`` lookup. Re-writes with identical values
    short-circuit; differing values stamp ``superseded_by`` on the
    predecessor.
    """
    if sum(x is not None for x in (property_id, building_id, liegenschaft_id)) != 1:
        raise ValueError(
            "exactly one of property_id / building_id / liegenschaft_id required"
        )

    if property_id is not None:
        scope_clause = (
            "property_id = :scope_id "
            "AND building_id IS NULL AND liegenschaft_id IS NULL"
        )
        scope_id = property_id
    elif building_id is not None:
        scope_clause = (
            "building_id = :scope_id "
            "AND property_id IS NULL AND liegenschaft_id IS NULL"
        )
        scope_id = building_id
    else:
        assert liegenschaft_id is not None
        scope_clause = (
            "liegenschaft_id = :scope_id "
            "AND property_id IS NULL AND building_id IS NULL"
        )
        scope_id = liegenschaft_id

    current = (
        await session.execute(
            text(
                f"""
                SELECT id, value FROM facts
                WHERE {scope_clause}
                  AND section = :section
                  AND field = :field
                  AND superseded_by IS NULL
                """
            ),
            {"scope_id": scope_id, "section": section, "field": field},
        )
    ).first()

    if current is not None and (current.value or "").strip() == value.strip():
        return False  # idempotent no-op

    insert_result = await session.execute(
        text(
            """
            INSERT INTO facts (property_id, building_id, liegenschaft_id,
                               section, field, value,
                               source_event_id, confidence, valid_from)
            VALUES (:pid, :bid, :lid, :section, :field, :value,
                    :eid, :conf, now())
            RETURNING id
            """
        ),
        {
            "pid": property_id,
            "bid": building_id,
            "lid": liegenschaft_id,
            "section": section,
            "field": field,
            "value": value,
            "eid": source_event_id,
            "conf": confidence,
        },
    )
    new_id: UUID = insert_result.scalar_one()

    if current is not None:
        await session.execute(
            text(
                """
                UPDATE facts
                SET superseded_by = :new_id, valid_to = now()
                WHERE id = :old_id AND superseded_by IS NULL
                """
            ),
            {"new_id": new_id, "old_id": current.id},
        )
    return True


async def extract_bank_facts(
    session: AsyncSession,
    *,
    event_id: UUID,
    property_id: UUID | None = None,
    building_id: UUID | None = None,
    liegenschaft_id: UUID | None = None,
    metadata: dict[str, Any],
) -> int:
    """Write deterministic facts for one bank event. Returns count written.

    Phase 8.1: scope is whichever of ``property_id`` / ``building_id`` /
    ``liegenschaft_id`` is set. No-op when all three are None
    (the event is genuinely unrouted).

    Section names per scope:
    - property      → ``financials.*``
    - building      → ``building_financials.*``
    - liegenschaft  → ``liegenschaft_financials.*``
    """
    kategorie = (metadata.get("kategorie") or "").lower()
    typ = (metadata.get("typ") or "").upper()
    amount = metadata.get("betrag")
    valuta = metadata.get("valuta")
    formatted = (
        f"{_format_amount(amount)} on {valuta}"
        if valuta
        else _format_amount(amount)
    )

    if property_id is not None:
        section = "financials"
        scope_kwargs: dict[str, UUID] = {"property_id": property_id}
    elif building_id is not None:
        section = "building_financials"
        scope_kwargs = {"building_id": building_id}
    elif liegenschaft_id is not None:
        section = "liegenschaft_financials"
        scope_kwargs = {"liegenschaft_id": liegenschaft_id}
    else:
        return 0

    written = 0
    if kategorie == "miete" and typ == "CREDIT" and property_id is not None:
        written += int(
            await _write_or_supersede_fact(
                session,
                **scope_kwargs,
                section=section,
                field="last_rent_payment",
                value=f"{formatted} (kategorie=miete, CREDIT, source=bank)",
                confidence=0.99,
                source_event_id=event_id,
            )
        )
    elif kategorie == "kaution" and typ == "CREDIT" and property_id is not None:
        written += int(
            await _write_or_supersede_fact(
                session,
                **scope_kwargs,
                section=section,
                field="last_deposit",
                value=f"{formatted} (kategorie=kaution, CREDIT, source=bank)",
                confidence=0.97,
                source_event_id=event_id,
            )
        )
    elif kategorie in {"hausgeld", "dienstleister", "versorger", "sonstige"}:
        # Building / Liegenschaft-level expense + payments. Use a
        # single rolling field per kategorie so re-runs supersede.
        verb = "received" if typ == "CREDIT" else "paid"
        written += int(
            await _write_or_supersede_fact(
                session,
                **scope_kwargs,
                section=section,
                field=f"last_{kategorie}_{verb}",
                value=(
                    f"{formatted} ({verb}, kategorie={kategorie}, "
                    f"source=bank)"
                ),
                confidence=0.95,
                source_event_id=event_id,
            )
        )
    return written


# -----------------------------------------------------------------------------
# Invoices
# -----------------------------------------------------------------------------


async def _resolve_contractor_id(
    session: AsyncSession, dl_id: str | None
) -> UUID | None:
    """Look up a Buena DL-XXX in the contractors table via ``contact->>'buena_dl_id'``."""
    if not dl_id:
        return None
    row = (
        await session.execute(
            text(
                """
                SELECT id FROM contractors
                WHERE contact->>'buena_dl_id' = :dl
                LIMIT 1
                """
            ),
            {"dl": dl_id},
        )
    ).first()
    return UUID(str(row.id)) if row else None


async def _ensure_serviced_by(
    session: AsyncSession,
    *,
    property_id: UUID,
    contractor_id: UUID,
) -> None:
    """Create a ``property→contractor (serviced_by)`` edge if absent."""
    existing = (
        await session.execute(
            text(
                """
                SELECT 1 FROM relationships
                WHERE from_type = 'property' AND from_id = :pid
                  AND to_type   = 'contractor' AND to_id   = :cid
                  AND relationship_type = 'serviced_by'
                """
            ),
            {"pid": property_id, "cid": contractor_id},
        )
    ).first()
    if existing is not None:
        return
    await session.execute(
        text(
            """
            INSERT INTO relationships
                (from_type, from_id, to_type, to_id, relationship_type)
            VALUES ('property', :pid, 'contractor', :cid, 'serviced_by')
            """
        ),
        {"pid": property_id, "cid": contractor_id},
    )


async def extract_invoice_facts(
    session: AsyncSession,
    *,
    event_id: UUID,
    property_id: UUID | None = None,
    building_id: UUID | None = None,
    liegenschaft_id: UUID | None = None,
    metadata: dict[str, Any],
) -> int:
    """Write deterministic facts for one invoice event.

    Phase 8.1: an invoice routes at one of three tiers; the section name
    follows the scope.
    """
    dl_id = metadata.get("dl_id") or _dl_id_from_filename(metadata.get("filename"))
    contractor_id = await _resolve_contractor_id(session, dl_id)

    if contractor_id is not None and property_id is not None:
        await _ensure_serviced_by(
            session, property_id=property_id, contractor_id=contractor_id
        )

    if property_id is not None:
        section = "maintenance"
        scope_kwargs: dict[str, UUID] = {"property_id": property_id}
    elif building_id is not None:
        section = "building_maintenance"
        scope_kwargs = {"building_id": building_id}
    elif liegenschaft_id is not None:
        section = "liegenschaft_maintenance"
        scope_kwargs = {"liegenschaft_id": liegenschaft_id}
    else:
        return 0

    filename = metadata.get("filename") or "unknown invoice"
    doctype = metadata.get("document_type") or "invoice"
    written = int(
        await _write_or_supersede_fact(
            session,
            **scope_kwargs,
            section=section,
            field="last_contractor_invoice",
            value=(
                f"{doctype} from contractor "
                f"{dl_id or '(unknown)'} — {filename}"
            ),
            confidence=0.9,
            source_event_id=event_id,
        )
    )
    return written


def _dl_id_from_filename(filename: str | None) -> str | None:
    """Pull a ``DL-NNN`` token out of a Buena invoice filename."""
    if not filename:
        return None
    import re  # noqa: PLC0415 — local import keeps regex hot path cheap
    match = re.search(r"\b(DL-\d{3,})\b", filename)
    return match.group(1) if match else None


async def stamp_processed(
    session: AsyncSession,
    event_id: UUID,
    *,
    property_id: UUID | None = None,
    building_id: UUID | None = None,
    liegenschaft_id: UUID | None = None,
    received_at: datetime | None = None,
) -> None:
    """Mark the structured event as processed_at + write the resolved scope IDs.

    Uses ``COALESCE`` so an event that was already routed (perhaps to a
    different tier on a prior run) keeps its earlier attribution unless
    the new value is a strict refinement. The re-route migration in
    :func:`re_route_unrouted_events` explicitly clears + rewrites when
    re-evaluation moves an event to a new scope.
    """
    await session.execute(
        text(
            """
            UPDATE events
            SET processed_at    = COALESCE(:ts, now()),
                property_id     = COALESCE(:pid, property_id),
                building_id     = COALESCE(:bid, building_id),
                liegenschaft_id = COALESCE(:lid, liegenschaft_id)
            WHERE id = :id
            """
        ),
        {
            "id": event_id,
            "pid": property_id,
            "bid": building_id,
            "lid": liegenschaft_id,
            "ts": received_at,
        },
    )
