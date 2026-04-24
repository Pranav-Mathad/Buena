"""Evaluate every signal rule and persist candidates.

The evaluator is the single scheduled entry point for the signal system.
It calls each rule's ``evaluate(session)``, dedupes the results against
already-open pending signals for the same ``(property_id, type)``, then:

1. Asks :mod:`backend.signals.drafter` to author a :class:`ProposedAction`.
2. Inserts a row into ``signals`` with ``status='pending'``.

The scheduler runs this every 30s; Phase 3 API endpoints expose a manual
``POST /signals/evaluate`` trigger for demo determinism.
"""

from __future__ import annotations

import json
from typing import Protocol
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_sessionmaker
from backend.signals import drafter
from backend.signals.rules import (
    cross_property_pattern,
    lease_expiring,
    recurring_maintenance,
)
from backend.signals.types import SignalCandidate

log = structlog.get_logger(__name__)


class _Rule(Protocol):
    """Structural type every rule module satisfies."""

    async def evaluate(self, session: AsyncSession) -> list[SignalCandidate]: ...


_RULES = [recurring_maintenance, lease_expiring, cross_property_pattern]


async def _already_open(
    session: AsyncSession,
    property_id: UUID | None,
    signal_type: str,
    action_hint: dict,
) -> bool:
    """Return True if a pending signal with the same natural key already exists.

    Natural key is ``(property_id, type, action_hint.subtype)`` for cross-
    property signals (so ``shared_boiler`` and ``year_cohort`` are distinct)
    and ``(property_id, type)`` for per-property rules.
    """
    subtype = action_hint.get("subtype") or action_hint.get("topic")
    params: dict[str, object] = {"type": signal_type}
    clauses = ["type = :type", "status = 'pending'"]
    if property_id is None:
        clauses.append("property_id IS NULL")
    else:
        clauses.append("property_id = :pid")
        params["pid"] = property_id
    if subtype:
        # proposed_action is {...top, payload: {hint: {topic|subtype: ...}}}.
        clauses.append(
            "(proposed_action->'payload'->'hint'->>'subtype' = :sub "
            "OR proposed_action->'payload'->'hint'->>'topic'  = :sub)"
        )
        params["sub"] = subtype
    query = f"SELECT 1 FROM signals WHERE {' AND '.join(clauses)} LIMIT 1"
    row = (await session.execute(text(query), params)).first()
    return row is not None


async def _persist(
    session: AsyncSession,
    candidate: SignalCandidate,
) -> UUID:
    """Draft + insert a candidate. Returns the new ``signals.id``."""
    action = await drafter.draft(session, candidate)
    result = await session.execute(
        text(
            """
            INSERT INTO signals
                (property_id, type, severity, message, evidence,
                 proposed_action, status, created_at)
            VALUES
                (:pid, :type, :sev, :msg, CAST(:evd AS JSONB),
                 CAST(:act AS JSONB), 'pending', now())
            RETURNING id
            """
        ),
        {
            "pid": candidate.property_id,
            "type": candidate.type,
            "sev": candidate.severity,
            "msg": candidate.message,
            "evd": json.dumps(candidate.evidence),
            "act": json.dumps(action.as_json()),
        },
    )
    signal_id: UUID = result.scalar_one()
    log.info(
        "signal.created",
        signal_id=str(signal_id),
        type=candidate.type,
        severity=candidate.severity,
        property_id=str(candidate.property_id) if candidate.property_id else None,
    )
    return signal_id


async def evaluate_all(session: AsyncSession | None = None) -> int:
    """Run every rule and persist new candidates. Returns how many fired.

    ``session`` is optional — if omitted (the usual scheduler path) a fresh
    session is opened from the global sessionmaker.
    """
    if session is not None:
        return await _run(session)
    factory = get_sessionmaker()
    async with factory() as fresh:
        count = await _run(fresh)
        await fresh.commit()
    return count


async def _run(session: AsyncSession) -> int:
    """Drive the rule set against ``session`` and persist new signals."""
    created = 0
    for module in _RULES:
        candidates = await module.evaluate(session)
        for candidate in candidates:
            if await _already_open(
                session, candidate.property_id, candidate.type, candidate.action_hint
            ):
                continue
            await _persist(session, candidate)
            created += 1
    if created:
        log.info("signal.batch", created=created)
    return created
