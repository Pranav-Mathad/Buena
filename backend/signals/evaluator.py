"""Evaluate every signal rule and persist candidates.

The evaluator is the single scheduled entry point for the signal system.
It calls each rule's ``evaluate(session)`` and, for every candidate:

1. Computes its natural key, ``type|property_id|subtype`` (see
   :func:`natural_key`), the identity of "the same finding" across ticks.
2. Skips it if a signal with that key is still pending, or was approved or
   rejected within ``signal_resolution_cooldown_days``. Rules are stateless
   and keep matching the same facts, so without the cooldown a decision
   made by a human would be undone on the next tick.
3. Otherwise asks :mod:`backend.signals.drafter` for a
   :class:`ProposedAction` and inserts a ``pending`` row.

The scheduler runs this every 30 seconds; ``POST /signals/evaluate`` runs
it on demand.
"""

from __future__ import annotations

import json
from typing import Any, Protocol
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import get_settings
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


_RULES = [
    recurring_maintenance,
    lease_expiring,
    cross_property_pattern,
]


def natural_key(property_id: UUID | None, signal_type: str, action_hint: dict[str, Any]) -> str:
    """Return the key that identifies one finding across evaluator ticks.

    ``type|<property id or 'portfolio'>|<subtype or topic>``. The subtype
    keeps findings that share a rule apart, e.g. ``shared_boiler`` vs
    ``year_cohort`` for cross-property patterns, or ``heating`` vs ``water``
    for recurring maintenance on one property.
    """
    subtype = action_hint.get("subtype") or action_hint.get("topic") or ""
    scope = str(property_id) if property_id is not None else "portfolio"
    return f"{signal_type}|{scope}|{subtype}"


async def _already_raised(session: AsyncSession, key: str, cooldown_days: int) -> bool:
    """True if ``key`` is pending, or was resolved within the cooldown window."""
    row = (
        await session.execute(
            text(
                """
                SELECT 1 FROM signals
                WHERE natural_key = :key
                  AND (status = 'pending'
                       OR resolved_at > now() - make_interval(days => :days))
                LIMIT 1
                """
            ),
            {"key": key, "days": cooldown_days},
        )
    ).first()
    return row is not None


async def _persist(
    session: AsyncSession,
    candidate: SignalCandidate,
    key: str,
) -> UUID:
    """Draft + insert a candidate. Returns the new ``signals.id``."""
    action = await drafter.draft(session, candidate)
    result = await session.execute(
        text(
            """
            INSERT INTO signals
                (property_id, type, severity, message, evidence,
                 proposed_action, status, created_at, natural_key)
            VALUES
                (:pid, :type, :sev, :msg, CAST(:evd AS JSONB),
                 CAST(:act AS JSONB), 'pending', now(), :key)
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
            "key": key,
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
    cooldown_days = get_settings().signal_resolution_cooldown_days
    created = 0
    for module in _RULES:
        candidates = await module.evaluate(session)
        for candidate in candidates:
            key = natural_key(candidate.property_id, candidate.type, candidate.action_hint)
            if await _already_raised(session, key, cooldown_days):
                continue
            await _persist(session, candidate, key)
            created += 1
    if created:
        log.info("signal.batch", created=created)
    return created
