"""Turn a :class:`SignalCandidate` into a :class:`ProposedAction`.

Uses Gemini Pro when ``GEMINI_API_KEY`` is configured — the Signal Quality
Bar in KEYSTONE Part I calls out "expert speaking, not a database emitting
rows" — and falls back to a deterministic template that still hits the
four-part structure: observation, risk, concrete next step, deadline.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.services.gemini import (
    GeminiUnavailable,
    draft_action_message,
    is_available as gemini_available,
)
from backend.signals.types import ProposedAction, SignalCandidate

log = structlog.get_logger(__name__)


async def _owner_context(
    session: AsyncSession, property_id: UUID | None
) -> dict[str, Any]:
    """Look up the owner/recipient details for the drafted action."""
    if property_id is None:
        return {
            "name": "Portfolio manager",
            "email": "portfolio@keystone.demo",
        }
    row = (
        await session.execute(
            text(
                """
                SELECT o.name AS owner_name, o.email AS owner_email
                FROM properties p
                LEFT JOIN owners o ON o.id = p.owner_id
                WHERE p.id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        return {"name": "Owner", "email": "owner@keystone.demo"}
    return {
        "name": row.owner_name or "Owner",
        "email": row.owner_email or "owner@keystone.demo",
    }


def _subject(candidate: SignalCandidate) -> str:
    """Short, specific subject line for the outbox row."""
    if candidate.type == "recurring_maintenance":
        topic = candidate.action_hint.get("topic", "maintenance")
        name = candidate.action_hint.get("property_name", "your property")
        return f"[Action needed] Sustained {topic} issue at {name}"
    if candidate.type == "lease_expiring":
        name = candidate.action_hint.get("property_name", "your property")
        days_left = candidate.action_hint.get("days_left", "soon")
        return f"[Decision] Lease at {name} expires in {days_left} days"
    if candidate.type == "cross_property_pattern":
        sub = candidate.action_hint.get("subtype", "pattern")
        if sub == "shared_boiler":
            return "[Urgent] Shared-boiler failure risk across building"
        if sub == "year_cohort":
            return "[Portfolio] Heating pattern in building cohort — inspection proposal"
        return "[Portfolio] Cross-property pattern detected"
    if candidate.type == "regulation_change":
        headline = candidate.action_hint.get("headline", "regulation update")
        return f"[Regulation] {headline[:80]}"
    return f"[Keystone] {candidate.type} signal"


def _template_fallback(candidate: SignalCandidate, owner_name: str) -> str:
    """Deterministic fallback when Gemini is unavailable."""
    hint = candidate.action_hint
    if candidate.type == "recurring_maintenance":
        return (
            f"{owner_name}, Keystone has logged {hint.get('occurrences', 'multiple')} "
            f"{hint.get('topic', 'maintenance')} incidents at "
            f"{hint.get('property_name', 'the property')} over the last four months — "
            "the cluster points to an underlying system failure rather than one-off "
            "complaints. We recommend dispatching the building's primary contractor "
            "for a full inspection within 48 hours and briefing the tenants in "
            "writing. Please approve this signal to notify the contractor and "
            "acknowledge the owner."
        )
    if candidate.type == "lease_expiring":
        return (
            f"{owner_name}, the lease at {hint.get('property_name', 'the property')} "
            f"expires {hint.get('end_date', 'soon')} — "
            f"{hint.get('days_left', '?')} days out. We recommend proposing a "
            "renewal this week at the current Mietspiegel-bounded rent and asking "
            "the tenant for a decision inside ten business days so we can plan for "
            "relisting if they decline. Approve to send the renewal draft."
        )
    if candidate.type == "cross_property_pattern":
        if hint.get("subtype") == "shared_boiler":
            return (
                f"Two or more units at {hint.get('building_address', 'the building')} "
                f"have logged heating incidents this winter — the pattern points at "
                "the shared central boiler rather than any single unit. Recommend "
                "scheduling a building-wide boiler inspection within 48 hours and "
                "briefing every occupant so tenants aren't blindsided. Approve to "
                "dispatch the contractor and notify Maria Schmidt."
            )
        return (
            f"{hint.get('cohort', 'Multiple')} properties had heating issues this "
            "winter — the correlation suggests shared equipment age rather than "
            "unit-level failures. Recommend a portfolio-wide boiler inspection "
            "within two weeks and budgeting for any units at end-of-life. Approve "
            "to line up contractors across the cohort."
        )
    if candidate.type == "regulation_change":
        headline = hint.get("headline", "a regulatory update")
        return (
            f"Tavily flagged a regulatory change: \"{headline}\". The update is "
            "likely to touch rent adjustments, inspection cadence, or compliance "
            "filings across the portfolio — we should review exposure inside the "
            "next seven days and queue any tenant communications that fall out of "
            "the change. Approve to circulate a short impact brief."
        )
    return (
        f"{owner_name}, Keystone has flagged a {candidate.type} pattern requiring "
        "attention. See evidence below and approve to dispatch the recommended "
        "follow-up."
    )


async def draft(
    session: AsyncSession, candidate: SignalCandidate
) -> ProposedAction:
    """Build the :class:`ProposedAction` for a candidate, Gemini Pro preferred."""
    recipient = await _owner_context(session, candidate.property_id)
    subject = _subject(candidate)
    evidence_summary = (
        candidate.context_excerpt
        or "\n".join(f"- {item}" for item in candidate.evidence[:5])
    )

    drafted_message: str | None = None
    if gemini_available():
        try:
            drafted_message = await draft_action_message(
                signal_type=candidate.type,
                severity=candidate.severity,
                context=candidate.message,
                evidence_summary=evidence_summary,
            )
        except GeminiUnavailable as exc:
            log.warning("drafter.gemini_unavailable", error=str(exc))
            drafted_message = None

    if not drafted_message:
        drafted_message = _template_fallback(candidate, recipient["name"])

    payload: dict[str, Any] = {
        "hint": candidate.action_hint,
        "owner_name": recipient["name"],
        "evidence_size": len(candidate.evidence),
    }

    return ProposedAction(
        type=str(candidate.action_hint.get("type", "owner_notification")),
        channel="email",
        recipient=str(recipient["email"]),
        subject=subject,
        drafted_message=drafted_message,
        payload=payload,
    )
