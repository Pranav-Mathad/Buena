"""Turn a :class:`SignalCandidate` into a :class:`ProposedAction`.

A signal is only useful if its message reads like an expert's recommendation
rather than a database row. The configured LLM provider writes the message
when available; otherwise a deterministic template per signal type does,
following the same four-part structure: observation, risk, concrete next
step, deadline.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.services.llm import LLMUnavailableError, get_provider
from backend.signals.types import ProposedAction, SignalCandidate

log = structlog.get_logger(__name__)

DRAFT_PROMPT_TEMPLATE = """You are drafting an operational message for a property manager.

Tone: crisp, specific, expert. Avoid vague softeners like 'maybe', 'might'.
Open with the observation and the risk. End with a concrete next step and a
deadline. 4-6 sentences, no signature block, no greetings.

Signal type: {signal_type}
Severity: {severity}
Property context:
{context}

Evidence summary:
{evidence_summary}

Write the message body now."""


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
    provider = get_provider()
    if provider is not None:
        prompt = DRAFT_PROMPT_TEMPLATE.format(
            signal_type=candidate.type,
            severity=candidate.severity,
            context=candidate.message or "(no prior context)",
            evidence_summary=evidence_summary or "(no evidence)",
        )
        try:
            drafted_message = await provider.complete_text(system=None, prompt=prompt)
        except LLMUnavailableError as exc:
            log.warning("drafter.llm_unavailable", provider=provider.name, error=str(exc)[:300])

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
