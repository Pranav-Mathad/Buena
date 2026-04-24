"""Shared types for the signal pipeline.

A rule produces :class:`SignalCandidate` objects; the evaluator dedupes and
persists them. The drafter then attaches a :class:`ProposedAction` payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class SignalCandidate:
    """A rule's proposal before drafting + persistence.

    Attributes:
        type: Rule name — this doubles as the ``signals.type`` column so the
            dedupe key ``(property_id, type, status='pending')`` works.
        property_id: Property the signal applies to. ``None`` for
            portfolio-level signals (cross_property_pattern fires one of these).
        severity: ``low | medium | high | urgent``.
        message: Human-readable one-liner used in the inbox listing.
        evidence: ``[{"event_id": ..., "fact_id": ...}]`` — the provenance the
            UI renders under the signal.
        context_excerpt: A short summary the drafter can use in its prompt.
        action_hint: Rule-provided scaffolding the drafter can expand
            (e.g. ``{"type": "dispatch_contractor", "recipient": "owner"}``).
    """

    type: str
    severity: str
    message: str
    property_id: UUID | None
    evidence: list[dict[str, str]] = field(default_factory=list)
    context_excerpt: str = ""
    action_hint: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProposedAction:
    """The drafted action persisted into ``signals.proposed_action``."""

    type: str
    channel: str
    recipient: str
    subject: str
    drafted_message: str
    payload: dict[str, Any] = field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        """Serialize to the JSONB shape expected in the ``signals`` table."""
        return {
            "type": self.type,
            "channel": self.channel,
            "recipient": self.recipient,
            "subject": self.subject,
            "drafted_message": self.drafted_message,
            "payload": self.payload,
        }
