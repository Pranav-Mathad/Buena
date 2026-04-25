"""Phase 9 Step 9.2 — constraint validator.

Sits between :mod:`backend.pipeline.differ` and :mod:`backend.pipeline.applier`.
The differ already filtered proposals on source-precedence /
confidence / recency; the validator adds *semantic* gates that are
hard to express as numbers — "the building's floor count is
immutable", "a rent change requires a lease addendum PDF", "a
compliance fact must come from an authoritative source".

Design contract:

- The validator takes a :class:`~backend.pipeline.differ.DiffPlan` and
  returns ``(filtered_plan, rejections)``. Every decision either
  survives untouched or moves to ``rejections`` with a constraint
  name + reason + (optional) ``required_source_type``.
- A ``Constraint`` is a small object with ``check(proposed, current,
  event) → ValidationResult``. It does *not* hit the DB; the caller
  passes the relevant current fact in. Constraints are pure functions
  over their inputs so they're trivial to unit-test.
- The :data:`REGISTRY` is module-level and populated at import time by
  the constraint files under :mod:`backend.pipeline.constraints`. New
  constraints register themselves via :func:`register`.
- Rejections persist into the ``rejected_updates`` table via
  :func:`persist_rejections` so the admin ``/rejected`` inbox can
  surface them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.pipeline.differ import DiffPlan, FactDecision

log = structlog.get_logger(__name__)


ValidationStatus = Literal["passed", "rejected", "needs_review"]


@dataclass(frozen=True)
class ValidationResult:
    """One constraint's verdict on one proposed :class:`FactDecision`."""

    status: ValidationStatus
    reason: str
    required_source_type: str | None = None

    @classmethod
    def passed(cls, reason: str = "ok") -> ValidationResult:
        return cls(status="passed", reason=reason)

    @classmethod
    def rejected(cls, reason: str, required_source_type: str | None = None) -> ValidationResult:
        return cls(
            status="rejected",
            reason=reason,
            required_source_type=required_source_type,
        )

    @classmethod
    def needs_review(
        cls, reason: str, required_source_type: str | None = None
    ) -> ValidationResult:
        return cls(
            status="needs_review",
            reason=reason,
            required_source_type=required_source_type,
        )


class Constraint(Protocol):
    """Marker protocol for a validator constraint.

    ``section`` and ``field`` describe the fact slot the constraint
    guards. ``field=None`` means *every field in this section*.
    """

    name: str
    section: str
    field: str | None

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        """Return the constraint's verdict on ``proposed``.

        Args:
            proposed: The candidate :class:`FactDecision` from the differ.
            current: The current fact at this ``(section, field)``, or
                ``None`` if no fact exists yet. Shape mirrors
                :func:`backend.pipeline.differ._current_facts`.
            event: A dict with ``source``, ``metadata`` (which carries
                ``document_type`` for PDF events), and any other
                event-level context constraints want to read.
        """
        ...


@dataclass(frozen=True)
class Rejection:
    """One rejected (or needs-review) proposal, ready to persist."""

    section: str
    field: str
    proposed_value: str
    proposed_confidence: float
    constraint_name: str
    reason: str
    required_source_type: str | None
    needs_review: bool

    @property
    def reviewed_status(self) -> str:
        return "needs_review" if self.needs_review else "pending"


# ----------------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------------


REGISTRY: dict[tuple[str, str], list[Constraint]] = {}


def register(constraint: Constraint) -> Constraint:
    """Register a constraint at module import.

    Returns the constraint so this can be used as a decorator on a
    factory function or instance assignment.
    """
    key = (constraint.section, constraint.field or "*")
    REGISTRY.setdefault(key, []).append(constraint)
    log.debug(
        "validator.register",
        name=constraint.name,
        section=constraint.section,
        field=constraint.field or "*",
    )
    return constraint


def constraints_for(section: str, field: str) -> list[Constraint]:
    """Return every constraint that applies to ``(section, field)``.

    Wildcards (``field=None`` registrations under ``"*"``) come *after*
    field-specific ones so the operator-facing rejection cites the
    most specific rule when both fire.
    """
    specific = REGISTRY.get((section, field), [])
    wildcards = REGISTRY.get((section, "*"), [])
    return specific + wildcards


def clear_registry() -> None:
    """Test helper — empty the registry between tests."""
    REGISTRY.clear()


# ----------------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------------


def validate(
    plan: DiffPlan,
    *,
    event: dict[str, Any],
    current_facts: dict[tuple[str, str], dict[str, Any]],
) -> tuple[DiffPlan, list[Rejection]]:
    """Apply every registered constraint to every decision in ``plan``.

    Args:
        plan: The :class:`DiffPlan` from the differ.
        event: Event-level context. Must include ``source`` (str) and
            ``metadata`` (dict) — constraints read
            ``metadata.document_type``.
        current_facts: Map of ``(section, field) → current fact dict``,
            shaped like the output of
            :func:`backend.pipeline.differ._current_facts`.

    Returns:
        ``(filtered_plan, rejections)``.
        ``filtered_plan.decisions`` only contains decisions every
        constraint passed on. ``rejections`` carries one
        :class:`Rejection` per ``rejected | needs_review`` verdict.
    """
    kept: list[FactDecision] = []
    rejections: list[Rejection] = []
    for decision in plan.decisions:
        verdict_constraint: Constraint | None = None
        verdict: ValidationResult = ValidationResult.passed()
        for constraint in constraints_for(decision.section, decision.field):
            v = constraint.check(
                decision,
                current_facts.get((decision.section, decision.field)),
                event,
            )
            if v.status != "passed":
                verdict_constraint = constraint
                verdict = v
                break
        if verdict.status == "passed":
            kept.append(decision)
            continue
        rejections.append(
            Rejection(
                section=decision.section,
                field=decision.field,
                proposed_value=decision.value,
                proposed_confidence=decision.confidence,
                constraint_name=(
                    verdict_constraint.name if verdict_constraint else "unknown"
                ),
                reason=verdict.reason,
                required_source_type=verdict.required_source_type,
                needs_review=verdict.status == "needs_review",
            )
        )
    log.info(
        "validator.plan",
        proposed=len(plan.decisions),
        passed=len(kept),
        rejected=sum(1 for r in rejections if not r.needs_review),
        needs_review=sum(1 for r in rejections if r.needs_review),
    )
    return DiffPlan(decisions=kept, skipped=plan.skipped), rejections


# ----------------------------------------------------------------------------
# Persistence
# ----------------------------------------------------------------------------


async def persist_rejections(
    session: AsyncSession,
    *,
    event_id: UUID,
    property_id: UUID | None,
    building_id: UUID | None,
    liegenschaft_id: UUID | None,
    rejections: list[Rejection],
) -> None:
    """Insert one :class:`Rejection` per row into ``rejected_updates``.

    Caller commits the surrounding transaction. The rows go in with
    ``reviewed_status`` set to ``pending`` (or ``needs_review`` when
    the constraint emitted a soft-reject).
    """
    if not rejections:
        return
    for r in rejections:
        await session.execute(
            text(
                """
                INSERT INTO rejected_updates (
                  event_id, property_id, building_id, liegenschaft_id,
                  proposed_section, proposed_field, proposed_value,
                  proposed_confidence, constraint_name, reason,
                  required_source_type, reviewed_status
                ) VALUES (
                  :eid, :pid, :bid, :lid,
                  :section, :field, :value,
                  :conf, :cname, :reason,
                  :rst, :status
                )
                """
            ),
            {
                "eid": event_id,
                "pid": property_id,
                "bid": building_id,
                "lid": liegenschaft_id,
                "section": r.section,
                "field": r.field,
                "value": r.proposed_value,
                "conf": r.proposed_confidence,
                "cname": r.constraint_name,
                "reason": r.reason,
                "rst": r.required_source_type,
                "status": r.reviewed_status,
            },
        )
    log.info(
        "validator.persist",
        event_id=str(event_id),
        rejected=sum(1 for r in rejections if not r.needs_review),
        needs_review=sum(1 for r in rejections if r.needs_review),
    )


# ----------------------------------------------------------------------------
# Helpers used across constraint files
# ----------------------------------------------------------------------------


def event_document_type(event: dict[str, Any]) -> str | None:
    """Pull ``metadata.document_type`` from an event dict, normalised.

    PDF connectors populate this enum (``lease | lease_addendum |
    kaufvertrag | structural_permit | vermessungsprotokoll | invoice |
    mahnung | other``). Constraints that gate on a specific subtype
    use this helper rather than reaching into the dict directly so the
    enum lookup stays in one place.
    """
    metadata = event.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            return None
    value = metadata.get("document_type") if isinstance(metadata, dict) else None
    if not value:
        return None
    return str(value).lower()


def event_source(event: dict[str, Any]) -> str:
    """Return ``event.source`` as a lowercase string (``email`` / ``pdf`` / …)."""
    return str(event.get("source") or "").lower()


__all__ = [
    "Constraint",
    "REGISTRY",
    "Rejection",
    "ValidationResult",
    "ValidationStatus",
    "clear_registry",
    "constraints_for",
    "event_document_type",
    "event_source",
    "persist_rejections",
    "register",
    "validate",
]
