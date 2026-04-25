"""Owner changes require a Kaufvertrag PDF.

A property changing owners is a Grundbuch-level event. The only
legal instrument is a notarised Kaufvertrag. Free-text claims of
ownership ("I bought the unit last week") and even invoices /
bank rows are insufficient evidence to mutate the owner of record.
"""

from __future__ import annotations

from typing import Any

from backend.pipeline.differ import FactDecision
from backend.pipeline.validator import (
    ValidationResult,
    event_document_type,
    event_source,
    register,
)


class OwnerChangeRequiresKaufvertrag:
    """Reject every owner change without a Kaufvertrag PDF."""

    name = "owner_change_requires_kaufvertrag"
    section = "overview"
    field = "owner_name"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        if current is None:
            return ValidationResult.passed("no prior owner, seeding")

        if event_source(event) == "pdf" and event_document_type(event) == "kaufvertrag":
            return ValidationResult.needs_review(
                "owner change with Kaufvertrag PDF; human confirmation "
                "required before applying — Grundbuch should be "
                "cross-checked",
                required_source_type="kaufvertrag",
            )

        return ValidationResult.rejected(
            "owner change requires a Kaufvertrag (document_type='kaufvertrag') "
            "PDF; free-text events, invoices, and bank rows cannot mutate "
            "the recorded owner",
            required_source_type="kaufvertrag",
        )


register(OwnerChangeRequiresKaufvertrag())
