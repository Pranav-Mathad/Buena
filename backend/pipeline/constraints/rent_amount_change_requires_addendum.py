"""Rent amount changes require a lease-addendum PDF.

Step 5's eval surfaced the failure mode directly: an owner-side email
saying "we might need to look at the rent at some point" produced a
spurious ``financials.rent_amount`` proposal. Even high-confidence
free-text events shouldn't move the recorded rent — the only legal
instrument that changes a tenancy's rent is an addendum to the
Mietvertrag.
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


class RentAmountChangeRequiresAddendum:
    """Free-text rent changes are rejected; addendum PDFs needs_review."""

    name = "rent_amount_change_requires_addendum"
    section = "financials"
    field = "rent_amount"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        if current is None:
            # First-time set — likely from stammdaten or initial lease.
            return ValidationResult.passed("no prior fact, seeding")

        if event_source(event) == "pdf" and event_document_type(event) == "lease_addendum":
            return ValidationResult.needs_review(
                "rent change with lease_addendum PDF; needs human "
                "confirmation before applying",
                required_source_type="lease_addendum",
            )

        return ValidationResult.rejected(
            "rent amount can only be changed by a lease_addendum PDF; "
            "free-text events (email, slack) cannot move the recorded "
            "rent",
            required_source_type="lease_addendum",
        )


register(RentAmountChangeRequiresAddendum())
