"""Building street address is immutable post-stammdaten.

A building's address is a stammdaten fact. Free-text events ("we
moved to Hauptstraße 12") almost always refer to *people moving
within the building*, not the building itself relocating. The only
legitimate source for an address change is a Bauamt / property-
authority document.
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


class BuildingAddressImmutable:
    """Reject free-text mutations of a building's street address."""

    name = "building_address_immutable"
    section = "building_overview"
    field = "address"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        if current is None:
            return ValidationResult.passed("no prior fact, seeding")

        if event_source(event) == "pdf" and event_document_type(event) in {
            "structural_permit",
            "vermessungsprotokoll",
            "kaufvertrag",
        }:
            return ValidationResult.needs_review(
                "building address change with authoritative document; "
                "human confirmation recommended",
                required_source_type="structural_permit | vermessungsprotokoll | kaufvertrag",
            )

        return ValidationResult.rejected(
            "building address is immutable from free-text events — "
            "an authoritative document (structural_permit, "
            "vermessungsprotokoll, or kaufvertrag) is required",
            required_source_type="structural_permit | vermessungsprotokoll | kaufvertrag",
        )


register(BuildingAddressImmutable())
