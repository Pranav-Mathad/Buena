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
    event_stammdaten,
    register,
    values_differ,
)

_AUTHORITATIVE_DOC_TYPES = {
    "structural_permit",
    "vermessungsprotokoll",
    "kaufvertrag",
}
_REQUIRED_DOC_LIST = "structural_permit | vermessungsprotokoll | kaufvertrag"


class BuildingAddressImmutable:
    """Reject free-text mutations of a building's street address.

    Strict mode: when no prior fact exists, fall back to
    ``buildings.address`` (loaded as ``stammdaten.building.address``).
    A claim that contradicts the stammdaten value is rejected even on
    the first write — buildings don't relocate because someone typed a
    different street into an email.
    """

    name = "building_address_immutable"
    section = "building_overview"
    field = "address"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        stammdaten_value = event_stammdaten(event, "building").get("address")
        is_authoritative_pdf = (
            event_source(event) == "pdf" and event_document_type(event) in _AUTHORITATIVE_DOC_TYPES
        )

        if current is None:
            if stammdaten_value and values_differ(proposed.value, stammdaten_value):
                if is_authoritative_pdf:
                    return ValidationResult.needs_review(
                        "building address contradicts stammdaten "
                        f"(stammdaten={stammdaten_value!r}, "
                        f"proposed={proposed.value!r}) with authoritative "
                        "document; needs human confirmation",
                        required_source_type=_REQUIRED_DOC_LIST,
                    )
                return ValidationResult.rejected(
                    "building address contradicts stammdaten "
                    f"({stammdaten_value!r} on file, event claims "
                    f"{proposed.value!r}) — only an authoritative document "
                    f"({_REQUIRED_DOC_LIST}) can revise a building's address",
                    required_source_type=_REQUIRED_DOC_LIST,
                )
            return ValidationResult.passed("no prior fact, seeding")

        if is_authoritative_pdf:
            return ValidationResult.needs_review(
                "building address change with authoritative document; "
                "human confirmation recommended",
                required_source_type=_REQUIRED_DOC_LIST,
            )

        return ValidationResult.rejected(
            "building address is immutable from free-text events — "
            "an authoritative document (structural_permit, "
            "vermessungsprotokoll, or kaufvertrag) is required",
            required_source_type=_REQUIRED_DOC_LIST,
        )


register(BuildingAddressImmutable())
