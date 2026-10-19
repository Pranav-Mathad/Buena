"""Building floor count is immutable post-stammdaten (PM's stated example).

Once the stammdaten loader sets a building's floor count, that value
is a physical property of the building: emails, Slack messages, and
free-text events cannot change it. The only legitimate source for a
revision is a structural permit PDF (``document_type=structural_permit``)
that documents an actual conversion.
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


class BuildingFloorCountImmutable:
    """Reject any non-structural-permit attempt to mutate floor count.

    Strict mode: when no prior fact exists, fall back to
    ``buildings.metadata.etagen`` (loaded as ``stammdaten.building.
    floor_count`` by the worker). A claim that contradicts the
    stammdaten value is rejected even on the very first write; the
    physical building doesn't grow new floors because a tenant emails
    about ``7. Stockwerk``.
    """

    name = "building_floor_count_immutable"
    section = "building_overview"
    field = "floor_count"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        stammdaten_value = event_stammdaten(event, "building").get("floor_count")
        is_permit_pdf = (
            event_source(event) == "pdf" and event_document_type(event) == "structural_permit"
        )

        if current is None:
            if stammdaten_value is not None and values_differ(proposed.value, stammdaten_value):
                if is_permit_pdf:
                    return ValidationResult.needs_review(
                        "building floor count change vs stammdaten "
                        f"(stammdaten={stammdaten_value}, "
                        f"proposed={proposed.value}) with structural_permit; "
                        "needs human confirmation",
                        required_source_type="structural_permit",
                    )
                return ValidationResult.rejected(
                    "building floor count contradicts stammdaten "
                    f"({stammdaten_value} on file, event claims "
                    f"{proposed.value}): only a structural_permit PDF "
                    "can revise a physical building property",
                    required_source_type="structural_permit",
                )
            return ValidationResult.passed("no prior fact, seeding")

        if is_permit_pdf:
            return ValidationResult.needs_review(
                "building floor count change requested with structural_permit; "
                "needs human confirmation",
                required_source_type="structural_permit",
            )

        return ValidationResult.rejected(
            "building floor count is immutable; a free-text event "
            "cannot change a physical building property; a "
            "structural_permit PDF is required",
            required_source_type="structural_permit",
        )


register(BuildingFloorCountImmutable())
