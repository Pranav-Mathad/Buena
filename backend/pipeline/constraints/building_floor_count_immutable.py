"""Building floor count is immutable post-stammdaten (PM's stated example).

Once the stammdaten loader sets a building's floor count, that value
is a physical property of the building — emails, Slack messages, and
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
    register,
)


class BuildingFloorCountImmutable:
    """Reject any non-structural-permit attempt to mutate floor count."""

    name = "building_floor_count_immutable"
    section = "building_overview"
    field = "floor_count"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        if current is None:
            # No prior fact — likely the very first stammdaten load.
            # Allow regardless of source so the seed path works.
            return ValidationResult.passed("no prior fact, seeding")

        # Identical writes are already short-circuited by the differ;
        # if we got here the value is changing.
        if event_source(event) == "pdf" and event_document_type(event) == "structural_permit":
            # Genuine structural change with a permit attached. Still
            # surface for human verification — floor changes are rare
            # enough that "needs_review" is the right default even
            # with the permit.
            return ValidationResult.needs_review(
                "building floor count change requested with structural_permit; "
                "needs human confirmation",
                required_source_type="structural_permit",
            )

        return ValidationResult.rejected(
            "building floor count is immutable; a free-text event "
            "cannot change a physical building property — a "
            "structural_permit PDF is required",
            required_source_type="structural_permit",
        )


register(BuildingFloorCountImmutable())
