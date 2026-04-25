"""Property square meters can move ±5 % from stammdaten before review.

A unit's ``wohnflaeche_qm`` lives in stammdaten and is occasionally
re-measured. Routine corrections of < 5 % (e.g. "we re-measured the
balcony") pass; a swing > 5 % is suspicious — either a typo, a
miscount of a different unit, or a real conversion that needs a
``vermessungsprotokoll`` to substantiate.
"""

from __future__ import annotations

import re
from typing import Any

from backend.pipeline.differ import FactDecision
from backend.pipeline.validator import (
    ValidationResult,
    event_document_type,
    event_source,
    register,
)

_NUM_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


def _parse_qm(raw: str | None) -> float | None:
    """Pull the first numeric value out of a fact's value string.

    Stammdaten stores ``wohnflaeche_qm`` as a string like
    ``"68,5 m²"``; the extractor sometimes emits ``"about 70 sqm"``.
    Either way, the first number is the canonical reading.
    """
    if not raw:
        return None
    match = _NUM_RE.search(raw)
    if match is None:
        return None
    try:
        return float(match.group(0).replace(",", "."))
    except ValueError:
        return None


class PropertySquareMetersTolerance:
    """Allow ±5 %; otherwise needs_review (or pass with a measurement PDF)."""

    name = "property_square_meters_tolerance"
    section = "overview"
    field = "square_meters_qm"
    tolerance_pct = 5.0

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        if current is None:
            return ValidationResult.passed("no prior fact, seeding")

        prior_qm = _parse_qm(str(current.get("value", "")))
        new_qm = _parse_qm(proposed.value)
        if prior_qm is None or new_qm is None or prior_qm == 0:
            # Couldn't parse — let the differ-skipped path handle it.
            return ValidationResult.passed("could not parse qm; deferring")

        delta_pct = abs(new_qm - prior_qm) / prior_qm * 100.0
        if delta_pct <= self.tolerance_pct:
            return ValidationResult.passed(
                f"within ±{self.tolerance_pct:.0f}% tolerance ({delta_pct:.1f}%)"
            )

        # Out of tolerance.
        if event_source(event) == "pdf" and event_document_type(event) == "vermessungsprotokoll":
            return ValidationResult.passed(
                f"{delta_pct:.1f}% change with vermessungsprotokoll attached"
            )

        return ValidationResult.needs_review(
            f"square-meter change is {delta_pct:.1f}% (tolerance "
            f"±{self.tolerance_pct:.0f}%); requires vermessungsprotokoll "
            f"or human confirmation",
            required_source_type="vermessungsprotokoll",
        )


register(PropertySquareMetersTolerance())
