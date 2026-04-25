"""Building year-built is immutable, full stop.

A building only has one year built; this is a Bauamt fact, not a
free-text fact. We reject *any* attempt to change it after the
stammdaten load — even structural permits don't change the year
built. Corrections are an admin action via the override endpoint
with a written audit reason.
"""

from __future__ import annotations

from typing import Any

from backend.pipeline.differ import FactDecision
from backend.pipeline.validator import ValidationResult, register


class BuildingYearBuiltImmutable:
    """Reject every mutation of ``year_built`` once it's set."""

    name = "building_year_built_immutable"
    section = "building_overview"
    field = "year_built"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        if current is None:
            return ValidationResult.passed("no prior fact, seeding")
        return ValidationResult.rejected(
            "building year-built is immutable; corrections require "
            "an explicit admin override with audit reason",
        )


register(BuildingYearBuiltImmutable())
