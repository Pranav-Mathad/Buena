"""Alias filtering for the single-property backfill CLI."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from connectors.base import ConnectorEvent
from scripts.backfill import alias_matcher, specific_aliases

BUILDING = "Musterstraße 26 14, 10115 Berlin"
ALIASES = ["EH-029", "HAUS-14", "WE 29", "Musterstraße 26", "Musterstraße 26 WE 29", "4. OG mitte"]


def _event(body: str) -> ConnectorEvent:
    return ConnectorEvent(
        source="email",
        source_ref="ref",
        raw_content=body,
        metadata={},
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_building_wide_aliases_are_dropped() -> None:
    keepers = specific_aliases(ALIASES, BUILDING)

    assert keepers == ["EH-029", "WE 29", "Musterstraße 26 WE 29", "4. OG mitte"]


def test_matcher_ignores_mail_for_other_units_in_the_building() -> None:
    keep = alias_matcher(specific_aliases(ALIASES, BUILDING))

    assert keep(_event("Wasserschaden in WE 29, bitte prüfen"))
    assert not keep(_event("Rundschreiben an alle Mieter der Musterstraße 26 (HAUS-14)"))


def test_matcher_refuses_to_run_without_unit_aliases() -> None:
    with pytest.raises(SystemExit):
        alias_matcher(specific_aliases(["HAUS-14", "Musterstraße 26"], BUILDING))
