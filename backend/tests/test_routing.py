"""Tests for the structured router.

The structured router resolves an event to a property, a building or a
Liegenschaft (WEG) from explicit references: EH / MIE unit ids, HAUS
ids, the bank ``kategorie`` and German WEG keywords.

These exercise the live routing precedence against a fresh DB seeded
with the Buena Stammdaten loader. Skipped when the dev Postgres or
the ``Extracted/`` folder is unavailable so the suite stays portable.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.pipeline.router import (
    WEG_KEYWORDS,
    StructuredRoute,
    route_structured,
)
from connectors import buena_loader
from connectors.base import DataMissingError
from connectors.migrations import apply_all as ensure_migrations

pytestmark = pytest.mark.asyncio


def _ensure_buena_stammdaten() -> None:
    """Idempotent loader call; costs nothing if data is already loaded."""
    try:
        buena_loader.load_from_disk()
    except DataMissingError as exc:
        pytest.skip(f"Buena dataset not available: {exc}")


async def _setup_or_skip() -> None:
    ensure_migrations()
    _ensure_buena_stammdaten()


@pytest.mark.usefixtures("db")
async def test_unit_event_routes_to_property() -> None:
    """``EH-NNN`` in metadata routes to the matching property."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        # Pick any property and use its EH alias
        row = (
            await session.execute(
                text("SELECT id, aliases FROM properties WHERE 'EH-005' = ANY(aliases) LIMIT 1")
            )
        ).first()
        if row is None:
            pytest.skip("EH-005 not present in this dataset")
        result = await route_structured(
            session,
            metadata={
                "eh_id": "EH-005",
                "kategorie": "miete",
                "verwendungszweck": "Miete 01/2024 EH-005",
            },
        )
    assert isinstance(result, StructuredRoute)
    assert result.method == "eh_alias"
    assert result.property_id is not None
    assert result.building_id is None
    assert result.liegenschaft_id is None


@pytest.mark.usefixtures("db")
async def test_haus_only_event_routes_to_building() -> None:
    """``HAUS-NN`` in raw_text with no unit ref routes to the building."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
            await session.execute(
                text("SELECT id FROM buildings WHERE metadata->>'buena_haus_id' = 'HAUS-12'")
            )
        ).first()
        if row is None:
            pytest.skip("HAUS-12 not present in this dataset")
        result = await route_structured(
            session,
            metadata={
                "verwendungszweck": "Dach-Reparatur HAUS-12",
                "kategorie": "dienstleister_haus",  # not in WEG_KATEGORIE
            },
        )
    assert result.method == "haus_alias"
    assert result.building_id is not None
    assert result.property_id is None
    assert result.liegenschaft_id is None


@pytest.mark.usefixtures("db")
async def test_weg_kategorie_routes_to_liegenschaft() -> None:
    """``kategorie=dienstleister`` with no EH/MIE/HAUS → Liegenschaft."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        result = await route_structured(
            session,
            metadata={
                "kategorie": "dienstleister",
                "verwendungszweck": "Rechnung R20240184 Sanitaer Demo GmbH",
            },
        )
    assert result.method == "weg_kategorie"
    assert result.liegenschaft_id is not None
    assert result.property_id is None
    assert result.building_id is None


@pytest.mark.usefixtures("db")
async def test_weg_keyword_case_insensitive_word_boundary() -> None:
    """German WEG keyword matching is case-insensitive + word-boundary."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        # Real-data variants: HAUSGELD vs hausgeld vs Hausgeld
        for body in [
            "HAUSGELD 01/2024 (kein EH)",
            "hausgeld 01/2024 (kein EH)",
            "Hausgeld 01/2024 (kein EH)",
        ]:
            result = await route_structured(
                session,
                metadata={"verwendungszweck": body},
            )
            assert result.is_routed, f"failed on body={body!r} reason={result.reason}"
            # Either weg_keyword (no kategorie) or weg_kategorie if kategorie inferred
            assert result.method in {"weg_keyword", "weg_kategorie"}


@pytest.mark.usefixtures("db")
async def test_unknown_event_is_unrouted() -> None:
    """No EH/MIE/HAUS/INV/kategorie/keyword → unrouted with reason."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        result = await route_structured(
            session,
            metadata={"verwendungszweck": "Random text without recognisable refs"},
        )
    assert not result.is_routed
    assert result.method == "unrouted"
    assert result.reason  # non-empty diagnostic


async def test_weg_keyword_set_includes_german_lexicon() -> None:
    """Sanity check on the keyword list: full Hausverwaltung vocabulary."""
    expected = {
        "hausgeld",
        "verwaltergebühr",
        "gemeinschaftskosten",
        "hausverwaltung",
        "weg",
        "sonderumlage",
    }
    assert expected.issubset(set(WEG_KEYWORDS))
