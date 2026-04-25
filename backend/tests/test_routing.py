"""Tests for the structured router (Phase 8.1).

These exercise the live routing precedence against a fresh DB seeded
with the Buena Stammdaten loader. Skipped when the dev Postgres or
the ``Extracted/`` folder is unavailable so the suite stays portable.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline.router import (
    StructuredRoute,
    WEG_KEYWORDS,
    route_structured,
)
from connectors import buena_archive, buena_loader
from connectors.base import DataMissing
from connectors.migrations import apply_all as ensure_migrations

pytestmark = pytest.mark.asyncio


def _reset_session_cache() -> None:
    """Drop the cached async engine + sessionmaker so the test gets a fresh loop.

    pytest-asyncio runs every coroutine test under its own event loop. The
    engine cached in ``backend.db.session.get_engine`` binds to the first
    loop it sees; later tests reach a dead loop unless we reset.
    """
    from backend.db import session as session_module  # noqa: PLC0415

    session_module.get_engine.cache_clear()
    session_module.get_sessionmaker.cache_clear()


async def _db_reachable() -> bool:
    _reset_session_cache()
    try:
        factory = get_sessionmaker()
        async with factory() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


def _ensure_buena_stammdaten() -> None:
    """Idempotent loader call — costs nothing if data is already loaded."""
    try:
        buena_loader.load_from_disk()
    except DataMissing as exc:
        pytest.skip(f"Buena dataset not available — {exc}")


async def _setup_or_skip() -> None:
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")
    ensure_migrations()
    _ensure_buena_stammdaten()


async def test_unit_event_routes_to_property() -> None:
    """``EH-NNN`` in metadata routes to the matching property."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        # Pick any property and use its EH alias
        row = (
            await session.execute(
                text(
                    "SELECT id, aliases FROM properties "
                    "WHERE 'EH-005' = ANY(aliases) LIMIT 1"
                )
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


async def test_haus_only_event_routes_to_building() -> None:
    """``HAUS-NN`` in raw_text with no unit ref routes to the building."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT id FROM buildings "
                    "WHERE metadata->>'buena_haus_id' = 'HAUS-12'"
                )
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


async def test_weg_kategorie_routes_to_liegenschaft() -> None:
    """``kategorie=dienstleister`` with no EH/MIE/HAUS → Liegenschaft."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        result = await route_structured(
            session,
            metadata={
                "kategorie": "dienstleister",
                "verwendungszweck": "Rechnung R20240184 Sanitaer Schulze GmbH",
            },
        )
    assert result.method == "weg_kategorie"
    assert result.liegenschaft_id is not None
    assert result.property_id is None
    assert result.building_id is None


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
    """Sanity check on the keyword list — full Hausverwaltung vocabulary."""
    expected = {
        "hausgeld",
        "verwaltergebühr",
        "gemeinschaftskosten",
        "hausverwaltung",
        "weg",
        "sonderumlage",
    }
    assert expected.issubset(set(WEG_KEYWORDS))
