"""Tests for the Step 6 email backfill — cost cap + dead-letter behaviour.

These tests exercise :mod:`connectors.buena_email_loader` against the
dev Postgres on :5433. They monkeypatch ``backend.pipeline.extractor``
so no Gemini calls are made — the assertions are about loop control,
the failed_events table, and the cost ledger's abort path. Skipped
when Postgres is unreachable so the suite stays portable.

**Test isolation:** every test gets a fresh, unique cost-ledger label
(``step6_test_<uuid>``) via the :func:`_unique_label` fixture so the
test suite can run while a real backfill is using the production
``step6_email_backfill`` row. Without this, the cleanup
``reset_label`` calls would delete the live row mid-run. The
production label is left strictly alone by the test surface.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.services.gemini import ExtractionResult
from connectors import buena_email_loader, cost_ledger as cost_ledger_module
from connectors.base import ConnectorEvent
from connectors.migrations import apply_all as ensure_migrations

pytestmark = pytest.mark.asyncio


def _reset_session_cache() -> None:
    """Match the pattern in backend/tests/test_routing.py."""
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


async def _setup_or_skip() -> None:
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")
    ensure_migrations()


def _connector_event(idx: int) -> ConnectorEvent:
    """Fabricate a routable test event.

    The body mentions ``Hausgeld`` so route_text_event lands it on the
    Liegenschaft if one is loaded; otherwise the event remains
    unrouted, which is fine for cost-cap tests since we override the
    extractor and force a property route via ``inserted-only`` paths
    in the targeted test below.
    """
    return ConnectorEvent(
        source="email",
        source_ref=f"test-cap-{idx:04d}@example.com",
        raw_content=f"From: a@b\nSubject: test {idx}\n\nbody {idx}",
        metadata={"from": "a@b", "subject": f"test {idx}"},
        received_at=datetime(2026, 4, 25, 9, 0, 0, tzinfo=timezone.utc),
    )


async def _ensure_one_property() -> Any:
    """Insert a fixture property so route_text_event can land on it."""
    factory = get_sessionmaker()
    async with factory() as session:
        existing = (
            await session.execute(
                text("SELECT id FROM properties WHERE name = :n"),
                {"n": "Test Cap Apt 1"},
            )
        ).first()
        if existing is not None:
            return existing.id
        # buildings.address NOT NULL; properties.address NOT NULL.
        building = (
            await session.execute(
                text(
                    "INSERT INTO buildings (address) VALUES "
                    "('Test Cap Building Address') RETURNING id"
                )
            )
        ).first()
        prop = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES (
                      'Test Cap Apt 1',
                      'Test Cap Building Address, Apt 1',
                      :bid,
                      ARRAY['Test Cap Apt 1']
                    )
                    RETURNING id
                    """
                ),
                {"bid": building.id},
            )
        ).first()
        await session.commit()
        return prop.id


async def _cleanup_test_events() -> None:
    """Drop any rows the prior runs left behind so tests are deterministic."""
    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text(
                "DELETE FROM events WHERE source_ref LIKE 'test-cap-%'"
            )
        )
        await session.commit()


async def test_cost_cap_aborts_loop_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the ledger hits the cap mid-run, the loop stops cleanly."""
    await _setup_or_skip()
    await _cleanup_test_events()
    # Per-test unique label so a concurrent live backfill can't be
    # corrupted by our reset_label calls. See module docstring.
    test_label = f"step6_test_{uuid4().hex[:8]}"
    monkeypatch.setattr(buena_email_loader, "LEDGER_LABEL", test_label)
    cost_ledger_module.reset_label(test_label)

    pid = await _ensure_one_property()

    # Force every event onto our fixture property by patching route_text_event.
    from backend.pipeline import router as router_module  # noqa: PLC0415

    async def _fake_route(session: Any, raw_content: str, *, metadata: Any = None) -> Any:
        return router_module.StructuredRoute(
            property_id=pid,
            method="test_fake",
            reason="forced for cost-cap test",
        )

    monkeypatch.setattr(
        buena_email_loader, "route_text_event", _fake_route, raising=True
    )

    # Patch the extractor to return a deterministic result that costs ~$0.0125
    # per call (10k prompt + 1k completion on Pro pricing). The cap is set
    # to $0.02 below — so the second call hits the post-charge cap.
    fake_result = ExtractionResult(
        category="other",
        priority="low",
        facts_to_update=[],
        summary="(test)",
        raw={"test": True},
        source="gemini",
        latency_ms=1.0,
        prompt_tokens=10_000,
        completion_tokens=1_000,
        model="gemini-2.5-pro",
    )

    async def _fake_extract(**_kwargs: Any) -> ExtractionResult:
        return fake_result

    monkeypatch.setattr(
        buena_email_loader, "run_extractor", _fake_extract, raising=True
    )

    # Iterate exactly N events from a fabricated list; bypass the .eml walker.
    events = [_connector_event(i) for i in range(5)]
    monkeypatch.setattr(
        buena_email_loader.eml_archive,
        "walk_directory",
        lambda _root: iter(events),
        raising=True,
    )

    summary = await buena_email_loader.backfill_emails(
        root=Path("/tmp/unused"),
        cap_usd=Decimal("0.02"),
        dead_letter_after=3,
        reprocess_historical=False,
        limit=None,
    )

    assert summary.aborted_on_cost_cap is True, summary.as_json()
    # First call: $0.0125 → ledger goes to $0.0125 (below cap).
    # Second call: $0.0125 → $0.025 > $0.02 cap → CostCapExceeded raised.
    assert summary.extraction_attempts <= 5
    assert Decimal(summary.cumulative_usd) > Decimal("0.02")

    # Subsequent run must refuse before issuing any further extraction.
    summary2 = await buena_email_loader.backfill_emails(
        root=Path("/tmp/unused"),
        cap_usd=Decimal("0.02"),
        limit=None,
    )
    assert summary2.aborted_on_cost_cap is True
    assert summary2.extraction_attempts == 0

    # Cleanup so re-runs of the suite stay deterministic.
    cost_ledger_module.reset_label(test_label)
    await _cleanup_test_events()


async def test_dead_letter_after_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """A persistent extractor failure increments retry_count across runs."""
    await _setup_or_skip()
    await _cleanup_test_events()
    test_label = f"step6_test_{uuid4().hex[:8]}"
    monkeypatch.setattr(buena_email_loader, "LEDGER_LABEL", test_label)
    cost_ledger_module.reset_label(test_label)

    pid = await _ensure_one_property()

    from backend.pipeline import router as router_module  # noqa: PLC0415

    async def _fake_route(session: Any, raw_content: str, *, metadata: Any = None) -> Any:
        return router_module.StructuredRoute(
            property_id=pid,
            method="test_fake",
            reason="forced for dead-letter test",
        )

    monkeypatch.setattr(
        buena_email_loader, "route_text_event", _fake_route, raising=True
    )

    async def _always_raises(**_kwargs: Any) -> ExtractionResult:
        raise RuntimeError("synthetic extractor failure")

    monkeypatch.setattr(
        buena_email_loader, "run_extractor", _always_raises, raising=True
    )

    events = [_connector_event(0)]  # single event re-tried across runs
    monkeypatch.setattr(
        buena_email_loader.eml_archive,
        "walk_directory",
        lambda _root: iter(events),
        raising=True,
    )

    # Three sequential runs — each should re-attempt while retry_count<3.
    for _ in range(3):
        await buena_email_loader.backfill_emails(
            root=Path("/tmp/unused"),
            cap_usd=Decimal("1.00"),
            dead_letter_after=3,
            reprocess_historical=False,
            limit=None,
        )

    factory = get_sessionmaker()
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT f.retry_count, e.processing_error
                    FROM events e
                    JOIN failed_events f ON f.event_id = e.id
                    WHERE e.source_ref = 'test-cap-0000@example.com'
                    """
                )
            )
        ).first()
    assert row is not None
    assert int(row.retry_count) >= 3, f"retry_count={row.retry_count}"
    assert row.processing_error is not None

    # A fourth run must not bump retry_count past dead_letter_after.
    summary4 = await buena_email_loader.backfill_emails(
        root=Path("/tmp/unused"),
        cap_usd=Decimal("1.00"),
        dead_letter_after=3,
        limit=None,
    )
    assert summary4.extraction_attempts == 0  # dead-lettered → skipped

    cost_ledger_module.reset_label(test_label)
    await _cleanup_test_events()
