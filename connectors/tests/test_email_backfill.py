"""Email backfill loop control: cost-cap abort and dead-lettering.

These tests run :mod:`connectors.buena_email_loader` against Postgres with
the extractor and router monkeypatched, so no LLM calls are made. The
assertions are about loop control, the ``failed_events`` table and the
cost ledger's abort path.

Isolation: the :func:`backfill_env` fixture gives each test its own
property, building and cost-ledger label, and deletes all of them (plus
every event the test inserted) on teardown, even when an assertion fails.
A unique label matters because ``reset_label`` deletes a ledger row: with a
shared label, running the suite next to a real backfill would wipe the
backfill's spend record mid-run.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.services.llm import ExtractionResult
from connectors import buena_email_loader
from connectors import cost_ledger as cost_ledger_module
from connectors.base import ConnectorEvent

pytestmark = pytest.mark.asyncio

_REF_PREFIX = "test-backfill-"


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
        source_ref=f"{_REF_PREFIX}{idx:04d}@example.com",
        raw_content=f"From: a@b\nSubject: test {idx}\n\nbody {idx}",
        metadata={"from": "a@b", "subject": f"test {idx}"},
        received_at=datetime(2026, 4, 25, 9, 0, 0, tzinfo=UTC),
    )


@dataclass(frozen=True)
class BackfillEnv:
    property_id: UUID
    ledger_label: str


@pytest.fixture
async def backfill_env(db: None, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[BackfillEnv]:
    """A throwaway property, building and ledger label, removed after the test.

    Every event the loader inserts is forced onto this property, so teardown
    can delete exactly what the test produced. The ledger label is unique per
    test so a backfill running against the same database is never touched.
    """
    suffix = uuid4().hex[:8]
    label = f"test_backfill_{suffix}"
    monkeypatch.setattr(buena_email_loader, "LEDGER_LABEL", label)
    cost_ledger_module.reset_label(label)

    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(text(f"DELETE FROM events WHERE source_ref LIKE '{_REF_PREFIX}%'"))
        building_id = (
            await session.execute(
                text("INSERT INTO buildings (address) VALUES (:a) RETURNING id"),
                {"a": f"Test Backfill Building {suffix}"},
            )
        ).scalar_one()
        property_id = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES (:n, :a, :bid, ARRAY[:n])
                    RETURNING id
                    """
                ),
                {
                    "n": f"Test Backfill Apt {suffix}",
                    "a": f"Test Backfill {suffix}",
                    "bid": building_id,
                },
            )
        ).scalar_one()
        await session.commit()

    try:
        yield BackfillEnv(property_id=property_id, ledger_label=label)
    finally:
        async with factory() as session:
            await session.execute(
                text(
                    "DELETE FROM failed_events WHERE event_id IN "
                    f"(SELECT id FROM events WHERE source_ref LIKE '{_REF_PREFIX}%')"
                )
            )
            await session.execute(
                text("DELETE FROM facts WHERE property_id = :pid"), {"pid": property_id}
            )
            await session.execute(
                text(f"DELETE FROM events WHERE source_ref LIKE '{_REF_PREFIX}%'")
            )
            await session.execute(
                text("DELETE FROM properties WHERE id = :pid"), {"pid": property_id}
            )
            await session.execute(
                text("DELETE FROM buildings WHERE id = :bid"), {"bid": building_id}
            )
            await session.commit()
        cost_ledger_module.reset_label(label)


def _force_route(monkeypatch: pytest.MonkeyPatch, property_id: UUID) -> None:
    """Route every event onto the fixture property."""
    from backend.pipeline import router as router_module  # noqa: PLC0415

    async def _fake_route(session: Any, raw_content: str, *, metadata: Any = None) -> Any:
        return router_module.StructuredRoute(
            property_id=property_id, method="test_fake", reason="forced by test"
        )

    monkeypatch.setattr(buena_email_loader, "route_text_event", _fake_route, raising=True)


async def test_cost_cap_aborts_loop_cleanly(
    backfill_env: BackfillEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the ledger hits the cap mid-run, the loop stops cleanly."""
    _force_route(monkeypatch, backfill_env.property_id)

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

    monkeypatch.setattr(buena_email_loader, "run_extractor", _fake_extract, raising=True)

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
    # Second call: $0.0125 → $0.025 > $0.02 cap → CostCapExceededError raised.
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


async def test_dead_letter_after_threshold(
    backfill_env: BackfillEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A persistent extractor failure increments retry_count across runs."""
    _force_route(monkeypatch, backfill_env.property_id)

    async def _always_raises(**_kwargs: Any) -> ExtractionResult:
        raise RuntimeError("synthetic extractor failure")

    monkeypatch.setattr(buena_email_loader, "run_extractor", _always_raises, raising=True)

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
                    WHERE e.source_ref = 'test-backfill-0000@example.com'
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
