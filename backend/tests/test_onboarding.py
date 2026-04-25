"""Tests for Phase 10 Step 10.3 — onboarding view.

The onboarding view has one Gemini call (the 12-month briefing) and
four deterministic sections. We monkeypatch the Gemini choke-point so
tests are offline + deterministic; the deterministic sections are
exercised end-to-end against a real DB row, so cleanup matters.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.services import gemini, onboarding
from connectors.migrations import apply_all as ensure_migrations

pytestmark = pytest.mark.asyncio


def _reset_session_cache() -> None:
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


async def _seed_property(
    *,
    metadata: dict[str, Any] | None = None,
) -> tuple[UUID, UUID]:
    """Insert one building + one property; return (building_id, property_id)."""
    factory = get_sessionmaker()
    async with factory() as session:
        b = (
            await session.execute(
                text(
                    "INSERT INTO buildings (address) VALUES "
                    "('Onboarding Test Bldg') RETURNING id"
                )
            )
        ).first()
        p = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases, metadata)
                    VALUES ('Onboarding Test Prop', 'Test Strasse 1', :bid,
                            ARRAY['x'], CAST(:meta AS JSONB))
                    RETURNING id
                    """
                ),
                {
                    "bid": b.id,
                    "meta": _json(metadata or {}),
                },
            )
        ).first()
        await session.commit()
    return b.id, p.id


def _json(value: dict[str, Any]) -> str:
    import json  # noqa: PLC0415

    return json.dumps(value)


async def _cleanup(building_id: UUID, property_id: UUID) -> None:
    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text("DELETE FROM rejected_updates WHERE property_id = :pid"),
            {"pid": property_id},
        )
        await session.execute(
            text("DELETE FROM uncertainty_events WHERE property_id = :pid"),
            {"pid": property_id},
        )
        await session.execute(
            text("DELETE FROM facts WHERE property_id = :pid"), {"pid": property_id}
        )
        await session.execute(
            text("DELETE FROM events WHERE property_id = :pid"),
            {"pid": property_id},
        )
        await session.execute(
            text("DELETE FROM properties WHERE id = :id"), {"id": property_id}
        )
        await session.execute(
            text("DELETE FROM buildings WHERE id = :id"), {"id": building_id}
        )
        await session.commit()


# ----------------------------------------------------------------------------
# Pure-unit: cache key
# ----------------------------------------------------------------------------


async def test_cache_key_stable_for_same_inputs() -> None:
    """Identical timestamps → identical cache key."""
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    a = onboarding._cache_key(ts, ts, ts)
    b = onboarding._cache_key(ts, ts, ts)
    assert a == b


async def test_cache_key_changes_when_any_timestamp_moves() -> None:
    """A shift in any of the three timestamps changes the cache key."""
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    later = datetime(2026, 4, 1, 12, 0, 1, tzinfo=timezone.utc)
    base = onboarding._cache_key(ts, ts, ts)
    assert onboarding._cache_key(later, ts, ts) != base
    assert onboarding._cache_key(ts, later, ts) != base
    assert onboarding._cache_key(ts, ts, later) != base


async def test_cache_key_handles_none_inputs() -> None:
    """An all-None state is a stable, hashable key."""
    a = onboarding._cache_key(None, None, None)
    b = onboarding._cache_key(None, None, None)
    assert a == b
    assert isinstance(a, str)


# ----------------------------------------------------------------------------
# DB integration — empty property renders gracefully
# ----------------------------------------------------------------------------


async def test_render_onboarding_empty_property(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bare property with no facts/events still produces all 5 sections."""
    await _setup_or_skip()
    bid, pid = await _seed_property(
        metadata={
            "kaltmiete": "1100 EUR",
            "kaution": "3300 EUR",
        }
    )
    try:
        # Force the Gemini-unavailable path so we don't hit the network.
        monkeypatch.setattr(gemini, "is_available", lambda: False, raising=True)

        factory = get_sessionmaker()
        async with factory() as session:
            md = await onboarding.render_onboarding(session, pid)

        assert "Onboarding Test Prop" in md
        # Stammdaten visible.
        assert "1100 EUR" in md
        # Five section titles render (English fallback for no-events property).
        assert "Property in 60 seconds" in md
        assert "Open issues right now" in md
        assert "Key context (last 12 months)" in md
        assert "Watch out for" in md
        assert "Where to look for more" in md
        # No-issues message.
        assert "No open issues" in md
        # Briefing degraded message present.
        assert "Briefing skipped" in md
    finally:
        await _cleanup(bid, pid)


# ----------------------------------------------------------------------------
# DB integration — open issues surface
# ----------------------------------------------------------------------------


async def test_render_onboarding_surfaces_open_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A property with an active leak fact + open uncertainty + pending rejection
    shows all three under "Open issues right now"."""
    await _setup_or_skip()
    bid, pid = await _seed_property()
    try:
        monkeypatch.setattr(gemini, "is_available", lambda: False, raising=True)

        factory = get_sessionmaker()
        async with factory() as session:
            # 1. An active maintenance fact.
            ref = f"onb-{uuid4().hex[:8]}"
            ev = (
                await session.execute(
                    text(
                        """
                        INSERT INTO events (source, source_ref, raw_content, property_id)
                        VALUES ('email', :ref, 'water leak in basement', :pid)
                        RETURNING id
                        """
                    ),
                    {"ref": ref, "pid": pid},
                )
            ).first()
            await session.execute(
                text(
                    """
                    INSERT INTO facts (
                      property_id, section, field, value, source_event_id,
                      confidence, valid_from
                    ) VALUES (
                      :pid, 'maintenance', 'open_water_leak',
                      'basement, ongoing', :eid, 0.92, now()
                    )
                    """
                ),
                {"pid": pid, "eid": ev.id},
            )
            # 2. An open uncertainty.
            await session.execute(
                text(
                    """
                    INSERT INTO uncertainty_events (
                      event_id, property_id, observation, hypothesis,
                      reason_uncertain, relevant_section, relevant_field,
                      source, status
                    ) VALUES (
                      :eid, :pid,
                      'we should adjust the rent at some point',
                      'rent_change_pending',
                      'vague mention without timeframe',
                      'financials', 'rent_amount',
                      'gemini', 'open'
                    )
                    """
                ),
                {"eid": ev.id, "pid": pid},
            )
            # 3. A pending rejection.
            await session.execute(
                text(
                    """
                    INSERT INTO rejected_updates (
                      event_id, property_id, proposed_section, proposed_field,
                      proposed_value, proposed_confidence, constraint_name,
                      reason, reviewed_status
                    ) VALUES (
                      :eid, :pid, 'building_overview', 'floor_count', '8',
                      0.9, 'building_floor_count_immutable',
                      'floor count is immutable from email source', 'pending'
                    )
                    """
                ),
                {"eid": ev.id, "pid": pid},
            )
            await session.commit()

        factory = get_sessionmaker()
        async with factory() as session:
            md = await onboarding.render_onboarding(session, pid)

        # Active maintenance surfaces the field name and value.
        assert "Open water leak" in md or "open_water_leak" in md.lower()
        assert "basement, ongoing" in md
        # Uncertainty surfaces the observation.
        assert "adjust the rent" in md
        # Rejection surfaces the constraint name.
        assert "building_floor_count_immutable" in md
    finally:
        await _cleanup(bid, pid)


# ----------------------------------------------------------------------------
# DB integration — Gemini briefing path with cache
# ----------------------------------------------------------------------------


async def test_briefing_caches_on_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """First render calls Gemini; second render reuses the cached briefing."""
    await _setup_or_skip()
    bid, pid = await _seed_property()
    try:
        # Force "available" + stub the Gemini drafter so the test stays offline.
        monkeypatch.setattr(gemini, "is_available", lambda: True, raising=True)
        call_count = {"n": 0}

        async def _stub_draft(**kwargs: Any) -> str:
            call_count["n"] += 1
            return (
                "- Property has scant history; expect onboarding gaps.\n"
                "- Gap: no current tenant on file.\n"
                "- Gap: no rent history captured."
            )

        monkeypatch.setattr(
            gemini, "draft_onboarding_briefing", _stub_draft, raising=True
        )

        factory = get_sessionmaker()
        async with factory() as session:
            md1 = await onboarding.render_onboarding(session, pid)
        assert "Gap: no current tenant" in md1
        assert call_count["n"] == 1

        async with factory() as session:
            md2 = await onboarding.render_onboarding(session, pid)
        assert "Gap: no current tenant" in md2
        assert call_count["n"] == 1, "second render must re-use the cache"
    finally:
        await _cleanup(bid, pid)


async def test_cache_invalidates_on_new_fact(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inserting a new fact bumps last-fact timestamp → cache key flips → Gemini called again."""
    await _setup_or_skip()
    bid, pid = await _seed_property()
    try:
        monkeypatch.setattr(gemini, "is_available", lambda: True, raising=True)
        call_count = {"n": 0}

        async def _stub_draft(**kwargs: Any) -> str:
            call_count["n"] += 1
            return f"- Briefing call #{call_count['n']}.\n- Gap: ongoing."

        monkeypatch.setattr(
            gemini, "draft_onboarding_briefing", _stub_draft, raising=True
        )

        factory = get_sessionmaker()
        async with factory() as session:
            await onboarding.render_onboarding(session, pid)
        assert call_count["n"] == 1

        # Insert a fact — moves last_fact and flips the cache key.
        async with factory() as session:
            ref = f"cache-{uuid4().hex[:8]}"
            ev = (
                await session.execute(
                    text(
                        """
                        INSERT INTO events (source, source_ref, raw_content, property_id)
                        VALUES ('email', :ref, 'new fact', :pid)
                        RETURNING id
                        """
                    ),
                    {"ref": ref, "pid": pid},
                )
            ).first()
            await session.execute(
                text(
                    """
                    INSERT INTO facts (
                      property_id, section, field, value, source_event_id,
                      confidence, valid_from
                    ) VALUES (
                      :pid, 'overview', 'note', 'something new', :eid, 0.9, now()
                    )
                    """
                ),
                {"pid": pid, "eid": ev.id},
            )
            await session.commit()

        async with factory() as session:
            md = await onboarding.render_onboarding(session, pid)
        assert call_count["n"] == 2, "new fact should have invalidated the cache"
        assert "Briefing call #2" in md
    finally:
        await _cleanup(bid, pid)


# ----------------------------------------------------------------------------
# DB integration — recurring patterns surface in "Watch out for"
# ----------------------------------------------------------------------------


async def test_watch_out_surfaces_recurring_constraint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A constraint that has triggered ≥2 times on this property surfaces under
    'Watch out for'. Single-trigger constraints do not (noise filter)."""
    await _setup_or_skip()
    bid, pid = await _seed_property()
    try:
        monkeypatch.setattr(gemini, "is_available", lambda: False, raising=True)

        factory = get_sessionmaker()
        async with factory() as session:
            # Three rejections for the same constraint, two of which have
            # already been dismissed (reviewed_status != pending). The
            # "Watch out for" section deliberately includes resolved rows.
            for status in ("pending", "dismissed", "dismissed"):
                ref = f"reject-{uuid4().hex[:8]}"
                ev = (
                    await session.execute(
                        text(
                            """
                            INSERT INTO events (
                              source, source_ref, raw_content, property_id
                            ) VALUES ('email', :ref, 'x', :pid) RETURNING id
                            """
                        ),
                        {"ref": ref, "pid": pid},
                    )
                ).first()
                await session.execute(
                    text(
                        """
                        INSERT INTO rejected_updates (
                          event_id, property_id, proposed_section, proposed_field,
                          proposed_value, constraint_name, reason, reviewed_status
                        ) VALUES (
                          :eid, :pid, 'building_overview', 'floor_count', '8',
                          'building_floor_count_immutable',
                          'immutable from email', :st
                        )
                        """
                    ),
                    {"eid": ev.id, "pid": pid, "st": status},
                )
            await session.commit()

        factory = get_sessionmaker()
        async with factory() as session:
            md = await onboarding.render_onboarding(session, pid)

        # The constraint should appear with a count >= 2.
        assert "building_floor_count_immutable" in md
        assert "3 times" in md or "triggered" in md
    finally:
        await _cleanup(bid, pid)


async def test_render_onboarding_404s_for_missing_property() -> None:
    """A missing property surfaces a clean ValueError from the renderer."""
    await _setup_or_skip()
    factory = get_sessionmaker()
    async with factory() as session:
        with pytest.raises(ValueError, match="not found"):
            await onboarding.render_onboarding(session, uuid4())
