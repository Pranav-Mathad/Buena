"""Tests for the onboarding view.

The onboarding view has one LLM call (the 12-month briefing) and
four deterministic sections. We monkeypatch the provider lookup
(``onboarding.get_provider``) so tests are offline + deterministic; the deterministic sections are
exercised end-to-end against a real DB row, so cleanup matters.
"""

from __future__ import annotations

from datetime import UTC
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.services import onboarding

pytestmark = pytest.mark.asyncio


class _StubProvider:
    """Stands in for an LLM provider; delegates complete_text to a test function."""

    name = "stub"
    model = "stub"

    def __init__(self, draft: Any) -> None:
        self._draft = draft

    async def complete_text(self, **kwargs: Any) -> str:
        return str(await self._draft(**kwargs))


async def _seed_property(
    *,
    metadata: dict[str, Any] | None = None,
) -> tuple[UUID, UUID]:
    """Insert one building + one property; return (building_id, property_id)."""
    factory = get_sessionmaker()
    async with factory() as session:
        b = (
            await session.execute(
                text("INSERT INTO buildings (address) VALUES ('Onboarding Test Bldg') RETURNING id")
            )
        ).one()
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
        ).one()
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
        await session.execute(text("DELETE FROM properties WHERE id = :id"), {"id": property_id})
        await session.execute(text("DELETE FROM buildings WHERE id = :id"), {"id": building_id})
        await session.commit()


# ----------------------------------------------------------------------------
# Pure-unit: cache key
# ----------------------------------------------------------------------------


async def test_cache_key_stable_for_same_inputs() -> None:
    """Identical timestamps → identical cache key."""
    from datetime import datetime

    ts = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    a = onboarding._cache_key(ts, ts, ts)
    b = onboarding._cache_key(ts, ts, ts)
    assert a == b


async def test_cache_key_changes_when_any_timestamp_moves() -> None:
    """A shift in any of the three timestamps changes the cache key."""
    from datetime import datetime

    ts = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    later = datetime(2026, 4, 1, 12, 0, 1, tzinfo=UTC)
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
# DB integration: empty property renders gracefully
# ----------------------------------------------------------------------------


@pytest.mark.usefixtures("db")
async def test_render_onboarding_empty_property(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bare property with no facts/events still produces all 5 sections."""
    bid, pid = await _seed_property(
        metadata={
            "kaltmiete": "1100 EUR",
            "kaution": "3300 EUR",
        }
    )
    try:
        # No LLM provider: the briefing degrades to a notice, nothing hits the network.
        monkeypatch.setattr(onboarding, "get_provider", lambda: None)

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
        # With no LLM reachable the briefing degrades to an explicit notice.
        assert "Briefing unavailable" in md
    finally:
        await _cleanup(bid, pid)


# ----------------------------------------------------------------------------
# DB integration: open issues surface
# ----------------------------------------------------------------------------


@pytest.mark.usefixtures("db")
async def test_render_onboarding_surfaces_open_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A property with an active leak fact + open uncertainty + pending rejection
    shows all three under "Open issues right now"."""
    bid, pid = await _seed_property()
    try:
        monkeypatch.setattr(onboarding, "get_provider", lambda: None)

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
            ).one()
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
# DB integration: LLM briefing path with cache
# ----------------------------------------------------------------------------


@pytest.mark.usefixtures("db")
async def test_briefing_caches_on_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """First render calls the LLM; second render reuses the cached briefing."""
    bid, pid = await _seed_property()
    try:
        # A stub provider keeps the test offline while exercising the LLM path.
        call_count = {"n": 0}

        async def _stub_draft(**kwargs: Any) -> str:
            call_count["n"] += 1
            return (
                "- Property has scant history; expect onboarding gaps.\n"
                "- Gap: no current tenant on file.\n"
                "- Gap: no rent history captured."
            )

        monkeypatch.setattr(onboarding, "get_provider", lambda: _StubProvider(_stub_draft))

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


@pytest.mark.usefixtures("db")
async def test_briefing_source_says_how_the_briefing_was_produced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No provider: 'unavailable'; then the LLM writes it ('llm'); then 'cache'."""
    bid, pid = await _seed_property()
    try:
        factory = get_sessionmaker()
        monkeypatch.setattr(onboarding, "get_provider", lambda: None)
        async with factory() as session:
            keyless = await onboarding.render_onboarding_document(session, pid)
        assert keyless.briefing_source == "unavailable"
        assert "Briefing unavailable" in keyless.markdown

        async def _stub_draft(**kwargs: Any) -> str:
            return "- Gap: no rent history captured."

        monkeypatch.setattr(onboarding, "get_provider", lambda: _StubProvider(_stub_draft))
        async with factory() as session:
            written = await onboarding.render_onboarding_document(session, pid)
        async with factory() as session:
            reread = await onboarding.render_onboarding_document(session, pid)
        assert written.briefing_source == "llm"
        assert reread.briefing_source == "cache"
    finally:
        await _cleanup(bid, pid)


@pytest.mark.usefixtures("db")
async def test_onboarding_endpoint_reports_a_keyless_briefing() -> None:
    """With no LLM key the API must not present the briefing as LLM-written."""
    from backend.main import app  # noqa: PLC0415

    bid, pid = await _seed_property()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(f"/admin/properties/{pid}/onboarding")
        assert response.status_code == 200
        body = response.json()
        assert body["briefing_source"] == "unavailable"
        assert body["cached"] is False
    finally:
        await _cleanup(bid, pid)


@pytest.mark.usefixtures("db")
async def test_cache_invalidates_on_new_fact(monkeypatch: pytest.MonkeyPatch) -> None:
    """A new fact changes the cache key, so the next render calls the LLM again."""
    bid, pid = await _seed_property()
    try:
        call_count = {"n": 0}

        async def _stub_draft(**kwargs: Any) -> str:
            call_count["n"] += 1
            return f"- Briefing call #{call_count['n']}.\n- Gap: ongoing."

        monkeypatch.setattr(onboarding, "get_provider", lambda: _StubProvider(_stub_draft))

        factory = get_sessionmaker()
        async with factory() as session:
            await onboarding.render_onboarding(session, pid)
        assert call_count["n"] == 1

        # Insert a fact: moves last_fact and flips the cache key.
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
            ).one()
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
# DB integration: recurring patterns surface in "Watch out for"
# ----------------------------------------------------------------------------


@pytest.mark.usefixtures("db")
async def test_watch_out_surfaces_recurring_constraint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A constraint that has triggered ≥2 times on this property surfaces under
    'Watch out for'. Single-trigger constraints do not (noise filter)."""
    bid, pid = await _seed_property()
    try:
        monkeypatch.setattr(onboarding, "get_provider", lambda: None)

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
                ).one()
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


@pytest.mark.usefixtures("db")
async def test_render_onboarding_404s_for_missing_property() -> None:
    """A missing property surfaces a clean ValueError from the renderer."""
    factory = get_sessionmaker()
    async with factory() as session:
        with pytest.raises(ValueError, match="not found"):
            await onboarding.render_onboarding(session, uuid4())
