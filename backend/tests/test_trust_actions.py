"""Tests for Phase 10 Step 10.6 — trust-layer action endpoints.

Two write surfaces:

1. ``POST /admin/uncertainties/{id}/resolve`` — promote-to-fact /
   dismiss, both audit-logged.
2. ``POST /admin/rejected/{id}/override`` — already shipped in Phase
   9.2; we re-verify here that the audit trail (approval_log) is
   actually written, since the user explicitly asked us to verify.

Both use httpx.AsyncClient + ASGITransport so the route's asyncpg
session shares the test's event loop.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
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


def _async_client() -> httpx.AsyncClient:
    """ASGI-transport client — see test_source_links for rationale."""
    from backend.main import app  # noqa: PLC0415

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    )


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


async def _seed_property() -> tuple[UUID, UUID, UUID]:
    """Insert one building + property + event; return ids."""
    factory = get_sessionmaker()
    async with factory() as session:
        b = (
            await session.execute(
                text(
                    "INSERT INTO buildings (address) VALUES "
                    "('Trust Action Test') RETURNING id"
                )
            )
        ).first()
        p = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES ('Trust Action Prop', 'addr', :bid, ARRAY['x'])
                    RETURNING id
                    """
                ),
                {"bid": b.id},
            )
        ).first()
        ref = f"trust-{uuid4().hex[:8]}"
        e = (
            await session.execute(
                text(
                    """
                    INSERT INTO events (source, source_ref, raw_content, property_id)
                    VALUES ('email', :ref, 'we should adjust the rent', :pid)
                    RETURNING id
                    """
                ),
                {"ref": ref, "pid": p.id},
            )
        ).first()
        await session.commit()
    return b.id, p.id, e.id


async def _seed_uncertainty(
    *,
    property_id: UUID,
    event_id: UUID,
    section: str = "financials",
    field: str = "rent_amount",
    hypothesis: str | None = "1300 EUR",
) -> UUID:
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
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
                      :hyp,
                      'vague mention without timeframe',
                      :section, :field,
                      'gemini', 'open'
                    )
                    RETURNING id
                    """
                ),
                {
                    "eid": event_id,
                    "pid": property_id,
                    "hyp": hypothesis,
                    "section": section,
                    "field": field,
                },
            )
        ).first()
        await session.commit()
    return UUID(str(row.id))


async def _seed_rejection(
    *,
    property_id: UUID,
    event_id: UUID,
) -> UUID:
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
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
                    RETURNING id
                    """
                ),
                {"eid": event_id, "pid": property_id},
            )
        ).first()
        await session.commit()
    return UUID(str(row.id))


async def _cleanup(building_id: UUID, property_id: UUID) -> None:
    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text("DELETE FROM approval_log WHERE target_id IN ("
                 "SELECT id::text FROM uncertainty_events WHERE property_id = :pid"
                 " UNION SELECT id::text FROM rejected_updates WHERE property_id = :pid"
                 ")"),
            {"pid": property_id},
        )
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


# ---------------------------------------------------------------------------
# Uncertainty: promote_to_fact happy path
# ---------------------------------------------------------------------------


async def test_promote_uses_explicit_value_when_provided() -> None:
    """A POST with action=promote_to_fact + value writes the explicit value as a fact."""
    await _setup_or_skip()
    bid, pid, eid = await _seed_property()
    uid = await _seed_uncertainty(property_id=pid, event_id=eid)
    try:
        async with _async_client() as client:
            resp = await client.post(
                f"/admin/uncertainties/{uid}/resolve",
                json={
                    "action": "promote_to_fact",
                    "value": "1450 EUR",
                    "reason": "tenant confirmed by phone today",
                    "reviewed_by": "ops@keystone.test",
                },
            )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["action"] == "promote_to_fact"
        assert body["new_status"] == "resolved"
        assert body["fact_id"] is not None

        factory = get_sessionmaker()
        async with factory() as session:
            fact = (
                await session.execute(
                    text(
                        """
                        SELECT section, field, value, confidence,
                               source_event_id
                        FROM facts WHERE id = :fid
                        """
                    ),
                    {"fid": body["fact_id"]},
                )
            ).first()
            assert fact is not None
            assert fact.section == "financials"
            assert fact.field == "rent_amount"
            assert fact.value == "1450 EUR"
            assert float(fact.confidence) >= 0.9
            assert fact.source_event_id == eid

            unc = (
                await session.execute(
                    text(
                        """
                        SELECT status, resolved_to_fact_id, resolved_at
                        FROM uncertainty_events WHERE id = :uid
                        """
                    ),
                    {"uid": uid},
                )
            ).first()
            assert unc.status == "resolved"
            assert unc.resolved_to_fact_id == UUID(body["fact_id"])
            assert unc.resolved_at is not None

            audit = (
                await session.execute(
                    text(
                        """
                        SELECT actor, action, target_type, payload
                        FROM approval_log
                        WHERE target_type = 'uncertainty_event'
                          AND target_id = :tid
                        """
                    ),
                    {"tid": str(uid)},
                )
            ).first()
            assert audit is not None
            assert audit.actor == "ops@keystone.test"
            assert audit.action == "uncertainty.promote_to_fact"
            payload = audit.payload if isinstance(audit.payload, dict) else json.loads(audit.payload)
            assert payload["value_written"] == "1450 EUR"
            assert payload["reason"] == "tenant confirmed by phone today"
    finally:
        await _cleanup(bid, pid)


async def test_promote_falls_back_to_hypothesis_when_no_value() -> None:
    """Omitting ``value`` falls back to the uncertainty's hypothesis."""
    await _setup_or_skip()
    bid, pid, eid = await _seed_property()
    uid = await _seed_uncertainty(
        property_id=pid, event_id=eid, hypothesis="1250 EUR"
    )
    try:
        async with _async_client() as client:
            resp = await client.post(
                f"/admin/uncertainties/{uid}/resolve",
                json={
                    "action": "promote_to_fact",
                    "reason": "agreed with hypothesis",
                    "reviewed_by": "ops@keystone.test",
                },
            )
        assert resp.status_code == 200, resp.text
        fact_id = resp.json()["fact_id"]

        factory = get_sessionmaker()
        async with factory() as session:
            fact = (
                await session.execute(
                    text("SELECT value FROM facts WHERE id = :fid"),
                    {"fid": fact_id},
                )
            ).first()
        assert fact.value == "1250 EUR"
    finally:
        await _cleanup(bid, pid)


async def test_promote_422s_when_no_value_and_no_hypothesis() -> None:
    """An uncertainty with NULL hypothesis + no body.value cannot be promoted."""
    await _setup_or_skip()
    bid, pid, eid = await _seed_property()
    uid = await _seed_uncertainty(
        property_id=pid, event_id=eid, hypothesis=None
    )
    try:
        async with _async_client() as client:
            resp = await client.post(
                f"/admin/uncertainties/{uid}/resolve",
                json={
                    "action": "promote_to_fact",
                    "reason": "no value to commit",
                    "reviewed_by": "ops@keystone.test",
                },
            )
        assert resp.status_code == 422
        assert "no value supplied" in resp.text or "hypothesis" in resp.text
    finally:
        await _cleanup(bid, pid)


# ---------------------------------------------------------------------------
# Uncertainty: dismiss
# ---------------------------------------------------------------------------


async def test_dismiss_does_not_write_a_fact_but_audit_logs() -> None:
    """``action=dismiss`` flips status without a fact + writes approval_log."""
    await _setup_or_skip()
    bid, pid, eid = await _seed_property()
    uid = await _seed_uncertainty(property_id=pid, event_id=eid)
    try:
        async with _async_client() as client:
            resp = await client.post(
                f"/admin/uncertainties/{uid}/resolve",
                json={
                    "action": "dismiss",
                    "reason": "off-topic — not actually about this property",
                    "reviewed_by": "ops@keystone.test",
                },
            )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["new_status"] == "dismissed"
        assert body["fact_id"] is None

        factory = get_sessionmaker()
        async with factory() as session:
            fact_count = (
                await session.execute(
                    text(
                        "SELECT COUNT(*) FROM facts "
                        "WHERE property_id = :pid AND field = 'rent_amount'"
                    ),
                    {"pid": pid},
                )
            ).scalar()
            assert fact_count == 0

            audit = (
                await session.execute(
                    text(
                        """
                        SELECT action, payload FROM approval_log
                        WHERE target_id = :tid
                        """
                    ),
                    {"tid": str(uid)},
                )
            ).first()
            assert audit is not None
            assert audit.action == "uncertainty.dismiss"
    finally:
        await _cleanup(bid, pid)


# ---------------------------------------------------------------------------
# Uncertainty: state-machine guards
# ---------------------------------------------------------------------------


async def test_resolve_404_for_missing_uncertainty() -> None:
    """An unknown uncertainty id returns 404."""
    await _setup_or_skip()
    async with _async_client() as client:
        resp = await client.post(
            f"/admin/uncertainties/{uuid4()}/resolve",
            json={
                "action": "dismiss",
                "reason": "nope",
                "reviewed_by": "ops",
            },
        )
    assert resp.status_code == 404


async def test_double_resolve_409s() -> None:
    """Resolving a non-open uncertainty fails with 409."""
    await _setup_or_skip()
    bid, pid, eid = await _seed_property()
    uid = await _seed_uncertainty(property_id=pid, event_id=eid)
    try:
        async with _async_client() as client:
            r1 = await client.post(
                f"/admin/uncertainties/{uid}/resolve",
                json={
                    "action": "dismiss",
                    "reason": "first close",
                    "reviewed_by": "ops",
                },
            )
            assert r1.status_code == 200
            r2 = await client.post(
                f"/admin/uncertainties/{uid}/resolve",
                json={
                    "action": "dismiss",
                    "reason": "second close",
                    "reviewed_by": "ops",
                },
            )
        assert r2.status_code == 409
    finally:
        await _cleanup(bid, pid)


# ---------------------------------------------------------------------------
# Existing rejected/override path — verify it audit-logs end-to-end
# ---------------------------------------------------------------------------


async def test_rejected_override_writes_fact_and_audit() -> None:
    """``decision=overridden`` writes a fact + an approval_log entry."""
    await _setup_or_skip()
    bid, pid, eid = await _seed_property()
    rid = await _seed_rejection(property_id=pid, event_id=eid)
    try:
        async with _async_client() as client:
            resp = await client.post(
                f"/admin/rejected/{rid}/override",
                json={
                    "decision": "overridden",
                    "reason": "verified with site survey today",
                    "reviewed_by": "ops@keystone.test",
                },
            )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["fact_written"] is True
        assert body["reviewed_status"] == "overridden"

        factory = get_sessionmaker()
        async with factory() as session:
            fact = (
                await session.execute(
                    text(
                        """
                        SELECT section, field, value, source_event_id
                        FROM facts
                        WHERE property_id = :pid
                          AND field = 'floor_count'
                        """
                    ),
                    {"pid": pid},
                )
            ).first()
            assert fact is not None
            assert fact.value == "8"
            assert fact.source_event_id == eid

            audit = (
                await session.execute(
                    text(
                        """
                        SELECT actor, action, target_type, payload
                        FROM approval_log
                        WHERE target_type = 'rejected_update'
                          AND target_id = :tid
                        """
                    ),
                    {"tid": str(rid)},
                )
            ).first()
            assert audit is not None
            assert audit.actor == "ops@keystone.test"
            assert audit.action == "rejection.overridden"
            payload: dict[str, Any] = (
                audit.payload if isinstance(audit.payload, dict) else json.loads(audit.payload)
            )
            assert payload["fact_written"] is True
            assert payload["reason_for_override"] == "verified with site survey today"
            assert payload["constraint_name"] == "building_floor_count_immutable"
    finally:
        await _cleanup(bid, pid)


async def test_rejected_dismiss_does_not_write_fact() -> None:
    """``decision=dismissed`` records the audit trail but writes no fact."""
    await _setup_or_skip()
    bid, pid, eid = await _seed_property()
    rid = await _seed_rejection(property_id=pid, event_id=eid)
    try:
        async with _async_client() as client:
            resp = await client.post(
                f"/admin/rejected/{rid}/override",
                json={
                    "decision": "dismissed",
                    "reason": "operator confirms 5 floors is correct",
                    "reviewed_by": "ops@keystone.test",
                },
            )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["fact_written"] is False

        factory = get_sessionmaker()
        async with factory() as session:
            fact_count = (
                await session.execute(
                    text(
                        "SELECT COUNT(*) FROM facts "
                        "WHERE property_id = :pid AND field = 'floor_count'"
                    ),
                    {"pid": pid},
                )
            ).scalar()
            assert fact_count == 0

            audit_action = (
                await session.execute(
                    text(
                        "SELECT action FROM approval_log "
                        "WHERE target_id = :tid"
                    ),
                    {"tid": str(rid)},
                )
            ).scalar()
            assert audit_action == "rejection.dismissed"
    finally:
        await _cleanup(bid, pid)
