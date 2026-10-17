"""Signal evaluator: dedupe by natural key, and no re-raising after a decision.

A fake rule stands in for the real rule set so each test controls exactly
which candidate fires and touches no other signals in the database. Approve
and reject go through the HTTP API, so the status transitions under test are
the ones operators trigger.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_sessionmaker
from backend.signals import drafter, evaluator
from backend.signals.evaluator import natural_key
from backend.signals.types import ProposedAction, SignalCandidate


@dataclass(frozen=True)
class Finding:
    property_id: UUID
    key: str


class _FixedRule:
    """Rule double that always reports the same finding."""

    def __init__(self, candidate: SignalCandidate) -> None:
        self.candidate = candidate

    async def evaluate(self, session: AsyncSession) -> list[SignalCandidate]:
        return [self.candidate]


async def _fake_draft(session: AsyncSession, candidate: SignalCandidate) -> ProposedAction:
    return ProposedAction(
        type="owner_notification",
        channel="email",
        recipient="owner@example.com",
        subject="Heating keeps failing",
        drafted_message="Three heating complaints in four months.",
        payload={"hint": candidate.action_hint},
    )


@pytest.fixture
async def finding(db: None, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Finding]:
    """A throwaway property with one recurring finding wired into the evaluator."""
    factory = get_sessionmaker()
    async with factory() as session:
        building_id = (
            await session.execute(
                text("INSERT INTO buildings (address) VALUES ('Test Signals Bldg') RETURNING id")
            )
        ).scalar_one()
        property_id = (
            await session.execute(
                text(
                    "INSERT INTO properties (name, address, building_id) "
                    "VALUES (:n, 'Test Signals 1', :bid) RETURNING id"
                ),
                {"n": f"Test Signals {uuid4().hex[:6]}", "bid": building_id},
            )
        ).scalar_one()
        await session.commit()

    hint = {"type": "dispatch_contractor", "topic": "heating"}
    candidate = SignalCandidate(
        type="recurring_maintenance",
        severity="urgent",
        message="3 heating complaints in 120 days",
        property_id=property_id,
        action_hint=hint,
    )
    monkeypatch.setattr(evaluator, "_RULES", [_FixedRule(candidate)])
    monkeypatch.setattr(drafter, "draft", _fake_draft)

    try:
        yield Finding(
            property_id=property_id,
            key=natural_key(property_id, "recurring_maintenance", hint),
        )
    finally:
        async with factory() as session:
            ids = "SELECT id FROM signals WHERE property_id = :pid"
            await session.execute(
                text(f"DELETE FROM outbox WHERE signal_id IN ({ids})"), {"pid": property_id}
            )
            await session.execute(
                text(f"DELETE FROM approval_log WHERE signal_id IN ({ids})"), {"pid": property_id}
            )
            await session.execute(
                text("DELETE FROM signals WHERE property_id = :pid"), {"pid": property_id}
            )
            await session.execute(
                text("DELETE FROM properties WHERE id = :pid"), {"pid": property_id}
            )
            await session.execute(
                text("DELETE FROM buildings WHERE id = :bid"), {"bid": building_id}
            )
            await session.commit()


async def _signals(finding: Finding) -> list[Any]:
    """Every signal raised for the test property, oldest first."""
    async with get_sessionmaker()() as session:
        return list(
            (
                await session.execute(
                    text(
                        "SELECT id, status, natural_key FROM signals "
                        "WHERE property_id = :pid ORDER BY created_at"
                    ),
                    {"pid": finding.property_id},
                )
            ).all()
        )


def _client() -> httpx.AsyncClient:
    from backend.main import app  # noqa: PLC0415

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def test_natural_key_separates_scope_and_subtype() -> None:
    pid = uuid4()

    assert natural_key(pid, "recurring_maintenance", {"topic": "heating"}) != natural_key(
        pid, "recurring_maintenance", {"topic": "water"}
    )
    assert natural_key(None, "cross_property_pattern", {"subtype": "shared_boiler"}) == (
        "cross_property_pattern|portfolio|shared_boiler"
    )


async def test_pending_signal_is_not_duplicated(finding: Finding) -> None:
    assert await evaluator.evaluate_all() == 1
    assert await evaluator.evaluate_all() == 0

    signals = await _signals(finding)
    assert [s.status for s in signals] == ["pending"]
    assert signals[0].natural_key == finding.key


async def test_approved_signal_is_not_raised_again(finding: Finding) -> None:
    await evaluator.evaluate_all()
    [signal] = await _signals(finding)
    async with _client() as client:
        response = await client.post(f"/signals/{signal.id}/approve")
    assert response.status_code == 200

    assert await evaluator.evaluate_all() == 0
    assert await evaluator.evaluate_all() == 0

    assert [s.status for s in await _signals(finding)] == ["resolved"]


async def test_rejected_signal_is_not_raised_again(finding: Finding) -> None:
    await evaluator.evaluate_all()
    [signal] = await _signals(finding)
    async with _client() as client:
        response = await client.post(f"/signals/{signal.id}/reject")
    assert response.status_code == 200

    assert await evaluator.evaluate_all() == 0

    assert [s.status for s in await _signals(finding)] == ["rejected"]


async def test_finding_is_raised_again_after_cooldown(finding: Finding) -> None:
    await evaluator.evaluate_all()
    [signal] = await _signals(finding)
    async with _client() as client:
        await client.post(f"/signals/{signal.id}/approve")
    async with get_sessionmaker()() as session:
        await session.execute(
            text("UPDATE signals SET resolved_at = now() - interval '8 days' WHERE id = :sid"),
            {"sid": signal.id},
        )
        await session.commit()

    assert await evaluator.evaluate_all() == 1

    assert [s.status for s in await _signals(finding)] == ["resolved", "pending"]
