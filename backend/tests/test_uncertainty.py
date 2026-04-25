"""Tests for Phase 9 Step 9.1 — uncertainty as first-class output.

Coverage:

* ``_apply_confidence_floor`` demotes sub-floor facts and preserves
  the rest.
* The Gemini schema's new ``uncertain[]`` round-trips into
  ``ExtractionResult.uncertain``.
* The validator's ``needs_review`` rejections route to
  ``uncertainty_events`` (via the worker helper) and are *not*
  written to ``rejected_updates``.
* End-to-end DB integration: a vague-rent email lands in
  ``uncertainty_events``, not in ``facts``.
* Step 9.2 regression: a building floor-count change from email
  still ends up in ``rejected_updates`` (hard-reject path), not in
  ``uncertainty_events``.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline import constraints  # noqa: F401 — registers constraints
from backend.pipeline.applier import apply as apply_plan
from backend.pipeline.applier import apply_uncertainties
from backend.pipeline.differ import DiffPlan, FactDecision, diff, load_current_facts
from backend.pipeline.extractor import (
    CONFIDENCE_FLOOR,
    _apply_confidence_floor,
)
from backend.pipeline.validator import (
    Rejection,
    persist_rejections,
    validate,
)
from backend.pipeline.worker import _needs_review_to_uncertainty
from backend.services.gemini import ExtractionResult
from connectors.migrations import apply_all as ensure_migrations

pytestmark_async = pytest.mark.asyncio


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


# ----------------------------------------------------------------------------
# Confidence floor — pure unit tests
# ----------------------------------------------------------------------------


def test_confidence_floor_keeps_high_confidence_facts() -> None:
    """A 0.95-confidence fact stays in facts_to_update."""
    result = ExtractionResult(
        category="maintenance",
        priority="high",
        facts_to_update=[
            {
                "section": "maintenance",
                "field": "open_water_leak",
                "value": "yes, basement",
                "confidence": 0.95,
            }
        ],
        summary="leak",
    )
    out = _apply_confidence_floor(result)
    assert len(out.facts_to_update) == 1
    assert out.uncertain == []


def test_confidence_floor_demotes_low_confidence_to_uncertain() -> None:
    """A sub-floor fact moves to uncertain[] with a clear reason."""
    result = ExtractionResult(
        category="financials",
        priority="low",
        facts_to_update=[
            {
                "section": "financials",
                "field": "rent_amount",
                "value": "maybe 1200 EUR",
                "confidence": 0.5,
            }
        ],
        summary="vague rent",
    )
    out = _apply_confidence_floor(result)
    assert out.facts_to_update == []
    assert len(out.uncertain) == 1
    item = out.uncertain[0]
    assert "below floor" in item["reason_uncertain"]
    assert item["relevant_section"] == "financials"
    assert item["relevant_field"] == "rent_amount"
    assert item["hypothesis"] == "maybe 1200 EUR"


def test_confidence_floor_partitions_mixed_list() -> None:
    """Mixed-confidence list splits cleanly above/below the floor."""
    result = ExtractionResult(
        category="other",
        priority="low",
        facts_to_update=[
            {"section": "maintenance", "field": "x", "value": "a", "confidence": 0.92},
            {"section": "lease", "field": "y", "value": "b", "confidence": 0.6},
            {"section": "financials", "field": "z", "value": "c", "confidence": 0.71},
        ],
        summary="mix",
    )
    out = _apply_confidence_floor(result)
    assert len(out.facts_to_update) == 2
    assert {f["field"] for f in out.facts_to_update} == {"x", "z"}
    assert len(out.uncertain) == 1
    assert out.uncertain[0]["relevant_field"] == "y"


def test_confidence_floor_threshold_is_inclusive_floor() -> None:
    """``confidence == CONFIDENCE_FLOOR`` passes; only strictly-below demotes."""
    result = ExtractionResult(
        category="x",
        priority="low",
        facts_to_update=[
            {"section": "s", "field": "f", "value": "v", "confidence": CONFIDENCE_FLOOR},
        ],
        summary="",
    )
    out = _apply_confidence_floor(result)
    assert len(out.facts_to_update) == 1


# ----------------------------------------------------------------------------
# ExtractionResult.uncertain round-trips
# ----------------------------------------------------------------------------


def test_extraction_result_uncertain_default_empty() -> None:
    """An ExtractionResult without explicit uncertain[] defaults to []."""
    r = ExtractionResult(
        category="other", priority="low", facts_to_update=[], summary=""
    )
    assert r.uncertain == []


def test_extraction_result_uncertain_carries_through() -> None:
    """Values in uncertain[] are preserved on construction."""
    items = [
        {
            "observation": "we should adjust the rent at some point",
            "reason_uncertain": "vague mention without timeframe",
            "relevant_section": "financials",
        }
    ]
    r = ExtractionResult(
        category="other",
        priority="low",
        facts_to_update=[],
        uncertain=items,
        summary="",
    )
    assert r.uncertain == items


# ----------------------------------------------------------------------------
# Validator needs_review → uncertainty mapping
# ----------------------------------------------------------------------------


def test_needs_review_routes_to_uncertainty_not_rejected() -> None:
    """A needs_review rejection from the validator becomes an uncertainty item."""
    rejection = Rejection(
        section="financials",
        field="rent_amount",
        proposed_value="1300 EUR",
        proposed_confidence=0.9,
        constraint_name="rent_amount_change_requires_addendum",
        reason="rent change with lease_addendum PDF; needs human confirmation",
        required_source_type="lease_addendum",
        needs_review=True,
    )
    item = _needs_review_to_uncertainty(rejection)
    assert item["relevant_section"] == "financials"
    assert item["relevant_field"] == "rent_amount"
    assert item["hypothesis"] == "1300 EUR"
    assert "rent change" in item["reason_uncertain"]
    assert item["source"].startswith("validator_needs_review:")


def test_validate_returns_both_kinds_separately() -> None:
    """validate() preserves the needs_review flag for downstream routing."""
    plan = DiffPlan(
        decisions=[
            FactDecision(
                section="financials",
                field="rent_amount",
                value="1300 EUR",
                confidence=0.9,
                supersedes_id=None,
                reason="test",
            ),
        ],
        skipped=[],
    )
    current_facts: dict[tuple[str, str], dict[str, Any]] = {
        ("financials", "rent_amount"): {
            "value": "1100 EUR",
            "confidence": 0.9,
            "source": "debug",
        }
    }
    # PDF + lease_addendum → needs_review
    _, rejections = validate(
        plan,
        event={"source": "pdf", "metadata": {"document_type": "lease_addendum"}},
        current_facts=current_facts,
    )
    assert len(rejections) == 1
    assert rejections[0].needs_review is True


# ----------------------------------------------------------------------------
# DB integration — vague rent email roundtrips through the pipeline
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uncertainty_event_lands_in_table_via_apply_uncertainties() -> None:
    """The applier helper inserts a row whose columns mirror the input dict."""
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")
    ensure_migrations()

    factory = get_sessionmaker()
    async with factory() as session:
        b = (
            await session.execute(
                text(
                    "INSERT INTO buildings (address) VALUES "
                    "('Test Uncertainty Bldg') RETURNING id"
                )
            )
        ).first()
        p = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES ('Test Uncertainty Prop', 'addr', :bid, ARRAY['x'])
                    RETURNING id
                    """
                ),
                {"bid": b.id},
            )
        ).first()
        ref = f"test-uncertainty-{uuid4().hex[:8]}"
        e = (
            await session.execute(
                text(
                    """
                    INSERT INTO events (source, source_ref, raw_content, property_id)
                    VALUES ('email', :ref, 'we might need to adjust the rent at some point', :pid)
                    RETURNING id
                    """
                ),
                {"ref": ref, "pid": p.id},
            )
        ).first()
        await session.commit()

    items = [
        {
            "observation": "we might need to adjust the rent at some point",
            "hypothesis": "rent_change_pending",
            "reason_uncertain": "vague mention without timeframe or amount",
            "relevant_section": "financials",
            "relevant_field": "rent_amount",
            "source": "gemini",
        }
    ]

    async with factory() as session:
        n = await apply_uncertainties(
            session, event_id=e.id, property_id=p.id, items=items
        )
        await session.commit()
    assert n == 1

    async with factory() as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT relevant_section, relevant_field, observation,
                           hypothesis, reason_uncertain, source, status
                    FROM uncertainty_events
                    WHERE event_id = :eid
                    """
                ),
                {"eid": e.id},
            )
        ).first()
        assert row is not None
        assert row.relevant_section == "financials"
        assert row.relevant_field == "rent_amount"
        assert row.observation.startswith("we might need to adjust the rent")
        assert row.hypothesis == "rent_change_pending"
        assert row.source == "gemini"
        assert row.status == "open"

        # Cleanup
        await session.execute(
            text("DELETE FROM uncertainty_events WHERE event_id = :eid"),
            {"eid": e.id},
        )
        await session.execute(text("DELETE FROM events WHERE id = :id"), {"id": e.id})
        await session.execute(
            text("DELETE FROM properties WHERE id = :id"), {"id": p.id}
        )
        await session.execute(text("DELETE FROM buildings WHERE id = :id"), {"id": b.id})
        await session.commit()


@pytest.mark.asyncio
async def test_floor_count_email_still_hard_rejects_after_step91() -> None:
    """Step 9.2 regression: a free-text floor-count change still goes to rejected_updates."""
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")
    ensure_migrations()

    factory = get_sessionmaker()
    async with factory() as session:
        b = (
            await session.execute(
                text(
                    "INSERT INTO buildings (address) VALUES "
                    "('Test 9.1 Regression') RETURNING id"
                )
            )
        ).first()
        p = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES ('Test 9.1 Prop', 'addr', :bid, ARRAY['x'])
                    RETURNING id
                    """
                ),
                {"bid": b.id},
            )
        ).first()
        # Pre-existing floor_count fact so the constraint sees a current value.
        ref0 = f"seed-{uuid4().hex[:8]}"
        e0 = (
            await session.execute(
                text(
                    """
                    INSERT INTO events (source, source_ref, raw_content, property_id)
                    VALUES ('debug', :ref, 'seed', :pid)
                    RETURNING id
                    """
                ),
                {"ref": ref0, "pid": p.id},
            )
        ).first()
        await session.execute(
            text(
                """
                INSERT INTO facts (
                  property_id, section, field, value, source_event_id,
                  confidence, valid_from
                ) VALUES (
                  :pid, 'building_overview', 'floor_count', '5', :eid, 0.95, now()
                )
                """
            ),
            {"pid": p.id, "eid": e0.id},
        )
        # Inbound email proposing floor_count = 8
        ref = f"reg-{uuid4().hex[:8]}"
        e = (
            await session.execute(
                text(
                    """
                    INSERT INTO events (source, source_ref, raw_content, property_id, metadata)
                    VALUES ('email', :ref, 'building has 8 floors now', :pid,
                            CAST('{}' AS JSONB))
                    RETURNING id
                    """
                ),
                {"ref": ref, "pid": p.id},
            )
        ).first()
        await session.commit()

    plan = DiffPlan(
        decisions=[
            FactDecision(
                section="building_overview",
                field="floor_count",
                value="8",
                confidence=0.9,
                supersedes_id=None,
                reason="email said so",
            )
        ],
        skipped=[],
    )
    factory = get_sessionmaker()
    async with factory() as session:
        current_facts = await load_current_facts(session, p.id)
        validated_plan, rejections = validate(
            plan,
            event={"source": "email", "metadata": {}},
            current_facts=current_facts,
        )
    assert validated_plan.decisions == []
    hard = [r for r in rejections if not r.needs_review]
    soft = [r for r in rejections if r.needs_review]
    assert len(hard) == 1
    assert hard[0].constraint_name == "building_floor_count_immutable"
    assert soft == []

    # Persist and verify the row is in rejected_updates, not uncertainty_events.
    async with factory() as session:
        await persist_rejections(
            session,
            event_id=e.id,
            property_id=p.id,
            building_id=None,
            liegenschaft_id=None,
            rejections=hard,
        )
        await session.commit()

    async with factory() as session:
        rej = (
            await session.execute(
                text(
                    "SELECT proposed_field, constraint_name, reviewed_status "
                    "FROM rejected_updates WHERE event_id = :eid"
                ),
                {"eid": e.id},
            )
        ).first()
        assert rej is not None
        assert rej.proposed_field == "floor_count"
        assert rej.constraint_name == "building_floor_count_immutable"
        assert rej.reviewed_status == "pending"
        unc_count = (
            await session.execute(
                text(
                    "SELECT COUNT(*) FROM uncertainty_events WHERE event_id = :eid"
                ),
                {"eid": e.id},
            )
        ).scalar()
        assert unc_count == 0, "floor-count rejection should not leak into uncertainty inbox"

        # Cleanup
        await session.execute(
            text("DELETE FROM rejected_updates WHERE event_id IN (:eid, :seed)"),
            {"eid": e.id, "seed": e0.id},
        )
        await session.execute(
            text("DELETE FROM facts WHERE property_id = :pid"), {"pid": p.id}
        )
        await session.execute(
            text("DELETE FROM events WHERE id IN (:eid, :seed)"),
            {"eid": e.id, "seed": e0.id},
        )
        await session.execute(
            text("DELETE FROM properties WHERE id = :pid"), {"pid": p.id}
        )
        await session.execute(
            text("DELETE FROM buildings WHERE id = :bid"), {"bid": b.id}
        )
        await session.commit()
