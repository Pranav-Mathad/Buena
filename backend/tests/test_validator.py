"""Tests for Phase 9 Step 9.2 — constraint validator.

Pure-function tests on each constraint (no DB) plus an end-to-end
``validate()`` test on a representative DiffPlan. Persistence to the
``rejected_updates`` table is tested separately via a DB-backed
roundtrip.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline import constraints  # noqa: F401 — registers everything
from backend.pipeline.differ import DiffPlan, FactDecision
from backend.pipeline.validator import (
    REGISTRY,
    ValidationResult,
    constraints_for,
    persist_rejections,
    validate,
)
from connectors.migrations import apply_all as ensure_migrations


def _decision(section: str, field: str, value: str = "100") -> FactDecision:
    """Tiny factory for a FactDecision under test."""
    return FactDecision(
        section=section,
        field=field,
        value=value,
        confidence=0.9,
        supersedes_id=None,
        reason="test",
    )


def _email(metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    """Email-shaped event."""
    return {"source": "email", "metadata": metadata or {}}


def _pdf(document_type: str | None = None) -> dict[str, Any]:
    """PDF-shaped event with optional document_type."""
    metadata: dict[str, Any] = {}
    if document_type is not None:
        metadata["document_type"] = document_type
    return {"source": "pdf", "metadata": metadata}


# ----------------------------------------------------------------------------
# Per-constraint unit tests
# ----------------------------------------------------------------------------


class TestBuildingFloorCount:
    def test_no_prior_passes(self) -> None:
        c = next(iter(constraints_for("building_overview", "floor_count")))
        v = c.check(_decision("building_overview", "floor_count", "5"), None, _email())
        assert v.status == "passed", v

    def test_email_change_rejected(self) -> None:
        c = next(iter(constraints_for("building_overview", "floor_count")))
        current = {"value": "5", "confidence": 0.9, "source": "debug"}
        v = c.check(
            _decision("building_overview", "floor_count", "8"),
            current,
            _email(),
        )
        assert v.status == "rejected"
        assert "structural_permit" in (v.required_source_type or "")

    def test_structural_permit_needs_review(self) -> None:
        c = next(iter(constraints_for("building_overview", "floor_count")))
        current = {"value": "5", "confidence": 0.9, "source": "pdf"}
        v = c.check(
            _decision("building_overview", "floor_count", "8"),
            current,
            _pdf(document_type="structural_permit"),
        )
        assert v.status == "needs_review"


class TestBuildingYearBuilt:
    def test_no_prior_passes(self) -> None:
        c = next(iter(constraints_for("building_overview", "year_built")))
        v = c.check(_decision("building_overview", "year_built", "1965"), None, _email())
        assert v.status == "passed"

    def test_any_change_rejected_even_with_pdf(self) -> None:
        c = next(iter(constraints_for("building_overview", "year_built")))
        current = {"value": "1965", "confidence": 0.9, "source": "debug"}
        v = c.check(
            _decision("building_overview", "year_built", "1970"),
            current,
            _pdf(document_type="structural_permit"),
        )
        # year-built is hard-immutable; even a permit doesn't move it.
        assert v.status == "rejected"


class TestRentAmountChange:
    def test_email_rent_change_rejected(self) -> None:
        c = next(iter(constraints_for("financials", "rent_amount")))
        current = {"value": "1100 EUR", "confidence": 0.9, "source": "debug"}
        v = c.check(
            _decision("financials", "rent_amount", "1300 EUR"),
            current,
            _email(),
        )
        assert v.status == "rejected"
        assert v.required_source_type == "lease_addendum"

    def test_lease_addendum_needs_review(self) -> None:
        c = next(iter(constraints_for("financials", "rent_amount")))
        current = {"value": "1100 EUR", "confidence": 0.9, "source": "pdf"}
        v = c.check(
            _decision("financials", "rent_amount", "1300 EUR"),
            current,
            _pdf(document_type="lease_addendum"),
        )
        assert v.status == "needs_review"

    def test_first_time_set_passes(self) -> None:
        c = next(iter(constraints_for("financials", "rent_amount")))
        v = c.check(
            _decision("financials", "rent_amount", "1100 EUR"),
            None,
            _email(),
        )
        assert v.status == "passed"


class TestTenantIdentityChange:
    def test_email_change_rejected(self) -> None:
        c = next(iter(constraints_for("tenants", "current_tenant_name")))
        current = {"value": "Anna Müller", "confidence": 0.9, "source": "debug"}
        v = c.check(
            _decision("tenants", "current_tenant_name", "Boris Schmidt"),
            current,
            _email(),
        )
        assert v.status == "rejected"
        assert v.required_source_type == "lease"

    def test_lease_pdf_needs_review(self) -> None:
        c = next(iter(constraints_for("tenants", "current_tenant_name")))
        current = {"value": "Anna Müller", "confidence": 0.9, "source": "pdf"}
        v = c.check(
            _decision("tenants", "current_tenant_name", "Boris Schmidt"),
            current,
            _pdf(document_type="lease"),
        )
        assert v.status == "needs_review"


class TestOwnerChange:
    def test_email_owner_change_rejected(self) -> None:
        c = next(iter(constraints_for("overview", "owner_name")))
        current = {"value": "Hans Becker", "confidence": 0.9, "source": "debug"}
        v = c.check(
            _decision("overview", "owner_name", "Petra Klein"),
            current,
            _email(),
        )
        assert v.status == "rejected"
        assert v.required_source_type == "kaufvertrag"

    def test_kaufvertrag_needs_review(self) -> None:
        c = next(iter(constraints_for("overview", "owner_name")))
        current = {"value": "Hans Becker", "confidence": 0.9, "source": "pdf"}
        v = c.check(
            _decision("overview", "owner_name", "Petra Klein"),
            current,
            _pdf(document_type="kaufvertrag"),
        )
        assert v.status == "needs_review"


class TestPropertySquareMeters:
    def test_within_tolerance_passes(self) -> None:
        c = next(iter(constraints_for("overview", "square_meters_qm")))
        current = {"value": "70 m²", "confidence": 0.9, "source": "debug"}
        v = c.check(
            _decision("overview", "square_meters_qm", "72 m²"),
            current,
            _email(),
        )
        assert v.status == "passed"

    def test_out_of_tolerance_needs_review(self) -> None:
        c = next(iter(constraints_for("overview", "square_meters_qm")))
        current = {"value": "70 m²", "confidence": 0.9, "source": "debug"}
        v = c.check(
            _decision("overview", "square_meters_qm", "85 m²"),
            current,
            _email(),
        )
        assert v.status == "needs_review"

    def test_out_of_tolerance_with_vermessung_passes(self) -> None:
        c = next(iter(constraints_for("overview", "square_meters_qm")))
        current = {"value": "70 m²", "confidence": 0.9, "source": "pdf"}
        v = c.check(
            _decision("overview", "square_meters_qm", "85 m²"),
            current,
            _pdf(document_type="vermessungsprotokoll"),
        )
        assert v.status == "passed"


class TestComplianceWildcard:
    def test_email_rejected_any_field(self) -> None:
        # Wildcard — should fire on any field under 'compliance'.
        cs = constraints_for("compliance", "brandschutznachweis")
        assert cs, "compliance wildcard not loaded"
        c = cs[0]
        v = c.check(
            _decision("compliance", "brandschutznachweis", "expires 2027"),
            None,
            _email(),
        )
        assert v.status == "rejected"

    def test_web_passes(self) -> None:
        c = constraints_for("compliance", "any_field")[0]
        v = c.check(
            _decision("compliance", "any_field", "x"),
            None,
            {"source": "web", "metadata": {}},
        )
        assert v.status == "passed"

    def test_pdf_unknown_doctype_needs_review(self) -> None:
        c = constraints_for("compliance", "any_field")[0]
        v = c.check(
            _decision("compliance", "any_field", "x"),
            None,
            _pdf(document_type=None),
        )
        assert v.status == "needs_review"

    def test_pdf_kaufvertrag_passes(self) -> None:
        c = constraints_for("compliance", "any_field")[0]
        v = c.check(
            _decision("compliance", "any_field", "x"),
            None,
            _pdf(document_type="kaufvertrag"),
        )
        assert v.status == "passed"

    def test_pdf_invoice_doctype_rejected(self) -> None:
        c = constraints_for("compliance", "any_field")[0]
        v = c.check(
            _decision("compliance", "any_field", "x"),
            None,
            _pdf(document_type="invoice"),
        )
        assert v.status == "rejected"


# ----------------------------------------------------------------------------
# validate() integration
# ----------------------------------------------------------------------------


def test_validate_filters_plan() -> None:
    """validate() removes rejected decisions and emits Rejection rows."""
    plan = DiffPlan(
        decisions=[
            _decision("financials", "rent_amount", "1300 EUR"),  # will be rejected
            _decision("maintenance", "open_water_leak", "yes"),  # no constraint
            _decision("overview", "owner_name", "Petra Klein"),  # will be rejected
        ],
        skipped=[],
    )
    current_facts: dict[tuple[str, str], dict[str, Any]] = {
        ("financials", "rent_amount"): {"value": "1100 EUR", "confidence": 0.9, "source": "debug"},
        ("overview", "owner_name"): {"value": "Hans Becker", "confidence": 0.9, "source": "debug"},
    }
    filtered, rejections = validate(plan, event=_email(), current_facts=current_facts)
    assert len(filtered.decisions) == 1
    assert filtered.decisions[0].field == "open_water_leak"
    assert len(rejections) == 2
    names = {r.constraint_name for r in rejections}
    assert "rent_amount_change_requires_addendum" in names
    assert "owner_change_requires_kaufvertrag" in names


def test_validate_first_match_wins() -> None:
    """When a decision triggers a constraint, the first verdict is reported."""
    plan = DiffPlan(
        decisions=[_decision("compliance", "x", "expires 2027")],
        skipped=[],
    )
    filtered, rejections = validate(plan, event=_email(), current_facts={})
    assert filtered.decisions == []
    assert len(rejections) == 1
    assert rejections[0].constraint_name.endswith(
        "compliance_facts_require_authoritative_source"
    )


# ----------------------------------------------------------------------------
# persist_rejections — DB roundtrip
# ----------------------------------------------------------------------------

pytestmark_db = pytest.mark.asyncio


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


@pytest.mark.asyncio
async def test_persist_rejections_roundtrip() -> None:
    """One decision in → one row in rejected_updates with correct columns."""
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")
    ensure_migrations()

    factory = get_sessionmaker()
    async with factory() as session:
        # Fixture: building + property + event
        b = (
            await session.execute(
                text(
                    "INSERT INTO buildings (address) VALUES "
                    "('Test Validator Bldg') RETURNING id"
                )
            )
        ).first()
        p = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES ('Test Validator Prop', 'Test Addr', :bid, ARRAY['x'])
                    RETURNING id
                    """
                ),
                {"bid": b.id},
            )
        ).first()
        ref = f"test-validator-{uuid4().hex[:8]}"
        e = (
            await session.execute(
                text(
                    """
                    INSERT INTO events (source, source_ref, raw_content, property_id)
                    VALUES ('email', :ref, 'test event body', :pid)
                    RETURNING id
                    """
                ),
                {"ref": ref, "pid": p.id},
            )
        ).first()
        await session.commit()

    # Use the validate() output shape directly.
    plan = DiffPlan(
        decisions=[_decision("financials", "rent_amount", "1300 EUR")],
        skipped=[],
    )
    current_facts: dict[tuple[str, str], dict[str, Any]] = {
        ("financials", "rent_amount"): {
            "value": "1100 EUR",
            "confidence": 0.9,
            "source": "debug",
        },
    }
    _, rejections = validate(plan, event=_email(), current_facts=current_facts)
    assert len(rejections) == 1

    async with factory() as session:
        await persist_rejections(
            session,
            event_id=e.id,
            property_id=p.id,
            building_id=None,
            liegenschaft_id=None,
            rejections=rejections,
        )
        await session.commit()

    async with factory() as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT proposed_section, proposed_field, proposed_value,
                           constraint_name, reviewed_status, required_source_type
                    FROM rejected_updates
                    WHERE event_id = :eid
                    """
                ),
                {"eid": e.id},
            )
        ).first()
        assert row is not None
        assert row.proposed_section == "financials"
        assert row.proposed_field == "rent_amount"
        assert row.constraint_name == "rent_amount_change_requires_addendum"
        assert row.reviewed_status == "pending"
        assert row.required_source_type == "lease_addendum"

        # Cleanup
        await session.execute(
            text("DELETE FROM rejected_updates WHERE event_id = :eid"),
            {"eid": e.id},
        )
        await session.execute(
            text("DELETE FROM events WHERE id = :eid"), {"eid": e.id}
        )
        await session.execute(
            text("DELETE FROM properties WHERE id = :pid"), {"pid": p.id}
        )
        await session.execute(
            text("DELETE FROM buildings WHERE id = :bid"), {"bid": b.id}
        )
        await session.commit()
