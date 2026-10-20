"""Pure parts of the worker's pipeline steps."""

from __future__ import annotations

from backend.pipeline.differ import DiffPlan, FactDecision
from backend.pipeline.validator import Rejection
from backend.pipeline.worker import _drop_hard_contradictions


def _decision(section: str, field: str) -> FactDecision:
    return FactDecision(
        section=section, field=field, value="v", confidence=0.9, supersedes_id=None, reason="new"
    )


def _contradiction(section: str, field: str, *, hard: bool) -> Rejection:
    return Rejection(
        section=section,
        field=field,
        proposed_value="v",
        proposed_confidence=0.0,
        constraint_name="semantic_consistency",
        reason="contradicts the file",
        required_source_type=None,
        needs_review=not hard,
    )


def test_hard_contradictions_remove_their_decision_and_soft_ones_keep_it() -> None:
    plan = DiffPlan(
        decisions=[
            _decision("overview", "floor_count"),
            _decision("tenants", "current_tenant_name"),
            _decision("maintenance", "open_water_damage"),
        ],
        skipped=[("lease.end_date", "unchanged")],
    )

    result = _drop_hard_contradictions(
        plan,
        [
            _contradiction("overview", "floor_count", hard=True),
            _contradiction("tenants", "current_tenant_name", hard=False),
        ],
    )

    assert [(d.section, d.field) for d in result.decisions] == [
        ("tenants", "current_tenant_name"),
        ("maintenance", "open_water_damage"),
    ]
    assert result.skipped == plan.skipped


def test_plan_is_unchanged_without_hard_contradictions() -> None:
    plan = DiffPlan(decisions=[_decision("overview", "floor_count")], skipped=[])

    assert (
        _drop_hard_contradictions(plan, [_contradiction("overview", "floor_count", hard=False)])
        is plan
    )
