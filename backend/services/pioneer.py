"""Pioneer / Fastino learning layer.

Phase 5 implements the self-contained version: we read the
``approval_log`` table, compute per-signal-type approval rates, and turn
those into priority weights the UI surfaces ("Keystone is prioritizing
heating issues based on your behavior"). The interface is shaped so a
real Pioneer / Fastino integration can replace the local calc by
swapping :func:`compute_learning` — the dashboard endpoint doesn't care.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SignalTypeStats:
    """Per-signal-type counts + derived rates for the learning dashboard."""

    type: str
    proposed: int
    approved: int
    rejected: int
    edited: int
    approval_rate: float
    priority_weight: float


@dataclass(frozen=True)
class LearningSnapshot:
    """What the Settings > Learning page binds against."""

    signal_types: list[SignalTypeStats]
    top_priority: str | None
    trend_line: str
    sample_size: int


def _priority_weight(approval_rate: float, proposed: int) -> float:
    """Map approval rate + sample size to a [0.5, 1.5] priority weight.

    The mapping is deliberately gentle (± 50% of baseline) so that a single
    rejection doesn't flip a rule off. Small-sample stats get anchored
    toward 1.0 until at least five proposals are in.
    """
    if proposed == 0:
        return 1.0
    shrink = min(proposed / 5.0, 1.0)  # ramp up confidence after 5 samples
    weight = 1.0 + (approval_rate - 0.5) * shrink
    return max(0.5, min(1.5, weight))


def _trend_line(stats: list[SignalTypeStats]) -> str:
    """Produce the human-facing summary line."""
    if not stats:
        return "Keystone has not yet observed enough approvals to personalize priorities."
    top = max(stats, key=lambda s: s.priority_weight)
    if top.priority_weight <= 1.0 + 1e-6:
        return (
            "Keystone has not yet re-prioritized — approval rates are still "
            "near baseline across signal types."
        )
    pretty = top.type.replace("_", " ")
    return (
        f"Keystone is prioritizing **{pretty}** based on your approval "
        f"behavior ({int(round(top.approval_rate * 100))}% approved over "
        f"{top.proposed} proposals)."
    )


async def compute_learning(session: AsyncSession) -> LearningSnapshot:
    """Return the approval-rate dashboard snapshot computed from ``approval_log``."""
    result = await session.execute(
        text(
            """
            WITH logged AS (
                SELECT s.type,
                       a.decision,
                       COUNT(*) AS n
                FROM approval_log a
                JOIN signals s ON s.id = a.signal_id
                GROUP BY s.type, a.decision
            ),
            proposed AS (
                SELECT type, COUNT(*) AS proposed
                FROM signals
                GROUP BY type
            )
            SELECT p.type AS type,
                   p.proposed AS proposed,
                   COALESCE(SUM(CASE WHEN l.decision = 'approved' THEN l.n END), 0)
                       AS approved,
                   COALESCE(SUM(CASE WHEN l.decision = 'rejected' THEN l.n END), 0)
                       AS rejected,
                   COALESCE(SUM(CASE WHEN l.decision = 'edited'   THEN l.n END), 0)
                       AS edited
            FROM proposed p
            LEFT JOIN logged l ON l.type = p.type
            GROUP BY p.type, p.proposed
            ORDER BY p.proposed DESC
            """
        )
    )

    stats: list[SignalTypeStats] = []
    sample_size = 0
    for row in result.all():
        proposed = int(row.proposed or 0)
        approved = int(row.approved or 0)
        rejected = int(row.rejected or 0)
        edited = int(row.edited or 0)
        decided = approved + rejected + edited
        approval_rate = (approved + edited * 0.8) / decided if decided else 0.0
        weight = _priority_weight(approval_rate, proposed)
        stats.append(
            SignalTypeStats(
                type=row.type,
                proposed=proposed,
                approved=approved,
                rejected=rejected,
                edited=edited,
                approval_rate=approval_rate,
                priority_weight=weight,
            )
        )
        sample_size += proposed

    top = max(stats, key=lambda s: s.priority_weight, default=None)
    snapshot = LearningSnapshot(
        signal_types=stats,
        top_priority=top.type if top else None,
        trend_line=_trend_line(stats),
        sample_size=sample_size,
    )
    log.info(
        "pioneer.learning",
        types=len(stats),
        samples=sample_size,
        top=snapshot.top_priority,
    )
    return snapshot


def snapshot_to_json(snapshot: LearningSnapshot) -> dict[str, Any]:
    """Serialise a :class:`LearningSnapshot` into the settings-endpoint payload."""
    return {
        "sample_size": snapshot.sample_size,
        "top_priority": snapshot.top_priority,
        "trend_line": snapshot.trend_line,
        "signal_types": [
            {
                "type": s.type,
                "proposed": s.proposed,
                "approved": s.approved,
                "rejected": s.rejected,
                "edited": s.edited,
                "approval_rate": round(s.approval_rate, 3),
                "priority_weight": round(s.priority_weight, 3),
            }
            for s in snapshot.signal_types
        ],
    }
