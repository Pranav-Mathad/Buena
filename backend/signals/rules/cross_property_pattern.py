"""Rule 3 — ``cross_property_pattern`` (the portfolio-intelligence closer).

Two flavors from Part IX + the demo script (beats 1:00 and 1:45):

A. **Shared building.** If ≥2 properties inside a single building each carry
   recurring heating facts in the last 120 days, fire a portfolio-level
   signal keyed on the building — this is the "3 heating complaints across
   units sharing the same boiler" beat.
B. **Building-year cohort.** Group properties by year bucket
   (``pre-1990``, ``1990-2010``, ``post-2010``) and if ≥3 properties in the
   same bucket have heating issues inside the window, fire a portfolio-wide
   inspection recommendation — this is beat 1:45.

Both cases emit a ``property_id=None`` portfolio signal so the UI can
render it in the portfolio view instead of under a single property.
"""

from __future__ import annotations

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.signals.types import SignalCandidate

log = structlog.get_logger(__name__)

WINDOW_DAYS = 120
HEAT_PATTERN = r"heat|boiler|radiator|hot water"


def _year_bucket(year: int | None) -> str | None:
    """Classify a building's year_built into a demo-friendly cohort label."""
    if year is None:
        return None
    if year < 1990:
        return "pre-1990"
    if year <= 2010:
        return "1990–2010"
    return "post-2010"


async def _shared_building(session: AsyncSession) -> list[SignalCandidate]:
    """Flavor A — buildings where multiple units share the same issue."""
    result = await session.execute(
        text(
            """
            SELECT b.id AS building_id, b.address,
                   b.year_built,
                   COUNT(DISTINCT f.property_id) AS affected_properties,
                   COUNT(*) AS total_incidents,
                   ARRAY_AGG(DISTINCT p.name) AS property_names,
                   ARRAY_AGG(f.id::text ORDER BY f.created_at) AS fact_ids,
                   ARRAY_AGG(f.source_event_id::text ORDER BY f.created_at)
                     AS event_ids
            FROM facts f
            JOIN properties p ON p.id = f.property_id
            JOIN buildings b ON b.id = p.building_id
            WHERE f.section = 'maintenance'
              AND f.superseded_by IS NULL
              AND f.created_at >= now() - (:window || ' days')::interval
              AND (f.value ~* :pattern OR f.field ~* :pattern)
            GROUP BY b.id, b.address, b.year_built
            HAVING COUNT(DISTINCT f.property_id) >= 2
               AND COUNT(*) >= 3
            """
        ),
        {"window": str(WINDOW_DAYS), "pattern": HEAT_PATTERN},
    )

    out: list[SignalCandidate] = []
    for row in result.all():
        names = ", ".join(row.property_names or [])
        bucket = _year_bucket(row.year_built)
        context_excerpt = (
            f"Building: {row.address}\n"
            f"Year built: {row.year_built} ({bucket or 'unknown'})\n"
            f"Units affected: {row.affected_properties} — {names}\n"
            f"Heating incidents (last {WINDOW_DAYS}d): {row.total_incidents}"
        )
        evidence = [
            {"event_id": ev, "fact_id": fid}
            for ev, fid in zip(row.event_ids or [], row.fact_ids or [], strict=False)
            if ev or fid
        ]
        out.append(
            SignalCandidate(
                type="cross_property_pattern",
                severity="urgent",
                property_id=None,
                message=(
                    f"{row.total_incidents} heating complaints across "
                    f"{row.affected_properties} units sharing the same "
                    f"{row.address} boiler — system failure likely."
                ),
                evidence=evidence,
                context_excerpt=context_excerpt,
                action_hint={
                    "type": "building_inspection",
                    "subtype": "shared_boiler",
                    "building_id": str(row.building_id),
                    "building_address": row.address,
                    "year_built": row.year_built,
                    "properties": row.property_names,
                    "incidents": int(row.total_incidents),
                },
            )
        )
    return out


async def _building_year_cohort(session: AsyncSession) -> list[SignalCandidate]:
    """Flavor B — portfolio-wide alert on a building-year cohort."""
    result = await session.execute(
        text(
            """
            SELECT b.id AS building_id,
                   b.year_built,
                   p.id AS property_id,
                   p.name AS property_name,
                   COUNT(*) AS incidents,
                   ARRAY_AGG(f.id::text) AS fact_ids,
                   ARRAY_AGG(f.source_event_id::text) AS event_ids
            FROM facts f
            JOIN properties p ON p.id = f.property_id
            JOIN buildings b ON b.id = p.building_id
            WHERE f.section = 'maintenance'
              AND f.superseded_by IS NULL
              AND f.created_at >= now() - (:window || ' days')::interval
              AND (f.value ~* :pattern OR f.field ~* :pattern)
            GROUP BY b.id, b.year_built, p.id, p.name
            """
        ),
        {"window": str(WINDOW_DAYS), "pattern": HEAT_PATTERN},
    )

    buckets: dict[str, dict[str, list]] = {}
    for row in result.all():
        bucket = _year_bucket(row.year_built)
        if bucket is None:
            continue
        slot = buckets.setdefault(
            bucket,
            {"properties": [], "fact_ids": [], "event_ids": [], "incidents": 0},
        )
        if row.property_name not in slot["properties"]:
            slot["properties"].append(row.property_name)
        slot["fact_ids"].extend(row.fact_ids or [])
        slot["event_ids"].extend(row.event_ids or [])
        slot["incidents"] += int(row.incidents or 0)

    out: list[SignalCandidate] = []
    for bucket, data in buckets.items():
        affected = len(data["properties"])
        if affected < 3:
            continue
        names = ", ".join(data["properties"])
        context_excerpt = (
            f"Year bucket: {bucket}\n"
            f"Affected properties: {affected} — {names}\n"
            f"Heating incidents (last {WINDOW_DAYS}d): {data['incidents']}"
        )
        evidence = [
            {"event_id": ev, "fact_id": fid}
            for ev, fid in zip(
                data["event_ids"], data["fact_ids"], strict=False
            )
            if ev or fid
        ]
        out.append(
            SignalCandidate(
                type="cross_property_pattern",
                severity="high",
                property_id=None,
                message=(
                    f"Keystone detected: {affected} {bucket} boiler "
                    f"properties had heating issues this winter — schedule "
                    f"portfolio-wide inspection?"
                ),
                evidence=evidence,
                context_excerpt=context_excerpt,
                action_hint={
                    "type": "portfolio_inspection",
                    "subtype": "year_cohort",
                    "cohort": bucket,
                    "properties": data["properties"],
                    "incidents": data["incidents"],
                },
            )
        )
    return out


async def evaluate(session: AsyncSession) -> list[SignalCandidate]:
    """Run both flavors and return the combined candidate list."""
    shared = await _shared_building(session)
    cohort = await _building_year_cohort(session)
    log.info(
        "rule.cross_property_pattern",
        shared=len(shared),
        cohort=len(cohort),
    )
    return [*shared, *cohort]
