"""Materialize rendered markdown into the per-scope file tables.

Phase 12 — every fact write fans out to update the file rows for the
fact's scope plus all dependent scopes. A property fact regenerates
the property's file. A building fact regenerates the building's file
plus every property file under that building. A liegenschaft fact
cascades through buildings and their properties. The materialize step
runs in the same transaction as the fact write, so file rows are
never stale relative to facts (KEYSTONE Part XIV invariant).

LLM-free by design: the renderer is deterministic markdown. Keeping
LLM calls out of this layer guarantees the materialize step burns no
spend, no matter how often it fires.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_sessionmaker
from backend.pipeline.renderer import (
    render_building_markdown,
    render_liegenschaft_markdown,
    render_property_full,
)

log = structlog.get_logger(__name__)


#: Bumped any time the renderer's emitted shape changes. The CLI's
#: ``materialize-all`` re-renders rows whose stored generation_version
#: is older than this constant; in-flight applier propagation always
#: writes the current value.
GENERATION_VERSION: int = 1

#: Acceptable values for the ``trigger_scope`` column. ``bootstrap`` is
#: reserved for the CLI/migration backfill paths that have no triggering
#: event; everything else is the tier of the fact write that fanned
#: out to this row.
_VALID_TRIGGER_SCOPES: frozenset[str] = frozenset(
    {"property", "building", "liegenschaft", "bootstrap"}
)


def _summary(text_value: str | None, *, limit: int = 500) -> str | None:
    """Trim a free-text trigger summary to a stable column width."""
    if text_value is None:
        return None
    cleaned = " ".join(text_value.split())
    if not cleaned:
        return None
    return cleaned[:limit]


async def _count_facts(session: AsyncSession, scope: str, scope_id: UUID) -> int:
    """Count current (non-superseded) facts attached to a scope."""
    column = {
        "property": "property_id",
        "building": "building_id",
        "liegenschaft": "liegenschaft_id",
    }[scope]
    row = await session.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM facts
            WHERE {column} = :sid AND superseded_by IS NULL
            """
        ),
        {"sid": scope_id},
    )
    return int(row.scalar_one() or 0)


async def materialize_property(
    session: AsyncSession,
    property_id: UUID,
    *,
    trigger_event_id: UUID | None = None,
    trigger_scope: str = "property",
    trigger_summary: str | None = None,
) -> int:
    """Render and upsert the property's markdown + content_index row.

    Phase 12+ — calls :func:`render_property_full` so the materialized
    row carries both ``content_md`` (human/agent reading) and
    ``content_index`` (typed JSON for programmatic consumers). Returns
    1 on write, 0 if the property doesn't exist.
    """
    if trigger_scope not in _VALID_TRIGGER_SCOPES:
        raise ValueError(f"trigger_scope {trigger_scope!r} not in {_VALID_TRIGGER_SCOPES}")

    try:
        result = await render_property_full(session, property_id)
    except ValueError:
        log.warning(
            "materializer.property.missing",
            property_id=str(property_id),
        )
        return 0
    fact_count = await _count_facts(session, "property", property_id)

    await session.execute(
        text(
            """
            INSERT INTO property_files (
              property_id, content_md, content_index, fact_count,
              last_rendered_at,
              trigger_event_id, trigger_scope, trigger_summary,
              generation_version
            ) VALUES (
              :pid, :md, CAST(:idx AS JSONB), :fc, now(),
              :eid, :scope, :summary, :ver
            )
            ON CONFLICT (property_id) DO UPDATE SET
              content_md = EXCLUDED.content_md,
              content_index = EXCLUDED.content_index,
              fact_count = EXCLUDED.fact_count,
              last_rendered_at = EXCLUDED.last_rendered_at,
              trigger_event_id = EXCLUDED.trigger_event_id,
              trigger_scope = EXCLUDED.trigger_scope,
              trigger_summary = EXCLUDED.trigger_summary,
              generation_version = EXCLUDED.generation_version
            """
        ),
        {
            "pid": property_id,
            "md": result.markdown,
            "idx": json.dumps(result.content_index),
            "fc": fact_count,
            "eid": trigger_event_id,
            "scope": trigger_scope,
            "summary": _summary(trigger_summary),
            "ver": GENERATION_VERSION,
        },
    )
    return 1


async def materialize_building(
    session: AsyncSession,
    building_id: UUID,
    *,
    trigger_event_id: UUID | None = None,
    trigger_scope: str = "building",
    trigger_summary: str | None = None,
) -> int:
    """Render and upsert the building's markdown row."""
    if trigger_scope not in _VALID_TRIGGER_SCOPES:
        raise ValueError(f"trigger_scope {trigger_scope!r} not in {_VALID_TRIGGER_SCOPES}")

    try:
        md = await render_building_markdown(session, building_id)
    except ValueError:
        log.warning(
            "materializer.building.missing",
            building_id=str(building_id),
        )
        return 0
    fact_count = await _count_facts(session, "building", building_id)

    await session.execute(
        text(
            """
            INSERT INTO building_files (
              building_id, content_md, fact_count, last_rendered_at,
              trigger_event_id, trigger_scope, trigger_summary,
              generation_version
            ) VALUES (
              :bid, :md, :fc, now(),
              :eid, :scope, :summary, :ver
            )
            ON CONFLICT (building_id) DO UPDATE SET
              content_md = EXCLUDED.content_md,
              fact_count = EXCLUDED.fact_count,
              last_rendered_at = EXCLUDED.last_rendered_at,
              trigger_event_id = EXCLUDED.trigger_event_id,
              trigger_scope = EXCLUDED.trigger_scope,
              trigger_summary = EXCLUDED.trigger_summary,
              generation_version = EXCLUDED.generation_version
            """
        ),
        {
            "bid": building_id,
            "md": md,
            "fc": fact_count,
            "eid": trigger_event_id,
            "scope": trigger_scope,
            "summary": _summary(trigger_summary),
            "ver": GENERATION_VERSION,
        },
    )
    return 1


async def materialize_liegenschaft(
    session: AsyncSession,
    liegenschaft_id: UUID,
    *,
    trigger_event_id: UUID | None = None,
    trigger_scope: str = "liegenschaft",
    trigger_summary: str | None = None,
) -> int:
    """Render and upsert the WEG/Liegenschaft markdown row."""
    if trigger_scope not in _VALID_TRIGGER_SCOPES:
        raise ValueError(f"trigger_scope {trigger_scope!r} not in {_VALID_TRIGGER_SCOPES}")

    try:
        md = await render_liegenschaft_markdown(session, liegenschaft_id)
    except ValueError:
        log.warning(
            "materializer.liegenschaft.missing",
            liegenschaft_id=str(liegenschaft_id),
        )
        return 0
    fact_count = await _count_facts(session, "liegenschaft", liegenschaft_id)

    await session.execute(
        text(
            """
            INSERT INTO liegenschaft_files (
              liegenschaft_id, content_md, fact_count, last_rendered_at,
              trigger_event_id, trigger_scope, trigger_summary,
              generation_version
            ) VALUES (
              :lid, :md, :fc, now(),
              :eid, :scope, :summary, :ver
            )
            ON CONFLICT (liegenschaft_id) DO UPDATE SET
              content_md = EXCLUDED.content_md,
              fact_count = EXCLUDED.fact_count,
              last_rendered_at = EXCLUDED.last_rendered_at,
              trigger_event_id = EXCLUDED.trigger_event_id,
              trigger_scope = EXCLUDED.trigger_scope,
              trigger_summary = EXCLUDED.trigger_summary,
              generation_version = EXCLUDED.generation_version
            """
        ),
        {
            "lid": liegenschaft_id,
            "md": md,
            "fc": fact_count,
            "eid": trigger_event_id,
            "scope": trigger_scope,
            "summary": _summary(trigger_summary),
            "ver": GENERATION_VERSION,
        },
    )
    return 1


async def materialize_all(session: AsyncSession) -> dict[str, int]:
    """Bootstrap every property/building/liegenschaft file row.

    Used by ``connectors.cli materialize-all`` after the migration runs
    (and by Cloud Run's startup script) to fill the cache up front.
    Caller commits the surrounding transaction.
    """
    properties_written = 0
    buildings_written = 0
    liegenschaften_written = 0

    rows = (await session.execute(text("SELECT id FROM properties"))).all()
    for r in rows:
        properties_written += await materialize_property(
            session,
            UUID(str(r.id)),
            trigger_scope="bootstrap",
            trigger_summary="Initial materialization (Phase 12 backfill)",
        )

    rows = (await session.execute(text("SELECT id FROM buildings"))).all()
    for r in rows:
        buildings_written += await materialize_building(
            session,
            UUID(str(r.id)),
            trigger_scope="bootstrap",
            trigger_summary="Initial materialization (Phase 12 backfill)",
        )

    rows = (await session.execute(text("SELECT id FROM liegenschaften"))).all()
    for r in rows:
        liegenschaften_written += await materialize_liegenschaft(
            session,
            UUID(str(r.id)),
            trigger_scope="bootstrap",
            trigger_summary="Initial materialization (Phase 12 backfill)",
        )

    log.info(
        "materializer.all.done",
        properties=properties_written,
        buildings=buildings_written,
        liegenschaften=liegenschaften_written,
    )
    return {
        "properties": properties_written,
        "buildings": buildings_written,
        "liegenschaften": liegenschaften_written,
    }


async def propagate_after_fact_write(
    session: AsyncSession,
    *,
    property_id: UUID | None = None,
    building_id: UUID | None = None,
    liegenschaft_id: UUID | None = None,
    trigger_event_id: UUID | None = None,
    trigger_summary: str | None = None,
) -> dict[str, int]:
    """Fan out materialization for a single fact write.

    Operates inside the caller's transaction — no session creation, no
    commit. The Phase 12 invariant (KEYSTONE Part XIV) is that the
    fact write and every dependent file row update share one
    transaction, so an exception here rolls the whole write back.

    Cascade rules:
      - ``property_id`` set → property's file refreshed.
      - ``building_id`` set → building's file refreshed AND every
        property under it gets ``trigger_scope='building'``.
      - ``liegenschaft_id`` set → WEG's file refreshed AND every
        building + property under it gets ``trigger_scope='liegenschaft'``.

    Scaling note: a single liegenschaft fact in Buena's data
    propagates to ~52 property files. Acceptable for Buena volume;
    the renderer is deterministic and fast (median 2 KB markdown).
    Documented as the scaling boundary in DECISIONS.md.
    """
    counts: dict[str, int] = {"properties": 0, "buildings": 0, "liegenschaften": 0}

    if liegenschaft_id is not None:
        counts["liegenschaften"] += await materialize_liegenschaft(
            session,
            liegenschaft_id,
            trigger_event_id=trigger_event_id,
            trigger_scope="liegenschaft",
            trigger_summary=trigger_summary,
        )
        buildings = (
            await session.execute(
                text("SELECT id FROM buildings WHERE liegenschaft_id = :lid"),
                {"lid": liegenschaft_id},
            )
        ).all()
        for b in buildings:
            counts["buildings"] += await materialize_building(
                session,
                UUID(str(b.id)),
                trigger_event_id=trigger_event_id,
                trigger_scope="liegenschaft",
                trigger_summary=trigger_summary,
            )
        properties = (
            await session.execute(
                text(
                    """
                    SELECT p.id FROM properties p
                    JOIN buildings b ON b.id = p.building_id
                    WHERE b.liegenschaft_id = :lid
                    """
                ),
                {"lid": liegenschaft_id},
            )
        ).all()
        for p in properties:
            counts["properties"] += await materialize_property(
                session,
                UUID(str(p.id)),
                trigger_event_id=trigger_event_id,
                trigger_scope="liegenschaft",
                trigger_summary=trigger_summary,
            )

    if building_id is not None:
        counts["buildings"] += await materialize_building(
            session,
            building_id,
            trigger_event_id=trigger_event_id,
            trigger_scope="building",
            trigger_summary=trigger_summary,
        )
        properties = (
            await session.execute(
                text("SELECT id FROM properties WHERE building_id = :bid"),
                {"bid": building_id},
            )
        ).all()
        for p in properties:
            counts["properties"] += await materialize_property(
                session,
                UUID(str(p.id)),
                trigger_event_id=trigger_event_id,
                trigger_scope="building",
                trigger_summary=trigger_summary,
            )

    if property_id is not None:
        counts["properties"] += await materialize_property(
            session,
            property_id,
            trigger_event_id=trigger_event_id,
            trigger_scope="property",
            trigger_summary=trigger_summary,
        )
    return counts


def run_materialize_all() -> dict[str, int]:
    """Sync wrapper for the CLI subcommand (and other top-level callers)."""

    async def _go() -> dict[str, int]:
        factory = get_sessionmaker()
        async with factory() as session:
            counts = await materialize_all(session)
            await session.commit()
            return counts

    return asyncio.run(_go())
