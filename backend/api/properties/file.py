"""The property file: rendered markdown and the structured, sectioned view."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.properties.file_sections import (
    _AUTHORITATIVE_DOCUMENT_TYPES,
    _CONTEXT_SECTIONS,
    _SECTION_LABELS,
    PropertyFile,
    PropertyFileFact,
    PropertyFileSection,
    _build_context_section,
    _build_stammdaten_section,
)
from backend.db.session import get_session
from backend.pipeline.renderer import (
    CONTEXT_LABELS,
    CONTEXT_LIMIT,
    _building_for_property,
    _detect_property_language,
    _fetch_stammdaten,
    _liegenschaft_for_building,
    _recent_events_for_scope,
    render_markdown,
)

router = APIRouter()
log = structlog.get_logger(__name__)


@router.get("/{property_id}/markdown", response_class=PlainTextResponse)
async def property_markdown(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the canonical markdown document for a property.

    Reads the materialized ``property_files`` row when one
    exists (the common path after the applier hook fired) and falls
    back to a live render only when the cache is missing. The cache is
    refreshed atomically on every fact write, so the materialized row
    is guaranteed to reflect the current fact set.

    The fallback path does not write back from a GET handler; the next
    fact write at this scope will populate it. ``materialize-all`` is
    the explicit bootstrap path. 30-second public Cache-Control + ETag
    based on ``last_rendered_at`` give browsers free perceived speed
    without staleness risk (SSE / polling already invalidate on real
    change).
    """
    cached = (
        await session.execute(
            text(
                "SELECT content_md, last_rendered_at FROM property_files WHERE property_id = :pid"
            ),
            {"pid": property_id},
        )
    ).first()
    if cached is not None:
        log.info(
            "properties.markdown.cache_hit",
            property_id=str(property_id),
            length=len(cached.content_md),
        )
        return PlainTextResponse(
            content=cached.content_md,
            media_type="text/markdown; charset=utf-8",
            headers={
                "Cache-Control": "public, max-age=30",
                "ETag": f'"{int(cached.last_rendered_at.timestamp())}"',
            },
        )

    try:
        body = await render_markdown(session, property_id)
    except ValueError as exc:
        log.info("properties.markdown.not_found", property_id=str(property_id))
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info(
        "properties.markdown.cache_miss_render",
        property_id=str(property_id),
        length=len(body),
    )
    return PlainTextResponse(content=body, media_type="text/markdown; charset=utf-8")


@router.get("/{property_id}/file", response_model=PropertyFile)
async def property_file(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PropertyFile:
    """Return the structured property file: sections + facts + uncertainties.

    The frontend's ``PropertyDetail`` page renders one section per group
    with a ``[source]`` button on each fact that opens the originating
    event. Open ``uncertainty_events`` are appended as a ``Needs Review``
    section so the user sees gaps explicitly rather than silently.

    The view is a true superset of the markdown: Stammdaten, Building
    Context, and WEG Context are surfaced here too so a property with
    zero extracted facts still renders its master record + parent-tier
    activity instead of an empty placeholder.
    """
    meta_row = (
        await session.execute(
            text(
                """
                SELECT id, name, address
                FROM properties
                WHERE id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if meta_row is None:
        raise HTTPException(status_code=404, detail="property not found")

    fact_rows = (
        await session.execute(
            text(
                """
                SELECT f.id AS fact_id,
                       f.section, f.field, f.value, f.confidence,
                       f.created_at,
                       f.human_edited, f.edited_by,
                       e.source AS event_source,
                       e.received_at AS event_received_at,
                       e.metadata->>'document_type' AS document_type,
                       f.source_event_id
                FROM facts f
                LEFT JOIN events e ON e.id = f.source_event_id
                WHERE f.property_id = :pid
                  AND f.superseded_by IS NULL
                ORDER BY f.section ASC, f.created_at DESC
                """
            ),
            {"pid": property_id},
        )
    ).all()

    uncertainty_rows = (
        await session.execute(
            text(
                """
                SELECT observation, hypothesis, reason_uncertain,
                       relevant_section, created_at
                FROM uncertainty_events
                WHERE property_id = :pid
                  AND status = 'open'
                ORDER BY created_at DESC
                """
            ),
            {"pid": property_id},
        )
    ).all()

    grouped: dict[str, list[PropertyFileFact]] = {}
    title_order: list[str] = []
    context_titles: set[str] = set()

    for row in fact_rows:
        title = _SECTION_LABELS.get(row.section, row.section.replace("_", " ").title())
        if title not in grouped:
            grouped[title] = []
            title_order.append(title)
        if row.section in _CONTEXT_SECTIONS:
            context_titles.add(title)

        # Prefer the document type for source labels (so PDFs show as
        # ``Mietvertrag`` rather than ``letter``); fall back to the raw
        # event source; ``stammdaten`` for facts that were never linked
        # to an event (master-data load). Hand-edited facts overshadow
        # all of these: the human is the source of truth.
        doc_type = (row.document_type or "").strip()
        if row.human_edited:
            source_label = "operator"
        elif doc_type and doc_type in _AUTHORITATIVE_DOCUMENT_TYPES:
            source_label = doc_type
        elif row.event_source:
            source_label = row.event_source
        else:
            source_label = "stammdaten"

        when = row.event_received_at or row.created_at
        grouped[title].append(
            PropertyFileFact(
                value=row.value,
                source=source_label,
                date=when.date().isoformat(),
                date_iso=when.isoformat() if when else None,
                confidence=(float(row.confidence) if row.confidence is not None else None),
                section=row.section,
                field=row.field,
                event_id=str(row.source_event_id) if row.source_event_id else None,
                fact_id=str(row.fact_id),
                human_edited=bool(row.human_edited),
                edited_by=row.edited_by,
            )
        )

    sections: list[PropertyFileSection] = [
        PropertyFileSection(
            title=title,
            facts=grouped[title],
            is_context=title in context_titles,
        )
        for title in title_order
    ]

    if uncertainty_rows:
        review_facts: list[PropertyFileFact] = []
        for u in uncertainty_rows:
            value = u.observation
            if u.hypothesis:
                value = f"{u.observation} (hypothesis: {u.hypothesis})"
            review_facts.append(
                PropertyFileFact(
                    value=value,
                    source="extractor",
                    date=u.created_at.date().isoformat(),
                    date_iso=u.created_at.isoformat() if u.created_at else None,
                    reason=u.reason_uncertain,
                    section=u.relevant_section or "",
                    field="",
                )
            )
        sections.append(
            PropertyFileSection(
                title="Needs Review (Zu prüfen)",
                facts=review_facts,
                is_uncertain=True,
            )
        )

    # Stammdaten + parent-tier context: same data the markdown view
    # surfaces. Detect language once so labels match the property's
    # dominant event language.
    lang = await _detect_property_language(session, property_id)

    stammdaten = await _fetch_stammdaten(session, property_id)
    stammdaten_section = _build_stammdaten_section(stammdaten, lang) if stammdaten else None
    if stammdaten_section is not None:
        # Stammdaten is the master record; prepend so it reads first.
        sections.insert(0, stammdaten_section)

    building_id, _ = await _building_for_property(session, property_id)
    if building_id is not None:
        building_events = await _recent_events_for_scope(
            session,
            scope="building",
            scope_id=building_id,
            limit=CONTEXT_LIMIT,
        )
        building_section = _build_context_section(
            building_events, title=CONTEXT_LABELS[lang]["building_context"]
        )
        if building_section is not None:
            sections.append(building_section)

        liegenschaft_id, _ = await _liegenschaft_for_building(session, building_id)
        if liegenschaft_id is not None:
            weg_events = await _recent_events_for_scope(
                session,
                scope="liegenschaft",
                scope_id=liegenschaft_id,
                limit=CONTEXT_LIMIT,
            )
            weg_section = _build_context_section(
                weg_events, title=CONTEXT_LABELS[lang]["weg_context"]
            )
            if weg_section is not None:
                sections.append(weg_section)

    log.info(
        "properties.file.render",
        property_id=str(property_id),
        sections=len(sections),
        facts=sum(len(s.facts) for s in sections),
    )
    return PropertyFile(
        id=meta_row.id,
        name=meta_row.name,
        address=meta_row.address,
        sections=sections,
    )
