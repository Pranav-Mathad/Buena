"""Properties API — listing, creation (with Tavily enrichment), markdown, activity."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.events import get_event_bus
from backend.pipeline.materializer import propagate_after_fact_write
from backend.pipeline.renderer import (
    CONTEXT_LABELS,
    CONTEXT_LIMIT,
    Stammdaten,
    _building_for_property,
    _context_body,
    _detect_property_language,
    _fetch_stammdaten,
    _format_eur,
    _liegenschaft_for_building,
    _recent_events_for_scope,
    render_markdown,
)
from backend.services import ask as ask_service
from backend.services import pioneer_llm
from backend.services.tavily import enrich_property

router = APIRouter(prefix="/properties", tags=["properties"])
log = structlog.get_logger(__name__)


class PropertySummary(BaseModel):
    """Compact property record used by the portfolio listing.

    Counts are computed at query time from indexed property_id lookups on
    ``events``, ``facts`` (current only — ``superseded_by IS NULL``),
    ``uncertainty_events`` (``status='open'``), and ``signals``
    (``status='pending'``). At Buena's 56-property scale this is ~224
    index hits per request — single-digit ms total.
    """

    id: UUID
    name: str
    address: str
    owner_name: str | None = None
    building_year_built: int | None = None
    events: int = 0
    facts: int = 0
    needs_review: int = 0
    open_issues: int = 0


class PropertyCreateRequest(BaseModel):
    """Payload for POST /properties."""

    name: str = Field(..., min_length=1)
    address: str = Field(..., min_length=1)
    aliases: list[str] = Field(default_factory=list)
    owner_id: UUID | None = None
    building_id: UUID | None = None


class PropertyCreateResponse(BaseModel):
    """Response for POST /properties."""

    id: UUID
    name: str
    address: str
    tavily_event_id: UUID | None


class EnrichmentResponse(BaseModel):
    """Response for POST /properties/{id}/enrich."""

    property_id: UUID
    tavily_event_id: UUID | None
    already_enriched: bool


class PropertySearchHit(BaseModel):
    """One result from ``GET /properties/search``."""

    id: UUID
    name: str
    address: str
    snippet: str
    score: float


class PropertyFileFact(BaseModel):
    """One row in a property file section, shaped for the frontend.

    Mirrors the UI's ``Fact`` type (``value`` / ``source`` / ``date``) and
    surfaces ``section`` / ``field`` / ``event_id`` so the renderer can
    deep-link to the originating event when the user clicks ``[source]``.
    ``fact_id`` is the canonical pointer the edit endpoint needs;
    ``human_edited`` flips the source label so the UI can render
    ``via operator`` instead of ``via email`` for hand-edited rows.
    """

    value: str
    source: str
    date: str
    date_iso: str | None = None
    confidence: float | None = None
    urgent: bool = False
    reason: str | None = None
    section: str
    field: str
    event_id: str | None = None
    fact_id: str | None = None
    human_edited: bool = False
    edited_by: str | None = None


class PropertyFileSection(BaseModel):
    """Display section grouping facts under a single heading."""

    title: str
    facts: list[PropertyFileFact]
    is_uncertain: bool = False
    is_context: bool = False


class PropertyFile(BaseModel):
    """Structured payload powering the PropertyDetail page."""

    id: UUID
    name: str
    address: str
    sections: list[PropertyFileSection]


class GraphNode(BaseModel):
    """A node in the property context graph."""

    id: str
    type: str  # 'property' | 'owner' | 'building' | 'tenant' | 'contractor'
    label: str
    metadata: dict[str, object] = {}


class GraphEdge(BaseModel):
    """An edge in the property context graph."""

    source: str
    target: str
    relationship: str


class PropertyGraph(BaseModel):
    """Response for ``GET /properties/{id}/graph``."""

    property_id: UUID
    nodes: list[GraphNode]
    edges: list[GraphEdge]


@router.get("", response_model=list[PropertySummary])
async def list_properties(
    session: AsyncSession = Depends(get_session),
) -> list[PropertySummary]:
    """Return every seeded property, joined with owner + building context."""
    result = await session.execute(
        text(
            """
            SELECT p.id, p.name, p.address,
                   o.name AS owner_name,
                   b.year_built AS building_year_built,
                   (SELECT COUNT(*) FROM events
                      WHERE property_id = p.id) AS events,
                   (SELECT COUNT(*) FROM facts
                      WHERE property_id = p.id
                        AND superseded_by IS NULL) AS facts,
                   (SELECT COUNT(*) FROM uncertainty_events
                      WHERE property_id = p.id
                        AND status = 'open') AS needs_review,
                   (SELECT COUNT(*) FROM signals
                      WHERE property_id = p.id
                        AND status = 'pending') AS open_issues
            FROM properties p
            LEFT JOIN owners o ON o.id = p.owner_id
            LEFT JOIN buildings b ON b.id = p.building_id
            ORDER BY p.created_at ASC
            """
        )
    )
    properties = [
        PropertySummary(
            id=row.id,
            name=row.name,
            address=row.address,
            owner_name=row.owner_name,
            building_year_built=row.building_year_built,
            events=int(row.events or 0),
            facts=int(row.facts or 0),
            needs_review=int(row.needs_review or 0),
            open_issues=int(row.open_issues or 0),
        )
        for row in result.all()
    ]
    log.info("properties.list", count=len(properties))
    return properties


@router.get("/search", response_model=list[PropertySearchHit])
async def search_properties(
    q: str = Query(..., min_length=1, description="Free-text search query"),
    limit: int = Query(default=5, ge=1, le=25),
    session: AsyncSession = Depends(get_session),
) -> list[PropertySearchHit]:
    """Keyword search across property name, address, aliases, and current facts.

    Per-term scoring — the query is split on whitespace and each term
    contributes independently, so ``"heating Berlin"`` picks up a heating
    fact *and* a Berlin address rather than needing both words to appear
    contiguously.

    Weights:
    - direct name/alias term hit      → 1.0
    - address term hit                → 0.8
    - fact value term hit             → 0.5 + min(count / 10, 0.45)
    """
    terms = [t for t in q.lower().split() if t.strip()] or [q.lower()]
    aggregated: dict[str, dict[str, Any]] = {}
    for term in terms:
        pattern = f"%{term}%"
        result = await session.execute(
            text(
                """
                WITH fact_hits AS (
                    SELECT f.property_id,
                           COUNT(*) AS hits,
                           (ARRAY_AGG(f.value ORDER BY f.created_at DESC))[1]
                               AS snippet
                    FROM facts f
                    WHERE f.superseded_by IS NULL
                      AND LOWER(f.value) LIKE :pat
                    GROUP BY f.property_id
                )
                SELECT p.id, p.name, p.address,
                       COALESCE(fh.hits, 0) AS hits,
                       fh.snippet AS fact_snippet,
                       CASE
                         WHEN LOWER(p.name) LIKE :pat THEN 1.0
                         WHEN EXISTS (
                           SELECT 1 FROM UNNEST(p.aliases) a
                           WHERE LOWER(a) LIKE :pat
                         ) THEN 0.95
                         WHEN LOWER(p.address) LIKE :pat THEN 0.8
                         WHEN COALESCE(fh.hits, 0) > 0
                           THEN 0.5 + LEAST(fh.hits::float / 10.0, 0.45)
                         ELSE 0.0
                       END AS score
                FROM properties p
                LEFT JOIN fact_hits fh ON fh.property_id = p.id
                """
            ),
            {"pat": pattern},
        )
        for row in result.all():
            score = float(row.score)
            if score <= 0:
                continue
            slot = aggregated.setdefault(
                str(row.id),
                {
                    "id": row.id,
                    "name": row.name,
                    "address": row.address,
                    "snippet": None,
                    "score": 0.0,
                },
            )
            slot["score"] += score
            if slot["snippet"] is None and row.fact_snippet:
                slot["snippet"] = row.fact_snippet

    hits = sorted(aggregated.values(), key=lambda r: r["score"], reverse=True)[:limit]
    log.info("properties.search", q=q, terms=len(terms), returned=len(hits))
    return [
        PropertySearchHit(
            id=h["id"],
            name=h["name"],
            address=h["address"],
            snippet=(h["snippet"] or f"{h['name']} — {h['address']}")[:220],
            score=min(float(h["score"]), 1.0 * len(terms)),
        )
        for h in hits
    ]


@router.get("/{property_id}/graph", response_model=PropertyGraph)
async def property_graph(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PropertyGraph:
    """Return the context graph for a property — owner, building, tenants, contractors."""
    prop_row = (
        await session.execute(
            text(
                """
                SELECT p.id, p.name, p.address,
                       o.id AS owner_id, o.name AS owner_name, o.email AS owner_email,
                       b.id AS building_id, b.address AS building_address,
                       b.year_built AS building_year
                FROM properties p
                LEFT JOIN owners o ON o.id = p.owner_id
                LEFT JOIN buildings b ON b.id = p.building_id
                WHERE p.id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if prop_row is None:
        raise HTTPException(status_code=404, detail="property not found")

    nodes: list[GraphNode] = [
        GraphNode(
            id=str(prop_row.id),
            type="property",
            label=prop_row.name,
            metadata={"address": prop_row.address},
        )
    ]
    edges: list[GraphEdge] = []

    if prop_row.owner_id is not None:
        nodes.append(
            GraphNode(
                id=str(prop_row.owner_id),
                type="owner",
                label=prop_row.owner_name or "Owner",
                metadata={"email": prop_row.owner_email or ""},
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(prop_row.owner_id),
                relationship="owned_by",
            )
        )

    if prop_row.building_id is not None:
        nodes.append(
            GraphNode(
                id=str(prop_row.building_id),
                type="building",
                label=f"Building — {prop_row.building_address}",
                metadata={
                    "address": prop_row.building_address or "",
                    "year_built": prop_row.building_year,
                },
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(prop_row.building_id),
                relationship="in_building",
            )
        )

    tenants = await session.execute(
        text(
            """
            SELECT id, name, email
            FROM tenants
            WHERE property_id = :pid
            ORDER BY name
            """
        ),
        {"pid": property_id},
    )
    for tenant in tenants.all():
        nodes.append(
            GraphNode(
                id=str(tenant.id),
                type="tenant",
                label=tenant.name,
                metadata={"email": tenant.email or ""},
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(tenant.id),
                relationship="occupied_by",
            )
        )

    contractors = await session.execute(
        text(
            """
            SELECT c.id, c.name, c.specialty, c.rating
            FROM relationships r
            JOIN contractors c ON c.id = r.to_id
            WHERE r.from_id = :pid
              AND r.from_type = 'property'
              AND r.to_type = 'contractor'
              AND r.relationship_type = 'serviced_by'
            """
        ),
        {"pid": property_id},
    )
    for contractor in contractors.all():
        nodes.append(
            GraphNode(
                id=str(contractor.id),
                type="contractor",
                label=contractor.name,
                metadata={
                    "specialty": contractor.specialty or "",
                    "rating": contractor.rating,
                },
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(contractor.id),
                relationship="serviced_by",
            )
        )

    log.info(
        "properties.graph",
        property_id=str(property_id),
        nodes=len(nodes),
        edges=len(edges),
    )
    return PropertyGraph(property_id=property_id, nodes=nodes, edges=edges)


@router.post("", response_model=PropertyCreateResponse, status_code=201)
async def create_property(
    payload: PropertyCreateRequest,
    session: AsyncSession = Depends(get_session),
) -> PropertyCreateResponse:
    """Create a property and kick off Tavily enrichment (runs once, per Part IV)."""
    result = await session.execute(
        text(
            """
            INSERT INTO properties (name, address, aliases, owner_id, building_id)
            VALUES (:name, :addr, :aliases, :owner, :building)
            RETURNING id
            """
        ),
        {
            "name": payload.name,
            "addr": payload.address,
            "aliases": payload.aliases,
            "owner": payload.owner_id,
            "building": payload.building_id,
        },
    )
    property_id: UUID = result.scalar_one()
    await session.commit()
    log.info("properties.create", property_id=str(property_id), name=payload.name)

    tavily_event_id = await enrich_property(property_id, payload.name, payload.address)
    return PropertyCreateResponse(
        id=property_id,
        name=payload.name,
        address=payload.address,
        tavily_event_id=tavily_event_id,
    )


@router.post("/{property_id}/enrich", response_model=EnrichmentResponse)
async def enrich_existing_property(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> EnrichmentResponse:
    """Re-run Tavily enrichment for an existing (seeded) property.

    Idempotent — a second call is a no-op. Intended as an admin utility so
    the seeded portfolio can show the "Updated from web sources" badge
    without requiring a reseed.
    """
    row = (
        await session.execute(
            text("SELECT name, address FROM properties WHERE id = :pid"),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="property not found")
    before_count = (
        await session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM events
                WHERE property_id = :pid AND source = 'web'
                  AND source_ref LIKE 'tavily:%'
                """
            ),
            {"pid": property_id},
        )
    ).scalar_one()

    event_id = await enrich_property(property_id, row.name, row.address)
    return EnrichmentResponse(
        property_id=property_id,
        tavily_event_id=event_id,
        already_enriched=event_id is None and int(before_count or 0) > 0,
    )


@router.get("/{property_id}/markdown", response_class=PlainTextResponse)
async def property_markdown(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the canonical markdown document for a property.

    Phase 12 — reads the materialized ``property_files`` row when one
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
                "SELECT content_md, last_rendered_at FROM property_files "
                "WHERE property_id = :pid"
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


# Display labels per backend section enum. Keep in sync with the
# section names in ``backend.services.gemini.EXTRACTION_SCHEMA``.
_SECTION_LABELS: dict[str, str] = {
    "overview": "Overview",
    "tenants": "Tenants",
    "lease": "Lease",
    "maintenance": "Maintenance",
    "financials": "Financials",
    "compliance": "Compliance",
    "activity": "Activity",
    "patterns": "Patterns",
    "building_financials": "Building Context",
    "building_maintenance": "Building Context",
    "building_compliance": "Building Context",
    "liegenschaft_financials": "WEG Context",
    "liegenschaft_maintenance": "WEG Context",
    "liegenschaft_compliance": "WEG Context",
}

# Sections that display under a "Context" heading rather than as a
# primary fact section — the frontend uses the ``isContext`` flag to
# render these with a quieter visual treatment.
_CONTEXT_SECTIONS: frozenset[str] = frozenset(
    {
        "building_financials",
        "building_maintenance",
        "building_compliance",
        "liegenschaft_financials",
        "liegenschaft_maintenance",
        "liegenschaft_compliance",
    }
)

# Sources whose presence implies a fact came from an authoritative
# document rather than an email — surfaced verbatim in the UI's "via"
# label so PDFs read as "Mietvertrag.pdf" instead of just "letter".
_AUTHORITATIVE_DOCUMENT_TYPES: frozenset[str] = frozenset(
    {"lease", "lease_addendum", "kaufvertrag", "structural_permit", "vermessungsprotokoll"}
)


def _build_stammdaten_section(
    s: Stammdaten, lang: str
) -> PropertyFileSection | None:
    """Emit a Stammdaten section mirroring the markdown view.

    Stammdaten lives outside the events/facts pipeline (it's the
    authoritative lease/owner registry loaded at ingest), so each row
    here carries ``source='stammdaten'`` and no ``fact_id`` — the edit
    affordance stays hidden because there's no fact row to override.
    """
    labels = CONTEXT_LABELS[lang]
    rows: list[tuple[str, str]] = []

    if s.unit_label:
        rows.append((labels["unit"], s.unit_label))
    if s.lage:
        rows.append(("Lage" if lang == "de" else "Location", s.lage))
    if s.size_qm is not None:
        size_str = f"{s.size_qm:.1f} m²".replace(".0 ", " ")
        rows.append((labels["size_qm"], size_str))
    if s.rooms is not None:
        rooms_str = f"{s.rooms:.1f}".rstrip("0").rstrip(".") or "0"
        rows.append((labels["rooms"], rooms_str))
    if s.owner_name:
        rows.append((labels["owner"], s.owner_name))
    if s.tenant_name:
        rows.append((labels["tenant"], s.tenant_name))
        if s.mietbeginn:
            rows.append((labels["lease_start"], s.mietbeginn))
        rows.append(
            (
                labels["lease_end"],
                s.mietende if s.mietende else labels["lease_open_ended"],
            )
        )
        kalt = _format_eur(s.kaltmiete)
        if kalt:
            rows.append((labels["rent_cold"], kalt))
        nk = _format_eur(s.nk_vorauszahlung)
        if nk:
            rows.append((labels["operating_costs"], nk))
        kaution = _format_eur(s.kaution)
        if kaution:
            rows.append((labels["deposit"], kaution))

    facts: list[PropertyFileFact] = []
    if not s.tenant_name:
        facts.append(
            PropertyFileFact(
                value=labels["no_active_tenant"],
                source="stammdaten",
                date="",
                section="stammdaten",
                field="tenant",
            )
        )
    for label, value in rows:
        facts.append(
            PropertyFileFact(
                value=f"{label}: {value}",
                source="stammdaten",
                date="",
                section="stammdaten",
                field=label.lower().replace(" ", "_"),
            )
        )

    if not facts:
        return None
    return PropertyFileSection(
        title=labels["stammdaten"],
        facts=facts,
        is_context=False,
    )


def _build_context_section(
    events: list[dict[str, str | int]],
    *,
    title: str,
) -> PropertyFileSection | None:
    """Turn recent building/liegenschaft events into a context section.

    Each event becomes a read-only fact: source pill = the event source
    (``email``/``bank``/``invoice``), event_id wires the source modal,
    no ``fact_id`` (these aren't facts — they're parent-tier activity).
    """
    facts: list[PropertyFileFact] = []
    for ev in events:
        received_iso = str(ev.get("received_at") or "")
        date_short = received_iso[:10] if received_iso else ""
        facts.append(
            PropertyFileFact(
                value=_context_body(ev),
                source=str(ev.get("source") or ""),
                date=date_short,
                date_iso=received_iso or None,
                section="context",
                field=str(ev.get("kategorie") or ""),
                event_id=str(ev.get("id") or "") or None,
            )
        )
    if not facts:
        return None
    return PropertyFileSection(
        title=title,
        facts=facts,
        is_context=True,
    )


@router.get("/{property_id}/file", response_model=PropertyFile)
async def property_file(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PropertyFile:
    """Return the structured property file — sections + facts + uncertainties.

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
        # all of these — the human is the source of truth.
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
                confidence=(
                    float(row.confidence) if row.confidence is not None else None
                ),
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
                value = f"{u.observation} — {u.hypothesis}"
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

    # Stammdaten + parent-tier context — same data the markdown view
    # surfaces. Detect language once so labels match the property's
    # dominant event language.
    lang = await _detect_property_language(session, property_id)

    stammdaten = await _fetch_stammdaten(session, property_id)
    stammdaten_section = (
        _build_stammdaten_section(stammdaten, lang) if stammdaten else None
    )
    if stammdaten_section is not None:
        # Stammdaten is the master record — prepend so it reads first.
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


class FactEditRequest(BaseModel):
    """Operator override of a fact value.

    ``edited_by`` defaults to ``operator``; pass a real user identifier
    when the UI has one. The endpoint sets ``human_edited=true`` on the
    new row, which the differ honors against future extractor proposals
    (see ``backend/pipeline/differ.py`` resolution rules).
    """

    value: str = Field(..., min_length=1, max_length=4_000)
    edited_by: str = Field(default="operator", min_length=1, max_length=120)


class FactEditResponse(BaseModel):
    """Result of a fact override — both the new and the superseded id."""

    fact_id: UUID
    superseded_fact_id: UUID
    property_id: UUID
    section: str
    field: str
    value: str
    edited_by: str
    edited_at: datetime


@router.patch(
    "/{property_id}/facts/{fact_id}",
    response_model=FactEditResponse,
)
async def edit_fact(
    property_id: UUID,
    fact_id: UUID,
    body: FactEditRequest,
    session: AsyncSession = Depends(get_session),
) -> FactEditResponse:
    """Operator override of a single fact.

    Inserts a new fact row carrying the operator's value with
    ``human_edited=true`` and supersedes the existing row, preserving
    the original ``source_event_id`` so the trace remains intact. The
    extractor's resolution rules treat ``human_edited`` rows as sticky:
    future proposals against the same ``(property_id, section, field)``
    coordinate are skipped with a ``preserved_human_edit`` reason.

    Broadcasts a ``fact_update`` payload on the event bus so the
    portfolio bell + per-property SSE listeners refresh in place.
    """
    current = (
        await session.execute(
            text(
                """
                SELECT id, property_id, section, field, value,
                       source_event_id, building_id, liegenschaft_id
                FROM facts
                WHERE id = :fid AND superseded_by IS NULL
                """
            ),
            {"fid": fact_id},
        )
    ).first()
    if current is None:
        raise HTTPException(
            status_code=404,
            detail="fact not found or already superseded",
        )
    if current.property_id != property_id:
        raise HTTPException(
            status_code=400,
            detail="fact does not belong to this property",
        )

    new_value = body.value.strip()
    if not new_value:
        raise HTTPException(status_code=400, detail="value must not be blank")
    if new_value == current.value:
        raise HTTPException(status_code=400, detail="value unchanged")

    inserted = (
        await session.execute(
            text(
                """
                INSERT INTO facts (
                    property_id, section, field, value, source_event_id,
                    confidence, valid_from, building_id, liegenschaft_id,
                    human_edited, edited_by, edited_at
                ) VALUES (
                    :pid, :section, :field, :value, :src, 1.0, now(),
                    :bid, :lid, TRUE, :who, now()
                )
                RETURNING id, edited_at
                """
            ),
            {
                "pid": property_id,
                "section": current.section,
                "field": current.field,
                "value": new_value,
                "src": current.source_event_id,
                "bid": current.building_id,
                "lid": current.liegenschaft_id,
                "who": body.edited_by,
            },
        )
    ).first()
    if inserted is None:  # pragma: no cover — RETURNING always yields a row
        raise HTTPException(status_code=500, detail="insert failed")

    await session.execute(
        text(
            """
            UPDATE facts
            SET superseded_by = :new_id, valid_to = now()
            WHERE id = :old_id
            """
        ),
        {"new_id": inserted.id, "old_id": current.id},
    )

    edit_summary = f"{body.edited_by} edited {current.section}.{current.field}"
    await propagate_after_fact_write(
        session,
        property_id=property_id,
        building_id=current.building_id,
        liegenschaft_id=current.liegenschaft_id,
        trigger_event_id=current.source_event_id,
        trigger_summary=edit_summary,
    )
    await session.commit()

    log.info(
        "properties.fact.edited",
        property_id=str(property_id),
        new_fact_id=str(inserted.id),
        superseded_fact_id=str(current.id),
        section=current.section,
        field=current.field,
        edited_by=body.edited_by,
    )

    await get_event_bus().publish(
        property_id,
        {
            "event_id": None,
            "property_id": str(property_id),
            "category": "fact.edited",
            "summary": f"{body.edited_by} edited {current.section}.{current.field}",
            "facts_written": 1,
        },
    )

    return FactEditResponse(
        fact_id=inserted.id,
        superseded_fact_id=current.id,
        property_id=property_id,
        section=current.section,
        field=current.field,
        value=new_value,
        edited_by=body.edited_by,
        edited_at=inserted.edited_at,
    )


class ActivityItem(BaseModel):
    """Activity feed row — a processed event + its one-line summary."""

    event_id: UUID
    source: str
    received_at: datetime
    processed_at: datetime | None
    summary: str | None
    facts_written: int


@router.get("/{property_id}/activity", response_model=list[ActivityItem])
async def property_activity(
    property_id: UUID,
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[ActivityItem]:
    """Return the ``limit`` most recent events + extraction summary for this property."""
    result = await session.execute(
        text(
            """
            SELECT e.id, e.source, e.received_at, e.processed_at,
                   sum.value AS summary,
                   (SELECT COUNT(*) FROM facts f WHERE f.source_event_id = e.id) AS facts_written
            FROM events e
            LEFT JOIN LATERAL (
                SELECT value FROM facts
                WHERE source_event_id = e.id AND section = 'activity'
                ORDER BY created_at ASC LIMIT 1
            ) sum ON TRUE
            WHERE e.property_id = :pid
            ORDER BY e.received_at DESC
            LIMIT :lim
            """
        ),
        {"pid": property_id, "lim": limit},
    )
    items = [
        ActivityItem(
            event_id=row.id,
            source=row.source,
            received_at=row.received_at,
            processed_at=row.processed_at,
            summary=row.summary,
            facts_written=int(row.facts_written or 0),
        )
        for row in result.all()
    ]
    log.info("properties.activity", property_id=str(property_id), count=len(items))
    return items


# -----------------------------------------------------------------------------
# Phase 11 — property-scoped Q&A
# -----------------------------------------------------------------------------


class AskRequest(BaseModel):
    """Payload for ``POST /properties/{id}/ask``."""

    question: str = Field(..., min_length=1, max_length=600)


class AskSourceItem(BaseModel):
    """Source citation used by both ``answered`` and ``insufficient_context``."""

    id: str
    channel: str
    date: str
    snippet: str


class AskResponse(BaseModel):
    """Q&A response with three status branches the frontend renders distinctly.

    Phase 9 trust-layer rule: an honest "I don't know" beats a confident
    hallucination. The ``insufficient_context`` and ``out_of_scope``
    branches keep ``answer`` ``None`` and surface the partial-context
    or out-of-scope reasoning explicitly.
    """

    status: str  # 'answered' | 'insufficient_context' | 'out_of_scope'
    answer: str | None
    confidence: str
    reasoning: str = ""
    sources: list[AskSourceItem] = Field(default_factory=list)
    partial_context: list[AskSourceItem] = Field(default_factory=list)
    elapsed_ms: float
    retrieved_count: int
    model: str


async def _fetch_ask_context(
    session: AsyncSession, property_id: UUID
) -> tuple[list[dict[str, Any]], dict[str, dict[str, str]]]:
    """Pull recent events for citation + a lookup mapping event_id → citation shape.

    The canonical context for the LLM is the rendered property markdown
    (fetched separately via :func:`render_markdown`). This helper only
    returns the raw event snippets that let Pioneer quote source text
    directly; the citation_lookup joins those event ids back to the
    ``{id, channel, date, snippet}`` shape the frontend expects.
    """
    event_rows = (
        await session.execute(
            text(
                """
                SELECT e.id, e.source,
                       COALESCE(e.received_at, e.processed_at, now()) AS rec,
                       LEFT(COALESCE(e.raw_content, ''), 320) AS snippet
                FROM events e
                WHERE e.property_id = :pid
                  AND COALESCE(e.received_at, e.processed_at) > now() - interval '90 days'
                ORDER BY rec DESC
                LIMIT 12
                """
            ),
            {"pid": property_id},
        )
    ).all()
    events: list[dict[str, Any]] = []
    citation_lookup: dict[str, dict[str, str]] = {}
    for r in event_rows:
        eid = str(r.id)
        snippet = str(r.snippet or "").replace("\n", " ").strip()
        events.append(
            {
                "id": eid,
                "source": str(r.source),
                "received_at": r.rec.isoformat() if r.rec else "",
                "snippet": snippet,
            }
        )
        citation_lookup[eid] = {
            "id": eid,
            "channel": str(r.source),
            "date": r.rec.date().isoformat() if r.rec else "",
            "snippet": snippet[:240],
        }
    return events, citation_lookup


@router.post("/{property_id}/ask", response_model=AskResponse)
async def property_ask(
    property_id: UUID,
    payload: AskRequest,
    session: AsyncSession = Depends(get_session),
) -> AskResponse:
    """Answer a question scoped to one property's facts + recent events."""
    meta_row = (
        await session.execute(
            text("SELECT name FROM properties WHERE id = :pid"),
            {"pid": property_id},
        )
    ).first()
    if meta_row is None:
        raise HTTPException(status_code=404, detail="property not found")

    events, citation_lookup = await _fetch_ask_context(session, property_id)
    try:
        property_file_markdown = await render_markdown(session, property_id)
    except ValueError as exc:
        # Should be unreachable — the property existence check above
        # already 404s — but guard anyway so we never feed a half-built
        # prompt to Pioneer.
        log.warning("properties.ask.render_failed", error=str(exc))
        raise HTTPException(status_code=404, detail="property not found") from exc

    try:
        result = await ask_service.answer_question(
            property_name=str(meta_row.name),
            question=payload.question,
            property_file_markdown=property_file_markdown,
            recent_events=events,
        )
    except pioneer_llm.PioneerUnavailable as exc:
        log.warning("properties.ask.unavailable", error=str(exc))
        raise HTTPException(
            status_code=503,
            detail=f"ask service unavailable: {exc}",
        ) from exc

    sources: list[AskSourceItem] = []
    partial_context: list[AskSourceItem] = []
    for eid in result["cited_event_ids"]:
        cite = citation_lookup.get(eid)
        if cite is not None:
            sources.append(AskSourceItem(**cite))
    for eid in result["partial_context_event_ids"]:
        cite = citation_lookup.get(eid)
        if cite is not None:
            partial_context.append(AskSourceItem(**cite))

    log.info(
        "properties.ask.respond",
        property_id=str(property_id),
        status=result["status"],
        sources=len(sources),
        partial=len(partial_context),
        retrieved=len(events),
    )

    return AskResponse(
        status=result["status"],
        answer=result["answer"],
        confidence=result["confidence"],
        reasoning=result["reasoning"],
        sources=sources,
        partial_context=partial_context,
        elapsed_ms=float(result["latency_ms"]),
        retrieved_count=len(events),
        model=str(result["model"]),
    )


# -----------------------------------------------------------------------------
# Phase 11 — raw inbox view for Compare page ("today: search the inbox")
# -----------------------------------------------------------------------------


class InboxEmail(BaseModel):
    """One raw email row for the "before Keystone" Compare view."""

    event_id: UUID
    date: str
    sender: str
    subject: str
    preview: str
    relevant: bool = Field(
        default=False,
        description=(
            "True when the search keyword(s) appeared in the raw body. "
            "Used to highlight matching rows visually."
        ),
    )


class InboxResponse(BaseModel):
    """Response for ``GET /properties/{id}/inbox``."""

    rows: list[InboxEmail]
    total_inbox_size: int = Field(
        ...,
        description="Total email events for this property (the haystack size).",
    )
    relevant_count: int
    keyword: str | None


def _extract_subject_and_from(raw: str) -> tuple[str, str]:
    """Cheap RFC-822-ish subject/from parser. Falls back to first line."""
    subject = ""
    sender = ""
    for line in raw.splitlines():
        lower = line.lower()
        if not subject and lower.startswith("subject:"):
            subject = line.split(":", 1)[1].strip()
        elif not sender and lower.startswith("from:"):
            sender = line.split(":", 1)[1].strip()
        if subject and sender:
            break
    if not subject:
        subject = raw.strip().split("\n", 1)[0][:80]
    return subject, sender


@router.get("/{property_id}/inbox", response_model=InboxResponse)
async def property_inbox(
    property_id: UUID,
    q: str | None = Query(
        default=None,
        description="Optional keyword. Rows whose raw body matches are flagged ``relevant=True``.",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> InboxResponse:
    """Email-style inbox view for the Compare page's "before" panel.

    Returns the property's most-recent email events with a parsed
    ``subject`` / ``from`` / ``preview``. When ``q`` is supplied the
    LIKE filter highlights matches but the list still shows the
    surrounding noise — that's the point of the comparison: the
    operator scrolls through everything, Keystone returns one answer.
    """
    total = (
        await session.execute(
            text(
                """
                SELECT COUNT(*) FROM events
                WHERE property_id = :pid AND source = 'email'
                """
            ),
            {"pid": property_id},
        )
    ).scalar_one()

    pattern = f"%{q}%" if q else None
    relevant_count = 0
    if pattern:
        relevant_count = (
            await session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM events
                    WHERE property_id = :pid AND source = 'email'
                      AND raw_content ILIKE :pat
                    """
                ),
                {"pid": property_id, "pat": pattern},
            )
        ).scalar_one()

    rows = (
        await session.execute(
            text(
                """
                SELECT id,
                       COALESCE(received_at, processed_at) AS dt,
                       LEFT(COALESCE(raw_content, ''), 600) AS body,
                       (CASE WHEN CAST(:pat AS text) IS NOT NULL
                             AND raw_content ILIKE CAST(:pat AS text)
                             THEN TRUE ELSE FALSE END) AS is_relevant
                FROM events
                WHERE property_id = :pid AND source = 'email'
                ORDER BY COALESCE(received_at, processed_at) DESC
                LIMIT :lim
                """
            ),
            {"pid": property_id, "pat": pattern, "lim": limit},
        )
    ).all()

    items: list[InboxEmail] = []
    for r in rows:
        body = str(r.body or "")
        subject, sender = _extract_subject_and_from(body)
        body_text = body
        # Strip headers from preview
        for hdr in ("From:", "Subject:", "Date:", "To:", "Message-ID:"):
            for line in body_text.splitlines():
                if line.lower().startswith(hdr.lower()):
                    body_text = body_text.replace(line + "\n", "")
        preview = body_text.replace("\n", " ").strip()[:120]
        items.append(
            InboxEmail(
                event_id=r.id,
                date=r.dt.date().isoformat() if r.dt else "",
                sender=sender or "(unknown)",
                subject=subject or "(no subject)",
                preview=preview,
                relevant=bool(r.is_relevant),
            )
        )

    log.info(
        "properties.inbox",
        property_id=str(property_id),
        q=q,
        returned=len(items),
        total=int(total),
        relevant=int(relevant_count),
    )
    return InboxResponse(
        rows=items,
        total_inbox_size=int(total),
        relevant_count=int(relevant_count),
        keyword=q,
    )
