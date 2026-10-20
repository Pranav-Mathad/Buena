"""Response models and section builders for the structured property file."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from backend.pipeline.renderer import (
    CONTEXT_LABELS,
    Stammdaten,
    _context_body,
    _format_eur,
)


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


# Display labels per backend section enum. Keep in sync with the
# section names in ``backend.services.llm.extraction.EXTRACTION_SCHEMA``.
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
# primary fact section; the frontend uses the ``isContext`` flag to
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
# document rather than an email. Surfaced verbatim in the UI's "via"
# label so PDFs read as "Mietvertrag.pdf" instead of just "letter".
_AUTHORITATIVE_DOCUMENT_TYPES: frozenset[str] = frozenset(
    {"lease", "lease_addendum", "kaufvertrag", "structural_permit", "vermessungsprotokoll"}
)


def _build_stammdaten_section(s: Stammdaten, lang: str) -> PropertyFileSection | None:
    """Emit a Stammdaten section mirroring the markdown view.

    Stammdaten lives outside the events/facts pipeline (it's the
    authoritative lease/owner registry loaded at ingest), so each row
    here carries ``source='stammdaten'`` and no ``fact_id``; the edit
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
    no ``fact_id`` (these aren't facts; they're parent-tier activity).
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
