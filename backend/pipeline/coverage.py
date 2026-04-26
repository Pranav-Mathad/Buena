"""Schema-completeness scorecard for a property file.

Phase 12+ — judges look at our markdown and ask "what's missing?". Today
the file shows what we know (facts + uncertainties); the *absence* of
data is invisible. This module declares per-section expected-fact
coordinates so the renderer can emit a compact ``## Coverage`` block
naming what's present (✓) and what's absent (⨯).

The expected-field lists are intentionally short and demo-focused — we
favour "the operator would notice if this were missing" over a complete
catalog. Add fields here as new fact-discovery rules ship; the absence
report grows naturally.
"""

from __future__ import annotations

from dataclasses import dataclass

from backend.pipeline.renderer import FactRow, Stammdaten


@dataclass(frozen=True)
class CoverageReport:
    """Per-area coverage summary returned by :func:`compute_coverage`."""

    stammdaten: list[tuple[str, bool]]  # ordered (label, present?)
    sections: dict[str, list[tuple[str, bool]]]
    total_present: int
    total_expected: int


# Stammdaten labels we score against the master record. Each tuple is
# ``(label_en, label_de, predicate(stammdaten) -> bool)``.
_STAMMDATEN_FIELDS: list[tuple[str, str, str]] = [
    ("unit", "Einheit", "unit_label"),
    ("size", "Wohnfläche", "size_qm"),
    ("rooms", "Zimmer", "rooms"),
    ("owner", "Eigentümer", "owner_name"),
    ("tenant", "Mieter", "tenant_name"),
    ("lease_start", "Mietbeginn", "mietbeginn"),
    ("lease_end", "Mietende", "mietende"),
    ("rent_cold", "Kaltmiete", "kaltmiete"),
    ("deposit", "Kaution", "kaution"),
]


# Fact-coordinate expectations per section. Entries are
# ``(section, field, label_en, label_de)``. Missing fields render as ⨯
# in the coverage block; present fields render as ✓.
_EXPECTED_FACTS: list[tuple[str, str, str, str]] = [
    ("lease", "termination_notice", "termination_notice", "Kündigung"),
    ("lease", "lease_end_date", "lease_end_date", "Mietende-Datum"),
    ("financials", "last_rent_payment", "last_rent_payment", "letzte Miete"),
    ("compliance", "brandschutz", "brandschutz", "Brandschutz"),
    ("maintenance", "open_water_damage", "open_water_damage", "Wasserschaden"),
]


def _has_value(s: Stammdaten, attr: str) -> bool:
    value = getattr(s, attr, None)
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def compute_coverage(
    stammdaten: Stammdaten | None,
    facts: list[FactRow],
    *,
    lang: str = "en",
) -> CoverageReport:
    """Score the property file against the expected-field tables above.

    Stammdaten coverage reads the master record directly (those fields
    don't live in ``facts`` so they're checked against the
    :class:`Stammdaten` object). Section coverage looks for any current
    fact at the matching ``(section, field)`` coordinate.
    """
    stamm_rows: list[tuple[str, bool]] = []
    if stammdaten is not None:
        for label_en, label_de, attr in _STAMMDATEN_FIELDS:
            label = label_de if lang == "de" else label_en
            stamm_rows.append((label, _has_value(stammdaten, attr)))

    fact_keys: set[tuple[str, str]] = {(f.section, f.field) for f in facts}
    sections: dict[str, list[tuple[str, bool]]] = {}
    for section, field, label_en, label_de in _EXPECTED_FACTS:
        label = label_de if lang == "de" else label_en
        sections.setdefault(section, []).append(
            (label, (section, field) in fact_keys)
        )

    total_present = sum(1 for _, ok in stamm_rows if ok) + sum(
        1 for rows in sections.values() for _, ok in rows if ok
    )
    total_expected = len(stamm_rows) + sum(len(rows) for rows in sections.values())
    return CoverageReport(
        stammdaten=stamm_rows,
        sections=sections,
        total_present=total_present,
        total_expected=total_expected,
    )


def render_coverage_block(report: CoverageReport, *, lang: str = "en") -> list[str]:
    """Emit the compact ``## Coverage`` markdown block.

    Reads as one line per area: ``Stammdaten: ✓ unit · ✓ size · ⨯ inspection``.
    Empty areas are skipped so the block stays tight.
    """
    title = "Abdeckung" if lang == "de" else "Coverage"
    stamm_label = "Stammdaten"

    lines: list[str] = [f"## {title}", ""]

    if report.stammdaten:
        joined = " · ".join(
            f"{'✓' if ok else '⨯'} {label}" for label, ok in report.stammdaten
        )
        lines.append(f"- **{stamm_label}** — {joined}")

    section_titles_de = {
        "lease": "Mietvertrag",
        "financials": "Finanzen",
        "compliance": "Compliance",
        "maintenance": "Wartung",
    }
    section_titles_en = {
        "lease": "Lease",
        "financials": "Financials",
        "compliance": "Compliance",
        "maintenance": "Maintenance",
    }
    titles = section_titles_de if lang == "de" else section_titles_en

    for section, rows in report.sections.items():
        joined = " · ".join(
            f"{'✓' if ok else '⨯'} {label}" for label, ok in rows
        )
        title_label = titles.get(section, section.replace("_", " ").title())
        lines.append(f"- **{title_label}** — {joined}")

    lines.append("")
    return lines


def coverage_to_index(report: CoverageReport) -> dict[str, object]:
    """Return a JSON-friendly dict for the ``content_index`` JSONB column."""
    return {
        "stammdaten": [
            {"label": label, "present": ok} for label, ok in report.stammdaten
        ],
        "sections": {
            section: [
                {"label": label, "present": ok} for label, ok in rows
            ]
            for section, rows in report.sections.items()
        },
        "total_present": report.total_present,
        "total_expected": report.total_expected,
    }
