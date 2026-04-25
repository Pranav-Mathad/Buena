"""Buena composite — the only place that knows Buena's directory shape.

Wires :mod:`csv_stammdaten`, :mod:`eml_archive`, :mod:`camt_bank`,
:mod:`pdf_invoice_archive`, and :mod:`pdf_letter_archive` against the
Buena dataset under ``Extracted/``.

Layout (verified against the dataset shipped 2026-04-25):

::

    Extracted/
    ├── stammdaten/
    │   ├── eigentuemer.csv     (35 rows)
    │   ├── einheiten.csv       (52 rows — units inside buildings)
    │   ├── mieter.csv          (26 rows — active + closed leases)
    │   ├── dienstleister.csv   (16 rows — contractors)
    │   └── stammdaten.json     (canonical: liegenschaft + gebaeude
    │                            + einheiten + eigentuemer + mieter
    │                            + dienstleister)
    ├── bank/
    │   └── bank_index.csv      (~1,619 transactions 2024-01..2025-12)
    ├── rechnungen/<YYYY-MM>/   (194 invoice PDFs)
    ├── briefe/<YYYY-MM>/       (135 letter PDFs)
    ├── emails/<YYYY-MM>/       (6,546 EMLs)
    └── incremental/day-NN/
        ├── incremental_manifest.json
        ├── emails_index.csv
        ├── rechnungen_index.csv
        ├── emails/<…>.eml
        ├── rechnungen/<…>.pdf
        └── bank/
            ├── bank_index.csv
            └── kontoauszug_delta.csv

Every connector applies PII redaction at yield time so no raw IBAN /
phone / email survives downstream.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import structlog

from connectors import camt_bank, eml_archive, pdf_invoice_archive, pdf_letter_archive, redact
from connectors.base import ConnectorEvent, DataMissing
from connectors.csv_stammdaten import ColumnMap, EntityType, parse_iso_date

log = structlog.get_logger(__name__)

DEFAULT_EXTRACTED_ROOT = Path(__file__).resolve().parents[1] / "Extracted"


def get_extracted_root(env_value: str | None = None) -> Path:
    """Resolve ``EXTRACTED_ROOT`` env (default ``Extracted/``)."""
    raw = env_value if env_value is not None else os.environ.get("EXTRACTED_ROOT", "")
    if raw:
        path = Path(raw).expanduser().resolve()
    else:
        path = DEFAULT_EXTRACTED_ROOT
    return path


def require_root(env_value: str | None = None) -> Path:
    """Like :func:`get_extracted_root` but raises if the folder is missing."""
    root = get_extracted_root(env_value)
    if not root.is_dir():
        raise DataMissing(
            f"Buena dataset not found at {root!s}; set EXTRACTED_ROOT or "
            "place the folder there."
        )
    return root


# -----------------------------------------------------------------------------
# Stammdaten — adapt Buena rows onto Phase 0's seed dataclasses
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class BuenaStammdaten:
    """Concrete Stammdaten payload after redaction + adaptation.

    ``liegenschaften`` is the WEG tier (`Wohnungseigentümergemeinschaft`,
    owners' association) — one Liegenschaft owns N Häuser; per-WEG
    events (Hausgeld, Verwaltergebühr, shared contractor fees) attach
    here, not to a single Haus.
    """

    liegenschaften: list[dict[str, Any]] = field(default_factory=list)
    owners: list[dict[str, Any]] = field(default_factory=list)
    buildings: list[dict[str, Any]] = field(default_factory=list)
    properties: list[dict[str, Any]] = field(default_factory=list)
    tenants: list[dict[str, Any]] = field(default_factory=list)
    contractors: list[dict[str, Any]] = field(default_factory=list)


CONTRACTOR_COLUMN_MAP = ColumnMap(
    required={
        "id": "id",
        "firma": "firma",
        "branche": "branche",
        "ansprechpartner": "ansprechpartner",
        "email": "email",
        "telefon": "telefon",
        "iban": "iban",
        "bic": "bic",
    },
    metadata_columns=(
        "strasse", "plz", "ort", "land", "ust_id", "steuernummer",
        "stil", "sprache", "vertrag_monatlich", "stundensatz",
    ),
    pii_columns=("email", "telefon", "iban"),
)

OWNER_COLUMN_MAP = ColumnMap(
    required={
        "id": "id", "anrede": "anrede", "vorname": "vorname",
        "nachname": "nachname", "firma": "firma",
        "email": "email", "telefon": "telefon", "iban": "iban", "bic": "bic",
    },
    metadata_columns=(
        "strasse", "plz", "ort", "land", "einheit_ids",
        "selbstnutzer", "sev_mandat", "beirat", "sprache",
    ),
    pii_columns=("email", "telefon", "iban"),
)

TENANT_COLUMN_MAP = ColumnMap(
    required={
        "id": "id", "anrede": "anrede", "vorname": "vorname",
        "nachname": "nachname", "email": "email", "telefon": "telefon",
        "einheit_id": "einheit_id", "eigentuemer_id": "eigentuemer_id",
        "mietbeginn": "mietbeginn", "mietende": "mietende",
        "kaltmiete": "kaltmiete", "nk_vorauszahlung": "nk_vorauszahlung",
        "kaution": "kaution", "iban": "iban", "bic": "bic",
    },
    metadata_columns=("sprache",),
    pii_columns=("email", "telefon", "iban"),
)


def _short_address(strasse: str, hausnr: str) -> str:
    """e.g. ``"Immanuelkirchstr. 26 · WE 01"``-style — passes through unchanged."""
    if not strasse:
        return hausnr
    if hausnr and hausnr not in strasse:
        return f"{strasse} {hausnr}".strip()
    return strasse


def load_stammdaten(root: Path) -> BuenaStammdaten:
    """Read every Stammdaten file and adapt to a redacted, Keystone-shaped payload."""
    stammdaten_dir = root / "stammdaten"
    json_path = stammdaten_dir / "stammdaten.json"
    if not json_path.is_file():
        raise DataMissing(f"missing stammdaten.json under {stammdaten_dir!s}")

    payload: dict[str, Any] = json.loads(json_path.read_text(encoding="utf-8"))

    # liegenschaft is a single dict in the Buena dataset (the WEG that
    # owns the three Häuser).
    liegenschaft = payload.get("liegenschaft", {}) or {}
    liegenschaften: list[dict[str, Any]] = []
    if liegenschaft:
        lie_id = str(liegenschaft.get("id") or "")
        liegenschaften.append(
            {
                "buena_liegenschaft_id": lie_id,
                "name": str(
                    liegenschaft.get("name")
                    or f"WEG {liegenschaft.get('strasse', '')}".strip()
                ),
                "metadata": {
                    "buena_liegenschaft_id": lie_id,
                    "strasse": liegenschaft.get("strasse"),
                    "plz": liegenschaft.get("plz"),
                    "ort": liegenschaft.get("ort"),
                },
            }
        )
    gebaeude_list: list[dict[str, Any]] = list(payload.get("gebaeude", []) or [])
    einheiten_list: list[dict[str, Any]] = list(payload.get("einheiten", []) or [])
    eigentuemer_list: list[dict[str, Any]] = list(payload.get("eigentuemer", []) or [])
    mieter_list: list[dict[str, Any]] = list(payload.get("mieter", []) or [])
    dienstleister_list: list[dict[str, Any]] = list(payload.get("dienstleister", []) or [])

    # ---- Buildings (one per gebaeude row, address from liegenschaft) ----
    base_address = (
        f"{liegenschaft.get('strasse', '')}, "
        f"{liegenschaft.get('plz', '')} {liegenschaft.get('ort', '')}"
    ).strip(", ")
    buildings: list[dict[str, Any]] = []
    for g in gebaeude_list:
        haus_id = str(g.get("id", ""))
        hausnr = str(g.get("hausnr", "") or "")
        building_address = (
            f"{liegenschaft.get('strasse', '')} {hausnr}, "
            f"{liegenschaft.get('plz', '')} {liegenschaft.get('ort', '')}"
        ).strip(", ").strip()
        buildings.append(
            {
                "buena_haus_id": haus_id,
                "buena_liegenschaft_id": liegenschaft.get("id"),
                "address": building_address or base_address,
                "year_built": g.get("baujahr"),
                "metadata": {
                    "buena_haus_id": haus_id,
                    "hausnr": hausnr,
                    "etagen": g.get("etagen"),
                    "fahrstuhl": g.get("fahrstuhl"),
                    "einheiten": g.get("einheiten"),
                    "liegenschaft_id": liegenschaft.get("id"),
                    "liegenschaft_name": liegenschaft.get("name"),
                },
            }
        )

    # ---- Owners (from eigentuemer JSON; PII redacted per row) ----
    owners: list[dict[str, Any]] = []
    for e in eigentuemer_list:
        full_name = " ".join(
            part for part in (e.get("vorname"), e.get("nachname")) if part
        ).strip() or (e.get("firma") or "Unbekannt")
        owners.append(
            {
                "buena_eig_id": e.get("id"),
                "name": full_name,
                "email": redact.email_redact(e.get("email")),
                "phone": redact.phone_last4(e.get("telefon")),
                "preferences": {
                    "buena_eig_id": e.get("id"),
                    "anrede": e.get("anrede"),
                    "firma": e.get("firma"),
                    "ort": e.get("ort"),
                    "iban_last4": redact.iban_last4(e.get("iban")),
                    "selbstnutzer": e.get("selbstnutzer"),
                    "sev_mandat": e.get("sev_mandat"),
                    "beirat": e.get("beirat"),
                    "sprache": e.get("sprache"),
                    "einheit_ids": list(e.get("einheit_ids") or []),
                },
            }
        )

    # ---- Properties (one per einheit row) ----
    # Resolve owner: pick the first eigentuemer whose einheit_ids contains
    # this einheit. The DB will store the resolved owner_id once we
    # upsert; here we just pass `buena_eig_id` along.
    eh_to_owner: dict[str, str] = {}
    for e in eigentuemer_list:
        for eh in e.get("einheit_ids") or []:
            eh_to_owner.setdefault(str(eh), str(e.get("id") or ""))

    properties: list[dict[str, Any]] = []
    for u in einheiten_list:
        eh_id = str(u.get("id", ""))
        haus_id = str(u.get("haus_id", ""))
        einheit_nr = str(u.get("einheit_nr") or "")
        lage = str(u.get("lage") or "")
        addr_short = _short_address(
            str(liegenschaft.get("strasse", "")),
            str(next(
                (g["hausnr"] for g in gebaeude_list if str(g.get("id")) == haus_id),
                "",
            )),
        )
        prop_name = (
            f"{liegenschaft.get('strasse', '').strip()} {einheit_nr}".strip()
            if liegenschaft.get("strasse")
            else f"{eh_id} {einheit_nr}".strip()
        )
        aliases = [
            eh_id,
            haus_id,
            einheit_nr,
            f"{addr_short} {einheit_nr}".strip(),
        ]
        if lage:
            aliases.append(lage)
        aliases = [a for a in aliases if a]

        properties.append(
            {
                "buena_eh_id": eh_id,
                "buena_haus_id": haus_id,
                "buena_eig_id": eh_to_owner.get(eh_id, ""),
                "name": prop_name,
                "address": (
                    f"{liegenschaft.get('strasse', '')} {einheit_nr}, "
                    f"{liegenschaft.get('plz', '')} {liegenschaft.get('ort', '')}"
                ).strip(", ").strip(),
                "aliases": aliases,
                "metadata": {
                    "buena_eh_id": eh_id,
                    "buena_haus_id": haus_id,
                    "einheit_nr": einheit_nr,
                    "lage": lage,
                    "typ": u.get("typ"),
                    "wohnflaeche_qm": u.get("wohnflaeche_qm"),
                    "zimmer": u.get("zimmer"),
                    "miteigentumsanteil": u.get("miteigentumsanteil"),
                },
            }
        )

    # ---- Tenants (PII redacted; only active or recently active) ----
    tenants: list[dict[str, Any]] = []
    for m in mieter_list:
        full_name = " ".join(
            part for part in (m.get("vorname"), m.get("nachname")) if part
        ).strip() or "Mieter"
        eh_id = str(m.get("einheit_id", ""))
        mietbeginn: date | None = parse_iso_date(m.get("mietbeginn"))
        mietende: date | None = parse_iso_date(m.get("mietende"))
        tenants.append(
            {
                "buena_mie_id": m.get("id"),
                "buena_eh_id": eh_id,
                "name": full_name,
                "email": redact.email_redact(m.get("email")),
                "phone": redact.phone_last4(m.get("telefon")),
                "move_in_date": mietbeginn,
                "metadata": {
                    "buena_mie_id": m.get("id"),
                    "buena_eh_id": eh_id,
                    "mietbeginn": (
                        mietbeginn.isoformat() if mietbeginn else None
                    ),
                    "mietende": (
                        mietende.isoformat() if mietende else None
                    ),
                    "kaltmiete": m.get("kaltmiete"),
                    "nk_vorauszahlung": m.get("nk_vorauszahlung"),
                    "kaution": m.get("kaution"),
                    "iban_last4": redact.iban_last4(m.get("iban")),
                    "sprache": m.get("sprache"),
                    "active": mietende is None,
                },
            }
        )

    # ---- Contractors ----
    contractors: list[dict[str, Any]] = []
    for d in dienstleister_list:
        contractors.append(
            {
                "buena_dl_id": d.get("id"),
                "name": str(d.get("firma") or d.get("ansprechpartner") or ""),
                "specialty": d.get("branche"),
                "rating": None,
                "contact": {
                    "buena_dl_id": d.get("id"),
                    "ansprechpartner": d.get("ansprechpartner"),
                    "email": redact.email_redact(d.get("email")),
                    "phone": redact.phone_last4(d.get("telefon")),
                    "ort": d.get("ort"),
                    "iban_last4": redact.iban_last4(d.get("iban")),
                    "ust_id": d.get("ust_id"),
                    "steuernummer": d.get("steuernummer"),
                    "stundensatz": d.get("stundensatz"),
                    "vertrag_monatlich": d.get("vertrag_monatlich"),
                    "sprache": d.get("sprache"),
                },
            }
        )

    log.info(
        "buena.stammdaten.load",
        liegenschaften=len(liegenschaften),
        owners=len(owners),
        buildings=len(buildings),
        properties=len(properties),
        tenants=len(tenants),
        contractors=len(contractors),
    )
    return BuenaStammdaten(
        liegenschaften=liegenschaften,
        owners=owners,
        buildings=buildings,
        properties=properties,
        tenants=tenants,
        contractors=contractors,
    )


# -----------------------------------------------------------------------------
# Event composers — these are what the CLI actually calls
# -----------------------------------------------------------------------------


def iter_emails(root: Path) -> Iterator[ConnectorEvent]:
    """Yield events for every archive ``.eml`` under ``root/emails/``."""
    yield from eml_archive.walk_directory(root / "emails")


def iter_bank(root: Path) -> Iterator[ConnectorEvent]:
    """Yield events for the master bank index."""
    yield from camt_bank.walk_csv(root / "bank" / "bank_index.csv")


def iter_invoices(
    root: Path, *, read_text: bool = False, use_llm: bool = False
) -> Iterator[ConnectorEvent]:
    """Yield events for every invoice PDF under ``root/rechnungen/``."""
    yield from pdf_invoice_archive.walk_directory(
        root / "rechnungen", read_text=read_text, use_llm=use_llm
    )


def iter_letters(
    root: Path, *, read_text: bool = False, use_llm: bool = False
) -> Iterator[ConnectorEvent]:
    """Yield events for every letter PDF under ``root/briefe/``."""
    yield from pdf_letter_archive.walk_directory(
        root / "briefe", read_text=read_text, use_llm=use_llm
    )


def iter_incremental_day(
    root: Path,
    day: int,
    *,
    read_text: bool = False,
    use_llm: bool = False,
) -> Iterator[ConnectorEvent]:
    """Yield events for one ``incremental/day-NN`` snapshot."""
    day_root = root / "incremental" / f"day-{day:02d}"
    if not day_root.is_dir():
        log.warning("buena.incremental.missing", day=day, path=str(day_root))
        return
    yield from eml_archive.walk_directory(day_root / "emails")
    yield from camt_bank.walk_csv(day_root / "bank" / "bank_index.csv")
    yield from pdf_invoice_archive.walk_directory(
        day_root / "rechnungen", read_text=read_text, use_llm=use_llm
    )
