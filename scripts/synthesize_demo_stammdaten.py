"""Synthesize a PII-free copy of ``Extracted/stammdaten/stammdaten.json``.

Why this script exists
======================
``Extracted/`` is gitignored (real Buena partner data, names + IBANs in the
clear). Cloud Run deploys and fresh clones therefore have no stammdaten and
``GET /properties`` returns only the 4 hand-crafted seed rows. This script
produces an equivalent ``Extracted_demo/stammdaten/stammdaten.json`` that is
safe to commit:

* All IDs preserved verbatim (``EH-NNN``, ``EIG-NNN``, ``MIE-NNN``, ``DL-NNN``,
  ``HAUS-12/14/16``, ``LIE-001``) — these are routing primary keys the rest
  of the system depends on.
* All foreign-key columns preserved (``einheit_id``, ``eigentuemer_id``,
  ``einheit_ids``, ``haus_id``).
* ``einheiten`` rows copied verbatim — no PII, just unit shape.
* Building shape (``etagen``, ``baujahr``, ``fahrstuhl``) preserved — used
  by the Building Context renderer.
* Operational numbers (``kaltmiete``, ``kaution``, ``wohnflaeche_qm``,
  ``miteigentumsanteil``) preserved — needed for realistic demo math.
* Liegenschaft demo address (Immanuelkirchstraße 26, 10405 Berlin) preserved
  — already public in KEYSTONE.md and the beginner's guide.
* Person/company names, emails, phones, IBANs, street addresses, tax IDs
  REPLACED with deterministic synthetic German values. The verwalter
  (Buena's real partner property manager) is also replaced.

Determinism
-----------
Each replacement is derived from ``hashlib.sha256(row_id)`` so the output
is reproducible across runs. A future run regenerates the same demo file.

Usage
-----
    python -m scripts.synthesize_demo_stammdaten

Reads from ``Extracted/stammdaten/stammdaten.json`` (must exist locally),
writes to ``Extracted_demo/stammdaten/stammdaten.json`` (committed).
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPO_ROOT / "Extracted" / "stammdaten" / "stammdaten.json"
TARGET_DIR = REPO_ROOT / "Extracted_demo" / "stammdaten"
TARGET = TARGET_DIR / "stammdaten.json"

GERMAN_FIRST_NAMES_M = [
    "Lukas", "Maximilian", "Felix", "Paul", "Jonas", "Ben", "Tim", "Tom",
    "Niklas", "Leon", "Finn", "David", "Daniel", "Samuel", "Erik",
    "Moritz", "Jakob", "Elias", "Noah", "Henry",
]
GERMAN_FIRST_NAMES_F = [
    "Anna", "Sophie", "Lena", "Marie", "Hannah", "Emma", "Lisa", "Sara",
    "Mia", "Lea", "Clara", "Laura", "Julia", "Lara", "Anja",
    "Charlotte", "Emilia", "Greta", "Ida", "Johanna",
]
GERMAN_LAST_NAMES = [
    "Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner",
    "Becker", "Schulz", "Hoffmann", "Schäfer", "Koch", "Bauer", "Richter",
    "Klein", "Wolf", "Schröder", "Neumann", "Schwarz", "Zimmermann",
    "Braun", "Krüger", "Hartmann", "Lange", "Werner", "Schmitz", "Krause",
    "Meier", "Lehmann", "Schmid",
]

# Per-contractor demo company names — same order/length as the source
# (16 rows). Mirrors the original branche so the demo still demonstrates
# the same vendor mix without using real corporate identities.
CONTRACTOR_FIRMA_BY_INDEX = [
    "Hausmeister Demo GmbH",
    "Aufzug Demo & Co. KG",
    "Heiztechnik Demo GmbH",
    "Reinigung Demo GmbH",
    "Gartenbau Demo GbR",
    "Schornstein Demo",
    "Versicherung Demo AG",
    "Strom Demo GmbH",
    "Gas Demo AG",
    "Wasser Demo GmbH",
    "Entsorgung Demo GmbH",
    "Elektro Demo e.K.",
    "Sanitär Demo GmbH",
    "Dach Demo GmbH",
    "Schließanlagen Demo Ltd.",
    "Fassaden Demo GmbH",
]


def _hash_int(seed: str, modulo: int) -> int:
    """Deterministic ``int`` in ``[0, modulo)`` derived from ``seed``."""
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % modulo


def _ascii_fold(value: str) -> str:
    """Strip umlauts/diacritics for use in email local-parts."""
    nfkd = unicodedata.normalize("NFKD", value)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _person_name(row_id: str, anrede: str | None) -> tuple[str, str]:
    """Return ``(vorname, nachname)`` deterministic from ``row_id``."""
    is_female = (anrede or "").strip().lower() == "frau"
    pool = GERMAN_FIRST_NAMES_F if is_female else GERMAN_FIRST_NAMES_M
    vorname = pool[_hash_int(row_id + "::v", len(pool))]
    nachname = GERMAN_LAST_NAMES[_hash_int(row_id + "::n", len(GERMAN_LAST_NAMES))]
    return vorname, nachname


def _email(vorname: str, nachname: str, domain: str = "example.de") -> str:
    local = f"{_ascii_fold(vorname)}.{_ascii_fold(nachname)}".lower()
    return f"{local}@{domain}"


def _phone(row_id: str) -> str:
    suffix = _hash_int(row_id + "::p", 10000)
    return f"+49 30 555 {suffix:04d}"


def _iban(row_id: str) -> str:
    """Return a 22-char synthetic German IBAN with deterministic last-4.

    All digits zero except the trailing 4 — preserves length + ``DE`` prefix
    so any IBAN regex still matches and ``redact.iban_last4`` produces a
    distinct value per row.
    """
    suffix = _hash_int(row_id + "::iban", 10000)
    return f"DE00000000000000000{suffix:04d}"


def _street(row_id: str) -> str:
    house_no = _hash_int(row_id + "::s", 99) + 1
    return f"Demoplatz {house_no}"


def _company_email(firma: str, domain: str = "example.de") -> str:
    slug = _ascii_fold(firma).lower()
    slug = "".join(ch if ch.isalnum() else "-" for ch in slug)
    slug = "-".join(seg for seg in slug.split("-") if seg)
    return f"info@{slug}.{domain.split('.')[-1]}"


def _ust_id(row_id: str) -> str:
    suffix = _hash_int(row_id + "::ust", 1_000_000_000)
    return f"DE{suffix:09d}"


def _steuernummer(row_id: str) -> str:
    a = _hash_int(row_id + "::st1", 100)
    b = _hash_int(row_id + "::st2", 1000)
    c = _hash_int(row_id + "::st3", 100000)
    return f"{a:02d}/{b:03d}/{c:05d}"


def synthesize_liegenschaft(src: dict[str, Any]) -> dict[str, Any]:
    """Keep public address fields, replace all verwalter PII."""
    return {
        "id": src["id"],
        "name": src["name"],
        "strasse": src["strasse"],
        "plz": src["plz"],
        "ort": src["ort"],
        "baujahr": src.get("baujahr"),
        "sanierung": src.get("sanierung"),
        "verwalter": "Hausverwaltung Demo GmbH",
        "verwalter_strasse": "Demoplatz 1",
        "verwalter_plz": "10117",
        "verwalter_ort": "Berlin",
        "verwalter_email": "info@hausverwaltung-demo.example",
        "verwalter_telefon": "+49 30 555 0001",
        "verwalter_iban": "DE00 0000 0000 0000 0000 01",
        "verwalter_bic": src.get("verwalter_bic"),
        "verwalter_bank": src.get("verwalter_bank"),
        "verwalter_steuernummer": "00/000/00000",
        "weg_bankkonto_iban": "DE00 0000 0000 0000 0000 02",
        "weg_bankkonto_bic": src.get("weg_bankkonto_bic"),
        "weg_bankkonto_bank": src.get("weg_bankkonto_bank"),
        "ruecklage_iban": "DE00 0000 0000 0000 0000 03",
        "ruecklage_bic": src.get("ruecklage_bic"),
    }


def synthesize_owner(src: dict[str, Any]) -> dict[str, Any]:
    row_id = src["id"]
    is_firma_only = bool(src.get("firma")) and not src.get("vorname")
    if is_firma_only:
        # Preserve "Firma" anrede shape; replace company name.
        firma = f"Demo Vermögensverwaltung {row_id[-3:]}"
        email = _company_email(firma)
        return {
            "id": row_id,
            "anrede": "Firma",
            "vorname": "",
            "nachname": "",
            "firma": firma,
            "strasse": _street(row_id),
            "plz": "10115",
            "ort": "Berlin",
            "land": src.get("land", "DE"),
            "email": email,
            "telefon": _phone(row_id),
            "iban": _iban(row_id),
            "bic": src.get("bic"),
            "einheit_ids": src.get("einheit_ids"),
            "selbstnutzer": src.get("selbstnutzer"),
            "sev_mandat": src.get("sev_mandat"),
            "beirat": src.get("beirat"),
            "sprache": src.get("sprache", "de"),
        }
    vorname, nachname = _person_name(row_id, src.get("anrede"))
    return {
        "id": row_id,
        "anrede": src.get("anrede"),
        "vorname": vorname,
        "nachname": nachname,
        "firma": "",
        "strasse": _street(row_id),
        "plz": "10115",
        "ort": "Berlin",
        "land": src.get("land", "DE"),
        "email": _email(vorname, nachname),
        "telefon": _phone(row_id),
        "iban": _iban(row_id),
        "bic": src.get("bic"),
        "einheit_ids": src.get("einheit_ids"),
        "selbstnutzer": src.get("selbstnutzer"),
        "sev_mandat": src.get("sev_mandat"),
        "beirat": src.get("beirat"),
        "sprache": src.get("sprache", "de"),
    }


def synthesize_tenant(src: dict[str, Any]) -> dict[str, Any]:
    row_id = src["id"]
    vorname, nachname = _person_name(row_id, src.get("anrede"))
    return {
        "id": row_id,
        "anrede": src.get("anrede"),
        "vorname": vorname,
        "nachname": nachname,
        "email": _email(vorname, nachname),
        "telefon": _phone(row_id),
        "einheit_id": src.get("einheit_id"),
        "eigentuemer_id": src.get("eigentuemer_id"),
        "mietbeginn": src.get("mietbeginn"),
        "mietende": src.get("mietende"),
        "kaltmiete": src.get("kaltmiete"),
        "nk_vorauszahlung": src.get("nk_vorauszahlung"),
        "kaution": src.get("kaution"),
        "iban": _iban(row_id),
        "bic": src.get("bic"),
        "sprache": src.get("sprache", "de"),
    }


def synthesize_contractor(src: dict[str, Any], index: int) -> dict[str, Any]:
    row_id = src["id"]
    firma = (
        CONTRACTOR_FIRMA_BY_INDEX[index]
        if index < len(CONTRACTOR_FIRMA_BY_INDEX)
        else f"Dienstleister Demo GmbH {row_id[-3:]}"
    )
    vorname, nachname = _person_name(row_id + "::contact", "Herr")
    return {
        "id": row_id,
        "firma": firma,
        "branche": src.get("branche"),
        "ansprechpartner": f"{vorname} {nachname}",
        "email": _company_email(firma),
        "telefon": _phone(row_id),
        "strasse": _street(row_id),
        "plz": "10115",
        "ort": "Berlin",
        "land": "DE",
        "iban": _iban(row_id),
        "bic": src.get("bic"),
        "ust_id": _ust_id(row_id),
        "steuernummer": _steuernummer(row_id),
        "stil": src.get("stil"),
        "sprache": src.get("sprache", "de"),
        "vertrag_monatlich": src.get("vertrag_monatlich"),
        "stundensatz": src.get("stundensatz"),
    }


def main() -> None:
    if not SOURCE.is_file():
        raise SystemExit(
            f"source stammdaten not found at {SOURCE}; this script must run "
            "from a checkout that has Extracted/ populated locally."
        )
    src = json.loads(SOURCE.read_text(encoding="utf-8"))

    out = {
        "liegenschaft": synthesize_liegenschaft(src["liegenschaft"]),
        "gebaeude": list(src["gebaeude"]),
        "einheiten": list(src["einheiten"]),
        "eigentuemer": [synthesize_owner(e) for e in src["eigentuemer"]],
        "mieter": [synthesize_tenant(m) for m in src["mieter"]],
        "dienstleister": [
            synthesize_contractor(d, i) for i, d in enumerate(src["dienstleister"])
        ],
    }

    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(
        json.dumps(out, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {TARGET}")
    print(
        f"  liegenschaft: 1   gebaeude: {len(out['gebaeude'])}   "
        f"einheiten: {len(out['einheiten'])}"
    )
    print(
        f"  eigentuemer: {len(out['eigentuemer'])}   "
        f"mieter: {len(out['mieter'])}   "
        f"dienstleister: {len(out['dienstleister'])}"
    )


if __name__ == "__main__":
    main()
