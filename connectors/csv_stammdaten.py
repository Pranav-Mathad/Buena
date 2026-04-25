"""Generic CSV master-data loader.

Parameterised by entity type (``owner | building | property | tenant
| contractor``) and a column mapping. The loader reads the CSV, applies
PII redaction, and adapts each row into one of Phase 0's seed
dataclasses (``OwnerSeed``, ``BuildingSeed``, …) so we can reuse the
existing ``seed.seed._upsert_*`` helpers.

The Buena composer (:mod:`connectors.buena_archive`) wires four CSV
readers — `eigentuemer`, `gebaeude/liegenschaft`, `mieter`,
`dienstleister` — plus the unit/property hybrid that comes from
`einheiten.csv` joined with `liegenschaft` rows in `stammdaten.json`.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, cast

import structlog

from connectors import redact

log = structlog.get_logger(__name__)


EntityType = Literal["owner", "building", "property", "tenant", "contractor"]


@dataclass(frozen=True)
class ColumnMap:
    """How a customer's CSV columns map onto Keystone's canonical fields.

    The keys are Keystone field names; the values are the customer's
    column header in the source CSV. ``metadata_columns`` is the
    pass-through bucket — every header listed here ends up under
    ``metadata`` after PII redaction.
    """

    required: Mapping[str, str]
    metadata_columns: tuple[str, ...] = ()
    pii_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class StammdatenRow:
    """Generic, redacted master-data row ready for an upsert."""

    entity_type: EntityType
    natural_key: str
    fields: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


def _parse_date(raw: str | None) -> date | None:
    """Best-effort ISO date parse. Returns ``None`` for blank / invalid input."""
    if not raw or not raw.strip():
        return None
    try:
        return datetime.fromisoformat(raw.strip()).date()
    except ValueError:
        try:
            return datetime.strptime(raw.strip(), "%Y-%m-%d").date()
        except ValueError:
            return None


def _redact_value(column: str, value: str, pii_columns: Iterable[str]) -> Any:
    """Apply per-column redaction. Recognised columns: iban, bic, telefon, email."""
    if column not in pii_columns:
        return value
    lower = column.lower()
    if "iban" in lower:
        return redact.iban_last4(value)
    if "telefon" in lower or "phone" in lower:
        return redact.phone_last4(value, keep_country_code=True)
    if "email" in lower or "mail" in lower:
        return redact.email_redact(value)
    if "bic" in lower:
        # BIC is bank identifier — not strictly PII but Buena treats it as
        # bank-internal. Keep as-is; downstream can scrub if needed.
        return value
    return redact.scrub_text(value)


def read_rows(
    path: Path,
    entity_type: EntityType,
    column_map: ColumnMap,
    *,
    delimiter: str = ",",
) -> Iterator[StammdatenRow]:
    """Stream rows from a CSV, applying redaction and the column map.

    Yields :class:`StammdatenRow` instances. Caller decides what to do
    with the row (typically: build the matching seed dataclass and call
    ``seed.seed._upsert_*``).
    """
    if not path.is_file():
        raise FileNotFoundError(f"stammdaten CSV not found: {path}")

    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        for raw_row in reader:
            redacted: dict[str, Any] = {}
            for column, value in raw_row.items():
                if value is None:
                    redacted[column] = None
                    continue
                redacted[column] = _redact_value(
                    column, value.strip(), column_map.pii_columns
                )

            mapped: dict[str, Any] = {}
            for canonical, source_col in column_map.required.items():
                mapped[canonical] = redacted.get(source_col, "")

            metadata: dict[str, Any] = {}
            for col in column_map.metadata_columns:
                if col in redacted:
                    metadata[col] = redacted[col]

            natural_key = str(mapped.get("id") or mapped.get("email") or
                              mapped.get("name") or "")
            log.debug(
                "stammdaten.row",
                entity_type=entity_type,
                natural_key=natural_key,
            )
            yield StammdatenRow(
                entity_type=cast(EntityType, entity_type),
                natural_key=natural_key,
                fields=mapped,
                metadata=metadata,
            )


def parse_iso_date(value: Any) -> date | None:
    """Public helper — exposed so composers can keep date logic consistent."""
    if value in (None, "", b""):
        return None
    if isinstance(value, date):
        return value
    return _parse_date(str(value))
