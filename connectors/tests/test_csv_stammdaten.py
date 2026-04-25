"""Tests for the generic CSV master-data loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from connectors import csv_stammdaten
from connectors.csv_stammdaten import ColumnMap


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def test_read_rows_redacts_pii_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "owners.csv"
    _write_csv(
        csv_path,
        header="id,name,email,telefon,iban,strasse",
        rows=[
            "EIG-001,Alice Schmidt,alice@residence.de,+49 30 1234 5678,DE94120300004034471349,Hauptstr. 1",
            "EIG-002,Bob Bauer,bob@example.de,030 87654321,DE99100100100249947174,Nebenstr. 2",
        ],
    )
    column_map = ColumnMap(
        required={
            "id": "id",
            "name": "name",
            "email": "email",
            "telefon": "telefon",
            "iban": "iban",
        },
        metadata_columns=("strasse",),
        pii_columns=("email", "telefon", "iban"),
    )

    rows = list(csv_stammdaten.read_rows(csv_path, "owner", column_map))
    assert len(rows) == 2

    first = rows[0]
    assert first.entity_type == "owner"
    assert first.natural_key == "EIG-001"
    assert first.fields["name"] == "Alice Schmidt"
    assert first.fields["email"] == "alice@example.com"
    assert "DE94120300004034471349" not in str(first.fields)
    assert first.fields["iban"].startswith("****")
    assert first.fields["iban"].endswith("1349")
    assert first.fields["telefon"].startswith("+49")
    # Metadata pass-through
    assert first.metadata["strasse"] == "Hauptstr. 1"


def test_read_rows_raises_on_missing_file(tmp_path: Path) -> None:
    column_map = ColumnMap(required={"id": "id"})
    with pytest.raises(FileNotFoundError):
        list(csv_stammdaten.read_rows(tmp_path / "nope.csv", "owner", column_map))


def test_parse_iso_date_handles_blank() -> None:
    assert csv_stammdaten.parse_iso_date("") is None
    assert csv_stammdaten.parse_iso_date(None) is None
    assert csv_stammdaten.parse_iso_date("not-a-date") is None
    parsed = csv_stammdaten.parse_iso_date("2024-09-01")
    assert parsed is not None
    assert parsed.year == 2024 and parsed.month == 9 and parsed.day == 1
