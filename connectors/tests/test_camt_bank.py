"""Tests for the bank CSV connector."""

from __future__ import annotations

from pathlib import Path

from connectors import camt_bank, redact


def _write_bank_csv(path: Path) -> None:
    path.write_text(
        "id,datum,typ,betrag,kategorie,gegen_name,verwendungszweck,referenz_id,error_types\n"
        "TX-00001,2024-01-01,CREDIT,1256.00,miete,Chantal Tasche,Miete 01/2024 EH-045,MIE-006,\n"
        "TX-00002,2024-01-15,DEBIT,650.00,dienstleister,Hausmeister Mueller GmbH,Wartung INV-00012 DE94100701240494519832,INV-00012,\n",
        encoding="utf-8",
    )


def test_parse_row_extracts_eh_and_mie(tmp_path: Path) -> None:
    csv_path = tmp_path / "bank.csv"
    _write_bank_csv(csv_path)

    events = list(camt_bank.walk_csv(csv_path))
    assert len(events) == 2

    first = events[0]
    assert first.source == "bank"
    assert first.metadata["eh_id"] == "EH-045"
    assert first.metadata["mie_id"] is None  # MIE-006 is in referenz_id, not vw
    assert first.metadata["betrag"] == "1256.00"
    assert first.metadata["typ"] == "CREDIT"
    assert first.metadata["betrag_signed"] == "1256.00"

    second = events[1]
    assert second.metadata["typ"] == "DEBIT"
    assert second.metadata["betrag_signed"] == "-650.00"
    assert second.metadata["invoice_ref"] == "INV-00012"
    # IBAN inside verwendungszweck must be scrubbed
    assert "DE94100701240494519832" not in second.raw_content
    assert "DE94100701240494519832" not in second.metadata["verwendungszweck"]
    redact.assert_no_raw_iban(second.metadata["verwendungszweck"])


def test_source_ref_stable_across_runs(tmp_path: Path) -> None:
    csv_path = tmp_path / "bank.csv"
    _write_bank_csv(csv_path)

    refs1 = [e.source_ref for e in camt_bank.walk_csv(csv_path)]
    refs2 = [e.source_ref for e in camt_bank.walk_csv(csv_path)]
    assert refs1 == refs2
    assert len(set(refs1)) == 2  # distinct rows produce distinct refs


def test_walk_csv_handles_missing_file(tmp_path: Path) -> None:
    events = list(camt_bank.walk_csv(tmp_path / "missing.csv"))
    assert events == []
