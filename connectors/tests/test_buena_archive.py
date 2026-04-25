"""Smoke tests for the Buena composite.

These tests run only when the ``Extracted/`` folder is actually
present on disk (skipped otherwise so the suite stays portable). They
exercise the PII contract end-to-end against the real customer dataset
and the cardinalities the survey reported.
"""

from __future__ import annotations

import pytest

from connectors import buena_archive, camt_bank, eml_archive, redact
from connectors.base import DataMissing


def _root_or_skip():
    try:
        return buena_archive.require_root()
    except DataMissing as exc:
        pytest.skip(f"Buena dataset not present locally — {exc}")


def test_get_extracted_root_falls_back_to_default() -> None:
    # With no env var explicitly set to anything, we get the default path
    # (which may or may not exist — that's fine for this assertion).
    root = buena_archive.get_extracted_root("")
    assert root.name == "Extracted"


def test_require_root_raises_for_missing_path(tmp_path) -> None:
    with pytest.raises(DataMissing):
        buena_archive.require_root(str(tmp_path / "does_not_exist"))


def test_load_stammdaten_cardinalities() -> None:
    root = _root_or_skip()
    payload = buena_archive.load_stammdaten(root)
    assert len(payload.buildings) == 3
    assert len(payload.properties) == 52
    assert len(payload.owners) == 35
    assert len(payload.tenants) == 26
    assert len(payload.contractors) == 16


def test_load_stammdaten_redacts_iban_in_owners_and_tenants() -> None:
    root = _root_or_skip()
    payload = buena_archive.load_stammdaten(root)
    for owner in payload.owners:
        iban_last4 = owner["preferences"].get("iban_last4")
        if iban_last4 is not None:
            assert iban_last4.startswith("****")
        for value in [str(owner.get("email") or ""), str(owner.get("phone") or "")]:
            redact.assert_no_raw_iban(value)
    for tenant in payload.tenants:
        iban_last4 = tenant["metadata"].get("iban_last4")
        if iban_last4 is not None:
            assert iban_last4.startswith("****")


def test_load_stammdaten_property_aliases_include_eh_and_haus_ids() -> None:
    root = _root_or_skip()
    payload = buena_archive.load_stammdaten(root)
    sample = payload.properties[0]
    assert sample["buena_eh_id"].startswith("EH-")
    assert sample["buena_haus_id"].startswith("HAUS-")
    assert sample["buena_eh_id"] in sample["aliases"]
    assert sample["buena_haus_id"] in sample["aliases"]


def test_iter_emails_first_event_is_redacted() -> None:
    root = _root_or_skip()
    walker = eml_archive.walk_directory(root / "emails")
    first = next(iter(walker), None)
    if first is None:
        pytest.skip("no emails in dataset")
    redact.assert_no_raw_iban(first.raw_content)
    for value in first.metadata.values():
        if isinstance(value, str):
            redact.assert_no_raw_iban(value)


def test_iter_bank_first_event_is_redacted() -> None:
    root = _root_or_skip()
    bank_csv = root / "bank" / "bank_index.csv"
    if not bank_csv.is_file():
        pytest.skip("no bank index in dataset")
    first = next(iter(camt_bank.walk_csv(bank_csv)), None)
    if first is None:
        pytest.skip("bank index empty")
    redact.assert_no_raw_iban(first.raw_content)
    redact.assert_no_raw_iban(first.metadata.get("verwendungszweck", ""))
