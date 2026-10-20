"""The sensitive-data check flags the blocked address, IBANs and Cloud Run URLs."""

from __future__ import annotations

import hashlib

import pytest

from scripts import check_sensitive_data as check

# Assembled at runtime so this file holds no literal the check would flag.
_IBAN = "DE" + "0" * 16 + "1234"
_RUN_APP_URL = "https://keystone-be-123.europe-west1." + "run.app"


@pytest.fixture
def block_stand_in(monkeypatch: pytest.MonkeyPatch) -> None:
    """Block "Teststraße 7" in place of the real address, which is not written here."""
    digest = hashlib.sha256(check.normalise_address("Test", "7").encode()).hexdigest()
    monkeypatch.setattr(check, "BLOCKED_ADDRESS_HASHES", frozenset({digest}))


@pytest.mark.parametrize(
    "spelling", ["Teststraße 7", "teststrasse 7", "Teststr. 7", "TESTSTRASSE  7"]
)
@pytest.mark.usefixtures("block_stand_in")
def test_blocked_address_is_found_in_every_spelling(spelling: str) -> None:
    problems = list(check.find_problems("docs/x.md", f"line one\nlives at {spelling}, Berlin"))

    assert problems == ["docs/x.md:2: blocked street address"]


@pytest.mark.usefixtures("block_stand_in")
def test_other_addresses_pass() -> None:
    text = "Musterstraße 26, 10115 Berlin, and Teststraße 8 next door"

    assert list(check.find_problems("docs/x.md", text)) == []


def test_iban_is_flagged_outside_the_demo_dataset_only() -> None:
    text = f"iban: {_IBAN}"

    assert list(check.find_problems("seed/x.json", text)) == ["seed/x.json:1: IBAN-shaped number"]
    assert list(check.find_problems("Extracted_demo/stammdaten/x.json", text)) == []


def test_cloud_run_url_is_flagged() -> None:
    text = f"VITE_API_BASE_URL={_RUN_APP_URL}"

    assert list(check.find_problems(".env.example", text)) == [".env.example:1: Cloud Run URL"]


def test_tracked_files_are_clean() -> None:
    """The repository itself passes, with the real blocklist."""
    assert check.check(check._tracked_files()) == []
