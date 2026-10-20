"""Rewrite expressions are derived from history without naming what they replace."""

from __future__ import annotations

import hashlib

import pytest

from scripts import build_rewrite_expressions as rewrite
from scripts import check_sensitive_data as check

# Assembled at runtime so this file holds no literal the sensitive-data check flags.
_IBAN = "DE" + "0" * 15 + "91234"
_URL = "https://svc-1.europe-west1." + "run.app"


@pytest.fixture
def block_stand_in(monkeypatch: pytest.MonkeyPatch) -> None:
    """Block "Teststraße 7" in place of the real address."""
    digest = hashlib.sha256(check.normalise_address("Test", "7").encode()).hexdigest()
    monkeypatch.setattr(rewrite, "BLOCKED_ADDRESS_HASHES", frozenset({digest}))


@pytest.mark.usefixtures("block_stand_in")
def test_address_rules_cover_postcode_casing_and_abbreviations() -> None:
    history = "Teststraße 7, 10999 Berlin\nteststraße 7\nTeststr. 7\nOtherstraße 7, 10999 Berlin"

    assert rewrite.address_rules(history) == [
        "Teststraße 7, 10999 Berlin==>Musterstraße 7, 10115 Berlin",
        "Test==>Muster",
        "test==>muster",
    ]


@pytest.mark.usefixtures("block_stand_in")
def test_no_address_rules_when_history_is_clean() -> None:
    assert rewrite.address_rules("Musterstraße 26, 10115 Berlin") == []


def test_url_rules_replace_each_cloud_run_url_once() -> None:
    assert rewrite.url_rules(f"{_URL}/docs and {_URL}") == [f"{_URL}==>https://api.example.com"]


def test_changed_runs_pair_removed_and_added_words() -> None:
    word_diff = "\n".join(
        ["--- a/x", "+++ b/x", " From: ", "-Real Person", "+Mia Schneider", " ok"]
    )

    assert rewrite.changed_runs(word_diff) == [("Real Person", "Mia Schneider")]


def test_name_rules_skip_runs_still_in_the_tree_and_ibans() -> None:
    pairs = [
        ("Real Person", "Mia Schneider"),
        ("Berlin", "Demo"),  # still in the current tree: not removed data
        (f"iban {_IBAN}", "iban built at runtime"),  # left to the IBAN rule
        ("1234", "5678"),  # no letters: not a name
        ("Real Person Senior", "Erik Schmid"),
    ]

    rules = rewrite.name_rules(pairs, current_tree="Musterstraße 26, 10115 Berlin")

    assert rules == ["Real Person Senior==>Erik Schmid", "Real Person==>Mia Schneider"]
