"""Tests for the PDF document-type classifier."""

from __future__ import annotations

import pytest

from connectors import document_type


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("20240124_DL-011_INV-00005.pdf", "invoice"),
        ("20240115_rechnung_aufzug.pdf", "invoice"),
        ("20250403_mahnung_LTR-042.pdf", "mahnung"),
        ("mietvertrag_apt_4b.pdf", "lease"),
        ("mietvertrag_addendum_apt_4b.pdf", "lease_addendum"),
        ("kaufvertrag_2025.pdf", "kaufvertrag"),
        ("baugenehmigung_haus12.pdf", "structural_permit"),
        ("vermessungsprotokoll.pdf", "vermessungsprotokoll"),
    ],
)
def test_filename_heuristic_matches(filename: str, expected: str) -> None:
    assert document_type.classify(filename, head_text="", use_llm=False) == expected


def test_unknown_filename_without_llm_returns_other() -> None:
    assert document_type.classify("zzzz_random.pdf", head_text="", use_llm=False) == "other"


def test_classify_does_not_call_llm_when_use_llm_is_false() -> None:
    # If the heuristic misses, we should get "other" without touching Gemini
    # (no exception, no charge to the cost ledger).
    out = document_type.classify(
        "foo_bar.pdf", head_text="some random text", use_llm=False
    )
    assert out == "other"


def test_document_types_all_present_in_runtime_enum() -> None:
    # Sanity check: the classifier output is one of the canonical values.
    assert "lease" in document_type.DOCUMENT_TYPES
    assert "invoice" in document_type.DOCUMENT_TYPES
    assert "other" in document_type.DOCUMENT_TYPES
    assert "lease_addendum" in document_type.DOCUMENT_TYPES
