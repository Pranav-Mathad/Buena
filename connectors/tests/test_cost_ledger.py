"""``reset_label`` deletes only test labels unless production use is explicit."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from uuid import uuid4

import psycopg2
import pytest

from backend.config import get_settings
from connectors import cost_ledger


def test_production_label_is_refused_before_touching_the_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_connection(url: str | None = None) -> None:
        raise AssertionError("reset_label opened a connection for a refused label")

    monkeypatch.setattr(cost_ledger, "_connect", no_connection)

    with pytest.raises(ValueError, match="step6_email_backfill"):
        cost_ledger.reset_label("step6_email_backfill")


@pytest.fixture
def label_with_spend(db: None) -> Iterator[str]:
    """A non-test ledger label with a row, deleted again after the test."""
    label = f"guardcheck_{uuid4().hex[:8]}"
    cost_ledger.ensure_label(label, "1.00")
    cost_ledger.charge(label, "0.25")
    yield label
    with psycopg2.connect(get_settings().database_url_sync) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM cost_ledger WHERE source_label = %s", (label,))
        conn.commit()


def test_refused_reset_keeps_the_spend_record(label_with_spend: str) -> None:
    with pytest.raises(ValueError):
        cost_ledger.reset_label(label_with_spend)

    state = cost_ledger.get_state(label_with_spend)
    assert state is not None
    assert state.cumulative_usd == Decimal("0.25")


def test_explicit_production_reset_deletes_the_row(label_with_spend: str) -> None:
    cost_ledger.reset_label(label_with_spend, allow_production=True)

    assert cost_ledger.get_state(label_with_spend) is None


def test_test_label_resets_without_the_flag(db: None) -> None:
    label = f"{cost_ledger.TEST_LABEL_PREFIX}guard_{uuid4().hex[:8]}"
    cost_ledger.ensure_label(label, "1.00")

    cost_ledger.reset_label(label)

    assert cost_ledger.get_state(label) is None
