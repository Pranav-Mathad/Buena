"""The schema runs on plain PostgreSQL: no extension-specific column types."""

from __future__ import annotations

import psycopg2
import pytest

from backend.config import get_settings


@pytest.mark.usefixtures("db")
def test_no_columns_need_an_extension() -> None:
    with psycopg2.connect(get_settings().database_url_sync) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND (column_name = 'embedding' OR data_type = 'USER-DEFINED')
            """
        )
        rows = cur.fetchall()
    conn.close()

    assert rows == []
