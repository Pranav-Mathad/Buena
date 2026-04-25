"""Tests for Phase 10 Step 10.4 — ``GET /files/<path>``.

Path safety is the only thing this endpoint exists to enforce, so the
tests focus on traversal attempts (`..`, absolute paths, NUL bytes,
escape via realpath) and on the happy path serving a real file.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.api import files as files_api
from backend.config import get_settings


@pytest.fixture()
def isolated_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point the files router at a tmp dir + reset its lru_cache."""
    monkeypatch.setattr(
        get_settings(), "keystone_files_root", str(tmp_path), raising=True
    )
    files_api._allowed_root.cache_clear()
    yield tmp_path
    files_api._allowed_root.cache_clear()


def test_serves_a_file_inside_root(isolated_root: Path) -> None:
    """A file inside the root resolves and returns its bytes."""
    target = isolated_root / "invoice.pdf"
    target.write_bytes(b"%PDF-1.4 fake content")

    full = files_api._safe_resolve("invoice.pdf")
    assert full == os.path.realpath(str(target))


def test_blocks_relative_path_traversal(isolated_root: Path) -> None:
    """A ``..`` segment that escapes the root is refused with 403."""
    sibling = isolated_root.parent / "secret.txt"
    sibling.write_text("nope")

    with pytest.raises(HTTPException) as excinfo:
        files_api._safe_resolve("../secret.txt")
    assert excinfo.value.status_code in {403, 404}


def test_blocks_absolute_path(isolated_root: Path) -> None:
    """An absolute path is refused even if it would otherwise be reachable."""
    target = isolated_root / "x.pdf"
    target.write_bytes(b"x")

    with pytest.raises(HTTPException) as excinfo:
        files_api._safe_resolve(str(target))
    # path absolute → 400 by our explicit check
    assert excinfo.value.status_code == 400


def test_blocks_nul_byte(isolated_root: Path) -> None:
    """A NUL byte in the path is refused without touching the filesystem."""
    with pytest.raises(HTTPException) as excinfo:
        files_api._safe_resolve("ok.pdf\x00../etc/passwd")
    assert excinfo.value.status_code == 400


def test_blocks_symlink_escape(isolated_root: Path) -> None:
    """A symlink that points outside the root is refused via realpath check."""
    outside = isolated_root.parent / "outside.txt"
    outside.write_text("secret")
    link = isolated_root / "link.txt"
    os.symlink(str(outside), str(link))

    with pytest.raises(HTTPException) as excinfo:
        files_api._safe_resolve("link.txt")
    assert excinfo.value.status_code == 403


def test_404_for_missing_file(isolated_root: Path) -> None:
    """A path inside the root that doesn't point at anything returns 404."""
    with pytest.raises(HTTPException) as excinfo:
        files_api._safe_resolve("does_not_exist.pdf")
    assert excinfo.value.status_code == 404


def test_404_for_directory(isolated_root: Path) -> None:
    """Pointing at a directory is refused."""
    (isolated_root / "subdir").mkdir()
    with pytest.raises(HTTPException) as excinfo:
        files_api._safe_resolve("subdir")
    assert excinfo.value.status_code == 404


def test_endpoint_serves_file_via_test_client(isolated_root: Path) -> None:
    """Round-trip the FastAPI app: a real GET hits the file route."""
    target = isolated_root / "letter.pdf"
    target.write_bytes(b"%PDF-1.7 a real-looking doc")

    from backend.main import app  # noqa: PLC0415 — heavy import

    client = TestClient(app)
    resp = client.get("/files/letter.pdf")
    assert resp.status_code == 200
    assert resp.content == b"%PDF-1.7 a real-looking doc"
    assert resp.headers["content-type"] == "application/pdf"


def test_endpoint_403_on_traversal(isolated_root: Path) -> None:
    """The router refuses ``../...`` even when it URL-decodes successfully."""
    (isolated_root.parent / "secret.txt").write_text("nope")

    from backend.main import app  # noqa: PLC0415

    client = TestClient(app)
    # FastAPI's path converter forbids ``..`` segments at the URL layer
    # for some clients; we confirm whichever 4xx we get is in the safe range.
    resp = client.get("/files/../secret.txt")
    assert resp.status_code in {400, 403, 404}
