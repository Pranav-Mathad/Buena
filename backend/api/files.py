"""``GET /files/<path>`` — serve a document from the configured root.

Phase 10 Step 10.4. The renderer's ``[source: …]`` links resolve to
this endpoint for invoice / letter PDFs, so the trust mechanism only
holds if a click actually opens the original document.

Path safety is the only thing this module exists to enforce. The
configured root is resolved via :func:`os.path.realpath` once at startup;
every requested path is also realpath-resolved and required to start
with the root prefix. Anything else returns 403 — even legal-looking
relative paths that resolve to a sibling directory.
"""

from __future__ import annotations

import mimetypes
import os
from functools import lru_cache
from pathlib import Path

import structlog
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from backend.config import get_settings

router = APIRouter(prefix="/files", tags=["files"])
log = structlog.get_logger(__name__)


@lru_cache(maxsize=1)
def _allowed_root() -> str:
    """Realpath of the directory ``GET /files/...`` may serve from.

    Cached so the realpath cost is paid once. Resolves the configured
    setting against the process working directory if relative.
    """
    raw = get_settings().keystone_files_root
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    return os.path.realpath(str(candidate))


def _safe_resolve(relative_path: str) -> str:
    """Resolve ``relative_path`` against the allowed root or raise 403/404.

    Steps:

    1. Reject absolute paths outright — only relative segments allowed.
    2. Reject any segment that contains a NUL byte (defense in depth).
    3. Join + realpath; require the result to live under ``_allowed_root``.
    4. Require the file to exist and be a regular file.
    """
    if not relative_path or relative_path.startswith("/"):
        raise HTTPException(status_code=400, detail="path must be relative")
    if "\x00" in relative_path:
        raise HTTPException(status_code=400, detail="invalid path")

    root = _allowed_root()
    full = os.path.realpath(os.path.join(root, relative_path))
    # ``commonpath`` is stricter than prefix-match — handles trailing
    # slashes and the ``/etcc`` vs ``/etc`` case.
    try:
        common = os.path.commonpath([root, full])
    except ValueError as exc:
        raise HTTPException(status_code=403, detail="path escapes root") from exc
    if common != root:
        raise HTTPException(status_code=403, detail="path escapes root")
    if not os.path.exists(full):
        raise HTTPException(status_code=404, detail="file not found")
    if not os.path.isfile(full):
        raise HTTPException(status_code=404, detail="not a regular file")
    return full


@router.get("/{relative_path:path}")
async def serve_file(relative_path: str) -> FileResponse:
    """Serve one file from the allowed root with traversal protection.

    The renderer hands this URL out as-is in fact-source links, so we
    keep the contract minimal: success returns the file with a sensible
    Content-Type, every other case returns a clean 4xx so the UI can
    render a "document missing" badge. PDFs render inline in the
    browser; everything else falls back to the OS default.
    """
    full = _safe_resolve(relative_path)
    mime, _ = mimetypes.guess_type(full)
    if mime is None:
        mime = "application/octet-stream"
    log.info(
        "files.serve",
        path=relative_path,
        size=os.path.getsize(full),
        mime=mime,
    )
    return FileResponse(
        full,
        media_type=mime,
        filename=os.path.basename(full),
    )
