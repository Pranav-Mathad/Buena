"""Walk a directory of invoice PDFs and yield :class:`ConnectorEvent`s.

Each event:

- ``source`` = ``"invoice"``.
- ``source_ref`` = ``sha256(file_bytes)[:16]`` — content-addressable so
  re-uploads of the same bytes collapse.
- ``metadata.document_type`` is set via
  :func:`connectors.document_type.classify` (filename heuristics first;
  Gemini Flash fallback when heuristics fail and the cost ledger has
  budget).
- ``metadata.original_path`` retains the relative path for Phase 10
  document-linking.
- ``raw_content`` is the extracted text (PII scrubbed) when
  ``read_text=True``; otherwise a short header line. Reading text on
  every backfill row is expensive, so the default is metadata-only;
  callers can opt-in.

When the dataset ships a sibling ``rechnungen_index.csv`` (Buena's
incremental folders do), :func:`walk_index_csv` is the cheap path —
it yields events without opening the PDFs at all.
"""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from backend.services.pdf_extractor import extract_text
from connectors import redact
from connectors.base import ConnectorEvent
from connectors.document_type import DOCTYPE_LEDGER_LABEL, classify

log = structlog.get_logger(__name__)

PDF_HEAD_CHARS = 800
EVENT_SOURCE = "invoice"


def _hash16(data: bytes) -> str:
    """16-char content hash."""
    return hashlib.sha256(data).hexdigest()[:16]


def _parse_filename_date(name: str) -> datetime | None:
    """Parse the ``YYYYMMDD_`` prefix common across Buena's PDFs."""
    if len(name) < 8 or not name[:8].isdigit():
        return None
    try:
        return datetime.strptime(name[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_one(
    path: Path,
    *,
    root: Path | None = None,
    read_text: bool = False,
    use_llm: bool = True,
) -> ConnectorEvent:
    """Build a :class:`ConnectorEvent` from a single invoice PDF on disk."""
    raw_bytes = path.read_bytes()
    digest = _hash16(raw_bytes)

    head_text = ""
    body = ""
    if read_text:
        try:
            body = extract_text(raw_bytes)
        except Exception as exc:  # noqa: BLE001 — corrupt PDFs shouldn't kill the walk
            log.warning("invoice.extract_failed", path=str(path), error=str(exc))
            body = ""
        head_text = body[:PDF_HEAD_CHARS]

    document_type = classify(path.name, head_text, use_llm=use_llm and read_text)

    received_at = _parse_filename_date(path.name) or datetime.fromtimestamp(
        path.stat().st_mtime, tz=timezone.utc
    )

    relative_path = (
        str(path.relative_to(root)) if root is not None and path.is_relative_to(root)
        else str(path)
    )

    metadata: dict[str, Any] = {
        "filename": path.name,
        "original_path": relative_path,
        "content_sha256": digest,
        "bytes": len(raw_bytes),
        "document_type": document_type,
        "head_chars": len(head_text),
        "doctype_ledger_label": DOCTYPE_LEDGER_LABEL,
    }

    raw_content = (
        f"[{EVENT_SOURCE.upper()}: {path.name}]\n\n{redact.scrub_text(body)}"
        if body
        else f"[{EVENT_SOURCE.upper()}: {path.name}] (text not extracted)"
    )

    return ConnectorEvent(
        source=EVENT_SOURCE,
        source_ref=f"{path.name}:{digest}",
        raw_content=raw_content,
        metadata=metadata,
        received_at=received_at,
        document_type=document_type,
    )


def walk_directory(
    root: Path,
    *,
    read_text: bool = False,
    use_llm: bool = True,
) -> Iterator[ConnectorEvent]:
    """Yield events for every PDF under ``root`` (recursive)."""
    if not root.exists():
        log.warning("invoice.archive.missing", root=str(root))
        return
    paths = sorted(root.rglob("*.pdf"))
    log.info("invoice.archive.start", root=str(root), files=len(paths))
    for path in paths:
        try:
            yield parse_one(
                path, root=root, read_text=read_text, use_llm=use_llm
            )
        except Exception:  # noqa: BLE001
            log.exception("invoice.archive.parse_error", path=str(path))


def walk_index_csv(
    csv_path: Path,
    *,
    pdf_root: Path | None = None,
) -> Iterator[ConnectorEvent]:
    """Yield events from a Buena ``rechnungen_index.csv`` row-by-row.

    Cheap path: no PDF parsing, no Gemini. ``document_type`` is
    populated from the filename only.
    """
    if not csv_path.is_file():
        log.warning("invoice.index.missing", path=str(csv_path))
        return
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            filename = row.get("dateiname") or row.get("filename") or ""
            if not filename:
                continue
            doctype = classify(filename, head_text="", use_llm=False)
            received_at = _parse_filename_date(filename) or datetime.now(timezone.utc)
            content_sha256 = row.get("content_sha256") or _hash16(filename.encode())
            metadata: dict[str, Any] = {
                "filename": filename,
                "original_path": str(
                    pdf_root.joinpath(filename)
                    if pdf_root is not None
                    else filename
                ),
                "content_sha256": content_sha256,
                "document_type": doctype,
                "from_index": True,
                "head_chars": 0,
            }
            for col, value in row.items():
                if col not in metadata and value not in (None, ""):
                    metadata[col] = value

            yield ConnectorEvent(
                source=EVENT_SOURCE,
                source_ref=f"{filename}:{content_sha256[:16]}",
                raw_content=(
                    f"[{EVENT_SOURCE.upper()}: {filename}] (index row)"
                ),
                metadata=metadata,
                received_at=received_at,
                document_type=doctype,
            )
