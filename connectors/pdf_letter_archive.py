"""Walk a directory of letter PDFs and yield :class:`ConnectorEvent`s.

Same pattern as :mod:`connectors.pdf_invoice_archive` but with
``source='letter'`` so the structured extractor can give Mahnungen
their own routing path. Filename heuristics catch the common Buena
naming patterns (``mahnung_*``, ``etv_protokoll_*``,
``mietvertrag_*``, ``mietvertrag_addendum_*``).
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from backend.services.pdf_extractor import extract_text
from connectors import redact
from connectors.base import ConnectorEvent
from connectors.document_type import DOCTYPE_LEDGER_LABEL, classify
from connectors.pdf_invoice_archive import (
    PDF_HEAD_CHARS,
    _hash16,
    _parse_filename_date,
)

log = structlog.get_logger(__name__)

EVENT_SOURCE = "letter"


def parse_one(
    path: Path,
    *,
    root: Path | None = None,
    read_text: bool = False,
    use_llm: bool = True,
) -> ConnectorEvent:
    """Build a :class:`ConnectorEvent` from a single letter PDF on disk."""
    raw_bytes = path.read_bytes()
    digest = _hash16(raw_bytes)

    body = ""
    head_text = ""
    if read_text:
        try:
            body = extract_text(raw_bytes)
        except Exception as exc:  # noqa: BLE001
            log.warning("letter.extract_failed", path=str(path), error=str(exc))
            body = ""
        head_text = body[:PDF_HEAD_CHARS]

    document_type = classify(path.name, head_text, use_llm=use_llm and read_text)

    received_at = _parse_filename_date(path.name) or datetime.fromtimestamp(
        path.stat().st_mtime
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
        else f"Letter {path.name} — awaiting extraction"
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
    """Yield events for every PDF under ``root``."""
    if not root.exists():
        log.warning("letter.archive.missing", root=str(root))
        return
    paths = sorted(root.rglob("*.pdf"))
    log.info("letter.archive.start", root=str(root), files=len(paths))
    for path in paths:
        try:
            yield parse_one(
                path, root=root, read_text=read_text, use_llm=use_llm
            )
        except Exception:  # noqa: BLE001
            log.exception("letter.archive.parse_error", path=str(path))


def walk_index_csv(
    csv_path: Path,
    *,
    pdf_root: Path | None = None,
) -> Iterator[ConnectorEvent]:
    """Yield events from a Buena ``briefe_index.csv`` row-by-row."""
    if not csv_path.is_file():
        log.warning("letter.index.missing", path=str(csv_path))
        return
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            filename = row.get("dateiname") or row.get("filename") or ""
            if not filename:
                continue
            doctype = classify(filename, head_text="", use_llm=False)
            received_at = _parse_filename_date(filename) or datetime.now()
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
                    f"Letter {filename} — awaiting extraction"
                ),
                metadata=metadata,
                received_at=received_at,
                document_type=doctype,
            )
