"""Thin wrapper around ``pdfplumber`` for turning a PDF into an event body."""

from __future__ import annotations

from io import BytesIO

import pdfplumber
import structlog

log = structlog.get_logger(__name__)

MAX_PAGES = 25


def extract_text(data: bytes, *, max_pages: int = MAX_PAGES) -> str:
    """Extract text from ``data`` (PDF bytes); concatenate pages with form feeds.

    ``max_pages`` is a defensive cap so a hostile upload can't pin the event
    loop while the worker holds an event lock.
    """
    buf = BytesIO(data)
    pages: list[str] = []
    with pdfplumber.open(buf) as pdf:
        for idx, page in enumerate(pdf.pages):
            if idx >= max_pages:
                log.warning("pdf.max_pages_reached", cap=max_pages)
                break
            text = page.extract_text() or ""
            pages.append(text.strip())
    body = "\n\n".join(part for part in pages if part)
    log.info("pdf.extract", pages=len(pages), chars=len(body))
    return body
