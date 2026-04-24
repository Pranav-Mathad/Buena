"""PDF upload endpoint — drop a PDF, get a pipeline event."""

from __future__ import annotations

import hashlib
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.events import insert_event
from backend.pipeline.worker import process_batch
from backend.services.pdf_extractor import extract_text

router = APIRouter(prefix="/uploads", tags=["uploads"])
log = structlog.get_logger(__name__)


class PdfUploadResponse(BaseModel):
    """Response for POST /uploads/pdf."""

    event_id: UUID
    inserted: bool
    processed: int
    characters_extracted: int


@router.post("/pdf", response_model=PdfUploadResponse)
async def upload_pdf(
    file: UploadFile = File(..., description="PDF document to ingest"),
    property_id: UUID | None = Form(
        default=None,
        description="Optional property hint; otherwise the router infers it from content.",
    ),
    session: AsyncSession = Depends(get_session),
) -> PdfUploadResponse:
    """Parse the PDF, insert an event, drain the worker once."""
    if file.content_type not in {"application/pdf", "application/x-pdf", None}:
        raise HTTPException(status_code=415, detail=f"unsupported content-type: {file.content_type}")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")

    try:
        body = extract_text(data)
    except Exception as exc:  # noqa: BLE001 — surface extractor errors as 400s
        log.exception("pdf.extract.failed", filename=file.filename)
        raise HTTPException(status_code=400, detail=f"could not parse PDF: {exc}") from exc

    if not body.strip():
        raise HTTPException(status_code=422, detail="no extractable text in PDF")

    # Content hash + filename gives us a stable idempotency key without storing blobs.
    digest = hashlib.sha256(data).hexdigest()
    source_ref = f"{file.filename or 'upload.pdf'}:{digest[:16]}"

    event_id, inserted = await insert_event(
        session,
        source="pdf",
        source_ref=source_ref,
        raw_content=f"[PDF: {file.filename}]\n\n{body}",
        property_id=property_id,
        metadata={
            "filename": file.filename,
            "content_sha256": digest,
            "bytes": len(data),
        },
    )
    await session.commit()
    processed = await process_batch(max_events=5)
    log.info(
        "uploads.pdf.done",
        filename=file.filename,
        event_id=str(event_id),
        inserted=inserted,
        processed=processed,
        chars=len(body),
    )
    return PdfUploadResponse(
        event_id=event_id,
        inserted=inserted,
        processed=processed,
        characters_extracted=len(body),
    )
