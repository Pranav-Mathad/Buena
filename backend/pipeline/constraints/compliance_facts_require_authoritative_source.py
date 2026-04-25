"""Compliance facts require an authoritative source (web or PDF).

Compliance state — Brandschutznachweis, Mietpreisbremse status, Bauamt
deadlines, fire-safety certificates — must come from the authority
itself or its public web record. An email forwarding rumour-of-fact
is not sufficient evidence to mutate a compliance fact, even when
the email is internally consistent and confidently extracted.

This is the only constraint registered with ``field=None`` (wildcard)
so every field under ``compliance`` and ``liegenschaft_compliance`` is
gated. New fields don't need a separate constraint to inherit the
rule.
"""

from __future__ import annotations

from typing import Any

from backend.pipeline.differ import FactDecision
from backend.pipeline.validator import (
    ValidationResult,
    event_document_type,
    event_source,
    register,
)


_AUTHORITATIVE_PDF_TYPES: frozenset[str] = frozenset(
    {"structural_permit", "vermessungsprotokoll", "kaufvertrag"}
)


class ComplianceFactsRequireAuthoritativeSource:
    """Email/Slack-sourced compliance proposals are rejected.

    Web-sourced (Tavily) and PDF-with-recognised-document-type
    proposals pass without review since the source itself carries
    weight. PDFs whose document_type is ``other`` or unset land in
    needs_review — we know there's a document, we don't know if it's
    the right one.
    """

    name = "compliance_facts_require_authoritative_source"
    section = "compliance"
    field: str | None = None  # wildcard

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        source = event_source(event)
        doctype = event_document_type(event)

        if source == "web":
            # Tavily / authority-page lookup. Trust the source.
            return ValidationResult.passed("web source, authoritative")

        if source == "pdf":
            if doctype in _AUTHORITATIVE_PDF_TYPES:
                return ValidationResult.passed(
                    f"pdf source with authoritative document_type={doctype!r}"
                )
            if doctype in {None, "other", "unknown"}:
                return ValidationResult.needs_review(
                    "compliance proposal from a PDF without a recognised "
                    "document_type; classify the PDF before applying",
                    required_source_type="structural_permit | vermessungsprotokoll | kaufvertrag",
                )
            # Doctype like 'invoice' / 'mahnung' / 'lease' — wrong kind.
            return ValidationResult.rejected(
                f"compliance proposal from a PDF whose document_type is "
                f"{doctype!r} — that is not an authoritative source for "
                f"compliance facts",
                required_source_type="structural_permit | vermessungsprotokoll | kaufvertrag",
            )

        # email, slack, debug, or unknown — reject.
        return ValidationResult.rejected(
            f"compliance facts require an authoritative source "
            f"(web lookup or an authority PDF) — a {source or 'unknown'}-"
            f"sourced event is not sufficient",
            required_source_type="web | structural_permit | vermessungsprotokoll | kaufvertrag",
        )


# Register under both the property-level and liegenschaft-level
# compliance sections so a single rule covers both scopes.
_instance = ComplianceFactsRequireAuthoritativeSource()
register(_instance)


class LiegenschaftComplianceFactsRequireAuthoritativeSource(
    ComplianceFactsRequireAuthoritativeSource
):
    """WEG-level alias for the same rule, registered under ``liegenschaft_compliance``."""

    name = "liegenschaft_compliance_facts_require_authoritative_source"
    section = "liegenschaft_compliance"


register(LiegenschaftComplianceFactsRequireAuthoritativeSource())
