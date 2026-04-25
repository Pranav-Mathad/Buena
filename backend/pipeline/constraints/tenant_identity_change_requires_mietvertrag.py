"""Tenant identity changes require a Mietvertrag PDF.

A new tenant moving in is a binding legal change — Buena's stammdaten
records ``current_tenant`` per unit, and free-text events are not
sufficient evidence to mutate it. An incoming Mietvertrag PDF
(``document_type=lease``) routes through the validator with
``needs_review`` so the operator confirms the new occupancy.
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


class TenantIdentityChangeRequiresMietvertrag:
    """Reject email-sourced tenant changes; lease PDFs needs_review."""

    name = "tenant_identity_change_requires_mietvertrag"
    section = "tenants"
    field = "current_tenant_name"

    def check(
        self,
        proposed: FactDecision,
        current: dict[str, Any] | None,
        event: dict[str, Any],
    ) -> ValidationResult:
        if current is None:
            return ValidationResult.passed("no prior tenant; first occupancy")

        if event_source(event) == "pdf" and event_document_type(event) == "lease":
            return ValidationResult.needs_review(
                "tenant identity change with Mietvertrag PDF; needs "
                "human confirmation before applying",
                required_source_type="lease",
            )

        return ValidationResult.rejected(
            "tenant identity can only be changed by a Mietvertrag "
            "(document_type='lease') PDF; free-text events cannot "
            "mutate the recorded tenant",
            required_source_type="lease",
        )


register(TenantIdentityChangeRequiresMietvertrag())
