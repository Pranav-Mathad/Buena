"""Phase 9 Step 9.2 — constraint definitions.

Importing this package registers every constraint into
:data:`backend.pipeline.validator.REGISTRY`. The validator looks up
constraints by ``(section, field)`` so the matrix is auditable: every
new constraint is a small file under this package, name-equals-class
so the rejected-updates row points at exactly the file that fired.

Add a new constraint:

1. New module ``backend/pipeline/constraints/<name>.py`` that imports
   :func:`backend.pipeline.validator.register` and calls it on a
   constraint instance.
2. Append the import to this ``__init__.py`` so it loads at startup.
3. Add a unit test under ``backend/tests/test_constraints.py``
   covering passed / rejected / needs_review.
"""

from __future__ import annotations

from backend.pipeline.constraints import (  # noqa: F401 — import for side-effect
    building_address_immutable,
    building_floor_count_immutable,
    building_year_built_immutable,
    compliance_facts_require_authoritative_source,
    owner_change_requires_kaufvertrag,
    property_square_meters_tolerance,
    rent_amount_change_requires_addendum,
    tenant_identity_change_requires_mietvertrag,
)
