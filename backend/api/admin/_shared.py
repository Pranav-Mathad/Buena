"""Helpers shared by the admin modules."""

from __future__ import annotations

import json
from typing import Any


def json_param(value: dict[str, Any]) -> str:
    """Serialize a small dict for a ``CAST(:param AS JSONB)`` bind parameter."""
    return json.dumps(value)
