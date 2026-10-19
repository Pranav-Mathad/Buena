"""Shared definition of a maintenance *incident* for the signal rules.

An incident is one reporting event. Rules count incidents, not facts:

- Every matching maintenance fact counts, superseded or not. The extractor
  files repeat reports of one issue under the same field, so each new report
  supersedes the previous fact; counting only current facts would never see
  more than one.
- Each source event counts once. An operator edit keeps its fact's source
  event, so editing a fact does not add an incident. A fact without an event
  counts once on its own.
- The time window applies to when the incident was reported (the event's
  ``received_at``), not to when the fact row was written, which can be much
  later for a backfill or a seed.
"""

from __future__ import annotations

# A CTE named ``incidents`` with one row per incident: property_id, id (the
# newest matching fact of the incident), source_event_id, value, reported_at.
# Parameters: ``:window`` (days, as text) and ``:pattern`` (a regex matched
# case-insensitively against the fact's value or field).
INCIDENTS_CTE = """
incidents AS (
    SELECT DISTINCT ON (f.property_id, COALESCE(f.source_event_id, f.id))
           f.property_id, f.id, f.source_event_id, f.value,
           COALESCE(e.received_at, f.created_at) AS reported_at
    FROM facts f
    LEFT JOIN events e ON e.id = f.source_event_id
    WHERE f.section = 'maintenance'
      AND COALESCE(e.received_at, f.created_at)
          >= now() - (:window || ' days')::interval
      AND (f.value ~* :pattern OR f.field ~* :pattern)
    ORDER BY f.property_id, COALESCE(f.source_event_id, f.id), f.created_at DESC
)
"""
