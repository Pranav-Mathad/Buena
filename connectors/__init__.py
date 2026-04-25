"""Customer-agnostic data connectors.

A *connector* is anything that turns a customer's raw data — folder of
EMLs, bank CSV, master-data spreadsheet, archive of PDFs — into the
event/fact stream Keystone's pipeline already consumes.

Reuses (does not replace):

- ``backend.pipeline.events.insert_event`` for idempotent event writes.
- ``backend.pipeline.{router,extractor,differ,applier}`` for the live
  worker path (connectors usually call ``insert_event`` and let the
  worker pick the events up; structured connectors stamp
  ``processed_at`` and write facts directly).
- ``seed.seed._upsert_*`` helpers for master-data upserts.

The first composer (``connectors.buena_archive``) targets the Buena
hackathon dataset. The next customer gets a sibling composer; the
shape-handling primitives below are shared.
"""

from __future__ import annotations
