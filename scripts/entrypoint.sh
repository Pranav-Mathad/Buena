#!/bin/sh
# Container entrypoint for the backend image.
#
#   RUN_MIGRATIONS=true    apply the base schema (if missing), the bundled sample
#                          properties and every additive migration. All steps are
#                          idempotent, so it is safe on every start.
#   LOAD_SAMPLE_DATA=true  also load the synthetic master data in Extracted_demo/
#                          (1 Liegenschaft, 3 buildings, 52 units) and render
#                          every property file. Idempotent.
#   PORT                   listen port (default 8000).
set -eu

if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
  python -m seed.seed
  python -c "from connectors.migrations import apply_all; apply_all()"
fi

if [ "${LOAD_SAMPLE_DATA:-false}" = "true" ]; then
  EXTRACTED_ROOT=/app/Extracted_demo python -m connectors.cli load-stammdaten --source buena
  python -m connectors.cli materialize-all
fi

exec uvicorn backend.main:app --host 0.0.0.0 --port "${PORT:-8000}"
