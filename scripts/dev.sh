#!/usr/bin/env bash
# Start the Keystone backend (FastAPI, --reload) and frontend (Vite) together
# without Docker. Ctrl-C stops both. Output is interleaved on stdout.
#
# Expects Postgres to be reachable at DATABASE_URL (e.g. `docker compose up
# postgres`) and a Python venv at ./.venv.
#
# Override via env vars:
#   BACKEND_PORT=8000 FRONTEND_PORT=8080 ./scripts/dev.sh
#   VITE_API_BASE_URL=http://localhost:8000 ./scripts/dev.sh
#   FRONTEND_DIR=/path/to/frontend ./scripts/dev.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FRONTEND_DIR="${FRONTEND_DIR:-$BACKEND_DIR/frontend}"

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-8080}"
export VITE_API_BASE_URL="${VITE_API_BASE_URL:-http://localhost:$BACKEND_PORT}"

# Sanity checks ---------------------------------------------------------------

if [[ ! -d "$FRONTEND_DIR" ]]; then
  echo "ERROR: frontend dir not found at $FRONTEND_DIR" >&2
  echo "       set FRONTEND_DIR env var to override" >&2
  exit 1
fi

if [[ ! -f "$BACKEND_DIR/.venv/bin/activate" ]]; then
  echo "ERROR: python venv not found at $BACKEND_DIR/.venv" >&2
  echo "       create it with: python3.11 -m venv .venv && pip install -e '.[dev]'" >&2
  exit 1
fi

if [[ ! -f "$FRONTEND_DIR/package.json" ]]; then
  echo "ERROR: $FRONTEND_DIR has no package.json" >&2
  exit 1
fi

# Free up ports if anything's stale ------------------------------------------

for port in "$BACKEND_PORT" "$FRONTEND_PORT"; do
  pids=$(lsof -ti:"$port" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    echo "→ killing stale process(es) on :$port: $(echo "$pids" | tr '\n' ' ')"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    sleep 1
  fi
done

# Launch ----------------------------------------------------------------------

echo "→ backend   http://localhost:$BACKEND_PORT  (uvicorn --reload)"
(
  cd "$BACKEND_DIR"
  # shellcheck disable=SC1091
  source .venv/bin/activate
  exec uvicorn backend.main:app --reload --port "$BACKEND_PORT"
) &
BACKEND_PID=$!

echo "→ frontend  http://localhost:$FRONTEND_PORT  (vite)"
(
  cd "$FRONTEND_DIR"
  exec npm run dev -- --port "$FRONTEND_PORT"
) &
FRONTEND_PID=$!

# Cleanup ---------------------------------------------------------------------

cleanup() {
  trap '' INT TERM EXIT  # disarm so we don't recurse
  echo ""
  echo "→ stopping…"
  kill -TERM "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
  sleep 1
  kill -KILL "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
  echo "→ done"
}
trap cleanup INT TERM EXIT

# Block on whichever child exits first ---------------------------------------

wait
