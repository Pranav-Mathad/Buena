# Keystone

> The operational brain for property management. Turns fragmented communication into a living, trusted context layer per property, and proactively surfaces signals for human approval.

## Quick start (5 steps)

1. **Clone + env**
   ```bash
   git clone <repo-url> keystone && cd keystone
   cp .env.example .env
   ```

2. **Python deps** (Python 3.11+ required)
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -e ".[dev]"
   ```

3. **Start Postgres + pgvector**
   ```bash
   docker compose up -d
   ```

4. **Seed the demo dataset and apply migrations** (inserts properties and creates all required tables)
   ```bash
   python -m seed.seed
   python -c "from connectors.migrations import apply_all; apply_all()"
   ```

5. **Run the backend**
   ```bash
   uvicorn backend.main:app --reload
   ```

## Verify

```bash
curl http://localhost:8000/health
curl http://localhost:8000/properties
curl http://localhost:8000/properties/<id>/markdown
```

See [docs/KEYSTONE.md](docs/KEYSTONE.md) for the project constitution and [docs/DECISIONS.md](docs/DECISIONS.md) for non-obvious calls.
