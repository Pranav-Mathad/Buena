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

4. **Seed the demo dataset** (applies schema, inserts 4 properties with 6 months of history)
   ```bash
   python -m seed.seed
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

See [KEYSTONE.md](KEYSTONE.md) for the project constitution and [DECISIONS.md](DECISIONS.md) for non-obvious calls.
