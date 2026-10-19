# Project facts

Every measurable claim made about this repository, with the command or file
it comes from. If a number appears in the README or another doc, it must be
here first. Numbers are re-measured when the code they describe changes; each
section names the commit it was measured at.

## Size

Measured at commit `7d01925` (2026-10-19). Physical lines including blanks and
comments.

| Claim | Value | Command |
|---|---:|---|
| Python, excluding tests | 24,158 lines | `git ls-files '*.py' \| grep -v -E '(^\|/)tests/\|(^\|/)conftest\.py$' \| xargs wc -l \| tail -1` |
| of which `backend/` | 15,342 | same, restricted to `backend/*.py` |
| of which `connectors/` | 5,057 | same, restricted to `connectors/*.py` |
| of which `eval/`, `seed/`, `scripts/`, `mcp_server/`, `tools/` | 1,117 / 1,385 / 911 / 259 / 87 | same, per directory |
| Python tests (including `conftest.py`) | 5,815 lines | `git ls-files '*.py' \| grep -E '(^\|/)tests/\|(^\|/)conftest\.py$' \| xargs wc -l \| tail -1` |
| TypeScript/TSX, excluding `components/ui` and tests | 4,626 lines | `git ls-files 'frontend/src/*.ts' 'frontend/src/*.tsx' \| grep -v -E 'components/ui/\|\.test\.\|/test/' \| xargs wc -l \| tail -1` |
| Frontend tests and test helpers | 681 lines | `git ls-files 'frontend/src/*.test.ts' 'frontend/src/*.test.tsx' 'frontend/src/test/*' \| xargs wc -l \| tail -1` |
| Generated shadcn components kept | 339 lines | `git ls-files 'frontend/src/components/ui/*' \| xargs wc -l \| tail -1` |
| Frontend dependencies | 14 runtime, 21 dev | `node -e 'const p=require("./frontend/package.json"); console.log(Object.keys(p.dependencies).length, Object.keys(p.devDependencies).length)'` |
| HTTP routes | 52 | `git grep -h -c -E '@(router\|portfolio_router)\.(get\|post\|patch\|put\|delete)\(' -- backend/api \| paste -sd+ - \| bc` |
| Additive migrations | 15 | entries in `_MIGRATIONS`, `connectors/migrations.py` |
| Rule constraints | 8 | modules in `backend/pipeline/constraints/` |
| Signal rules | 3 | `_RULES` in `backend/signals/evaluator.py` |
| MCP tools | 5 | `grep -c '@mcp.tool' mcp_server/main.py` |

## Static checks

At commit `7d01925`: `ruff check .` and `ruff format --check .` report no
issues; `mypy` (strict, over backend, connectors, eval, mcp_server, seed,
scripts, tools) reports "no issues found in 173 source files";
`python scripts/check_sensitive_data.py` reports nothing; in `frontend/`,
`npm run format:check`, `npm run lint` and `npm run typecheck` pass.

## Tests

Measured at commit `7d01925` against a freshly seeded `postgres:16`, no API
keys set, without the private dataset. A clean-machine rehearsal of the CI job (a fresh Python 3.11 venv against an
empty `postgres:16` container, at commit `3ab355b`) gave the same result for
the tests that existed then: 203 passed, 13 skipped, 0 failed.

| Suite | Passed | Skipped | Failed | Command |
|---|---:|---:|---:|---|
| `backend/tests` | 139 | 8 | 0 | `pytest backend/tests -q` |
| `connectors/tests` | 59 | 5 | 0 | `pytest connectors/tests -q` |
| `eval/tests` | 4 | 0 | 0 | `pytest eval/tests -q` |
| `mcp_server/tests` | 3 | 0 | 0 | `pytest mcp_server/tests -q` |
| `scripts/tests` | 13 | 0 | 0 | `pytest scripts/tests -q` |
| All five | 218 | 13 | 0 | `pytest -q` (all five suites are in `testpaths`) |
| Frontend | 22 | 0 | 0 | `cd frontend && npm test` (11 files) |

All 13 skips need the private dataset under `Extracted/` and say so in the
skip reason. Before this cleanup the three suites that existed then gave 126
passed, 14 skipped, 1 failed (measured 2026-10-17 at commit `38deac2`).

## Extraction quality

From [`docs/eval-runs/`](eval-runs/); details in [EVALUATION.md](EVALUATION.md).

| Claim | Value | Source |
|---|---:|---|
| Eval set size | 30 emails, 26 expected facts | `eval/ground_truth/emails_v1.jsonl`; expected column of any report |
| Category accuracy, Gemini | 73.3% | `2026-04-25-step5.md` |
| Category accuracy, Pioneer | 73.3% | `2026-04-26-emails_v1-pioneer.md` |
| Category accuracy, rule-based (no key) | 63.3% | `2026-10-17-emails_v1-rule-based.md` |
| Routing accuracy, all three runs | 93.3% | same three reports |
| Micro P / R / F1, Gemini and Pioneer | 0.79 / 1.00 / 0.88 | 26 key-correct of 33 extracted and 26 expected, summed from the per-section tables |
| Micro P / R / F1, rule-based | 0.42 / 0.42 / 0.42 | 11 key-correct of 26 extracted and 26 expected |
| Lease F1, Gemini and Pioneer | 0.62 (P 0.44, R 1.00) | per-section table in both reports |
| Lease F1, rule-based | 0.67 (P 0.50, R 1.00) | `2026-10-17-emails_v1-rule-based.md` |
| Value-match of key-correct facts | 3 of 26 (Gemini), 2 of 26 (Pioneer), 0 of 11 (rule-based) | value-match column × TP, summed |
| Observed accuracy in the 0.90 to 1.00 confidence bucket | 0.79 (Gemini), 0.81 (Pioneer) | calibration tables |

## Pipeline latency

**Keyless path, end to end over HTTP.** 10 sequential
`POST /debug/trigger_event` calls (insert, route, rule-based extract, diff,
validate, apply, materialize, commit) against a local backend:

| n | Mean | Median | Min | Max |
|---:|---:|---:|---:|---:|
| 10 | 40.2 ms | 32.8 ms | 17.0 ms | 149.2 ms |

The maximum is the first call after startup. Measured 2026-10-18 at commit
`3ab355b` on an Apple M4 laptop, Postgres 16 in Docker, synthetic master
data loaded (52 units), no API keys. All 10 events routed to `Musterstraße 26
WE 29` and wrote at least one fact. Setup and command:

```bash
python -m seed.seed
python -c "from connectors.migrations import apply_all; apply_all()"
EXTRACTED_ROOT=Extracted_demo python -m connectors.cli load-stammdaten --source buena
python -m connectors.cli materialize-all
uvicorn backend.main:app --port 8010 &
for i in $(seq 1 10); do
  curl -s -o /dev/null -w '%{time_total}\n' -X POST localhost:8010/debug/trigger_event \
    -H 'content-type: application/json' \
    -d "{\"source_ref\":\"latency-$i-$RANDOM\",\"raw_content\":\"From: tenant@example.com\nSubject: Heizung\n\nDie Heizung in WE 29 ist seit heute morgen ausgefallen ($i).\"}"
done
```

**LLM extraction call latency.** Over 100 successful Gemini Pro extraction
calls during the email backfill: mean 12.6 s, p50 10.9 s, p90 18.5 s, p99 and
max 37.1 s. Source: `docs/eval-runs/2026-04-25-step6-partial.md`. With an LLM
configured, per-event latency is dominated by this call.

## Backfill

From `docs/eval-runs/2026-04-25-step6-partial.md` (2026-04-25):

| Claim | Value |
|---|---:|
| Emails in the archive | 6,546 |
| Events inserted before the run stopped | 251 (3.8%) |
| Facts written | 85 |
| Routed to a property / Liegenschaft / building / unrouted | 99 / 46 / 0 / 106 |
| Spend | $0.4868 of a $20.00 cap |
| Concurrency, wall-clock until the hang | 4 workers, 5 min 24 s |

## Sample data

| Claim | Value | Command |
|---|---:|---|
| Synthetic master data | 1 Liegenschaft, 3 buildings, 52 units, 35 owners, 26 tenants (25 active), 16 contractors | `EXTRACTED_ROOT=Extracted_demo python -m connectors.cli load-stammdaten --source buena --json` |
| Hand-written seed | 4 properties, 3 buildings, 29 events, 62 facts | `python -m seed.seed` (final `seed.done` log line) |
| Sample events for WE 29 | 6 emails, all routed to WE 29; 8 current facts, no API key | `python -m connectors.cli load-sample-events --json` after the master data load, then `GET /properties` (measured on a fresh `docker compose up`, 2026-10-19) |
