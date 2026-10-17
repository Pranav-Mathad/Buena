<!--
Provenance (added by hand; everything below the runner header is runner output, unedited).
Command:  python -m eval.runner --set emails_v1 --out docs/eval-runs/2026-10-17-emails_v1-rule-based.md
Commit:   8695bde
Keys:     none set (no GEMINI_API_KEY, no PIONEER_API_KEY), so the extractor ran its
          rule-based fallback; token spend below is 0/0, confirming no LLM calls.
          The runner's "Backend" line reflects its --via default, not the extractor used.
Database: Postgres 15 (pgvector image) after `python -m seed.seed`, migrations, and
          `EXTRACTED_ROOT=Extracted_demo python -m connectors.cli load-stammdaten --source buena`
          (synthetic master data: 1 Liegenschaft, 3 buildings, 52 units).
Input:    eval/ground_truth/emails_v1.jsonl after name pseudonymisation (2026-10-17).
-->

_Backend: **gemini**_

# Eval report — emails_v1

- rows scored: **30**
- category accuracy: **63.3%**
- routing accuracy: **93.3%**
- token spend: prompt=0, completion=0

## Per-section P / R / F1

| section | expected | extracted | TP | P | R | F1 | value-match |
|---|---:|---:|---:|---:|---:|---:|---:|
| `compliance` | 2 | 2 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `financials` | 4 | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `lease` | 4 | 8 | 4 | 0.50 | 1.00 | 0.67 | 0.00 |
| `liegenschaft_compliance` | 1 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `liegenschaft_financials` | 5 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `liegenschaft_maintenance` | 1 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `maintenance` | 7 | 8 | 5 | 0.62 | 0.71 | 0.67 | 0.00 |
| `overview` | 2 | 7 | 2 | 0.29 | 1.00 | 0.44 | 0.00 |

## Calibration curve

| bucket | n | correct | observed accuracy |
|---|---:|---:|---:|
| 0.00–0.50 | 0 | 0 | 0.00 |
| 0.50–0.60 | 0 | 0 | 0.00 |
| 0.60–0.70 | 0 | 0 | 0.00 |
| 0.70–0.80 | 26 | 11 | 0.42 |
| 0.80–0.90 | 0 | 0 | 0.00 |
| 0.90–1.00 | 0 | 0 | 0.00 |

## Top 19 failures

- `EMAIL-06555` — category 'complaint'→'maintenance'; missed maintenance.open_water_damage, financials.rent_reduction_announced; spurious maintenance.open_water_leak
- `EMAIL-00335` — category 'complaint'→'maintenance'; missed maintenance.open_water_damage, financials.rent_reduction_announced; spurious maintenance.open_water_leak
- `EMAIL-06574` — category 'other'→'tenant_change'; scope 'unrouted'→'liegenschaft'; spurious lease.termination_notice
- `EMAIL-06549` — category 'payment'→'maintenance'; missed liegenschaft_financials.annual_water_statement_2025; spurious maintenance.open_water_leak
- `EMAIL-03537` — category 'other'→'owner_communication'; scope 'unrouted'→'liegenschaft'; spurious overview.sale_intent
- `EMAIL-06548` — missed financials.deposit_return_pending; spurious lease.termination_notice
- `EMAIL-06564` — missed liegenschaft_financials.sonderumlage_einspruch; spurious overview.sale_intent
- `EMAIL-06547` — missed liegenschaft_financials.sonderumlage_einspruch; spurious overview.sale_intent
- `EMAIL-03933` — missed financials.nebenkosten_dispute; spurious financials.payment_mention
- `EMAIL-02430` — missed compliance.modernization_inquiry; spurious compliance.regulatory_mention
- `EMAIL-06497` — missed compliance.modernization_inquiry; spurious compliance.regulatory_mention
- `EMAIL-06554` — category 'payment'→'other'; missed liegenschaft_financials.outstanding_invoice
- `EMAIL-01246` — category 'maintenance'→'other'; missed liegenschaft_maintenance.heating_service_scheduled
- `EMAIL-00452` — category 'owner_communication'→'tenant_change'; spurious lease.termination_notice
- `EMAIL-03714` — category 'payment'→'other'; missed liegenschaft_financials.outstanding_invoice
- `EMAIL-06527` — category 'owner_communication'→'tenant_change'; spurious lease.termination_notice
- `EMAIL-01970` — category 'compliance'→'other'; missed liegenschaft_compliance.brandschutz_request
- `EMAIL-06553` — spurious overview.sale_intent
- `EMAIL-03065` — spurious overview.sale_intent

## Cost ledger

- label: `step5_eval`
- cumulative spend: **$0.0000**
- cap: $5.00
- exhausted: False
