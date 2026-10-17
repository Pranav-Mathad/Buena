_Backend: **pioneer**_

# Eval report — emails_v1

- rows scored: **30**
- category accuracy: **73.3%**
- routing accuracy: **93.3%**
- token spend: prompt=106773, completion=10771

## Per-section P / R / F1

| section | expected | extracted | TP | P | R | F1 | value-match |
|---|---:|---:|---:|---:|---:|---:|---:|
| `compliance` | 2 | 2 | 2 | 1.00 | 1.00 | 1.00 | 0.00 |
| `financials` | 4 | 5 | 4 | 0.80 | 1.00 | 0.89 | 0.00 |
| `lease` | 4 | 9 | 4 | 0.44 | 1.00 | 0.62 | 0.00 |
| `liegenschaft_compliance` | 1 | 1 | 1 | 1.00 | 1.00 | 1.00 | 1.00 |
| `liegenschaft_financials` | 5 | 6 | 5 | 0.83 | 1.00 | 0.91 | 0.00 |
| `liegenschaft_maintenance` | 1 | 1 | 1 | 1.00 | 1.00 | 1.00 | 0.00 |
| `maintenance` | 7 | 7 | 7 | 1.00 | 1.00 | 1.00 | 0.14 |
| `overview` | 2 | 2 | 2 | 1.00 | 1.00 | 1.00 | 0.00 |

## Calibration curve

| bucket | n | correct | observed accuracy |
|---|---:|---:|---:|
| 0.00–0.50 | 0 | 0 | 0.00 |
| 0.50–0.60 | 0 | 0 | 0.00 |
| 0.60–0.70 | 0 | 0 | 0.00 |
| 0.70–0.80 | 1 | 0 | 0.00 |
| 0.80–0.90 | 0 | 0 | 0.00 |
| 0.90–1.00 | 32 | 26 | 0.81 |

## Top 8 failures

- `EMAIL-06574` — category 'other'→'tenant_change'; scope 'unrouted'→'liegenschaft'; spurious lease.termination_notice
- `EMAIL-03537` — category 'other'→'payment'; scope 'unrouted'→'liegenschaft'; spurious liegenschaft_financials.outstanding_invoice
- `EMAIL-06527` — category 'owner_communication'→'tenant_change'; spurious lease.termination_notice, financials.deposit_return_pending
- `EMAIL-06553` — category 'owner_communication'→'tenant_change'; spurious lease.termination_notice
- `EMAIL-00452` — category 'owner_communication'→'tenant_change'; spurious lease.termination_notice
- `EMAIL-03065` — category 'owner_communication'→'tenant_change'; spurious lease.termination_notice
- `EMAIL-02430` — category 'compliance'→'owner_communication'
- `EMAIL-06497` — category 'compliance'→'owner_communication'

## Cost ledger

- label: `step5_eval_pioneer`
- cumulative spend: **$0.0000**
- cap: $5.00
- exhausted: False
