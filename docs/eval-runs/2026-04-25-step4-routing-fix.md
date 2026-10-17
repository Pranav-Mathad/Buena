# Eval report — emails_v1

- rows scored: **30**
- category accuracy: **16.7%**
- routing accuracy: **93.3%**
- token spend: prompt=0, completion=0

## Per-section P / R / F1

| section | expected | extracted | TP | P | R | F1 | value-match |
|---|---:|---:|---:|---:|---:|---:|---:|
| `compliance` | 2 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `financials` | 4 | 5 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `lease` | 4 | 7 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `liegenschaft_compliance` | 1 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `liegenschaft_financials` | 5 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `liegenschaft_maintenance` | 1 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `maintenance` | 7 | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |
| `overview` | 2 | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |

## Calibration curve

| bucket | n | correct | observed accuracy |
|---|---:|---:|---:|
| 0.00–0.50 | 0 | 0 | 0.00 |
| 0.50–0.60 | 0 | 0 | 0.00 |
| 0.60–0.70 | 0 | 0 | 0.00 |
| 0.70–0.80 | 13 | 0 | 0.00 |
| 0.80–0.90 | 0 | 0 | 0.00 |
| 0.90–1.00 | 0 | 0 | 0.00 |

## Top 20 failures

- `EMAIL-06564` — category 'owner_communication'→'payment'; missed liegenschaft_financials.sonderumlage_einspruch; spurious financials.payment_mention
- `EMAIL-06555` — category 'complaint'→'other'; missed maintenance.open_water_damage, financials.rent_reduction_announced
- `EMAIL-06574` — category 'other'→'lease'; scope 'unrouted'→'liegenschaft'; spurious lease.renewal_discussion
- `EMAIL-06547` — category 'owner_communication'→'payment'; missed liegenschaft_financials.sonderumlage_einspruch; spurious financials.payment_mention
- `EMAIL-01246` — category 'maintenance'→'lease'; missed liegenschaft_maintenance.heating_service_scheduled; spurious lease.renewal_discussion
- `EMAIL-00335` — category 'complaint'→'other'; missed maintenance.open_water_damage, financials.rent_reduction_announced
- `EMAIL-03549` — category 'tenant_change'→'lease'; missed lease.termination_notice; spurious lease.renewal_discussion
- `EMAIL-00214` — category 'tenant_change'→'lease'; missed lease.termination_notice; spurious lease.renewal_discussion
- `EMAIL-06554` — missed liegenschaft_financials.outstanding_invoice; spurious financials.payment_mention
- `EMAIL-06549` — missed liegenschaft_financials.annual_water_statement_2025; spurious financials.payment_mention
- `EMAIL-03714` — missed liegenschaft_financials.outstanding_invoice; spurious financials.payment_mention
- `EMAIL-06101` — missed maintenance.open_water_leak; spurious maintenance.latest_water_issue
- `EMAIL-06548` — category 'tenant_change'→'other'; missed financials.deposit_return_pending
- `EMAIL-06562` — category 'owner_communication'→'other'; missed overview.sale_intent
- `EMAIL-06561` — category 'tenant_change'→'other'; missed lease.termination_notice
- `EMAIL-06552` — category 'tenant_change'→'other'; missed lease.termination_notice
- `EMAIL-06585` — category 'maintenance'→'other'; missed maintenance.key_lost
- `EMAIL-03933` — category 'payment'→'other'; missed financials.nebenkosten_dispute
- `EMAIL-00452` — category 'owner_communication'→'lease'; spurious lease.renewal_discussion
- `EMAIL-04031` — category 'maintenance'→'other'; missed maintenance.open_water_leak
