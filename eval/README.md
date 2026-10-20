# Extraction evaluation harness

Ground truth, runner and metrics for `backend/pipeline/extractor.py` and the
router. Run reports are committed under
[`docs/eval-runs/`](../docs/eval-runs/); the current results and known
weaknesses are summarized in [`docs/EVALUATION.md`](../docs/EVALUATION.md).
Any change to the extractor or its prompts should come with a new run.

## Layout

```
eval/
├── README.md
├── ground_truth/
│   └── emails_v1.jsonl          # one JSON object per line, schema below
├── runner.py                    # python -m eval.runner --set emails_v1
├── metrics.py                   # P/R/F1, calibration curve, routing accuracy
└── tests/                       # unit tests and a smoke run on a fixture file
```

## Sampling methodology (`emails_v1`)

30 emails total, drawn from `Extracted/emails/` and
`Extracted/incremental/day-*/emails/`:

- **12 incremental**: sampled uniformly across `day-01` to `day-10` with
  `random.seed(42)`. These are the emails the incremental feed and the
  replay engine process, so their extraction quality matters most.
- **18 archive**: stratified into six buckets of three emails each, by
  keyword:
  - `heating`: `heiz|heizung|kalt|warmwasser|thermostat`
  - `water`: `wasser|leck|tropf|feucht|schimmel|rohr`
  - `payment`: `miete|zahlung|mahnung|überweisung|lastschrift|säumig`
  - `lease`: `mietvertrag|kündigung|verlängerung|nachmieter`
  - `compliance`: `mietpreisbremse|verordnung|prüfung|abnahme|brandschutz|weg|hausverwaltung`
  - `other`: none of the above

The bucketing primary-keyword is preserved in the row's
`ground_truth.notes` field for traceability.

The script that drew the sample was not committed; the JSONL file is the
sample. Person and company names were pseudonymised deterministically on
2026-10-17; labels were not changed.

### Expanding the set

30 rows give wide error bars: one fact moves a section's F1 noticeably. The
planned expansion is to about 100 rows, with LLM-drafted annotations audited
by hand before commit. It has not been done.

## Ground-truth row schema

One JSON object per line. Every field is required unless marked
optional.

```jsonl
{
  "event_id": "EMAIL-04031",
  "source": "email",
  "raw_content": "<full body, redacted via connectors.redact.scrub_text>",
  "metadata": {
    "from": "<redacted>",
    "subject": "<original subject>",
    "date": "2025-03-16T16:10:00+00:00",
    "filename_hint": "incremental/day-01/.../EMAIL-XXXX.eml"
  },
  "ground_truth": {
    "category": "maintenance",
    "priority": "high",
    "facts_to_update": [
      {
        "section": "maintenance",
        "field": "open_water_issue",
        "value": "Wasserleitung im Bad tropft seit 14.03.",
        "confidence_min": 0.8
      }
    ],
    "expected_property_alias": "EH-007",
    "expected_scope": "property",
    "notes": "annotator commentary; sampling bucket; edge cases"
  }
}
```

Field semantics:

- `event_id`: stable identifier; matches `events.source_ref` for the
  email when ingested.
- `source` / `metadata`: what the connector produced after
  redaction. `raw_content` is what gets fed to the extractor; it
  must be PII-redacted (no raw IBAN, full E.164 phone, or full email
  domain).
- `ground_truth.category` / `priority`: single canonical value from
  the `category` and `priority` enums in `EXTRACTION_SCHEMA`
  (`backend/services/gemini.py`).
- `ground_truth.facts_to_update[]`: every fact the email *should*
  produce. Annotator is conservative: omit anything they're not sure
  the model could pick up.
- `ground_truth.confidence_min`: the floor below which we'd consider
  the extraction "weak"; the runner uses this for calibration
  scoring.
- `ground_truth.expected_property_alias` / `expected_scope`:
  routing oracle (`property | building | liegenschaft |
  unrouted`).
- `ground_truth.notes`: the sampling bucket plus the annotator's
  reasoning for the hard calls.

## Runner

```
python -m eval.runner --set emails_v1
python -m eval.runner --set emails_v1 --out docs/eval-runs/<date>-<name>.md
```

Reads ground-truth file, runs each row through
`backend.pipeline.extractor.extract`, builds a
:class:`eval.metrics.Report`. The runner prints a markdown summary
and (with `--out`) writes a dated report into `docs/eval-runs/`.

## Metrics

`eval.metrics.Report` carries:

- Per-section **precision / recall / F1**. Match key is
  `(section, field)`: value-equality is reported separately.
- **Routing accuracy**: fraction of rows whose extracted scope
  matches `ground_truth.expected_scope`.
- **Calibration curve**: extractions bucketed by reported
  `confidence` (0.5 to 0.6, ..., 0.9 to 1.0); for each bucket, the
  fraction of extractions whose `(section, field)` key is correct. Value
  agreement is deliberately excluded here (it is the separate value-match
  column) so paraphrasing does not distort calibration. A well-calibrated
  extractor's bucket midpoint is close to its observed accuracy.
- **Top-20 failures**: the rows where the extractor most diverged
  from ground truth, with a one-line diff.
- **Token spend**: prompt and completion tokens summed across the run, for
  cost reasoning.

## Operating principle

Honest numbers, no smoothing. If a metric drops between two runs, that is
the finding, and it is discussed before the change is merged.
The first eval run in `docs/eval-runs/` becomes the baseline.
