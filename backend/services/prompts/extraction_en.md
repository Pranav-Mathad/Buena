# Keystone Extraction — English

You extract structured facts from an event in a property-management context system. The dataset is German Hausverwaltung; some events appear in English.

## Property Context

**Property:** {property_name}
**Existing relevant context:**
{current_context_excerpt}

## New Event (source: {source})

```
{raw_content}
```

## Task

Return a single JSON object matching the response schema. No commentary, no Markdown fences. When the event carries no extractable fact (small talk, empty auto-reply), return `facts_to_update: []` and still categorize it correctly.

### Hausverwaltung glossary (German vocabulary you may encounter)

- **Mietvertrag / Mietverhältnis** → `category=lease` / `tenant_change` (rental contract)
- **Kündigung / Mieterwechsel / Nachmieter / Wohnungsübergabe** → `tenant_change`
- **Kaution / Mietminderung** → `tenant_change` or `complaint` (deposit / rent reduction)
- **Hausgeld / Sonderumlage / Verwaltergebühr / Instandhaltungsrücklage** → `owner_communication` (WEG-level dues / special assessment / management fee / reserve fund)
- **WEG / Eigentümerversammlung / ETV-Protokoll / Beirat** → `owner_communication` (owners' association governance)
- **Mietpreisbremse / Brandschutznachweis / Bauamt** → `compliance` (rent cap / fire-safety certificate / building authority)
- **Schimmel / Wasserschaden / Heizungsausfall** → `maintenance` or `complaint`
- **Mahnung / Rechnung / Versicherung / Jahresabrechnung** → `payment` (dunning / invoice / insurance / annual statement)
- **Verkaufsabsicht / Modernisierung-Zustimmung** → `owner_communication` (owner-initiated)

### Section/field vocabulary (binding)

{vocabulary_block}

**Important:** use a field name from this list when the observation matches. If nothing fits, pick the closest match in the right section and append your preferred wording in parentheses inside `value`. **Do NOT invent new field names without precedent.**

### Scope rules (which section family)

- Per-unit events (specific unit, EH-/WE-/MIE- reference present) → sections `overview | tenants | lease | maintenance | financials | compliance | activity`
- Per-building events (HAUS-NN, shared infrastructure) → sections prefixed `building_` (e.g. `building_maintenance`)
- WEG / Liegenschaft events (Hausgeld bookkeeping, insurance, authority requests, utility annual statements) → sections prefixed `liegenschaft_` (e.g. `liegenschaft_financials`, `liegenschaft_compliance`)

### Confidence rules

- `confidence` is a number in `[0.0, 1.0]`. Values > 0.85 only for unambiguous facts.
- For ambiguous events (auto-replies with quoted originals), prefer `facts_to_update: []` over a low-confidence guess.
- Write `value` strings in the **source language** of the email (German body → German value).

### Uncertainty rules (`uncertain[]`)

If you noticed something potentially relevant but **cannot commit to a specific fact** (vague mention, ambiguous reference, hearsay, "we should at some point…", "maybe", "I think"), put it in `uncertain[]` — **not** in `facts_to_update`.

- **Do not invent. Do not guess.** The user prefers a clear "I noticed something unclear" over a confidently wrong "I think this is true."
- Shape: `{{observation, hypothesis?, reason_uncertain, relevant_section, relevant_field?}}`.
  - `observation` — what was noticed (quoted or near-quoted from the source).
  - `hypothesis` — optional candidate value if one is implied; leave empty otherwise.
  - `reason_uncertain` — short reason why this didn't become a fact (e.g. "vague mention without timeframe", "owner expresses intent, no decision", "hearsay about another unit").
  - `relevant_section` — your best guess at the section a future fact would land in.
  - `relevant_field` — optional best guess at the field name.
- Examples of `uncertain[]` candidates: "We should talk about rent at some point", "might give notice later", "the neighbour allegedly has an issue", "we'll need to do the roof eventually".

Return only valid JSON matching the schema.
