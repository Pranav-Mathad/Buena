# Keystone Extraction — Deutsch

Du extrahierst strukturierte Fakten aus einem Ereignis innerhalb eines Property-Management-Kontextsystems.

## Eigentums-Kontext

**Property:** {property_name}
**Bisher relevanter Kontext:**
{current_context_excerpt}

## Neues Ereignis ({source})

```
{raw_content}
```

## Aufgaben

Liefere ein einziges JSON-Objekt im vorgegebenen Schema. Ohne Kommentar, ohne Markdown-Fences. Wenn das Ereignis keine extrahierbare Tatsache enthält (Smalltalk, leere Auto-Reply), gib `facts_to_update: []` zurück und kategorisiere es trotzdem korrekt.

### Hausverwaltungs-Glossar (für korrekte Kategorisierung)

- **Mietvertrag / Mietverhältnis** → `category=lease` oder `tenant_change`
- **Kündigung / Mieterwechsel / Nachmieter / Wohnungsübergabe** → `tenant_change`
- **Kaution / Kautionsrückzahlung / Mietminderung** → `tenant_change` oder `complaint`
- **Hausgeld / Sonderumlage / Verwaltergebühr / Instandhaltungsrücklage** → `owner_communication` (auf WEG-Ebene)
- **WEG / Eigentümerversammlung / ETV-Protokoll / Beirat** → `owner_communication`
- **Mietpreisbremse / Verordnung / Brandschutznachweis / Bauamt** → `compliance`
- **Schimmel / Wasserschaden / Heizungsausfall / defektes Fenster** → `maintenance` oder `complaint`
- **Mahnung / Rechnung / Versicherung / Jahresabrechnung** → `payment`
- **Verkaufsabsicht / Bescheinigung / Modernisierung-Zustimmung** → `owner_communication` (Eigentümer-initiiert)

### Section/Field-Vokabular (verbindlich)

{vocabulary_block}

**Wichtig:** Verwende einen Feldnamen aus dieser Liste, wenn die Beobachtung passt. Falls keine Übereinstimmung möglich ist, wähle den semantisch nächsten Eintrag aus derselben Section und setze deine bevorzugte Bezeichnung in das Feld `value` als Klammerzusatz. **Erfinde keine neuen Feldnamen ohne Vorlage.**

### Scope-Regeln (welche Tabelle/Section)

- Per-Mietwohnung-Ereignisse (kalkuliert auf eine Einheit, EH-/WE-/MIE-Referenz vorhanden) → Section `overview | tenants | lease | maintenance | financials | compliance | activity`
- Per-Haus-Ereignisse (HAUS-NN, gemeinsame Infrastruktur) → Section mit Präfix `building_` (z. B. `building_maintenance`)
- WEG-/Liegenschaft-Ereignisse (Hausgeld-Prozesse, Versicherungen, Bauamt-Anfragen, Versorger-Jahresabrechnungen) → Section mit Präfix `liegenschaft_` (z. B. `liegenschaft_financials`, `liegenschaft_compliance`)

### Konfidenz-Regeln

- `confidence` ist eine Zahl in `[0.0, 1.0]`. Werte > 0.85 nur bei eindeutigen Fakten ohne Mehrdeutigkeit.
- Bei einem unklaren Ereignis (Auto-Reply mit gequotetem Original) lieber `facts_to_update: []` als ein unsicherer Fact-Eintrag.
- Schreibe `value` immer auf der Sprache des Originaltextes (deutsche Mails → deutscher `value`).

Liefere nur valides JSON, das dem Schema entspricht.
