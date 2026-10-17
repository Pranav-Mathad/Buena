export const API_BASE_URL = "https://api.keystone.demo";

export type Property = {
  id: string;
  name: string;
  address: string;
  stats: { events: number; facts: number; needsReview: number };
  status: "active";
  openIssues: number;
  isHero?: boolean;
};

export const properties: Property[] = [
  {
    id: "509393da-6806-49ef-9e59-3da0213008cd",
    name: "Musterstraße 26 WE 29",
    address: "Musterstraße 26, 10115 Berlin",
    stats: { events: 92, facts: 11, needsReview: 2 },
    status: "active",
    openIssues: 3,
    isHero: true,
  },
  {
    id: "berliner-12-4b",
    name: "Berliner Straße 12, 4B",
    address: "Berliner Straße 12, 10115 Berlin",
    stats: { events: 14, facts: 8, needsReview: 0 },
    status: "active",
    openIssues: 1,
  },
  {
    id: "berliner-12-2a",
    name: "Berliner Straße 12, 2A",
    address: "Berliner Straße 12, 10115 Berlin",
    stats: { events: 9, facts: 7, needsReview: 0 },
    status: "active",
    openIssues: 0,
  },
  {
    id: "wedding-7-ground",
    name: "Müllerstraße 7, EG",
    address: "Müllerstraße 7, 13353 Berlin",
    stats: { events: 11, facts: 6, needsReview: 1 },
    status: "active",
    openIssues: 0,
  },
];

export type Fact = {
  value: string;
  source: string;
  date: string;
  dateIso?: string;
  urgent?: boolean;
  reason?: string;
  eventId?: string;
  factId?: string;
  humanEdited?: boolean;
  editedBy?: string | null;
};

export type Section = {
  title: string;
  facts: Fact[];
  isUncertain?: boolean;
  isContext?: boolean;
};

export const propertyFile = {
  id: "509393da-6806-49ef-9e59-3da0213008cd",
  name: "Musterstraße 26 WE 29",
  address: "Musterstraße 26, 10115 Berlin",
  sections: [
    {
      title: "Overview",
      facts: [
        { value: "4. OG mitte, 85 m², 3 Zimmer", source: "stammdaten", date: "2024-01-15" },
        { value: "Kaltmiete 1.420 EUR, Warmmiete 1.680 EUR", source: "stammdaten", date: "2024-01-15" },
      ],
    },
    {
      title: "Tenants",
      facts: [
        { value: "Laura Schmitz — Mietverhältnis seit 2022-03-01", source: "Mietvertrag.pdf", date: "2022-03-01" },
      ],
    },
    {
      title: "Maintenance",
      facts: [
        { value: "Wasser läuft seit gestern durch die Küchendecke; Schaden noch nicht behoben.", source: "email", date: "2026-04-23", urgent: true },
        { value: "Lärmbeschwerden aus der Wohnung darüber; Mieterin bittet um offizielle Abmahnung.", source: "email", date: "2026-04-15" },
        { value: "Defektes Fenster im Schlafzimmer seit 3 Monaten unbehoben.", source: "email", date: "2026-01-20" },
      ],
    },
    {
      title: "Financials",
      facts: [
        { value: "15% Mietminderung angekündigt bis Behebung des Wasserschadens.", source: "email", date: "2026-04-23" },
        { value: "Letzte Mietzahlung: 1.680 EUR am 2026-04-01", source: "bank", date: "2026-04-01" },
        { value: "Kaution 4.260 EUR hinterlegt am 2022-03-01", source: "Mietvertrag.pdf", date: "2022-03-01" },
      ],
    },
    {
      title: "Compliance",
      facts: [
        { value: "Brandschutznachweis gültig bis 2027-06-30", source: "Brandschutz.pdf", date: "2024-06-30" },
      ],
    },
    {
      title: "Needs Review (Zu prüfen)",
      isUncertain: true,
      facts: [
        { value: "Mieterin erwähnte beiläufig 'die neue Miete' — unklar ob Mieterhöhung angekündigt wurde.", source: "email", date: "2026-04-10", reason: "ambiguous reference" },
      ],
    },
    {
      title: "Building Context",
      isContext: true,
      facts: [
        { value: "Hausgeld März 2026: 4.200 EUR an Hausverwaltung gezahlt.", source: "bank", date: "2026-03-15" },
        { value: "Aufzugswartung Termin: 2026-05-12.", source: "email", date: "2026-04-01" },
      ],
    },
    {
      title: "WEG Context",
      isContext: true,
      facts: [
        { value: "Eigentümerversammlung 2026: 2026-06-15, 19:00, Restaurant Lindenhof.", source: "letter.pdf", date: "2026-04-05" },
        { value: "Sonderumlage 12.000 EUR für Dachsanierung beschlossen.", source: "letter.pdf", date: "2025-09-20" },
      ],
    },
  ] as Section[],
};

export type AskResponse =
  | {
      status: "answered";
      answer: string;
      confidence: string;
      sources: { id: string; channel: string; date: string; snippet: string }[];
      elapsed_ms: number;
      retrieved_count: number;
    }
  | {
      status: "insufficient_context";
      answer: null;
      confidence: string;
      reasoning: string;
      partial_context: { id: string; channel: string; date: string; snippet: string }[];
      elapsed_ms: number;
      retrieved_count: number;
    }
  | {
      status: "out_of_scope";
      answer: null;
      confidence: string;
      reasoning: string;
      elapsed_ms: number;
    };

export const askResponses: Record<string, AskResponse> = {
  "What's going on with the water damage?": {
    status: "answered",
    answer:
      "Wasser läuft seit dem 2026-04-23 durch die Küchendecke der Mieterin (Laura Schmitz). Der Schaden wurde gemeldet, ist aber noch nicht behoben. Die Mieterin hat als Reaktion eine Mietminderung von 15% angekündigt, die ab Mai 2026 in Kraft tritt, bis das Problem behoben ist.",
    confidence: "high",
    sources: [
      { id: "evt-001", channel: "email", date: "2026-04-23", snippet: "Seit gestern läuft Wasser durch die Decke meiner Küche..." },
      { id: "evt-002", channel: "email", date: "2026-04-23", snippet: "Ich kündige an, die Miete diesen Monat um 15% zu mindern..." },
    ],
    elapsed_ms: 3217,
    retrieved_count: 8,
  },
  "Hat der Mieter eine Mietminderung angekündigt?": {
    status: "answered",
    answer:
      "Ja. Laura Schmitz hat am 2026-04-23 eine 15% Mietminderung angekündigt, ausgelöst durch den unbehobenen Wasserschaden in der Küche. Die Minderung tritt ab Mai 2026 in Kraft.",
    confidence: "high",
    sources: [
      { id: "evt-002", channel: "email", date: "2026-04-23", snippet: "...die Miete diesen Monat um 15% zu mindern, bis das Problem behoben ist." },
    ],
    elapsed_ms: 2784,
    retrieved_count: 6,
  },
  "Wird der Mieter den Vertrag verlängern?": {
    status: "insufficient_context",
    answer: null,
    confidence: "insufficient context",
    reasoning: "Found the lease end_date and recent communications, but nothing in the events that indicates renewal intent either way.",
    partial_context: [
      { id: "evt-010", channel: "Mietvertrag.pdf", date: "2022-03-01", snippet: "Mietverhältnis auf unbestimmte Zeit, frühestes Kündigungsdatum 2025-03-01..." },
      { id: "evt-002", channel: "email", date: "2026-04-23", snippet: "Mietminderung wegen Wasserschaden — kein Hinweis auf Kündigung oder Verlängerung." },
    ],
    elapsed_ms: 2103,
    retrieved_count: 12,
  },
  "What's the tenant's bank account number?": {
    status: "out_of_scope",
    answer: null,
    confidence: "out of scope",
    reasoning: "Banking details, full names, and contractual structured data live in the ERP, not in the context layer.",
    elapsed_ms: 410,
  },
};

export const suggestedQuestions = [
  "What's going on with the water damage?",
  "Hat der Mieter eine Mietminderung angekündigt?",
  "Gibt es Lärmbeschwerden?",
  "Wann läuft der Mietvertrag aus?",
];

export const compareQuestions = [
  "What's going on with the water damage?",
  "Hat der Mieter eine Mietminderung angekündigt?",
  "Wird der Mieter den Vertrag verlängern?",
  "What's the tenant's bank account number?",
];

export const recentActivity = [
  { time: "2h ago", text: "Wasserschaden gemeldet", property: "WE 29" },
  { time: "4h ago", text: "Mietminderung 15% angekündigt", property: "WE 29" },
  { time: "yesterday", text: "Hausgeldzahlung erhalten", property: "Liegenschaft Musterstr." },
  { time: "yesterday", text: "Brandschutznachweis erneuert", property: "Berliner Str. 12" },
  { time: "2 days ago", text: "Lärmbeschwerde eingegangen", property: "WE 29" },
];

export const briefingMarkdown = `**Briefing — Musterstr. 26 WE 29**

Stand: 2026-04-25, basierend auf 92 Ereignissen seit Einzug.

- Akut: Wasserschaden in der Küche seit 2 Tagen unbehoben. Mieterin hat 15% Mietminderung ab Mai angekündigt.
- Konfliktlinie: Lärmbeschwerden gegen die Wohnung darüber ziehen sich seit Januar; bisher keine offizielle Abmahnung ausgesprochen.
- Compliance: Brandschutznachweis gültig bis 2027.
- WEG: Sonderumlage Dach (12.000 EUR) im September beschlossen, Hausgeld läuft regulär.
- Lücke: Ein beiläufiger Hinweis auf 'die neue Miete' im April-Mail; Mieterhöhung wurde nicht in den Unterlagen bestätigt.
- Lücke: Status der Aufzugswartung 12. Mai unbekannt — Termin steht, Bestätigung fehlt.`;

export type DraftExample = {
  key: string;
  label: string;
  incoming: { property: string; channel: "Email" | "Slack"; tone: string; subject: string; body: string };
  drafted: {
    subject: string;
    body: string;
    context: { id: string; channel: string; time: string; snippet: string; whyRelevant: string }[];
    knows_about_incident: boolean;
    elapsed_ms: number;
  };
};

export const draftExamples: DraftExample[] = [
  {
    key: "door",
    label: "Door not working (4th tenant)",
    incoming: {
      property: "Musterstraße 26 WE 29",
      channel: "Email",
      tone: "Neutral",
      subject: "Eingangstür kaputt!",
      body: "Hallo, die Eingangstür unseres Hauses funktioniert nicht mehr richtig. Ich komme gerade kaum noch rein. Können Sie das schnell beheben?",
    },
    drafted: {
      subject: "Re: Eingangstür kaputt!",
      body: `Sehr geehrte/r Bewohner/in,

vielen Dank für Ihre Nachricht. Wir sind über das Problem mit der Eingangstür bereits informiert — heute Vormittag hat sich bereits Frau Renner aus dem 4. Stock gemeldet. Ein Handwerker wurde umgehend beauftragt und wird voraussichtlich morgen zwischen 9 und 11 Uhr die Reparatur durchführen.

Ihren Ersatzschlüssel können Sie ab morgen Nachmittag in unserem Büro abholen. Die Adresse finden Sie auf unserer Website.

Bei weiteren Fragen melden Sie sich gerne.

Mit freundlichen Grüßen,
Hausverwaltung`,
      context: [
        { id: "evt-027", channel: "email", time: "4h ago", snippet: "Frau Renner: 'Eingangstür funktioniert nicht mehr...'", whyRelevant: "first report of same incident" },
        { id: "evt-028", channel: "email", time: "3h ago", snippet: "Hausverwaltung: Handwerker beauftragt, ETA morgen 9-11 Uhr", whyRelevant: "current status" },
      ],
      knows_about_incident: true,
      elapsed_ms: 2410,
    },
  },
  {
    key: "rent",
    label: "Rent increase question",
    incoming: {
      property: "Musterstraße 26 WE 29",
      channel: "Email",
      tone: "Neutral",
      subject: "Frage zur Miethöhe",
      body: "Hallo, ich habe gehört dass die Miete erhöht werden soll. Stimmt das? Wann und um wie viel?",
    },
    drafted: {
      subject: "Re: Frage zur Miethöhe",
      body: `Sehr geehrte/r Bewohner/in,

vielen Dank für Ihre Anfrage. Mir liegen aktuell keine Informationen über eine geplante Mieterhöhung für Ihre Wohnung vor. Ich kläre den Sachverhalt mit dem Eigentümer und melde mich innerhalb der nächsten 3 Werktage zurück.

Mit freundlichen Grüßen,
Hausverwaltung`,
      context: [],
      knows_about_incident: false,
      elapsed_ms: 1893,
    },
  },
  {
    key: "heating",
    label: "Heating outage (new)",
    incoming: {
      property: "Musterstraße 26 WE 29",
      channel: "Email",
      tone: "Warm",
      subject: "Heizung ausgefallen",
      body: "Seit heute Morgen ist die Heizung in der ganzen Wohnung kalt. Bitte dringend kümmern.",
    },
    drafted: {
      subject: "Re: Heizung ausgefallen",
      body: `Sehr geehrte/r Bewohner/in,

vielen Dank für Ihre Meldung — das tut mir leid zu hören. Mir liegen bisher keine weiteren Berichte zu einem Heizungsausfall im Haus vor; ich kontaktiere umgehend unseren Heizungstechniker und melde mich heute noch mit einem Termin zurück.

Falls Sie in der Zwischenzeit weitere Auffälligkeiten bemerken (z.B. ungewöhnliche Geräusche, Wasseraustritt), lassen Sie es mich bitte sofort wissen.

Mit freundlichen Grüßen,
Hausverwaltung`,
      context: [],
      knows_about_incident: false,
      elapsed_ms: 1640,
    },
  },
];

export type InboxEmail = {
  from: string;
  subject: string;
  date: string;
  preview: string;
  relevant: boolean;
};

export const inboxEmails: InboxEmail[] = [
  { from: "Laura Schmitz", subject: "Wasserschaden Küche — dringend", date: "2h ago", preview: "Seit gestern läuft Wasser durch die Decke meiner Küche...", relevant: true },
  { from: "Laura Schmitz", subject: "Mietminderung Mai 2026", date: "2h ago", preview: "Hiermit kündige ich an, die Miete diesen Monat um 15% zu mindern...", relevant: true },
  { from: "Hausverwaltung", subject: "Re: Aufzugswartung 12.05.", date: "1d ago", preview: "Termin bestätigt für den 12. Mai zwischen 9 und 12 Uhr.", relevant: false },
  { from: "WEG Verwalter", subject: "Einladung Eigentümerversammlung", date: "3d ago", preview: "Hiermit laden wir Sie zur Eigentümerversammlung am 15.06...", relevant: false },
  { from: "Laura Schmitz", subject: "Lärm aus Wohnung darüber", date: "1w ago", preview: "Seit Wochen wieder lautes Stampfen ab 23 Uhr — bitte Abmahnung...", relevant: true },
  { from: "Stadtwerke Berlin", subject: "Jahresabrechnung 2025", date: "2w ago", preview: "Anbei Ihre Jahresabrechnung für den Verbrauchszeitraum...", relevant: false },
  { from: "Brandschutz GmbH", subject: "Nachweis erneuert", date: "3w ago", preview: "Anbei der erneuerte Brandschutznachweis, gültig bis 2027-06-30.", relevant: false },
  { from: "Laura Schmitz", subject: "Frage zur Nebenkostenabrechnung", date: "1mo ago", preview: "Ich habe eine Frage zu Position 4 der NK-Abrechnung...", relevant: false },
  { from: "Hausmeister Service", subject: "Treppenhausreinigung Mai", date: "1mo ago", preview: "Anbei der aktualisierte Reinigungsplan für Mai 2026.", relevant: false },
  { from: "Laura Schmitz", subject: "Defektes Fenster Schlafzimmer", date: "3mo ago", preview: "Das Fenster im Schlafzimmer schließt nicht mehr richtig...", relevant: false },
  { from: "Versicherung Allianz", subject: "Police-Update", date: "3mo ago", preview: "Ihre Wohngebäudeversicherung wurde aktualisiert.", relevant: false },
  { from: "Vattenfall", subject: "Abschlag April", date: "1mo ago", preview: "Ihr neuer Abschlag beträgt 142 EUR ab Mai 2026.", relevant: false },
  { from: "Nachbarschaft Berlin", subject: "Newsletter April", date: "1mo ago", preview: "Neuigkeiten aus Ihrem Kiez im April 2026.", relevant: false },
  { from: "GLS Paketdienst", subject: "Paket nicht zustellbar", date: "2mo ago", preview: "Ihr Paket konnte nicht zugestellt werden, Abholung möglich.", relevant: false },
];
