// Static UI copy. Everything else the UI shows comes from the API.

/** Example questions offered under the Ask input. */
export const suggestedQuestions = [
  "What's going on with the water damage?",
  "Hat der Mieter eine Mietminderung angekündigt?",
  "Gibt es Lärmbeschwerden?",
  "Wann läuft der Mietvertrag aus?",
];

/**
 * Questions for the Compare page: two the property file answers, one where
 * the honest answer is "not enough context", and one about data Keystone
 * deliberately does not hold.
 */
export const compareQuestions = [
  "What's going on with the water damage?",
  "Hat der Mieter eine Mietminderung angekündigt?",
  "Wird der Mieter den Vertrag verlängern?",
  "What's the tenant's bank account number?",
];

export type SampleInboundMessage = {
  key: string;
  label: string;
  channel: "Email" | "Slack";
  tone: string;
  subject: string;
  body: string;
};

/** Inbound messages that prefill the Draft reply form. */
export const sampleInboundMessages: SampleInboundMessage[] = [
  {
    key: "door",
    label: "Door not working (repeat report)",
    channel: "Email",
    tone: "Neutral",
    subject: "Eingangstür kaputt!",
    body: "Hallo, die Eingangstür unseres Hauses funktioniert nicht mehr richtig. Ich komme gerade kaum noch rein. Können Sie das schnell beheben?",
  },
  {
    key: "rent",
    label: "Rent increase question",
    channel: "Email",
    tone: "Neutral",
    subject: "Frage zur Miethöhe",
    body: "Hallo, ich habe gehört dass die Miete erhöht werden soll. Stimmt das? Wann und um wie viel?",
  },
  {
    key: "heating",
    label: "Heating outage (new)",
    channel: "Email",
    tone: "Warm",
    subject: "Heizung ausgefallen",
    body: "Seit heute Morgen ist die Heizung in der ganzen Wohnung kalt. Bitte dringend kümmern.",
  },
];
