// Query keys in one place, so invalidations always match the queries they target.
export const queryKeys = {
  properties: ["properties"] as const,
  propertyFile: (id: string) => ["property-file", id] as const,
  propertyMarkdown: (id: string) => ["property-markdown", id] as const,
  propertyInbox: (id: string, keyword: string | undefined) =>
    ["property-inbox", id, keyword ?? ""] as const,
  eventRaw: (eventId: string) => ["event-raw", eventId] as const,
  portfolioSummary: ["portfolio-summary"] as const,
  portfolioActivity: ["portfolio-activity"] as const,
  pendingSignals: ["inbox-signals"] as const,
  openUncertainties: ["inbox-uncertainties"] as const,
  pendingRejections: ["inbox-rejections"] as const,
};
