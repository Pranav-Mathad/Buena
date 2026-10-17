import type { Property, Section } from "./data";
import {
  parseBriefingMarkdown,
  stripInlineMarkdown,
  type BriefingItem,
} from "./text";

export const API_BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ??
  "http://localhost:8080";

export const HERO_PROPERTY_ID =
  "509393da-6806-49ef-9e59-3da0213008cd";

type PropertySummaryDTO = {
  id: string;
  name: string;
  address: string;
  owner_name: string | null;
  building_year_built: number | null;
  events: number;
  facts: number;
  needs_review: number;
  open_issues: number;
};

// Map the backend's PropertySummary into the UI's Property type.
export async function fetchProperties(): Promise<Property[]> {
  const res = await fetch(`${API_BASE_URL}/properties`);
  if (!res.ok) {
    throw new Error(
      `GET /properties failed with ${res.status} ${res.statusText}`,
    );
  }
  const rows = (await res.json()) as PropertySummaryDTO[];
  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    address: r.address,
    stats: {
      events: r.events,
      facts: r.facts,
      needsReview: r.needs_review,
    },
    status: "active" as const,
    openIssues: r.open_issues,
    isHero: r.id === HERO_PROPERTY_ID,
  }));
}

export type PropertyFile = {
  id: string;
  name: string;
  address: string;
  sections: Section[];
};

type PropertyFileFactDTO = {
  value: string;
  source: string;
  date: string;
  date_iso: string | null;
  confidence: number | null;
  urgent: boolean;
  reason: string | null;
  section: string;
  field: string;
  event_id: string | null;
  fact_id: string | null;
  human_edited: boolean;
  edited_by: string | null;
};

type PropertyFileSectionDTO = {
  title: string;
  facts: PropertyFileFactDTO[];
  is_uncertain: boolean;
  is_context: boolean;
};

type PropertyFileDTO = {
  id: string;
  name: string;
  address: string;
  sections: PropertyFileSectionDTO[];
};

export type OnboardingResponse = {
  propertyId: string;
  items: BriefingItem[];
  cached: boolean;
};

type OnboardingDTO = {
  property_id: string;
  markdown: string;
  cached: boolean;
};

export async function fetchOnboarding(id: string): Promise<OnboardingResponse> {
  const res = await fetch(`${API_BASE_URL}/admin/properties/${id}/onboarding`);
  if (!res.ok) {
    throw new Error(
      `GET /admin/properties/${id}/onboarding failed with ${res.status} ${res.statusText}`,
    );
  }
  const dto = (await res.json()) as OnboardingDTO;
  return {
    propertyId: dto.property_id,
    items: parseBriefingMarkdown(dto.markdown),
    cached: dto.cached,
  };
}

export type AskResult = {
  status: "answered" | "insufficient_context" | "out_of_scope";
  answer: string | null;
  confidence: string;
  reasoning: string;
  sources: { id: string; channel: string; date: string; snippet: string }[];
  partialContext: { id: string; channel: string; date: string; snippet: string }[];
  elapsedMs: number;
  retrievedCount: number;
  model: string;
};

type AskDTO = {
  status: string;
  answer: string | null;
  confidence: string;
  reasoning: string;
  sources: { id: string; channel: string; date: string; snippet: string }[];
  partial_context: { id: string; channel: string; date: string; snippet: string }[];
  elapsed_ms: number;
  retrieved_count: number;
  model: string;
};

export async function fetchAsk(
  propertyId: string,
  question: string,
): Promise<AskResult> {
  const res = await fetch(
    `${API_BASE_URL}/properties/${propertyId}/ask`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    },
  );
  if (!res.ok) {
    throw new Error(
      `POST /properties/${propertyId}/ask failed with ${res.status} ${res.statusText}`,
    );
  }
  const dto = (await res.json()) as AskDTO;
  const status =
    dto.status === "answered" ||
    dto.status === "insufficient_context" ||
    dto.status === "out_of_scope"
      ? dto.status
      : "insufficient_context";
  return {
    status,
    answer: dto.answer ? stripInlineMarkdown(dto.answer) : null,
    confidence: stripInlineMarkdown(dto.confidence),
    reasoning: stripInlineMarkdown(dto.reasoning),
    sources: dto.sources.map((s) => ({
      ...s,
      snippet: stripInlineMarkdown(s.snippet),
    })),
    partialContext: dto.partial_context.map((s) => ({
      ...s,
      snippet: stripInlineMarkdown(s.snippet),
    })),
    elapsedMs: dto.elapsed_ms,
    retrievedCount: dto.retrieved_count,
    model: dto.model,
  };
}

export type DraftReplyContext = {
  id: string;
  channel: string;
  time: string;
  snippet: string;
  whyRelevant: string;
};

export type DraftReplyResult = {
  subject: string;
  body: string;
  context: DraftReplyContext[];
  knowsAboutIncident: boolean;
  elapsedMs: number;
  model: string;
};

export type DraftReplyInput = {
  propertyId?: string;
  propertyName?: string;
  channel: "Email" | "Slack";
  tone: string;
  subject?: string;
  body: string;
};

type DraftContextDTO = {
  id: string;
  channel: string;
  time: string;
  snippet: string;
  why_relevant: string;
};

type DraftReplyDTO = {
  subject: string;
  body: string;
  context: DraftContextDTO[];
  knows_about_incident: boolean;
  elapsed_ms: number;
  model: string;
};

export async function fetchDraftReply(
  input: DraftReplyInput,
): Promise<DraftReplyResult> {
  const res = await fetch(`${API_BASE_URL}/draft-reply`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      property_id: input.propertyId,
      property_name: input.propertyName,
      channel: input.channel,
      tone: input.tone,
      subject: input.subject,
      body: input.body,
    }),
  });
  if (!res.ok) {
    throw new Error(
      `POST /draft-reply failed with ${res.status} ${res.statusText}`,
    );
  }
  const dto = (await res.json()) as DraftReplyDTO;
  return {
    subject: stripInlineMarkdown(dto.subject),
    body: stripInlineMarkdown(dto.body),
    context: dto.context.map((c) => ({
      id: c.id,
      channel: c.channel,
      time: c.time,
      snippet: stripInlineMarkdown(c.snippet),
      whyRelevant: stripInlineMarkdown(c.why_relevant),
    })),
    knowsAboutIncident: dto.knows_about_incident,
    elapsedMs: dto.elapsed_ms,
    model: dto.model,
  };
}

export type PortfolioActivity = {
  eventId: string;
  receivedAt: string;
  source: string;
  text: string;
  propertyId: string | null;
  propertyName: string | null;
};

type PortfolioActivityDTO = {
  event_id: string;
  received_at: string;
  source: string;
  text: string;
  property_id: string | null;
  property_name: string | null;
};

export async function fetchPortfolioActivity(
  limit = 8,
): Promise<PortfolioActivity[]> {
  const res = await fetch(`${API_BASE_URL}/portfolio/activity?limit=${limit}`);
  if (!res.ok) {
    throw new Error(
      `GET /portfolio/activity failed with ${res.status} ${res.statusText}`,
    );
  }
  const rows = (await res.json()) as PortfolioActivityDTO[];
  return rows.map((r) => ({
    eventId: r.event_id,
    receivedAt: r.received_at,
    source: r.source,
    text: r.text,
    propertyId: r.property_id,
    propertyName: r.property_name,
  }));
}

// Format an ISO timestamp into the activity-feed's relative-time
// convention ("2h ago", "yesterday", "3 days ago").
export function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "";
  const diffMs = Date.now() - then;
  const minutes = Math.floor(diffMs / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days === 1) return "yesterday";
  if (days < 7) return `${days} days ago`;
  return new Date(iso).toLocaleDateString("de-DE");
}

export type PortfolioSummary = {
  properties: number;
  pendingSignals: number;
  resolvedSignals: number;
  pendingPortfolioSignals: number;
  eventsLastHour: number;
  factsToday: number;
};

type PortfolioSummaryDTO = {
  properties: number;
  pending_signals: number;
  resolved_signals: number;
  pending_portfolio_signals: number;
  events_last_hour: number;
  facts_today: number;
};

export async function fetchPortfolioSummary(): Promise<PortfolioSummary> {
  const res = await fetch(`${API_BASE_URL}/portfolio/summary`);
  if (!res.ok) {
    throw new Error(
      `GET /portfolio/summary failed with ${res.status} ${res.statusText}`,
    );
  }
  const dto = (await res.json()) as PortfolioSummaryDTO;
  return {
    properties: dto.properties,
    pendingSignals: dto.pending_signals,
    resolvedSignals: dto.resolved_signals,
    pendingPortfolioSignals: dto.pending_portfolio_signals,
    eventsLastHour: dto.events_last_hour,
    factsToday: dto.facts_today,
  };
}

export type InboxSignal = {
  id: string;
  propertyId: string | null;
  propertyName: string | null;
  type: string;
  severity: string;
  message: string;
  status: string;
  createdAt: string;
};

type InboxSignalDTO = {
  id: string;
  property_id: string | null;
  property_name: string | null;
  type: string;
  severity: string;
  message: string;
  status: string;
  created_at: string;
};

export async function fetchPendingSignals(
  limit = 50,
): Promise<InboxSignal[]> {
  const res = await fetch(
    `${API_BASE_URL}/signals?status=pending&limit=${limit}`,
  );
  if (!res.ok) {
    throw new Error(
      `GET /signals failed with ${res.status} ${res.statusText}`,
    );
  }
  const rows = (await res.json()) as InboxSignalDTO[];
  return rows.map((r) => ({
    id: r.id,
    propertyId: r.property_id,
    propertyName: r.property_name,
    type: r.type,
    severity: r.severity,
    message: r.message,
    status: r.status,
    createdAt: r.created_at,
  }));
}

export type InboxUncertainty = {
  uncertaintyId: string;
  eventId: string;
  propertyId: string | null;
  propertyName: string | null;
  relevantSection: string | null;
  observation: string;
  hypothesis: string | null;
  reasonUncertain: string;
  source: string;
  status: string;
  createdAt: string;
};

type InboxUncertaintyDTO = {
  uncertainty_id: string;
  event_id: string;
  property_id: string | null;
  property_name: string | null;
  relevant_section: string | null;
  observation: string;
  hypothesis: string | null;
  reason_uncertain: string;
  source: string;
  status: string;
  created_at: string;
};

export async function fetchOpenUncertainties(
  limit = 50,
): Promise<InboxUncertainty[]> {
  const res = await fetch(
    `${API_BASE_URL}/admin/uncertainties?status=open&limit=${limit}`,
  );
  if (!res.ok) {
    throw new Error(
      `GET /admin/uncertainties failed with ${res.status} ${res.statusText}`,
    );
  }
  const rows = (await res.json()) as InboxUncertaintyDTO[];
  return rows.map((r) => ({
    uncertaintyId: r.uncertainty_id,
    eventId: r.event_id,
    propertyId: r.property_id,
    propertyName: r.property_name,
    relevantSection: r.relevant_section,
    observation: r.observation,
    hypothesis: r.hypothesis,
    reasonUncertain: r.reason_uncertain,
    source: r.source,
    status: r.status,
    createdAt: r.created_at,
  }));
}

export type InboxRejection = {
  rejectionId: string;
  eventId: string;
  propertyId: string | null;
  propertyName: string | null;
  proposedSection: string;
  proposedField: string;
  proposedValue: string;
  constraintName: string;
  reason: string;
  reviewedStatus: string;
  createdAt: string;
  snippet: string;
};

type InboxRejectionDTO = {
  rejection_id: string;
  event_id: string;
  property_id: string | null;
  property_name: string | null;
  proposed_section: string;
  proposed_field: string;
  proposed_value: string;
  constraint_name: string;
  reason: string;
  reviewed_status: string;
  created_at: string;
  snippet: string;
};

export async function fetchPendingRejections(
  limit = 50,
): Promise<InboxRejection[]> {
  const res = await fetch(
    `${API_BASE_URL}/admin/rejected?status=pending&limit=${limit}`,
  );
  if (!res.ok) {
    throw new Error(
      `GET /admin/rejected failed with ${res.status} ${res.statusText}`,
    );
  }
  const rows = (await res.json()) as InboxRejectionDTO[];
  return rows.map((r) => ({
    rejectionId: r.rejection_id,
    eventId: r.event_id,
    propertyId: r.property_id,
    propertyName: r.property_name,
    proposedSection: r.proposed_section,
    proposedField: r.proposed_field,
    proposedValue: r.proposed_value,
    constraintName: r.constraint_name,
    reason: r.reason,
    reviewedStatus: r.reviewed_status,
    createdAt: r.created_at,
    snippet: r.snippet,
  }));
}

export type InboxRow = {
  eventId: string;
  date: string;
  sender: string;
  subject: string;
  preview: string;
  relevant: boolean;
};

export type InboxView = {
  rows: InboxRow[];
  totalInboxSize: number;
  relevantCount: number;
  keyword: string | null;
};

type InboxRowDTO = {
  event_id: string;
  date: string;
  sender: string;
  subject: string;
  preview: string;
  relevant: boolean;
};

type InboxResponseDTO = {
  rows: InboxRowDTO[];
  total_inbox_size: number;
  relevant_count: number;
  keyword: string | null;
};

export async function fetchPropertyInbox(
  propertyId: string,
  q?: string,
  limit = 20,
): Promise<InboxView> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (q && q.trim()) params.set("q", q.trim());
  const res = await fetch(
    `${API_BASE_URL}/properties/${propertyId}/inbox?${params.toString()}`,
  );
  if (!res.ok) {
    throw new Error(
      `GET /properties/${propertyId}/inbox failed with ${res.status} ${res.statusText}`,
    );
  }
  const dto = (await res.json()) as InboxResponseDTO;
  return {
    rows: dto.rows.map((r) => ({
      eventId: r.event_id,
      date: r.date,
      sender: r.sender,
      subject: r.subject,
      preview: r.preview,
      relevant: r.relevant,
    })),
    totalInboxSize: dto.total_inbox_size,
    relevantCount: dto.relevant_count,
    keyword: dto.keyword,
  };
}

export async function fetchEventRaw(eventId: string): Promise<string> {
  const res = await fetch(`${API_BASE_URL}/events/${eventId}/raw`);
  if (!res.ok) {
    throw new Error(
      `GET /events/${eventId}/raw failed with ${res.status} ${res.statusText}`,
    );
  }
  return await res.text();
}

export async function fetchPropertyFile(id: string): Promise<PropertyFile> {
  const res = await fetch(`${API_BASE_URL}/properties/${id}/file`);
  if (!res.ok) {
    throw new Error(
      `GET /properties/${id}/file failed with ${res.status} ${res.statusText}`,
    );
  }
  const dto = (await res.json()) as PropertyFileDTO;
  return {
    id: dto.id,
    name: dto.name,
    address: dto.address,
    sections: dto.sections.map((s) => ({
      title: s.title,
      isUncertain: s.is_uncertain || undefined,
      isContext: s.is_context || undefined,
      facts: s.facts.map((f) => ({
        value: stripInlineMarkdown(f.value),
        source: f.source,
        date: f.date,
        dateIso: f.date_iso ?? undefined,
        urgent: f.urgent || undefined,
        reason: f.reason ? stripInlineMarkdown(f.reason) : undefined,
        eventId: f.event_id ?? undefined,
        factId: f.fact_id ?? undefined,
        humanEdited: f.human_edited || undefined,
        editedBy: f.edited_by,
      })),
    })),
  };
}


export type FactEditResult = {
  factId: string;
  supersededFactId: string;
  propertyId: string;
  section: string;
  field: string;
  value: string;
  editedBy: string;
  editedAt: string;
};

type FactEditDTO = {
  fact_id: string;
  superseded_fact_id: string;
  property_id: string;
  section: string;
  field: string;
  value: string;
  edited_by: string;
  edited_at: string;
};

// PATCH a fact's value. Server inserts a new ``human_edited=true`` row
// and supersedes the old one. Future extractor proposals against the
// same (property, section, field) coordinate will be skipped with a
// ``preserved_human_edit`` reason.
export async function editFact(
  propertyId: string,
  factId: string,
  value: string,
  editedBy = "operator",
): Promise<FactEditResult> {
  const res = await fetch(
    `${API_BASE_URL}/properties/${propertyId}/facts/${factId}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value, edited_by: editedBy }),
    },
  );
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(
      `PATCH /properties/${propertyId}/facts/${factId} failed: ${res.status} ${res.statusText}${detail ? ` — ${detail}` : ""}`,
    );
  }
  const dto = (await res.json()) as FactEditDTO;
  return {
    factId: dto.fact_id,
    supersededFactId: dto.superseded_fact_id,
    propertyId: dto.property_id,
    section: dto.section,
    field: dto.field,
    value: dto.value,
    editedBy: dto.edited_by,
    editedAt: dto.edited_at,
  };
}

export type MarkdownDoc = {
  body: string;
};

// GET the canonical markdown for a property — rendered live from
// facts, stammdaten, and uncertainties on every request.
export async function fetchPropertyMarkdown(
  id: string,
): Promise<MarkdownDoc> {
  const res = await fetch(`${API_BASE_URL}/properties/${id}/markdown`);
  if (!res.ok) {
    throw new Error(
      `GET /properties/${id}/markdown failed with ${res.status} ${res.statusText}`,
    );
  }
  return { body: await res.text() };
}

