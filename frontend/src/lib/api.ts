// Typed client for the Keystone REST API.
//
// Every endpoint goes through `request`, which prefixes API_BASE_URL and turns
// failures into typed errors: `NetworkError` when the API cannot be reached,
// `ApiError` (with status and the server's `detail`) for non-2xx responses.
// DTO types mirror the backend's snake_case JSON; each fetcher maps them to
// the camelCase shapes the UI uses.

import { API_BASE_URL } from "./config";
import { parseBriefingMarkdown, stripInlineMarkdown, type BriefingItem } from "./text";
import type { Property, Section } from "./types";

export { API_BASE_URL };

export class ApiError extends Error {
  constructor(
    readonly method: string,
    readonly path: string,
    readonly status: number,
    readonly detail: string | null,
  ) {
    super(`${method} ${path} failed with ${status}${detail ? `: ${detail}` : ""}`);
    this.name = "ApiError";
  }
}

export class NetworkError extends Error {
  constructor(
    readonly method: string,
    readonly path: string,
    cause: unknown,
  ) {
    super(`Could not reach the API (${method} ${path})`, { cause });
    this.name = "NetworkError";
  }
}

/**
 * Why a call to an LLM-backed endpoint (Ask, reply drafting) failed. The API
 * answers 503 when no LLM provider is configured, which is not the same as
 * the service being down.
 */
export function llmFailureKind(e: unknown): "no-key" | "failed" | "unreachable" {
  if (e instanceof ApiError) return e.status === 503 ? "no-key" : "failed";
  return "unreachable";
}

// FastAPI errors carry {"detail": "..."} or a list of validation issues.
async function readDetail(res: Response): Promise<string | null> {
  const body = await res.text().catch(() => "");
  if (!body) return null;
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed && typeof parsed === "object" && "detail" in parsed) {
      const detail = (parsed as { detail: unknown }).detail;
      return typeof detail === "string" ? detail : JSON.stringify(detail);
    }
  } catch {
    // Not JSON; fall through to the raw body.
  }
  return body.slice(0, 300);
}

async function request(path: string, init: RequestInit = {}): Promise<Response> {
  const method = (init.method ?? "GET").toUpperCase();
  let res: Response;
  try {
    res = await fetch(`${API_BASE_URL}${path}`, init);
  } catch (err) {
    throw new NetworkError(method, path, err);
  }
  if (!res.ok) {
    throw new ApiError(method, path, res.status, await readDetail(res));
  }
  return res;
}

/** GET (or any method via `init`) and parse the JSON body as `T`. */
export async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await request(path, init);
  return (await res.json()) as T;
}

async function sendJson<T>(method: "POST" | "PATCH", path: string, body: unknown): Promise<T> {
  return fetchJson<T>(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function fetchText(path: string): Promise<string> {
  return (await request(path)).text();
}

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
  is_hero: boolean;
};

export async function fetchProperties(): Promise<Property[]> {
  const rows = await fetchJson<PropertySummaryDTO[]>("/properties");
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
    isHero: r.is_hero,
    owner: r.owner_name,
    yearBuilt: r.building_year_built,
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

/**
 * Where the briefing's "Key context" section came from: written by the LLM
 * on this request, read from the cache, or unavailable because no provider
 * is configured (every other section is built from the property file).
 */
export type BriefingSource = "llm" | "cache" | "unavailable";

export type OnboardingResponse = {
  propertyId: string;
  items: BriefingItem[];
  source: BriefingSource;
};

type OnboardingDTO = {
  property_id: string;
  markdown: string;
  briefing_source: BriefingSource;
};

export async function fetchOnboarding(id: string): Promise<OnboardingResponse> {
  const dto = await fetchJson<OnboardingDTO>(`/admin/properties/${id}/onboarding`);
  return {
    propertyId: dto.property_id,
    items: parseBriefingMarkdown(dto.markdown),
    source: dto.briefing_source,
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

export async function fetchAsk(propertyId: string, question: string): Promise<AskResult> {
  const dto = await sendJson<AskDTO>("POST", `/properties/${propertyId}/ask`, { question });
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

export async function fetchDraftReply(input: DraftReplyInput): Promise<DraftReplyResult> {
  const dto = await sendJson<DraftReplyDTO>("POST", "/draft-reply", {
    property_id: input.propertyId,
    property_name: input.propertyName,
    channel: input.channel,
    tone: input.tone,
    subject: input.subject,
    body: input.body,
  });
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

export async function fetchPortfolioActivity(limit = 8): Promise<PortfolioActivity[]> {
  const rows = await fetchJson<PortfolioActivityDTO[]>(`/portfolio/activity?limit=${limit}`);
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
  const dto = await fetchJson<PortfolioSummaryDTO>("/portfolio/summary");
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

export async function fetchPendingSignals(limit = 50): Promise<InboxSignal[]> {
  const rows = await fetchJson<InboxSignalDTO[]>(`/signals?status=pending&limit=${limit}`);
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

export type SignalDecision = {
  signalId: string;
  status: string;
  outboxId: string | null;
};

type SignalActionDTO = {
  signal_id: string;
  status: string;
  outbox_id?: string | null;
};

/** Approve (writes the outbox row) or reject a pending signal. */
export async function decideSignal(
  signalId: string,
  decision: "approve" | "reject",
): Promise<SignalDecision> {
  const dto = await fetchJson<SignalActionDTO>(`/signals/${signalId}/${decision}`, {
    method: "POST",
  });
  return { signalId: dto.signal_id, status: dto.status, outboxId: dto.outbox_id ?? null };
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

export async function fetchOpenUncertainties(limit = 50): Promise<InboxUncertainty[]> {
  const rows = await fetchJson<InboxUncertaintyDTO[]>(
    `/admin/uncertainties?status=open&limit=${limit}`,
  );
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

export async function fetchPendingRejections(limit = 50): Promise<InboxRejection[]> {
  const rows = await fetchJson<InboxRejectionDTO[]>(
    `/admin/rejected?status=pending&limit=${limit}`,
  );
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
  const dto = await fetchJson<InboxResponseDTO>(`/properties/${propertyId}/inbox?${params}`);
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
  return fetchText(`/events/${eventId}/raw`);
}

export async function fetchPropertyFile(id: string): Promise<PropertyFile> {
  const dto = await fetchJson<PropertyFileDTO>(`/properties/${id}/file`);
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
  const dto = await sendJson<FactEditDTO>("PATCH", `/properties/${propertyId}/facts/${factId}`, {
    value,
    edited_by: editedBy,
  });
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

// The property's canonical markdown (master data, sourced facts, open
// uncertainties), served from the materialized file when available.
export async function fetchPropertyMarkdown(id: string): Promise<MarkdownDoc> {
  return { body: await fetchText(`/properties/${id}/markdown`) };
}
