import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { useProperties, usePropertyFile, queryKeys } from "@/hooks/queries";
import { API_BASE_URL } from "@/lib/api";
import { ChevronLeft, ClipboardList, Building2, X, Lightbulb, RefreshCw, FileText, ExternalLink, ChevronDown, Pencil } from "lucide-react";
import { editFact, fetchOnboarding, relativeTime } from "@/lib/api";
import type { Fact } from "@/lib/types";
import type { BriefingItem } from "@/lib/text";
import { Card, StatusPill, Swatch, PrimaryButton, SecondaryButton } from "@/components/ui-bits";
import { AskPanel } from "@/components/AskPanel";
import { MarkdownModal } from "@/components/MarkdownModal";
import { SourceModal, type SourceModalSource } from "@/components/SourceModal";

const Tooltip = ({ children, label }: { children: React.ReactNode; label: string }) => (
  <span className="relative group/tt">
    {children}
    <span className="pointer-events-none absolute left-1/2 -translate-x-1/2 -top-9 whitespace-nowrap rounded-md bg-foreground text-background text-[11px] font-mono px-2 py-1 opacity-0 group-hover/tt:opacity-100 transition-opacity z-20">
      {label}
    </span>
  </span>
);

const TRUNCATE_THRESHOLD = 240;

const FactRow = ({
  fact,
  urgent,
  onOpenSource,
  onEdit,
}: {
  fact: Fact;
  urgent?: boolean;
  onOpenSource: (s: SourceModalSource) => void;
  onEdit?: (f: Fact) => void;
}) => {
  const [expanded, setExpanded] = useState(false);
  const isUrgent = urgent || fact.urgent;
  const isLong = (fact.value?.length ?? 0) > TRUNCATE_THRESHOLD;
  const editable = !!fact.factId && !!onEdit;
  const isOperatorEdit = fact.humanEdited || fact.source === "operator";

  return (
    <div
      className={`group relative py-4 px-4 -mx-4 rounded-xl border-b border-border last:border-0 transition-colors hover:bg-foreground/[0.015] ${
        isUrgent ? "bg-warning/[0.04]" : ""
      }`}
    >
      {isUrgent && (
        <span className="absolute left-0 top-5 bottom-5 w-[3px] rounded-full bg-warning" />
      )}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2 min-w-0">
          <span
            className={`font-mono text-[10.5px] uppercase tracking-[0.12em] px-2 py-0.5 rounded-full border ${
              isOperatorEdit
                ? "border-success/40 bg-success/10 text-success"
                : "border-border bg-card text-muted-foreground/80"
            }`}
          >
            {fact.source}
          </span>
          <Tooltip label={fact.dateIso ?? fact.date}>
            <span className="font-mono text-[11.5px] text-muted-foreground cursor-default">
              {fact.dateIso ? relativeTime(fact.dateIso) : fact.date}
            </span>
          </Tooltip>
          {isUrgent && (
            <span className="font-mono text-[10.5px] uppercase tracking-[0.12em] text-warning px-2 py-0.5 rounded-full bg-warning/10 ring-1 ring-warning/30">
              urgent
            </span>
          )}
          {isOperatorEdit && fact.editedBy && (
            <Tooltip label={`Edited by ${fact.editedBy}`}>
              <span className="font-mono text-[10.5px] text-success cursor-default">
                ✓ human-edited
              </span>
            </Tooltip>
          )}
        </div>
        <div className="flex items-center gap-3">
          {editable && (
            <Tooltip label="Edit this fact (overrides extractor proposals)">
              <button
                type="button"
                onClick={() => onEdit?.(fact)}
                className="inline-flex items-center gap-1 text-[11.5px] font-mono text-muted-foreground hover:text-foreground transition-colors opacity-0 group-hover:opacity-100 focus:opacity-100"
              >
                <Pencil className="w-3 h-3" />
                edit
              </button>
            </Tooltip>
          )}
          <Tooltip label="Open original event">
            <button
              type="button"
              onClick={() =>
                onOpenSource({
                  channel: fact.source,
                  date: fact.date,
                  snippet: fact.value,
                  eventId: fact.eventId,
                })
              }
              className="inline-flex items-center gap-1 text-[11.5px] font-mono text-muted-foreground hover:text-foreground transition-colors"
            >
              source
              <ExternalLink className="w-3 h-3" />
            </button>
          </Tooltip>
        </div>
      </div>

      <p
        className={`mt-2 text-[14px] leading-snug whitespace-pre-line ${
          !expanded && isLong ? "line-clamp-3" : ""
        }`}
      >
        {fact.value}
      </p>

      {isLong && (
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="mt-2 inline-flex items-center gap-1 text-[12px] text-muted-foreground hover:text-foreground transition-colors"
        >
          {expanded ? "Show less" : "Read more"}
          <ChevronDown
            className={`w-3 h-3 transition-transform ${
              expanded ? "rotate-180" : ""
            }`}
          />
        </button>
      )}

      {fact.reason && (
        <p className="mt-2 text-[12.5px] italic text-muted-foreground leading-snug">
          {fact.reason}
        </p>
      )}
    </div>
  );
};

type EditFactState = {
  fact: Fact;
  section: string;
};

const EditFactModal = ({
  state,
  propertyId,
  onClose,
  onSaved,
}: {
  state: EditFactState;
  propertyId: string;
  onClose: () => void;
  onSaved: () => void;
}) => {
  const [value, setValue] = useState(state.fact.value);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const trimmed = value.trim();
  const unchanged = trimmed === state.fact.value.trim();
  const empty = trimmed.length === 0;
  const canSave = !saving && !unchanged && !empty;

  const handleSave = async () => {
    if (!canSave || !state.fact.factId) return;
    setSaving(true);
    setError(null);
    try {
      await editFact(propertyId, state.fact.factId, trimmed);
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : "edit failed");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 animate-fade-in">
      <div className="absolute inset-0 bg-foreground/30 backdrop-blur-[2px]" onClick={onClose} />
      <div className="relative bg-card border border-border rounded-2xl max-w-xl w-full p-7 shadow-card">
        <div className="flex items-start justify-between mb-1">
          <div className="flex items-center gap-3">
            <Swatch Icon={Pencil} />
            <div>
              <h2 className="text-[20px] font-semibold tracking-tight">Edit fact</h2>
              <p className="font-mono text-[11px] text-muted-foreground uppercase tracking-wider mt-0.5">
                {state.section} · {state.fact.source}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="w-9 h-9 rounded-lg flex items-center justify-center hover:bg-foreground/[0.04]">
            <X className="w-4 h-4" />
          </button>
        </div>
        <p className="mt-4 text-[12.5px] text-muted-foreground leading-relaxed">
          Saving marks this fact as <span className="font-mono">human-edited</span>. Future
          extractor proposals against the same field will be skipped — the
          operator becomes the source of truth until you change it again.
        </p>
        <textarea
          value={value}
          onChange={(e) => setValue(e.target.value)}
          autoFocus
          rows={5}
          className="mt-4 w-full rounded-lg border border-border bg-background px-3 py-2.5 text-[14px] leading-snug focus:outline-none focus:ring-2 focus:ring-foreground/20 resize-y"
        />
        {error && (
          <p className="mt-3 text-[12.5px] font-mono text-warning leading-relaxed">
            {error}
          </p>
        )}
        <div className="mt-5 pt-4 border-t border-border flex items-center justify-between">
          <span className="font-mono text-[11px] text-muted-foreground">
            {unchanged ? "no changes" : empty ? "value cannot be empty" : `${trimmed.length} chars`}
          </span>
          <div className="flex items-center gap-2">
            <SecondaryButton onClick={onClose} disabled={saving}>
              Cancel
            </SecondaryButton>
            <PrimaryButton onClick={handleSave} disabled={!canSave}>
              {saving ? (
                <>
                  <span className="dot bg-primary-foreground animate-pulse-soft" />
                  Saving…
                </>
              ) : (
                "Save edit"
              )}
            </PrimaryButton>
          </div>
        </div>
      </div>
    </div>
  );
};

type BriefingModalProps = {
  items: BriefingItem[];
  latencyMs: number;
  cached: boolean;
  onClose: () => void;
  onRegenerate: () => Promise<void>;
};

const BriefingModal = ({
  items,
  latencyMs,
  cached,
  onClose,
  onRegenerate,
}: BriefingModalProps) => {
  const bulletCount = items.filter((it) => it.type === "bullet").length;
  const [regenerating, setRegenerating] = useState(false);

  const handleRegenerate = async () => {
    if (regenerating) return;
    setRegenerating(true);
    try {
      await onRegenerate();
    } finally {
      setRegenerating(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 animate-fade-in">
      <div className="absolute inset-0 bg-foreground/30 backdrop-blur-[2px]" onClick={onClose} />
      <div className="relative bg-card border border-border rounded-2xl max-w-2xl w-full max-h-[85vh] overflow-y-auto p-8 shadow-card">
        <div className="flex items-start justify-between mb-2">
          <div className="flex items-center gap-3">
            <Swatch Icon={ClipboardList} />
            <h2 className="text-[22px] font-semibold tracking-tight">Onboarding briefing</h2>
          </div>
          <button onClick={onClose} className="w-9 h-9 rounded-lg flex items-center justify-center hover:bg-foreground/[0.04]">
            <X className="w-4 h-4" />
          </button>
        </div>
        <div className="font-mono text-[11px] text-muted-foreground uppercase tracking-wider mb-6">
          {cached ? "Cached" : "Generated by the LLM provider"}
          {" · "}
          {(latencyMs / 1000).toFixed(1)}s · {bulletCount} bullets
        </div>
        {regenerating ? (
          <div className="py-12 flex items-center justify-center gap-3 text-muted-foreground animate-fade-in">
            <span className="dot bg-foreground/60 animate-pulse-soft" />
            <span className="text-[14px]">Re-generating briefing...</span>
          </div>
        ) : (
          <div className="space-y-3 text-[15px] leading-relaxed animate-fade-in">
            {items.map((item, i) => {
              if (item.type === "heading") {
                return (
                  <h3 key={i} className="font-semibold text-[16px]">
                    {item.text}
                  </h3>
                );
              }
              if (item.type === "bullet") {
                if (item.hasGap) {
                  return (
                    <div key={i} className="border-l-2 border-warning pl-3 flex gap-3">
                      <span className="dot mt-2.5 bg-muted-foreground/50" />
                      <p className="flex-1 text-muted-foreground">{item.text}</p>
                    </div>
                  );
                }
                return (
                  <div key={i} className="flex gap-3">
                    <span className="dot mt-2.5 bg-foreground/60" />
                    <p className="flex-1">{item.text}</p>
                  </div>
                );
              }
              return (
                <p key={i} className="text-[15px] leading-relaxed">
                  {item.text}
                </p>
              );
            })}
          </div>
        )}
        <div className="mt-6 pt-5 border-t border-border flex justify-end">
          <SecondaryButton onClick={handleRegenerate} disabled={regenerating}>
            {regenerating ? (
              <>
                <span className="dot bg-foreground animate-pulse-soft" />
                Re-generating...
              </>
            ) : (
              <>
                <RefreshCw className="w-3.5 h-3.5" />
                Re-generate
              </>
            )}
          </SecondaryButton>
        </div>
      </div>
    </div>
  );
};

const BriefingBanner = ({
  onGenerate,
  generating,
}: {
  onGenerate: () => void;
  generating: boolean;
}) => (
  <Card className="relative overflow-hidden p-6">
    <div className="absolute inset-0 bg-gradient-to-r from-warning/[0.05] via-transparent to-success/[0.05] animate-pulse-subtle pointer-events-none" />
    <div className="relative flex flex-wrap items-center justify-between gap-6">
      <div className="flex items-start gap-4 min-w-0">
        <Swatch Icon={Lightbulb} className="mt-0.5" />
        <div className="min-w-0">
          <h2 className="text-[18px] font-semibold tracking-tight">
            60-second briefing for a new property manager
          </h2>
          <p className="text-[13px] text-muted-foreground mt-1 leading-relaxed">
            Reads the property file, surfaces what's urgent, what's stable, and what's missing —
            in the manager's language.
          </p>
        </div>
      </div>
      <PrimaryButton onClick={onGenerate} disabled={generating} className="px-5 py-2.5 shrink-0">
        {generating ? (
          <>
            <span className="dot bg-primary-foreground animate-pulse-soft" />
            Generating...
          </>
        ) : (
          "Generate briefing"
        )}
      </PrimaryButton>
    </div>
  </Card>
);

const PropertyDetail = () => {
  const { id } = useParams();
  const [briefingOpen, setBriefingOpen] = useState(false);
  const [briefingGenerating, setBriefingGenerating] = useState(false);
  const [markdownOpen, setMarkdownOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"file" | "ask">("file");
  const [openSource, setOpenSource] = useState<SourceModalSource | null>(null);
  const [editing, setEditing] = useState<EditFactState | null>(null);

  // Shares the cache with the portfolio listing, so it resolves instantly
  // when navigating in from there.
  const { data: livePropertiesRaw } = useProperties();
  const properties = livePropertiesRaw ?? [];

  // The structured property file. A failed request shows an error card; an
  // empty file shows the "no facts yet" state.
  const { data: liveFile, isError: fileError } = usePropertyFile(id);

  const propertyMeta = properties.find((p) => p.id === id);
  const heroProperty = properties.find((p) => p.isHero);

  // Live file only — no mock substitution. An empty live file (sections=[])
  // means extraction has not produced any facts yet for this property; we
  // surface that honestly rather than impersonating the hero's data.
  const liveFileHasContent = !!liveFile && liveFile.sections.length > 0;
  const file = liveFileHasContent ? liveFile : null;

  // Briefing and Ask are offered for any property with at least one event:
  // with sparse context the model answers "I don't know", which is more
  // useful than hiding the feature.
  const hasContent = (propertyMeta?.stats?.events ?? 0) > 0;

  // SSE: subscribe to /properties/{id}/events. The worker publishes a
  // ``fact_update`` payload after each event is processed; invalidating the
  // queries re-fetches the file in place, so new facts appear without a
  // reload.
  const queryClient = useQueryClient();
  const [liveStatus, setLiveStatus] = useState<"connecting" | "live" | "offline">(
    "connecting",
  );
  const [pulse, setPulse] = useState(0);
  const lastEventRef = useRef<string | null>(null);

  useEffect(() => {
    if (!id) return;
    const url = `${API_BASE_URL}/properties/${id}/events`;
    const es = new EventSource(url);
    es.addEventListener("hello", () => setLiveStatus("live"));
    es.addEventListener("fact_update", (raw: MessageEvent) => {
      try {
        const payload = JSON.parse(raw.data) as {
          event_id?: string;
          facts_written?: number;
          summary?: string;
        };
        if (payload.event_id) lastEventRef.current = payload.event_id;
        // Re-fetch the structured file + activity, plus the portfolio
        // listing so card stats also update without a full refresh.
        queryClient.invalidateQueries({ queryKey: queryKeys.propertyFile(id) });
        queryClient.invalidateQueries({ queryKey: queryKeys.properties });
        queryClient.invalidateQueries({ queryKey: queryKeys.portfolioSummary });
        queryClient.invalidateQueries({ queryKey: queryKeys.portfolioActivity });
        setPulse((n) => n + 1);
      } catch {
        // Malformed payload — ignore. EventSource auto-reconnects.
      }
    });
    es.onerror = () => setLiveStatus("offline");
    return () => {
      es.close();
    };
  }, [id, queryClient]);

  const [briefingItems, setBriefingItems] = useState<BriefingItem[]>([]);
  const [briefingLatencyMs, setBriefingLatencyMs] = useState(0);
  const [briefingCached, setBriefingCached] = useState(false);
  const [briefingError, setBriefingError] = useState<string | null>(null);

  const runBriefingFetch = async (): Promise<boolean> => {
    if (!id) return false;
    const started = performance.now();
    setBriefingError(null);
    try {
      const data = await fetchOnboarding(id);
      setBriefingItems(data.items);
      setBriefingLatencyMs(performance.now() - started);
      setBriefingCached(data.cached);
      return true;
    } catch (e) {
      setBriefingItems([]);
      setBriefingLatencyMs(0);
      setBriefingCached(false);
      setBriefingError(
        e instanceof Error ? e.message : "briefing service unreachable",
      );
      return false;
    }
  };

  const startBriefingGeneration = async () => {
    if (briefingGenerating || briefingOpen) return;
    setBriefingGenerating(true);
    try {
      const ok = await runBriefingFetch();
      if (ok) setBriefingOpen(true);
    } finally {
      setBriefingGenerating(false);
    }
  };

  if (!propertyMeta) {
    const loading = livePropertiesRaw === undefined;
    return (
      <div className="max-w-[1400px] mx-auto px-6 lg:px-10 py-12 animate-fade-in">
        <Link to="/" className="inline-flex items-center gap-1 text-[13px] text-muted-foreground hover:text-foreground transition-colors">
          <ChevronLeft className="w-4 h-4" /> Portfolio
        </Link>
        <Card className="mt-8 p-10 text-center">
          {loading ? (
            <p className="text-muted-foreground text-[14px] font-mono">
              loading property…
            </p>
          ) : (
            <>
              <h2 className="text-[20px] font-semibold">Property not found</h2>
              <p className="text-[14px] text-muted-foreground mt-2">
                {id
                  ? `No property with id ${id} in the backend.`
                  : "No property id in the URL."}
              </p>
            </>
          )}
        </Card>
      </div>
    );
  }

  return (
    <div className="max-w-[1400px] mx-auto px-6 lg:px-10 py-10 animate-fade-in">
      <Link to="/" className="inline-flex items-center gap-1 text-[13px] text-muted-foreground hover:text-foreground transition-colors">
        <ChevronLeft className="w-4 h-4" /> Portfolio
      </Link>

      <div className="mt-6 flex items-start gap-4">
        <Swatch Icon={Building2} className="!w-10 !h-10" />
        <div>
          <h1 className="display text-[32px] tracking-tight">{propertyMeta.name}</h1>
          <p className="text-[15px] text-muted-foreground mt-1">{propertyMeta.address}</p>
          <div className="mt-4 flex flex-wrap items-center gap-2">
            <StatusPill variant="neutral">{propertyMeta.stats.events} events</StatusPill>
            <StatusPill variant="neutral">{propertyMeta.stats.facts} facts</StatusPill>
            <span
              key={`pulse-${pulse}`}
              className={`pill text-[12px] flex items-center gap-1.5 transition-colors duration-500 ${
                pulse > 0 ? "bg-success/15 border-success/40" : "bg-card"
              }`}
              title={
                liveStatus === "live"
                  ? "Subscribed to live updates"
                  : liveStatus === "offline"
                    ? "SSE connection lost — retrying"
                    : "Connecting…"
              }
            >
              <span
                className={`dot ${
                  liveStatus === "live"
                    ? "bg-success animate-pulse-soft"
                    : liveStatus === "offline"
                      ? "bg-warning"
                      : "bg-muted-foreground/50"
                }`}
              />
              <span className="font-mono">
                {liveStatus === "live"
                  ? pulse > 0
                    ? `+${pulse} live update${pulse === 1 ? "" : "s"}`
                    : "live"
                  : liveStatus}
              </span>
            </span>
            <button
              onClick={() => setMarkdownOpen(true)}
              className="pill text-[12px] flex items-center gap-1.5 hover:border-foreground/30 transition-colors bg-card"
              title="Preview, download or edit the markdown file for this property"
            >
              <FileText className="w-3.5 h-3.5" />
              <span>Property file</span>
            </button>
          </div>
        </div>
      </div>

      {hasContent && (
        <div className="mt-8">
          <BriefingBanner onGenerate={startBriefingGeneration} generating={briefingGenerating} />
          {briefingError && (
            <p className="mt-2 text-[12px] font-mono text-warning">
              briefing failed: {briefingError}
            </p>
          )}
        </div>
      )}

      {/* Mobile tabs */}
      <div className="mt-8 lg:hidden flex gap-1 border-b border-border">
        {(["file", "ask"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setActiveTab(t)}
            className={`px-4 py-2 text-[14px] border-b-2 -mb-px transition-colors ${
              activeTab === t ? "border-foreground text-foreground" : "border-transparent text-muted-foreground"
            }`}
          >
            {t === "file" ? "Property file" : "Ask"}
          </button>
        ))}
      </div>

      <div className="mt-10 grid grid-cols-1 lg:grid-cols-5 gap-10">
        {/* Property file — 60% */}
        <div className={`lg:col-span-3 ${activeTab === "ask" ? "hidden lg:block" : ""}`}>
          {fileError ? (
            <Card className="p-8">
              <h2 className="text-[22px] font-semibold tracking-tight">
                Could not load the property file
              </h2>
              <p className="text-[15px] text-muted-foreground mt-3 leading-relaxed">
                The request to the API failed. Check that the backend is running and reload the
                page.
              </p>
            </Card>
          ) : file ? (
            <div className="space-y-10">
              {file.sections.map((s) => {
                const isContext = s.isContext;
                const isUncertain = s.isUncertain;
                return (
                  <section
                    key={s.title}
                    data-section-kind={isUncertain ? "uncertain" : isContext ? "context" : "facts"}
                    className={isUncertain ? "bg-subdued rounded-2xl p-6 border border-border" : ""}
                  >
                    {isContext && <hr className="border-border mb-6" />}
                    <div className="flex items-center gap-3">
                      {isUncertain && <ClipboardList className="w-4 h-4 text-muted-foreground" />}
                      <h2 className={`font-semibold tracking-tight ${isContext ? "text-[18px] text-muted-foreground" : "text-[22px]"}`}>
                        {s.title}
                      </h2>
                      {s.facts.length > 0 && (
                        <span className="font-mono text-[11.5px] text-muted-foreground px-2 py-0.5 rounded-full border border-border bg-card">
                          {s.facts.length}
                        </span>
                      )}
                    </div>
                    <div className="mt-4 space-y-1">
                      {s.facts.map((f, i) => (
                        <FactRow
                          key={f.factId ?? i}
                          fact={f}
                          onOpenSource={setOpenSource}
                          onEdit={
                            s.isUncertain
                              ? undefined
                              : (fact) => setEditing({ fact, section: s.title })
                          }
                        />
                      ))}
                    </div>
                  </section>
                );
              })}
            </div>
          ) : (
            <Card className="p-8 space-y-6">
              <div>
                <h2 className="text-[22px] font-semibold tracking-tight">No facts yet</h2>
                <p className="text-[15px] text-muted-foreground mt-3 leading-relaxed">
                  Keystone has {propertyMeta.stats.events} events for this property but has not
                  written any facts from them. Facts appear here as the worker extracts them; the
                  page updates on its own when it does.
                </p>
              </div>
              {heroProperty && heroProperty.id !== propertyMeta.id && (
                <div className="text-[13px] text-muted-foreground border-t border-border pt-5">
                  <Link
                    to={`/properties/${heroProperty.id}`}
                    className="underline underline-offset-2 hover:text-foreground"
                  >
                    Open the featured property: {heroProperty.name}
                  </Link>
                </div>
              )}
            </Card>
          )}
        </div>

        {/* Q&A — 40%, sticky */}
        <div className={`lg:col-span-2 ${activeTab === "file" ? "hidden lg:block" : ""}`}>
          <div className="lg:sticky lg:top-24">
            <Card className="p-6">
              <AskPanel unavailable={!hasContent} propertyId={id} />
            </Card>
          </div>
        </div>
      </div>

      {briefingOpen && (
        <BriefingModal
          items={briefingItems}
          latencyMs={briefingLatencyMs}
          cached={briefingCached}
          onClose={() => setBriefingOpen(false)}
          onRegenerate={async () => { await runBriefingFetch(); }}
        />
      )}
      {id && (
        <MarkdownModal
          propertyId={id}
          propertyName={propertyMeta.name}
          open={markdownOpen}
          onClose={() => setMarkdownOpen(false)}
        />
      )}
      <SourceModal source={openSource} onOpenChange={(open) => !open && setOpenSource(null)} />
      {editing && id && (
        <EditFactModal
          state={editing}
          propertyId={id}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null);
            queryClient.invalidateQueries({ queryKey: queryKeys.propertyFile(id) });
            queryClient.invalidateQueries({ queryKey: queryKeys.properties });
            queryClient.invalidateQueries({ queryKey: queryKeys.propertyMarkdown(id) });
          }}
        />
      )}
    </div>
  );
};

export default PropertyDetail;
