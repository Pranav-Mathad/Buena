import { useEffect, useState } from "react";
import { useFeaturedProperty } from "@/hooks/queries";
import { fetchAsk, llmFailureKind } from "@/lib/api";
import { adaptAskResult, type AskResponse } from "@/lib/ask";
import { suggestedQuestions } from "@/lib/constants";
import { Card, PrimaryButton } from "./ui-bits";
import { AlertTriangle, ExternalLink, HelpCircle } from "lucide-react";
import { SourceModal, type SourceModalSource } from "./SourceModal";

/** Heading for a failed Ask request; the error message below it has the detail. */
function describeError(e: unknown): string {
  const kind = llmFailureKind(e);
  if (kind === "no-key") return "Ask needs a language model";
  return kind === "failed" ? "The question could not be answered" : "Ask is unavailable";
}

const SourceCard = ({
  s,
  label,
  onOpenSource,
}: {
  s: { channel: string; date: string; snippet: string };
  label?: string;
  onOpenSource: (s: SourceModalSource) => void;
}) => (
  <div className="border border-border rounded-xl p-4 bg-card shadow-card">
    <div className="font-mono text-[12px] text-muted-foreground">
      {s.channel} · {s.date}
    </div>
    {label && <div className="font-mono text-[11px] text-muted-foreground mt-1">{label}</div>}
    <p className="text-[14px] mt-2 leading-relaxed">{s.snippet}</p>
    <div className="mt-3 flex items-center justify-end">
      <button
        type="button"
        onClick={() => onOpenSource({ channel: s.channel, date: s.date, snippet: s.snippet })}
        className="text-[12px] inline-flex items-center gap-1 hover:underline cursor-pointer"
      >
        open source <ExternalLink className="w-3 h-3" />
      </button>
    </div>
  </div>
);

export const AskPanel = ({
  presetQuestion,
  compact,
  unavailable,
  propertyId,
}: {
  presetQuestion?: string;
  compact?: boolean;
  unavailable?: boolean;
  // When omitted, the panel asks about the property the API lists first.
  propertyId?: string;
}) => {
  const [input, setInput] = useState(presetQuestion ?? "");
  const [submitted, setSubmitted] = useState<string | null>(presetQuestion ?? null);
  const [openSource, setOpenSource] = useState<SourceModalSource | null>(null);
  const [response, setResponse] = useState<AskResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [errorTitle, setErrorTitle] = useState("");

  const heroProperty = useFeaturedProperty();

  const targetPropertyId = propertyId ?? heroProperty?.id;

  // Sync when the preset changes (search page).
  useEffect(() => {
    if (presetQuestion !== undefined) {
      setInput(presetQuestion);
      setSubmitted(presetQuestion);
    }
  }, [presetQuestion]);

  // Fetch on submit. A failed request is shown as a failure, never
  // replaced by a canned answer.
  useEffect(() => {
    if (!submitted || unavailable || !targetPropertyId) {
      setResponse(null);
      setErrorMsg(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setErrorMsg(null);
    fetchAsk(targetPropertyId, submitted)
      .then((r) => {
        if (cancelled) return;
        setResponse(adaptAskResult(r));
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setResponse(null);
        setErrorTitle(describeError(e));
        setErrorMsg(e instanceof Error ? e.message : "Ask is unavailable");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [submitted, targetPropertyId, unavailable]);

  const submit = () => {
    if (!input.trim() || loading) return;
    setSubmitted(input.trim());
  };

  return (
    <div className={compact ? "" : "space-y-6"}>
      {!compact && (
        <>
          <div>
            <h2 className="text-[22px] font-semibold tracking-tight">Ask the file</h2>
            <p className="text-[13px] text-muted-foreground mt-2 leading-relaxed">
              Answers come from this property's events and facts, each with its source. When the
              file does not cover a question, the answer says so.
            </p>
          </div>

          <div className="space-y-3">
            <div className="relative">
              <input
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && submit()}
                disabled={loading}
                placeholder="Ask a question about this property"
                className="field h-12 pl-4 pr-24 text-[15px]"
              />
              <PrimaryButton
                onClick={submit}
                disabled={!input.trim() || loading}
                className="absolute right-1.5 top-1/2 -translate-y-1/2 h-9 px-4 py-0 text-[13px]"
              >
                {loading ? "Answering…" : "Ask"}
              </PrimaryButton>
            </div>
            <div className="flex flex-wrap gap-2">
              {suggestedQuestions.map((q) => (
                <button
                  key={q}
                  disabled={loading}
                  onClick={() => {
                    if (loading) return;
                    setInput(q);
                    setSubmitted(q);
                  }}
                  className="pill bg-card font-normal hover:border-foreground/30 text-[12px] transition-colors text-left disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:border-border"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        </>
      )}

      {loading && submitted && !unavailable && (
        <div className="animate-fade-in flex items-center gap-3 text-muted-foreground">
          <span className="dot bg-foreground/60 animate-pulse-soft" />
          <span className="text-[14px]">Answering…</span>
        </div>
      )}

      {errorMsg && (
        <Card className="p-5 border-warning/40">
          <div className="flex items-center gap-2 text-warning">
            <AlertTriangle className="w-4 h-4" />
            <span className="text-[14px] font-medium">{errorTitle}</span>
          </div>
          <p className="text-[12px] font-mono text-muted-foreground mt-2">{errorMsg}</p>
        </Card>
      )}

      {response && !loading && (
        <div className="animate-fade-in space-y-4">
          {response.status === "answered" && (
            <>
              <div className="flex items-center justify-between">
                <span className="pill bg-card">
                  <span className="dot bg-success" />
                  answered
                </span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  confidence: {response.confidence}
                </span>
              </div>
              <p className="text-[16px] leading-relaxed">{response.answer}</p>
              <div>
                <div className="text-[13px] text-muted-foreground mb-3">Sources</div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {response.sources.map((s) => (
                    <SourceCard key={s.id} s={s} onOpenSource={setOpenSource} />
                  ))}
                </div>
              </div>
              <div className="font-mono text-[12px] text-muted-foreground">
                Answered in {(response.elapsed_ms / 1000).toFixed(1)}s · retrieved{" "}
                {response.retrieved_count} events
              </div>
            </>
          )}

          {response.status === "insufficient_context" && (
            <>
              <div className="flex items-center justify-between">
                <span className="pill bg-card">
                  <HelpCircle className="w-3.5 h-3.5 text-muted-foreground" strokeWidth={1.75} />
                  Not enough information
                </span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  confidence: {response.confidence}
                </span>
              </div>
              <div className="bg-subdued border border-border rounded-2xl p-6 space-y-6">
                <p className="text-[15px]">
                  The property file does not hold enough information to answer this.
                </p>
                <p className="text-[13px] text-muted-foreground">
                  <em>
                    <strong>{response.reasoning}</strong>
                  </em>
                </p>
              </div>
              {response.partial_context.length > 0 && (
                <>
                  <hr className="border-border" />
                  <div>
                    <div className="text-[14px] font-semibold text-foreground mb-3">
                      Related context
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      {response.partial_context.map((s) => (
                        <SourceCard
                          key={s.id}
                          s={s}
                          label="related, not an answer"
                          onOpenSource={setOpenSource}
                        />
                      ))}
                    </div>
                  </div>
                </>
              )}
              <div className="font-mono text-[12px] text-muted-foreground">
                Answered in {(response.elapsed_ms / 1000).toFixed(1)}s · retrieved{" "}
                {response.retrieved_count} events
              </div>
            </>
          )}

          {response.status === "out_of_scope" && (
            <>
              <div className="flex items-center justify-between">
                <span className="pill bg-card">
                  <span className="dot bg-muted-foreground/40" />
                  out of scope
                </span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  confidence: {response.confidence}
                </span>
              </div>
              <Card className="p-5">
                <p className="text-[15px]">
                  This information is not kept in the property file. Check the ERP or the lease and
                  owner records.
                </p>
                <p className="text-[13px] italic text-muted-foreground mt-2">
                  {response.reasoning}
                </p>
              </Card>
              <div className="font-mono text-[12px] text-muted-foreground">
                Answered in {(response.elapsed_ms / 1000).toFixed(1)}s
              </div>
            </>
          )}
        </div>
      )}
      {submitted && unavailable && (
        <div className="animate-fade-in bg-subdued border border-border rounded-2xl p-6 space-y-3">
          <p className="text-[15px]">Ask is available once this property has events.</p>
        </div>
      )}
      <SourceModal source={openSource} onOpenChange={(open) => !open && setOpenSource(null)} />
    </div>
  );
};
