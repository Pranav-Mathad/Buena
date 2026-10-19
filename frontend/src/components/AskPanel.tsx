import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useFeaturedProperty } from "@/hooks/queries";
import { ApiError, fetchAsk } from "@/lib/api";
import { adaptAskResult, type AskResponse } from "@/lib/ask";
import { suggestedQuestions } from "@/lib/constants";
import { Card, PrimaryButton } from "./ui-bits";
import { AlertTriangle, ExternalLink, HelpCircle } from "lucide-react";
import { SourceModal, type SourceModalSource } from "./SourceModal";

/** Heading for a failed Ask request; the error message below it has the detail. */
function describeError(e: unknown): string {
  // The API answers 503 when no LLM provider is configured.
  if (e instanceof ApiError) {
    return e.status === 503 ? "Ask needs an LLM provider key" : "Q&A request failed";
  }
  return "Q&A service unreachable";
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
  <div className="border border-border rounded-xl p-4 bg-card">
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
  // When omitted, the panel asks about the featured property (Compare
  // asks its questions without choosing a property).
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

  // Sync when the preset changes (Compare view).
  useEffect(() => {
    if (presetQuestion !== undefined) {
      setInput(presetQuestion);
      setSubmitted(presetQuestion);
    }
  }, [presetQuestion]);

  // Fetch on submit. No mock fallback: if the backend errors we
  // surface the failure honestly so the user knows the answer they
  // see is real or not at all.
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
        setErrorMsg(e instanceof Error ? e.message : "Q&A service unreachable");
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
            <h2 className="text-[22px] font-semibold tracking-tight">Ask anything</h2>
            <p className="text-[13px] text-muted-foreground mt-2 leading-relaxed">
              We answer from this property's events, facts, and parent-tier context. We say "I don't
              know" when we don't have enough.
            </p>
          </div>

          <div className="space-y-3">
            <input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && submit()}
              disabled={loading}
              placeholder="ask anything about this property"
              className="w-full h-14 rounded-xl border border-border bg-card px-4 text-[16px] outline-none focus:border-foreground/40 transition-colors placeholder:text-muted-foreground disabled:opacity-60 disabled:cursor-not-allowed"
            />
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
                  className="pill bg-card hover:border-foreground/30 text-[12px] transition-colors text-left disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:border-border"
                >
                  {q}
                </button>
              ))}
            </div>
            <div className="flex justify-end">
              <PrimaryButton
                onClick={submit}
                disabled={!input.trim() || loading}
                className="px-6 py-2.5"
              >
                {loading ? "Thinking…" : "Ask"}
              </PrimaryButton>
            </div>
          </div>
        </>
      )}

      {loading && submitted && !unavailable && (
        <div className="animate-fade-in flex items-center gap-3 text-muted-foreground">
          <span className="dot bg-foreground/60 animate-pulse-soft" />
          <span className="text-[14px]">Thinking…</span>
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
                  <HelpCircle className="w-3.5 h-3.5 text-muted-foreground" strokeWidth={1.75} />I
                  don't know
                </span>
                <span className="font-mono text-[12px] text-muted-foreground">
                  confidence: {response.confidence}
                </span>
              </div>
              <div className="bg-subdued border border-border rounded-2xl p-6 space-y-6">
                <p className="text-[15px]">
                  I don't have enough information to answer that confidently.
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
                      Here's what I do know about this topic
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      {response.partial_context.map((s) => (
                        <SourceCard
                          key={s.id}
                          s={s}
                          label="partial context, not an answer"
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
                  That information lives in the ERP, not in the context layer. Try{" "}
                  <a className="underline">[link to ERP]</a> or check the lease/owner records
                  directly.
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
          <p className="text-[15px]">Q&amp;A is available once the property file is populated.</p>
          {heroProperty && (
            <p className="text-[13px] text-muted-foreground">
              Try{" "}
              <Link
                to={`/properties/${heroProperty.id}`}
                className="underline underline-offset-2 hover:text-foreground"
              >
                {heroProperty.name}
              </Link>{" "}
              in the meantime.
            </p>
          )}
        </div>
      )}
      <SourceModal source={openSource} onOpenChange={(open) => !open && setOpenSource(null)} />
    </div>
  );
};
