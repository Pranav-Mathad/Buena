import { useState } from "react";
import { useFeaturedProperty, useProperties } from "@/hooks/queries";
import { fetchDraftReply, type DraftReplyContext } from "@/lib/api";
import { sampleInboundMessages, type SampleInboundMessage } from "@/lib/constants";
import { Card, PrimaryButton, SecondaryButton } from "@/components/ui-bits";
import { Copy, Pencil } from "lucide-react";

const GENERIC_BODY = `Sehr geehrte/r Bewohner/in,

vielen Dank für Ihre Nachricht. Wir prüfen das Anliegen und melden uns zeitnah zurück.

Mit freundlichen Grüßen,
Hausverwaltung`;

type DraftedReply = {
  subject: string;
  body: string;
  context: DraftReplyContext[];
  knows_about_incident: boolean;
  elapsed_ms: number;
};

const DraftReply = () => {
  const [example, setExample] = useState<SampleInboundMessage | null>(null);
  const [form, setForm] = useState({
    property: "",
    channel: "Email" as "Email" | "Slack",
    tone: "Neutral",
    subject: "",
    body: "",
  });
  const [drafted, setDrafted] = useState<DraftedReply | null>(null);
  const [drafting, setDrafting] = useState(false);
  const [draftError, setDraftError] = useState<string | null>(null);

  // The property list fills the selector and maps the chosen name back to
  // its id for the API call. Until the user picks one, the featured
  // property is selected.
  const { data: properties } = useProperties();
  const featured = useFeaturedProperty();
  const selectedProperty = form.property || featured?.name || "";

  const loadExample = (ex: SampleInboundMessage) => {
    setExample(ex);
    setForm({
      property: selectedProperty,
      channel: ex.channel,
      tone: ex.tone,
      subject: ex.subject,
      body: ex.body,
    });
    setDrafted(null);
    setDraftError(null);
  };

  const generate = async () => {
    if (drafting) return;
    setDrafting(true);
    setDraftError(null);
    try {
      const matched = properties?.find((p) => p.name === selectedProperty);
      const result = await fetchDraftReply({
        propertyId: matched?.id,
        propertyName: selectedProperty,
        channel: form.channel,
        tone: form.tone,
        subject: form.subject,
        body: form.body,
      });
      setDrafted({
        subject: result.subject,
        body: result.body,
        context: result.context.map((c) => ({
          id: c.id,
          channel: c.channel,
          time: c.time,
          snippet: c.snippet,
          whyRelevant: c.whyRelevant,
        })),
        knows_about_incident: result.knowsAboutIncident,
        elapsed_ms: result.elapsedMs,
      });
    } catch (e) {
      // No canned fallback: a failed request is shown as a failure, never
      // replaced by a reply the model did not write.
      setDrafted(null);
      setDraftError(
        e instanceof Error
          ? `Drafting service unreachable — ${e.message}`
          : "Drafting service unreachable.",
      );
    } finally {
      setDrafting(false);
    }
  };

  const showSplit = !!drafted && drafted.knows_about_incident;

  return (
    <div className="max-w-[1400px] mx-auto px-6 lg:px-10 py-12 animate-fade-in">
      <div className="mb-10 max-w-3xl">
        <h1 className="display text-[40px] tracking-tight">Draft reply</h1>
        <p className="text-muted-foreground mt-3 text-[15px] leading-relaxed">
          A drafted answer that already knows what happened.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Incoming */}
        <Card className="p-6">
          <h2 className="text-[22px] font-semibold tracking-tight">Incoming message</h2>
          <div className="mt-5 flex flex-wrap gap-2">
            <span className="text-[13px] text-muted-foreground mr-1 self-center">load example:</span>
            {sampleInboundMessages.map((ex) => (
              <button
                key={ex.key}
                onClick={() => loadExample(ex)}
                className={`pill text-[12px] hover:border-foreground/30 transition-colors ${
                  example?.key === ex.key ? "border-foreground/40 bg-foreground/[0.03]" : "bg-card"
                }`}
              >
                {ex.label}
              </button>
            ))}
          </div>

          <div className="mt-6 space-y-4">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <Field label="Property">
                <select
                  value={selectedProperty}
                  onChange={(e) => setForm({ ...form, property: e.target.value })}
                  className="w-full bg-card border border-border rounded-lg px-3 py-2 text-[14px] outline-none focus:border-foreground/40"
                >
                  {(properties ?? []).map((p) => (
                    <option key={p.id} value={p.name}>
                      {p.name}
                    </option>
                  ))}
                </select>
              </Field>
              <Field label="Channel">
                <select
                  value={form.channel}
                  onChange={(e) => setForm({ ...form, channel: e.target.value as "Email" | "Slack" })}
                  className="w-full bg-card border border-border rounded-lg px-3 py-2 text-[14px] outline-none focus:border-foreground/40"
                >
                  <option>Email</option>
                  <option>Slack</option>
                </select>
              </Field>
              <Field label="Tone">
                <select
                  value={form.tone}
                  onChange={(e) => setForm({ ...form, tone: e.target.value })}
                  className="w-full bg-card border border-border rounded-lg px-3 py-2 text-[14px] outline-none focus:border-foreground/40"
                >
                  <option>Warm</option>
                  <option>Neutral</option>
                  <option>Formal</option>
                </select>
              </Field>
            </div>
            {form.channel === "Email" && (
              <Field label="Subject">
                <input
                  value={form.subject}
                  onChange={(e) => setForm({ ...form, subject: e.target.value })}
                  className="w-full bg-card border border-border rounded-lg px-3 py-2 text-[14px] outline-none focus:border-foreground/40"
                />
              </Field>
            )}
            <Field label="Body">
              <textarea
                rows={10}
                value={form.body}
                onChange={(e) => setForm({ ...form, body: e.target.value })}
                className="w-full bg-card border border-border rounded-lg px-3 py-3 text-[14px] outline-none focus:border-foreground/40 resize-none leading-relaxed"
              />
            </Field>
            <div className="flex justify-end items-center gap-3">
              {draftError && (
                <span className="text-[12px] font-mono text-warning">
                  {draftError}
                </span>
              )}
              <PrimaryButton
                onClick={generate}
                disabled={!form.body.trim() || drafting}
              >
                {drafting ? (
                  <>
                    <span className="dot bg-primary-foreground animate-pulse-soft" />
                    Drafting...
                  </>
                ) : (
                  "Draft reply"
                )}
              </PrimaryButton>
            </div>
          </div>
        </Card>

        {/* Drafted */}
        <div>
          {!drafted && (
            <Card className="p-6">
              <h2 className="text-[22px] font-semibold tracking-tight">Drafted reply</h2>
              <p className="mt-6 text-muted-foreground italic text-[14px]">
                Submit a message on the left to see the drafted reply.
              </p>
            </Card>
          )}

          {drafted && !showSplit && (
            <Card className="p-6">
              <h2 className="text-[22px] font-semibold tracking-tight">Drafted reply</h2>
              <SingleDraftedContent drafted={drafted} />
            </Card>
          )}

          {drafted && showSplit && (
            <div className="animate-fade-in">
              <h3 className="text-[18px] font-semibold tracking-tight mb-6">
                What changes when the system has context?
              </h3>
              <div className="relative grid grid-cols-1 md:grid-cols-2 gap-10 md:gap-12">
                {/* LEFT — Without Keystone */}
                <div>
                  <div className="text-[12px] uppercase tracking-wider text-muted-foreground font-medium">
                    Without Keystone — generic reply
                  </div>
                  <div className="mt-5 space-y-4 font-sans text-foreground/80">
                    <div>
                      <div className="text-[12px] text-muted-foreground mb-1">Subject</div>
                      <p className="text-[14px]">Re: {form.subject || "(no subject)"}</p>
                    </div>
                    <div>
                      <div className="text-[12px] text-muted-foreground mb-1">Body</div>
                      <p className="text-[14px] leading-relaxed whitespace-pre-line">
                        {GENERIC_BODY}
                      </p>
                    </div>
                    <div className="font-mono text-[11px] text-muted-foreground pt-2">
                      no context · drafted in 0.3s
                    </div>
                  </div>
                </div>

                {/* RIGHT — With Keystone */}
                <div>
                  <div className="text-[12px] uppercase tracking-wider text-muted-foreground font-medium">
                    With Keystone — context-aware reply
                  </div>
                  <div className="mt-5">
                    <SingleDraftedContent
                      drafted={drafted}
                      footerOverride={`${drafted.context.length} related event${
                        drafted.context.length === 1 ? "" : "s"
                      } · drafted in ${(drafted.elapsed_ms / 1000).toFixed(1)}s`}
                    />
                  </div>
                </div>

                {/* Centered divider with label */}
                <div className="hidden md:flex absolute inset-y-0 left-1/2 -translate-x-1/2 items-center justify-center pointer-events-none">
                  <div className="absolute inset-y-0 w-px bg-border" />
                  <div className="relative bg-background px-2 py-1 text-[10px] font-mono uppercase tracking-wider text-muted-foreground whitespace-nowrap">
                    Same input. Different memory.
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const SingleDraftedContent = ({
  drafted,
  footerOverride,
}: {
  drafted: DraftedReply;
  footerOverride?: string;
}) => (
  <div className="space-y-5 animate-fade-in">
    <div className="space-y-2">
      <div className="rounded-2xl border border-border bg-card px-4 py-3 flex items-start gap-3 animate-fade-in-up">
        <span
          className={`dot mt-1.5 shrink-0 ${
            drafted.knows_about_incident ? "bg-success" : "bg-muted-foreground/50"
          }`}
        />
        <div>
          <p className="text-[15px] font-medium">
            {drafted.knows_about_incident
              ? "Keystone already knows about this incident"
              : "First report — drafting a holding reply"}
          </p>
          {drafted.knows_about_incident && drafted.context.length > 0 && (
            <p className="text-[13px] text-muted-foreground mt-0.5">
              {drafted.context.length} related event
              {drafted.context.length === 1 ? "" : "s"} found in the last 24h
            </p>
          )}
        </div>
      </div>
      <p className="text-[13px] italic text-muted-foreground">
        {drafted.knows_about_incident
          ? "Drafting a reply that references prior context"
          : "No prior context found for this issue"}
      </p>
    </div>
    {drafted.subject && (
      <div>
        <div className="text-[13px] text-muted-foreground mb-1">Subject</div>
        <p className="text-[15px] font-medium">{drafted.subject}</p>
      </div>
    )}
    <div>
      <div className="text-[13px] text-muted-foreground mb-1">Body</div>
      <p className="text-[15px] leading-relaxed whitespace-pre-line">{drafted.body}</p>
    </div>
    {drafted.context.length > 0 && (
      <>
        <hr className="border-border" />
        <div>
          <div className="text-[13px] text-muted-foreground mb-3">Context used</div>
          <div className="space-y-3">
            {drafted.context.map((c) => (
              <div key={c.id} className="border border-border rounded-xl p-4">
                <div className="font-mono text-[12px] text-muted-foreground">
                  {c.id} · {c.channel} · {c.time}
                </div>
                <p className="text-[14px] mt-2">{c.snippet}</p>
                <p className="text-[12px] italic text-muted-foreground mt-2">{c.whyRelevant}</p>
              </div>
            ))}
          </div>
        </div>
      </>
    )}
    <div className="font-mono text-[12px] text-muted-foreground">
      {footerOverride ?? `Drafted in ${(drafted.elapsed_ms / 1000).toFixed(1)}s`}
    </div>
    <div className="flex gap-2">
      <SecondaryButton><Copy className="w-3.5 h-3.5" /> Copy</SecondaryButton>
      <SecondaryButton><Pencil className="w-3.5 h-3.5" /> Edit</SecondaryButton>
    </div>
  </div>
);

const Field = ({ label, children }: { label: string; children: React.ReactNode }) => (
  <label className="block">
    <span className="text-[12px] text-muted-foreground uppercase tracking-wider font-medium">{label}</span>
    <div className="mt-1.5">{children}</div>
  </label>
);

export default DraftReply;
