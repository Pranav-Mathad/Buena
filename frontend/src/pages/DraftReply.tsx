import { useEffect, useState } from "react";
import { toast } from "sonner";
import { Check, Copy, Mail, Pencil } from "lucide-react";
import { useFeaturedProperty, useProperties, usePropertyInbox } from "@/hooks/queries";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  fetchDraftReply,
  fetchEventRaw,
  llmFailureKind,
  type DraftReplyContext,
  type InboxRow,
} from "@/lib/api";
import { parseRawMessage } from "@/lib/message";
import { cn } from "@/lib/utils";
import { Card, Eyebrow, PrimaryButton, SecondaryButton, Select } from "@/components/ui-bits";

type DraftedReply = {
  subject: string;
  body: string;
  context: DraftReplyContext[];
  knowsAboutIncident: boolean;
  elapsedMs: number;
};

const DraftReply = () => {
  useDocumentTitle("Draft a reply");
  const { data: properties } = useProperties();
  const featured = useFeaturedProperty();
  const [propertyId, setPropertyId] = useState("");
  const selectedId = propertyId || featured?.id || properties?.[0]?.id || "";
  const selected = properties?.find((p) => p.id === selectedId);

  const [form, setForm] = useState({
    channel: "Email" as "Email" | "Slack",
    tone: "Neutral",
    subject: "",
    body: "",
  });
  const [loadingMessage, setLoadingMessage] = useState<string | null>(null);
  const [loadedMessage, setLoadedMessage] = useState<string | null>(null);
  const [drafted, setDrafted] = useState<DraftedReply | null>(null);
  const [drafting, setDrafting] = useState(false);
  const [draftError, setDraftError] = useState<string | null>(null);

  // The property's most recent messages, so a reply can start from one of them.
  const inbox = usePropertyInbox(selectedId || undefined, undefined);
  const recent = (inbox.data?.rows ?? []).slice(0, 5);

  const loadMessage = async (row: InboxRow) => {
    if (loadingMessage) return;
    setLoadingMessage(row.eventId);
    try {
      const parsed = parseRawMessage(await fetchEventRaw(row.eventId));
      setForm((f) => ({
        ...f,
        channel: "Email",
        subject: parsed.subject || row.subject,
        body: parsed.body,
      }));
      setLoadedMessage(row.eventId);
      setDrafted(null);
      setDraftError(null);
    } catch {
      toast.error("The message could not be loaded");
    } finally {
      setLoadingMessage(null);
    }
  };

  const generate = async () => {
    if (drafting) return;
    setDrafting(true);
    setDraftError(null);
    try {
      const result = await fetchDraftReply({
        propertyId: selected?.id,
        propertyName: selected?.name,
        channel: form.channel,
        tone: form.tone,
        subject: form.subject,
        body: form.body,
      });
      setDrafted({
        subject: result.subject,
        body: result.body,
        context: result.context,
        knowsAboutIncident: result.knowsAboutIncident,
        elapsedMs: result.elapsedMs,
      });
    } catch (e) {
      // A failed request is shown as a failure, never replaced by a reply the
      // model did not write.
      setDrafted(null);
      const kind = llmFailureKind(e);
      const title =
        kind === "no-key"
          ? "Reply drafting needs a language model"
          : kind === "failed"
            ? "The reply could not be drafted"
            : "Reply drafting could not be reached";
      setDraftError(e instanceof Error ? `${title}: ${e.message}` : `${title}.`);
    } finally {
      setDrafting(false);
    }
  };

  return (
    <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 py-10 lg:py-12 animate-fade-in">
      <div className="mb-8 lg:mb-10 max-w-3xl">
        <Eyebrow>Replies</Eyebrow>
        <h1 className="display mt-3 text-[clamp(30px,4vw,40px)]">Draft a reply</h1>
        <p className="text-muted-foreground mt-3 text-[15px] leading-relaxed">
          Reply to a tenant or owner with the property's history taken into account.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <Card className="p-6">
          <h2 className="text-[22px] font-semibold tracking-tight">Incoming message</h2>

          <div className="mt-5 grid grid-cols-1 sm:grid-cols-3 gap-3">
            <Field label="Property">
              <Select
                value={selectedId}
                onChange={(e) => {
                  setPropertyId(e.target.value);
                  setLoadedMessage(null);
                }}
                className="w-full"
              >
                {(properties ?? []).map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
              </Select>
            </Field>
            <Field label="Channel">
              <Select
                value={form.channel}
                onChange={(e) => setForm({ ...form, channel: e.target.value as "Email" | "Slack" })}
                className="w-full"
              >
                <option>Email</option>
                <option>Slack</option>
              </Select>
            </Field>
            <Field label="Tone">
              <Select
                value={form.tone}
                onChange={(e) => setForm({ ...form, tone: e.target.value })}
                className="w-full"
              >
                <option>Warm</option>
                <option>Neutral</option>
                <option>Formal</option>
              </Select>
            </Field>
          </div>

          {recent.length > 0 && (
            <div className="mt-5">
              <span className="eyebrow">Recent messages</span>
              <ul className="mt-2 rounded-xl border border-border divide-y divide-border overflow-hidden">
                {recent.map((row) => (
                  <li key={row.eventId}>
                    <button
                      type="button"
                      onClick={() => loadMessage(row)}
                      disabled={loadingMessage !== null}
                      className={cn(
                        "w-full text-left px-3.5 py-2.5 flex items-center gap-3 transition-colors hover:bg-foreground/[0.03] disabled:cursor-wait",
                        loadedMessage === row.eventId && "bg-brand/[0.06]",
                      )}
                    >
                      <Mail className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
                      <span className="min-w-0 flex-1 truncate text-[13.5px]">
                        {row.subject || "(no subject)"}
                        <span className="text-muted-foreground"> · {row.sender}</span>
                      </span>
                      <span className="font-mono text-[11px] text-muted-foreground shrink-0">
                        {loadingMessage === row.eventId ? "loading…" : row.date}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}

          <div className="mt-5 space-y-4">
            {form.channel === "Email" && (
              <Field label="Subject">
                <input
                  value={form.subject}
                  onChange={(e) => setForm({ ...form, subject: e.target.value })}
                  className="field"
                />
              </Field>
            )}
            <Field label="Body">
              <textarea
                rows={10}
                value={form.body}
                onChange={(e) => setForm({ ...form, body: e.target.value })}
                placeholder="Paste the message or pick one above"
                className="field resize-none leading-relaxed"
              />
            </Field>
            <div className="flex justify-end items-center gap-3">
              {draftError && (
                <span role="alert" className="flex-1 min-w-0 text-[12px] font-mono text-warning">
                  {draftError}
                </span>
              )}
              <PrimaryButton
                onClick={generate}
                disabled={!form.body.trim() || drafting}
                className="shrink-0 whitespace-nowrap"
              >
                {drafting ? (
                  <>
                    <span className="dot bg-primary-foreground animate-pulse-soft" />
                    Drafting…
                  </>
                ) : (
                  "Draft reply"
                )}
              </PrimaryButton>
            </div>
          </div>
        </Card>

        <Card className="p-6 self-start">
          <h2 className="text-[22px] font-semibold tracking-tight">Drafted reply</h2>
          {drafted ? (
            <DraftedContent drafted={drafted} />
          ) : (
            <p className="mt-6 text-muted-foreground text-[14px]">
              The drafted reply appears here, with the events it drew on.
            </p>
          )}
        </Card>
      </div>
    </div>
  );
};

const DraftedContent = ({ drafted }: { drafted: DraftedReply }) => {
  // The body can be touched up in place before it is copied out.
  const [body, setBody] = useState(drafted.body);
  const [editing, setEditing] = useState(false);
  useEffect(() => {
    setBody(drafted.body);
    setEditing(false);
  }, [drafted.body]);

  const copy = async () => {
    const text = drafted.subject ? `${drafted.subject}\n\n${body}` : body;
    try {
      await navigator.clipboard.writeText(text);
      toast.success("Reply copied to the clipboard");
    } catch {
      toast.error("Could not access the clipboard");
    }
  };

  const related = drafted.context.length;
  const status = drafted.knowsAboutIncident
    ? related > 0
      ? `Based on ${related} related event${related === 1 ? "" : "s"} in the property file`
      : "Based on related history in the property file"
    : "No related events in the property file";

  return (
    <div className="mt-6 space-y-5 animate-fade-in">
      <div className="rounded-xl border border-border bg-subdued px-4 py-3 flex items-center gap-3">
        <span
          className={cn(
            "dot shrink-0",
            drafted.knowsAboutIncident ? "bg-success" : "bg-muted-foreground/50",
          )}
        />
        <p className="text-[14px] font-medium">{status}</p>
      </div>
      {drafted.subject && (
        <div>
          <div className="text-[13px] text-muted-foreground mb-1">Subject</div>
          <p className="text-[15px] font-medium">{drafted.subject}</p>
        </div>
      )}
      <div>
        <div className="text-[13px] text-muted-foreground mb-1">Body</div>
        {editing ? (
          <textarea
            value={body}
            onChange={(e) => setBody(e.target.value)}
            rows={Math.max(6, body.split("\n").length + 1)}
            autoFocus
            aria-label="Reply body"
            className="field text-[15px] leading-relaxed resize-y"
          />
        ) : (
          <p className="text-[15px] leading-relaxed whitespace-pre-line">{body}</p>
        )}
      </div>
      {related > 0 && (
        <>
          <hr className="border-border" />
          <div>
            <div className="text-[13px] text-muted-foreground mb-3">Context used</div>
            <div className="space-y-3">
              {drafted.context.map((c) => (
                <div key={c.id} className="border border-border rounded-xl p-4">
                  <div className="font-mono text-[12px] text-muted-foreground">
                    {c.channel} · {c.time}
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
        Drafted in {(drafted.elapsedMs / 1000).toFixed(1)}s
      </div>
      <div className="flex gap-2">
        <SecondaryButton onClick={copy}>
          <Copy className="w-3.5 h-3.5" /> Copy
        </SecondaryButton>
        <SecondaryButton onClick={() => setEditing((v) => !v)}>
          {editing ? (
            <>
              <Check className="w-3.5 h-3.5" /> Done
            </>
          ) : (
            <>
              <Pencil className="w-3.5 h-3.5" /> Edit
            </>
          )}
        </SecondaryButton>
      </div>
    </div>
  );
};

const Field = ({ label, children }: { label: string; children: React.ReactNode }) => (
  <label className="block">
    <span className="eyebrow">{label}</span>
    <div className="mt-1.5">{children}</div>
  </label>
);

export default DraftReply;
