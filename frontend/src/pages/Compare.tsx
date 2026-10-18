import { useMemo, useState } from "react";
import { useFeaturedProperty, usePropertyInbox } from "@/hooks/queries";
import { compareQuestions } from "@/lib/constants";
import { Card } from "@/components/ui-bits";
import { AskPanel } from "@/components/AskPanel";
import { Search } from "lucide-react";

// Extract the most distinctive German keyword from a free-text question.
// We bias toward the actual nouns/topics rather than question words —
// "Was passiert mit dem Wasserschaden?" → "wasserschaden".
function extractKeyword(question: string): string {
  const lower = question.toLowerCase();
  const matches: { keyword: string; weight: number }[] = [];
  const dictionary: { keyword: string; weight: number; aliases: string[] }[] = [
    { keyword: "wasser", weight: 5, aliases: ["water", "wasserschaden"] },
    { keyword: "miet", weight: 4, aliases: ["miete", "rent"] },
    { keyword: "mietminderung", weight: 4, aliases: ["rent reduction"] },
    { keyword: "verlänger", weight: 3, aliases: ["verlang", "renew"] },
    { keyword: "kündig", weight: 3, aliases: ["kuendig", "termination"] },
    { keyword: "lärm", weight: 3, aliases: ["laerm", "noise"] },
    { keyword: "heizung", weight: 3, aliases: ["heating"] },
    { keyword: "kaution", weight: 3, aliases: ["deposit"] },
    { keyword: "schlüssel", weight: 3, aliases: ["schluessel", "key"] },
    { keyword: "iban", weight: 1, aliases: ["bank", "konto"] },
  ];
  for (const entry of dictionary) {
    const probes = [entry.keyword, ...entry.aliases];
    if (probes.some((p) => lower.includes(p))) {
      matches.push({ keyword: entry.keyword, weight: entry.weight });
    }
  }
  if (matches.length > 0) {
    matches.sort((a, b) => b.weight - a.weight);
    return matches[0].keyword;
  }
  // Fallback: first word over 4 chars that isn't a question marker.
  const stop = new Set(["was", "wie", "wer", "wann", "wo", "warum", "what", "how", "why"]);
  for (const w of lower.replace(/[^a-zA-Zäöüß\s]/g, " ").split(/\s+/)) {
    if (w.length >= 5 && !stop.has(w)) return w;
  }
  return question.slice(0, 24);
}

const Compare = () => {
  const [question, setQuestion] = useState(compareQuestions[0]);

  const featured = useFeaturedProperty();
  const heroId = featured?.id ?? "";
  const heroName = featured?.name ?? "";

  const keyword = useMemo(() => extractKeyword(question), [question]);

  const inboxQuery = usePropertyInbox(featured?.id, keyword);

  const inboxRows = inboxQuery.data?.rows ?? [];
  const totalInbox = inboxQuery.data?.totalInboxSize ?? 0;
  const relevantCount = inboxQuery.data?.relevantCount ?? 0;

  const footerText = inboxQuery.isLoading
    ? "loading inbox…"
    : inboxQuery.isError
      ? "Backend unreachable — showing empty inbox"
      : inboxRows.length === 0
        ? `no inbox events match this property`
        : relevantCount > 0
          ? `${relevantCount} of ${totalInbox} emails match "${keyword}" · estimated ${Math.max(
              1,
              Math.round(relevantCount * 0.5),
            )}min to read`
          : `0 of ${totalInbox} emails match "${keyword}" · check the lease PDF or ERP`;

  return (
    <div className="max-w-[1400px] mx-auto px-6 lg:px-10 py-12 animate-fade-in">
      <div className="max-w-3xl">
        <h1 className="display text-[clamp(32px,4.5vw,48px)] tracking-tight">
          Side by side: raw inbox vs. Keystone
        </h1>
        <p className="mt-4 text-[16px] text-muted-foreground leading-relaxed">
          Same property. Same question. Different answer experience.
        </p>
      </div>

      <div className="mt-8 flex flex-wrap items-center gap-3">
        <select
          defaultValue={heroId}
          className="bg-card border border-border rounded-lg px-3 py-2 text-[14px] outline-none focus:border-foreground/40"
        >
          <option value={heroId}>{heroName}</option>
        </select>
        <select
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          className="bg-card border border-border rounded-lg px-3 py-2 text-[14px] outline-none focus:border-foreground/40 min-w-[280px]"
        >
          {compareQuestions.map((q) => (
            <option key={q}>{q}</option>
          ))}
        </select>
      </div>

      <div className="mt-10 grid grid-cols-1 lg:grid-cols-2 gap-0 lg:divide-x divide-border">
        {/* Today: real inbox scroll */}
        <div className="lg:pr-8">
          <div className="text-[12px] uppercase tracking-wider text-muted-foreground font-medium">
            what a property manager does today
          </div>
          <h2 className="text-[20px] font-semibold mt-2">Today: search the inbox</h2>

          <div className="mt-4 text-[12px] text-muted-foreground font-mono">
            showing inbox results for property: {heroName.split(" ").slice(-2).join(" ")}
          </div>

          <div className="mt-3 border border-border bg-card overflow-hidden">
            <div className="flex items-center gap-2 px-2 py-1.5 border-b border-border bg-muted/40 font-mono">
              <Search className="w-3 h-3 text-muted-foreground" />
              <span className="text-[12px]">{keyword}</span>
            </div>
            {inboxQuery.isLoading ? (
              <div className="px-3 py-16 text-center text-[13px] text-muted-foreground font-mono">
                loading inbox…
              </div>
            ) : inboxRows.length === 0 ? (
              <div className="px-3 py-16 text-center text-[13px] text-muted-foreground font-mono">
                {inboxQuery.isError
                  ? "Backend unreachable — inbox view unavailable."
                  : "No email events for this property."}
              </div>
            ) : (
              <ul className="divide-y divide-border max-h-[480px] overflow-y-auto">
                {inboxRows.map((e) => (
                  <li
                    key={e.eventId}
                    className={`grid grid-cols-12 gap-2 px-3 py-1.5 text-[12px] leading-tight border-l-2 ${
                      e.relevant ? "border-l-warning" : "border-l-transparent"
                    } ${e.relevant ? "" : "text-muted-foreground"}`}
                  >
                    <span className="col-span-2 truncate font-mono text-muted-foreground">
                      {e.date}
                    </span>
                    <span className="col-span-3 truncate">{e.sender}</span>
                    <span className="col-span-7 truncate">
                      <span className={e.relevant ? "text-foreground" : ""}>{e.subject}</span>
                      <span className="ml-1 opacity-60">— {e.preview}</span>
                    </span>
                  </li>
                ))}
              </ul>
            )}
            <div className="px-3 py-2 border-t border-border text-[11px] text-muted-foreground font-mono">
              {footerText}
            </div>
          </div>
        </div>

        {/* With Keystone: the same question asked of the property file */}
        <div className="lg:pl-8 mt-12 lg:mt-0">
          <div className="text-[12px] uppercase tracking-wider text-muted-foreground font-medium">
            with Keystone
          </div>
          <h2 className="text-[20px] font-semibold mt-2">Now: ask Keystone</h2>
          <Card className="p-6 mt-6">
            <AskPanel presetQuestion={question} compact propertyId={featured?.id} />
          </Card>
        </div>
      </div>

      {/* What changed — derived from real numbers */}
      <Card className="mt-16 p-8">
        <h3 className="text-[16px] font-semibold">What changed</h3>
        <ul className="mt-5 space-y-3 text-[15px]">
          <li className="flex items-baseline gap-3">
            <span className="font-mono text-[13px] text-muted-foreground w-8">→</span>
            <span>
              {totalInbox > 0 ? (
                <>
                  Scrolling {totalInbox} emails for "{keyword}" — Keystone's answer cites the{" "}
                  {relevantCount > 0 ? relevantCount : "relevant"} events directly, in seconds.
                </>
              ) : (
                <>One sourced answer instead of an inbox scroll.</>
              )}
            </span>
          </li>
          <li className="flex items-baseline gap-3">
            <span className="font-mono text-[13px] text-muted-foreground w-8">→</span>
            <span>
              When the context doesn't have an answer, Keystone says "I don't know" — the inbox just
              keeps scrolling.
            </span>
          </li>
          <li className="flex items-baseline gap-3">
            <span className="font-mono text-[13px] text-muted-foreground w-8">→</span>
            <span>
              Sources stay clickable on both sides — but only Keystone tells you <em>which</em>{" "}
              emails matter.
            </span>
          </li>
        </ul>
      </Card>
    </div>
  );
};

export default Compare;
