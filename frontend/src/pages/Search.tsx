import { useMemo, useState } from "react";
import { Search as SearchIcon } from "lucide-react";
import { useFeaturedProperty, useProperties, usePropertyInbox } from "@/hooks/queries";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { suggestedQuestions } from "@/lib/constants";
import { cn } from "@/lib/utils";
import { Card, Eyebrow, Select } from "@/components/ui-bits";
import { AskPanel } from "@/components/AskPanel";

// Pick the most distinctive topic word in a free-text question, so the inbox
// is searched for "wasserschaden" rather than "was" or "passiert".
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
  // Fallback: the first word of five or more letters that is not a question word.
  const stop = new Set(["was", "wie", "wer", "wann", "wo", "warum", "what", "how", "why"]);
  for (const w of lower.replace(/[^a-zA-Zäöüß\s]/g, " ").split(/\s+/)) {
    if (w.length >= 5 && !stop.has(w)) return w;
  }
  return question.slice(0, 24);
}

const Search = () => {
  useDocumentTitle("Search");
  const { data: properties } = useProperties();
  const featured = useFeaturedProperty();
  const [propertyId, setPropertyId] = useState("");
  const selectedId = propertyId || featured?.id || properties?.[0]?.id || "";
  const selected = properties?.find((p) => p.id === selectedId);

  const [input, setInput] = useState(suggestedQuestions[0]);
  const [question, setQuestion] = useState(suggestedQuestions[0]);
  const keyword = useMemo(() => extractKeyword(question), [question]);

  const inboxQuery = usePropertyInbox(selectedId || undefined, keyword);
  const rows = inboxQuery.data?.rows ?? [];
  const total = inboxQuery.data?.totalInboxSize ?? 0;
  const matching = inboxQuery.data?.relevantCount ?? 0;

  const ask = (q: string) => {
    const trimmed = q.trim();
    if (!trimmed) return;
    setInput(trimmed);
    setQuestion(trimmed);
  };

  const footer = inboxQuery.isLoading
    ? "Loading…"
    : inboxQuery.isError
      ? "The inbox could not be loaded."
      : total === 0
        ? "No messages for this property."
        : matching > 0
          ? `${matching} of ${total} messages mention "${keyword}"`
          : `None of the ${total} messages mention "${keyword}"`;

  return (
    <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 py-10 lg:py-12 animate-fade-in">
      <div className="max-w-3xl">
        <Eyebrow>Search</Eyebrow>
        <h1 className="display mt-3 text-[clamp(30px,4vw,42px)]">Search a property's inbox</h1>
        <p className="mt-4 text-[16px] text-muted-foreground leading-relaxed">
          Find the messages behind a question, next to the answer from the property file.
        </p>
      </div>

      <div className="mt-8 flex flex-col sm:flex-row sm:items-center gap-3">
        <Select
          value={selectedId}
          onChange={(e) => setPropertyId(e.target.value)}
          aria-label="Property"
          className="w-full sm:w-[300px]"
        >
          {(properties ?? []).map((p) => (
            <option key={p.id} value={p.id}>
              {p.name}
            </option>
          ))}
        </Select>
        <label className="relative flex-1 min-w-0">
          <SearchIcon className="pointer-events-none absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && ask(input)}
            placeholder="Ask a question about this property"
            aria-label="Question"
            className="field pl-10"
          />
        </label>
      </div>
      <div className="mt-3 flex flex-wrap gap-2">
        {suggestedQuestions.map((q) => (
          <button
            key={q}
            type="button"
            onClick={() => ask(q)}
            className={cn(
              "pill font-normal text-[12px] transition-colors hover:border-foreground/30",
              q === question ? "border-brand/40 bg-brand/[0.07] text-brand" : "bg-card",
            )}
          >
            {q}
          </button>
        ))}
      </div>

      <div className="mt-10 grid grid-cols-1 lg:grid-cols-2 gap-10 lg:gap-0 lg:divide-x divide-border">
        <div className="lg:pr-8 min-w-0">
          <Eyebrow>Raw inbox</Eyebrow>
          <h2 className="text-[20px] font-semibold tracking-tight mt-2">
            {selected ? `Messages for ${selected.name}` : "Messages"}
          </h2>

          <div className="mt-4 border border-border bg-card rounded-xl overflow-hidden shadow-card">
            <div className="flex items-center gap-2 px-3 py-2 border-b border-border bg-subdued font-mono">
              <SearchIcon className="w-3 h-3 text-muted-foreground" />
              <span className="text-[12px]">{keyword}</span>
            </div>
            {inboxQuery.isLoading ? (
              <div className="px-3 py-16 text-center text-[13px] text-muted-foreground">
                Loading…
              </div>
            ) : rows.length === 0 ? (
              <div className="px-3 py-16 text-center text-[13px] text-muted-foreground">
                {inboxQuery.isError
                  ? "The inbox could not be loaded."
                  : "No messages for this property."}
              </div>
            ) : (
              <ul className="divide-y divide-border max-h-[480px] overflow-y-auto">
                {rows.map((e) => (
                  <li
                    key={e.eventId}
                    className={cn(
                      "grid grid-cols-12 gap-2 px-3 py-1.5 text-[12px] leading-tight border-l-2",
                      e.relevant
                        ? "border-l-warning"
                        : "border-l-transparent text-muted-foreground",
                    )}
                  >
                    <span className="col-span-2 truncate font-mono text-muted-foreground">
                      {e.date}
                    </span>
                    <span className="col-span-3 truncate">{e.sender}</span>
                    <span className="col-span-7 truncate">
                      <span className={e.relevant ? "text-foreground" : ""}>{e.subject}</span>
                      <span className="ml-1 opacity-60">· {e.preview}</span>
                    </span>
                  </li>
                ))}
              </ul>
            )}
            <div className="px-3 py-2 border-t border-border text-[11px] text-muted-foreground font-mono">
              {footer}
            </div>
          </div>
        </div>

        <div className="lg:pl-8 min-w-0">
          <Eyebrow>Property file</Eyebrow>
          <h2 className="text-[20px] font-semibold tracking-tight mt-2">Answer</h2>
          <Card className="p-6 mt-4">
            <AskPanel presetQuestion={question} compact propertyId={selectedId || undefined} />
          </Card>
        </div>
      </div>
    </div>
  );
};

export default Search;
