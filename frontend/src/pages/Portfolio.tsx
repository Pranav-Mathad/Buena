import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { AlertTriangle, ArrowUpRight, Building2, Search } from "lucide-react";
import { usePortfolioActivity, useProperties } from "@/hooks/queries";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { relativeTime } from "@/lib/api";
import type { Property } from "@/lib/types";
import { cn } from "@/lib/utils";
import { Card, Skeleton, StatusPill, Swatch } from "@/components/ui-bits";
import { PortfolioStats } from "@/components/PortfolioStats";

/** The part of the address the name does not already say, e.g. "10115 Berlin". */
function locality(p: Property): string {
  if (p.address.startsWith(p.name)) {
    const rest = p.address.slice(p.name.length).replace(/^[\s,]+/, "");
    if (rest) return rest;
  }
  return p.address;
}

/** "Berlin", "Berlin and Hamburg", "Berlin, Hamburg and Munich", "Berlin, Hamburg and 3 more". */
function listCities(cities: string[]): string {
  if (cities.length <= 3) {
    return cities.length <= 1
      ? (cities[0] ?? "")
      : `${cities.slice(0, -1).join(", ")} and ${cities[cities.length - 1]}`;
  }
  return `${cities.slice(0, 2).join(", ")} and ${cities.length - 2} more`;
}

const Portfolio = () => {
  useDocumentTitle("Portfolio");
  const { data: properties, isLoading, isError, refetch } = useProperties();
  const { data: liveActivity, isError: activityError } = usePortfolioActivity(10);
  const [query, setQuery] = useState("");

  // The property the API marks first, then the API's order.
  const sorted = useMemo(
    () => [...(properties ?? [])].sort((a, b) => Number(!!b.isHero) - Number(!!a.isHero)),
    [properties],
  );
  const cities = useMemo(() => {
    const seen = new Set<string>();
    for (const p of sorted) {
      const city = p.address.trim().split(/\s+/).pop();
      if (city) seen.add(city);
    }
    return [...seen];
  }, [sorted]);

  const needle = query.trim().toLowerCase();
  const visible = needle
    ? sorted.filter((p) =>
        [p.name, p.address, p.owner ?? ""].some((s) => s.toLowerCase().includes(needle)),
      )
    : sorted;

  return (
    <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 animate-fade-in">
      {/* Header and live numbers */}
      <section className="pt-10 lg:pt-14 pb-10 lg:pb-14 grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_minmax(0,560px)] gap-8 lg:gap-16 items-center">
        <div>
          <h1 className="display text-[clamp(32px,4vw,44px)]">Portfolio</h1>
          <p className="mt-3 text-[16px] text-muted-foreground leading-relaxed">
            {isLoading
              ? "Loading…"
              : `${sorted.length} ${sorted.length === 1 ? "property" : "properties"}` +
                (cities.length > 0 ? ` in ${listCities(cities)}` : "")}
          </p>
        </div>
        <PortfolioStats />
      </section>

      {/* Properties and the activity rail */}
      <section className="pb-16 grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_320px] xl:grid-cols-[minmax(0,1fr)_360px] gap-10 lg:gap-12 items-start">
        <div className="min-w-0">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h2 className="text-[20px] font-semibold tracking-tight">Properties</h2>
              <p className="text-[13px] text-muted-foreground mt-1">
                {needle
                  ? `${visible.length} of ${sorted.length} match`
                  : "Open a property to see its file"}
              </p>
            </div>
            <label className="relative w-full sm:w-[300px]">
              <Search className="pointer-events-none absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Filter by name, address or owner"
                aria-label="Filter properties"
                className="field pl-10 py-2"
              />
            </label>
          </div>

          {isLoading && (
            <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-4">
              {Array.from({ length: 6 }, (_, i) => (
                <Skeleton key={i} className="h-[172px] rounded-2xl" />
              ))}
            </div>
          )}
          {isError && (
            <Card className="mt-6 p-8 text-center border-warning/40">
              <div className="flex items-center justify-center gap-3 text-warning">
                <AlertTriangle className="w-4 h-4" />
                <span className="text-[14px] font-medium">Could not load the portfolio</span>
              </div>
              <p className="text-[13px] text-muted-foreground mt-2">The API did not respond.</p>
              <button
                onClick={() => refetch()}
                className="mt-4 pill bg-card hover:border-foreground/30 transition-colors"
              >
                Retry
              </button>
            </Card>
          )}
          {!isLoading && !isError && visible.length === 0 && (
            <p className="mt-6 text-[14px] text-muted-foreground">No properties match "{query}".</p>
          )}
          <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-4">
            {visible.map((p) => (
              <PropertyCard key={p.id} property={p} />
            ))}
          </div>
        </div>

        <aside className="lg:sticky lg:top-20 min-w-0">
          <h2 className="text-[20px] font-semibold tracking-tight">Recent activity</h2>
          <p className="text-[13px] text-muted-foreground mt-1">
            Latest events across the portfolio
          </p>
          {activityError && (
            <p className="mt-4 text-[13px] text-warning">The activity feed is unavailable.</p>
          )}
          {!activityError && liveActivity && liveActivity.length === 0 && (
            <p className="mt-4 text-[13px] text-muted-foreground">No activity yet.</p>
          )}
          <ol className="mt-5 border-l border-border ml-1.5">
            {(activityError ? [] : (liveActivity ?? [])).map((a) => (
              <li key={a.eventId} className="relative pl-5 pb-5 last:pb-0">
                <span className="absolute left-0 top-[7px] -translate-x-1/2 w-[7px] h-[7px] rounded-full bg-brand/70 ring-4 ring-background" />
                <div className="font-mono text-[11px] text-muted-foreground">
                  {relativeTime(a.receivedAt)} · {a.source}
                </div>
                <p className="mt-1 text-[14px] leading-snug line-clamp-2">{a.text}</p>
                {a.propertyId ? (
                  <Link
                    to={`/properties/${a.propertyId}`}
                    className="mt-1.5 inline-block text-[12.5px] text-muted-foreground hover:text-foreground underline-offset-2 hover:underline transition-colors"
                  >
                    {a.propertyName ?? "Unrouted"}
                  </Link>
                ) : (
                  <span className="mt-1.5 inline-block text-[12.5px] text-muted-foreground">
                    Unrouted
                  </span>
                )}
              </li>
            ))}
          </ol>
        </aside>
      </section>
    </div>
  );
};

const PropertyCard = ({ property: p }: { property: Property }) => {
  const flags = [
    p.stats.needsReview > 0 && `${p.stats.needsReview} to review`,
    p.openIssues > 0 && `${p.openIssues} open issue${p.openIssues > 1 ? "s" : ""}`,
  ].filter((f): f is string => typeof f === "string");
  return (
    <Link to={`/properties/${p.id}`} className="group block h-full rounded-2xl">
      <Card className="h-full p-5 flex flex-col hover:-translate-y-0.5 hover:shadow-pop hover:border-foreground/15">
        <div className="flex items-start justify-between gap-3">
          <Swatch Icon={Building2} />
          <ArrowUpRight className="w-4 h-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
        </div>
        <h3 className="mt-4 text-[15px] font-semibold leading-snug">{p.name}</h3>
        <p className="mt-0.5 text-[13px] text-muted-foreground truncate">{locality(p)}</p>
        {p.owner && (
          <p className="mt-0.5 text-[12.5px] text-muted-foreground/80 truncate">
            {p.owner}
            {p.yearBuilt ? ` · built ${p.yearBuilt}` : ""}
          </p>
        )}
        <div className="mt-auto pt-5 flex flex-wrap items-center justify-between gap-2">
          <span className="font-mono text-[12px] text-muted-foreground">
            {p.stats.events} events · {p.stats.facts} facts
          </span>
          {flags.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {flags.map((f) => (
                <StatusPill key={f} variant="warning" className={cn("text-[11.5px]")}>
                  {f}
                </StatusPill>
              ))}
            </div>
          )}
        </div>
      </Card>
    </Link>
  );
};

export default Portfolio;
