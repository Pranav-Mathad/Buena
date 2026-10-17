import { Link } from "react-router-dom";
import { Building2, ArrowUpRight, AlertTriangle } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { fetchPortfolioActivity, fetchProperties, relativeTime } from "@/lib/api";
import { Card, StatusPill, Swatch } from "@/components/ui-bits";
import { ActivityTicker } from "@/components/ActivityTicker";

const Portfolio = () => {
  const { data: properties, isLoading, isError, refetch } = useQuery({
    queryKey: ["properties"],
    queryFn: fetchProperties,
    staleTime: 30_000,
  });

  const { data: liveActivity, isError: activityError } = useQuery({
    queryKey: ["portfolio-activity"],
    queryFn: () => fetchPortfolioActivity(8),
    staleTime: 30_000,
  });

  const activityRows =
    !activityError && liveActivity
      ? liveActivity.map((a) => ({
          time: relativeTime(a.receivedAt),
          text: a.text,
          property: a.propertyName ?? "(unrouted)",
        }))
      : [];

  return (
    <div className="max-w-[1400px] mx-auto px-6 lg:px-10 animate-fade-in">
      {/* Hero */}
      <section className="pt-20 pb-24">
        <div className="flex flex-wrap items-start justify-between gap-8">
          <div className="max-w-4xl">
            <h1 className="display text-[clamp(40px,6vw,68px)]">
              Operations brain for property management.
            </h1>
            <p className="mt-8 text-[17px] text-muted-foreground max-w-[600px] leading-relaxed">
              Every email, invoice, and bank record turned into a living, sourced file per property.
              Ask anything. Get sourced answers. Hear "I don't know" when we don't know.
            </p>
          </div>
          <div className="shrink-0">
            <ActivityTicker />
          </div>
        </div>
      </section>

      {/* Property grid */}
      <section className="pb-20">
        {isLoading && (
          <p className="text-[13px] text-muted-foreground mb-4 font-mono">
            loading properties from {import.meta.env.VITE_API_BASE_URL ?? "backend"}…
          </p>
        )}
        {isError && (
          <Card className="p-8 text-center mb-6 border-warning/40">
            <div className="flex items-center justify-center gap-3 text-warning">
              <AlertTriangle className="w-4 h-4" />
              <span className="text-[14px] font-medium">
                Backend unreachable
              </span>
            </div>
            <p className="text-[13px] text-muted-foreground mt-2 font-mono">
              GET /properties failed. Check that uvicorn is running on{" "}
              {import.meta.env.VITE_API_BASE_URL ?? "the configured base URL"}.
            </p>
            <button
              onClick={() => refetch()}
              className="mt-4 pill text-[12px] hover:border-foreground/30"
            >
              retry
            </button>
          </Card>
        )}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 lg:gap-8">
          {(properties ?? []).map((p) => (
            <Link
              key={p.id}
              to={`/properties/${p.id}`}
              className="group"
            >
              <Card
                className={`p-6 hover:-translate-y-0.5 hover:border-foreground/20 ${
                  p.isHero ? "ring-1 ring-foreground/[0.04]" : ""
                }`}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex items-start gap-3 min-w-0">
                    <Swatch Icon={Building2} />
                    <div className="min-w-0">
                      <h2 className="text-[16px] font-semibold leading-snug truncate">{p.name}</h2>
                      <p className="text-[13px] text-muted-foreground mt-1">{p.address}</p>
                    </div>
                  </div>
                  <ArrowUpRight className="w-4 h-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity shrink-0" />
                </div>
                <div className="mt-6 font-mono text-[13px] text-muted-foreground">
                  {p.stats.events} events · {p.stats.facts} facts
                  {p.stats.needsReview > 0 && ` · ${p.stats.needsReview} needs review`}
                </div>
                <div className="mt-5 flex flex-wrap gap-2">
                  <StatusPill variant="active">active</StatusPill>
                  {p.openIssues > 0 && (
                    <StatusPill variant="warning">
                      {p.openIssues} open issue{p.openIssues > 1 ? "s" : ""}
                    </StatusPill>
                  )}
                </div>
              </Card>
            </Link>
          ))}
        </div>
      </section>

      {/* Recent activity */}
      <section className="pb-24">
        <h2 className="text-[22px] font-semibold tracking-tight">Recent activity across all properties</h2>
        {activityError && (
          <p className="text-[13px] text-warning mt-3 font-mono">
            activity feed unreachable — backend down or empty.
          </p>
        )}
        {!activityError && activityRows.length === 0 && (
          <p className="text-[13px] text-muted-foreground mt-3 font-mono">
            no recent activity — try{" "}
            <code className="bg-muted px-1.5 py-0.5 rounded">
              POST /admin/buena/advance_day
            </code>
          </p>
        )}
        <div className="mt-8 border-l border-border ml-2">
          {activityRows.map((a, i) => (
            <div key={i} className="relative pl-8 pb-7 last:pb-0">
              <span className="absolute left-0 top-1.5 -translate-x-1/2 w-2 h-2 rounded-full bg-foreground/30" />
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <span className="font-mono text-[13px] text-muted-foreground w-24 shrink-0">{a.time}</span>
                <span className="text-[15px]">{a.text}</span>
                <span className="pill bg-card text-[12px] py-0.5 px-2.5">{a.property}</span>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
};

export default Portfolio;
