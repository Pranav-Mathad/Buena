import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { usePortfolioSummary, useProperties } from "@/hooks/queries";
import { cn } from "@/lib/utils";
import { Card } from "./ui-bits";

const POLL_INTERVAL_MS = 30_000;
const COUNT_UP_MS = 200;
const FLASH_MS = 400;

function useCountUp(target: number) {
  const [display, setDisplay] = useState(target);
  const [pulsing, setPulsing] = useState(false);
  const prevRef = useRef(target);

  useEffect(() => {
    if (prevRef.current === target) return;
    const from = prevRef.current;
    const to = target;
    prevRef.current = to;
    const start = performance.now();
    let raf = 0;
    setPulsing(true);

    const step = (now: number) => {
      const t = Math.min(1, (now - start) / COUNT_UP_MS);
      setDisplay(Math.round(from + (to - from) * t));
      if (t < 1) raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    const flashTimer = window.setTimeout(() => setPulsing(false), FLASH_MS);

    return () => {
      cancelAnimationFrame(raf);
      window.clearTimeout(flashTimer);
    };
  }, [target]);

  return { display, pulsing };
}

const Tile = ({
  label,
  value,
  hint,
  pulsing,
  to,
}: {
  label: string;
  value: number;
  hint?: string;
  pulsing?: boolean;
  to?: string;
}) => {
  const className = cn(
    "flex flex-col px-5 py-4 bg-card transition-colors duration-300",
    pulsing && "bg-warning/10",
    to && "hover:bg-foreground/[0.025]",
  );
  const body = (
    <>
      <span className="text-[12.5px] text-muted-foreground leading-none">{label}</span>
      <span className="mt-2.5 text-[28px] font-semibold tracking-tight leading-none">
        {value.toLocaleString("de-DE")}
      </span>
      {hint && <span className="mt-2 font-mono text-[11px] text-muted-foreground">{hint}</span>}
    </>
  );
  return to ? (
    <Link to={to} className={className}>
      {body}
    </Link>
  ) : (
    <div className={className}>{body}</div>
  );
};

/** Four live portfolio numbers, polled from the API. */
export const PortfolioStats = () => {
  const { data, isError } = usePortfolioSummary(POLL_INTERVAL_MS);
  const { data: properties } = useProperties();

  const openIssues = (properties ?? []).reduce((n, p) => n + p.openIssues, 0);
  const events = data?.eventsLastHour ?? 0;
  const facts = data?.factsToday ?? 0;
  const signals = data?.pendingSignals ?? 0;

  const eventsAnim = useCountUp(events);
  const factsAnim = useCountUp(facts);

  return (
    <Card className="overflow-hidden">
      <div className="grid grid-cols-2 gap-px bg-border">
        <Tile label="Open issues" value={openIssues} hint="across all properties" />
        <Tile
          label="Events received"
          value={eventsAnim.display}
          pulsing={eventsAnim.pulsing}
          hint="last hour"
        />
        <Tile
          label="Facts written"
          value={factsAnim.display}
          pulsing={factsAnim.pulsing}
          hint="today"
        />
        <Tile label="Signals pending" value={signals} hint="awaiting a decision" to="/inbox" />
      </div>
      <div className="px-5 py-2.5 border-t border-border flex items-center gap-2 font-mono text-[11px] text-muted-foreground">
        <span className={cn("dot", isError ? "bg-warning" : "bg-success animate-pulse-soft")} />
        {isError ? "offline · retrying" : `live · updated every ${POLL_INTERVAL_MS / 1000} s`}
      </div>
    </Card>
  );
};
