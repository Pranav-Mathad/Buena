import { useEffect, useRef, useState } from "react";
import { usePortfolioSummary } from "@/hooks/queries";

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

const Stat = ({
  value,
  pulsing,
  label,
}: {
  value: number;
  pulsing: boolean;
  label: string;
}) => (
  <div
    className={`flex items-baseline gap-2 px-1.5 py-0.5 -mx-1.5 rounded-md transition-colors duration-300 ${
      pulsing ? "bg-warning/20" : ""
    }`}
  >
    <span className="font-mono text-[13px] font-medium tabular-nums">
      {value.toLocaleString("de-DE")}
    </span>
    <span className="text-[12px] text-muted-foreground">{label}</span>
  </div>
);

export const ActivityTicker = () => {
  const { data, isError } = usePortfolioSummary(POLL_INTERVAL_MS);

  const events = data?.eventsLastHour ?? 0;
  const facts = data?.factsToday ?? 0;
  const signals = data?.pendingSignals ?? 0;

  const eventsAnim = useCountUp(events);
  const factsAnim = useCountUp(facts);

  return (
    <div className="border border-border rounded-2xl bg-card p-3 min-w-[260px] shadow-card">
      <div className="flex items-center gap-2">
        <span className={`dot ${isError ? "bg-warning" : "bg-success"} animate-pulse-soft`} />
        <span className="font-mono text-[11px] text-muted-foreground uppercase tracking-wider">
          {isError ? "offline" : "live"}
        </span>
      </div>
      <div className="mt-2 space-y-1">
        <Stat
          value={eventsAnim.display}
          pulsing={eventsAnim.pulsing}
          label="events ingested · last hour"
        />
        <Stat
          value={factsAnim.display}
          pulsing={factsAnim.pulsing}
          label="facts created · today"
        />
        <Stat value={signals} pulsing={false} label="signals pending" />
      </div>
      <div className="mt-2 pt-2 border-t border-border font-mono text-[11px] text-muted-foreground">
        live · cloud run · europe-west10
      </div>
    </div>
  );
};
