import { useState } from "react";
import { fetchOnboarding } from "@/lib/api";
import type { BriefingItem } from "@/lib/text";

/**
 * State for the onboarding briefing: generating it, the open modal, and
 * re-generating from inside the modal.
 *
 * `start` opens the modal only when the fetch succeeds; a failure leaves it
 * closed and sets `error`. Latency is measured in the browser, so a cached
 * briefing shows how long the round trip took, not the original generation.
 */
export function useBriefing(id: string | undefined) {
  const [open, setOpen] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [items, setItems] = useState<BriefingItem[]>([]);
  const [latencyMs, setLatencyMs] = useState(0);
  const [cached, setCached] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = async (): Promise<boolean> => {
    if (!id) return false;
    const started = performance.now();
    setError(null);
    try {
      const data = await fetchOnboarding(id);
      setItems(data.items);
      setLatencyMs(performance.now() - started);
      setCached(data.cached);
      return true;
    } catch (e) {
      setItems([]);
      setLatencyMs(0);
      setCached(false);
      setError(e instanceof Error ? e.message : "briefing service unreachable");
      return false;
    }
  };

  const start = async () => {
    if (generating || open) return;
    setGenerating(true);
    try {
      if (await load()) setOpen(true);
    } finally {
      setGenerating(false);
    }
  };

  const regenerate = async () => {
    await load();
  };

  return {
    open,
    generating,
    items,
    latencyMs,
    cached,
    error,
    start,
    regenerate,
    close: () => setOpen(false),
  };
}
