import { useState } from "react";
import { fetchOnboarding, type BriefingSource } from "@/lib/api";
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
  const [source, setSource] = useState<BriefingSource>("unavailable");
  const [error, setError] = useState<string | null>(null);

  const load = async (): Promise<boolean> => {
    if (!id) return false;
    const started = performance.now();
    setError(null);
    try {
      const data = await fetchOnboarding(id);
      setItems(data.items);
      setLatencyMs(performance.now() - started);
      setSource(data.source);
      return true;
    } catch (e) {
      setItems([]);
      setLatencyMs(0);
      setSource("unavailable");
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
    source,
    error,
    start,
    regenerate,
    close: () => setOpen(false),
  };
}
