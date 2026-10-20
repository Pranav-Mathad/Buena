import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { queryKeys } from "@/hooks/queries";
import { API_BASE_URL } from "@/lib/config";

export type LiveStatus = "connecting" | "live" | "offline";

/**
 * Subscribe to `/properties/{id}/events` while the page is mounted.
 *
 * The worker publishes a `fact_update` payload after each event is
 * processed. Each one invalidates the property file and the portfolio
 * queries, so new facts and card stats appear without a reload, and bumps
 * `pulse`, the count of updates received since the page opened.
 */
export function useLivePropertyUpdates(id: string | undefined): {
  liveStatus: LiveStatus;
  pulse: number;
} {
  const queryClient = useQueryClient();
  const [liveStatus, setLiveStatus] = useState<LiveStatus>("connecting");
  const [pulse, setPulse] = useState(0);

  useEffect(() => {
    if (!id) return;
    const es = new EventSource(`${API_BASE_URL}/properties/${id}/events`);
    es.addEventListener("hello", () => setLiveStatus("live"));
    es.addEventListener("fact_update", (raw: MessageEvent) => {
      try {
        JSON.parse(raw.data);
      } catch {
        // Malformed payload: ignore it. EventSource reconnects on its own.
        return;
      }
      queryClient.invalidateQueries({ queryKey: queryKeys.propertyFile(id) });
      queryClient.invalidateQueries({ queryKey: queryKeys.properties });
      queryClient.invalidateQueries({ queryKey: queryKeys.portfolioSummary });
      queryClient.invalidateQueries({ queryKey: queryKeys.portfolioActivity });
      setPulse((n) => n + 1);
    });
    es.onerror = () => setLiveStatus("offline");
    return () => es.close();
  }, [id, queryClient]);

  return { liveStatus, pulse };
}
