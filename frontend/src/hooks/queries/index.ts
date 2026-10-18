// TanStack Query hooks for every backend resource the UI reads.
import { useQuery } from "@tanstack/react-query";
import {
  fetchEventRaw,
  fetchOpenUncertainties,
  fetchPendingRejections,
  fetchPendingSignals,
  fetchPortfolioActivity,
  fetchPortfolioSummary,
  fetchProperties,
  fetchPropertyFile,
  fetchPropertyInbox,
  fetchPropertyMarkdown,
} from "@/lib/api";
import type { Property } from "@/lib/types";
import { queryKeys } from "./keys";

export { queryKeys };

const LIST_STALE_MS = 30_000;
const INBOX_STALE_MS = 15_000;

export const useProperties = () =>
  useQuery({ queryKey: queryKeys.properties, queryFn: fetchProperties, staleTime: LIST_STALE_MS });

/** The property the API marks as featured (FEATURED_PROPERTY), if any. */
export const useFeaturedProperty = (): Property | undefined => {
  const { data } = useProperties();
  return data?.find((p) => p.isHero);
};

export const usePropertyFile = (id: string | undefined) =>
  useQuery({
    queryKey: queryKeys.propertyFile(id ?? ""),
    queryFn: () => fetchPropertyFile(id ?? ""),
    enabled: !!id,
    staleTime: LIST_STALE_MS,
  });

/** Always refetched when opened: the markdown reflects the latest writes. */
export const usePropertyMarkdown = (id: string, enabled: boolean) =>
  useQuery({
    queryKey: queryKeys.propertyMarkdown(id),
    queryFn: () => fetchPropertyMarkdown(id),
    enabled,
    staleTime: 0,
    gcTime: 0,
  });

export const usePropertyInbox = (id: string | undefined, keyword: string | undefined) =>
  useQuery({
    queryKey: queryKeys.propertyInbox(id ?? "", keyword),
    queryFn: () => fetchPropertyInbox(id ?? "", keyword, 20),
    enabled: !!id,
    staleTime: LIST_STALE_MS,
  });

export const useEventRaw = (eventId: string | undefined) =>
  useQuery({
    queryKey: queryKeys.eventRaw(eventId ?? ""),
    queryFn: () => fetchEventRaw(eventId ?? ""),
    enabled: !!eventId,
    staleTime: 60_000,
  });

export const usePortfolioSummary = (pollMs?: number) =>
  useQuery({
    queryKey: queryKeys.portfolioSummary,
    queryFn: fetchPortfolioSummary,
    refetchInterval: pollMs,
    staleTime: pollMs ? pollMs / 2 : LIST_STALE_MS,
  });

export const usePortfolioActivity = (limit = 8) =>
  useQuery({
    queryKey: queryKeys.portfolioActivity,
    queryFn: () => fetchPortfolioActivity(limit),
    staleTime: LIST_STALE_MS,
  });

export const usePendingSignals = () =>
  useQuery({
    queryKey: queryKeys.pendingSignals,
    queryFn: () => fetchPendingSignals(50),
    staleTime: INBOX_STALE_MS,
  });

export const useOpenUncertainties = () =>
  useQuery({
    queryKey: queryKeys.openUncertainties,
    queryFn: () => fetchOpenUncertainties(50),
    staleTime: INBOX_STALE_MS,
  });

export const usePendingRejections = () =>
  useQuery({
    queryKey: queryKeys.pendingRejections,
    queryFn: () => fetchPendingRejections(50),
    staleTime: INBOX_STALE_MS,
  });
