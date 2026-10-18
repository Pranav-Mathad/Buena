import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { ArrowUpRight, Sparkles, X } from "lucide-react";
import { toast } from "sonner";
import { API_BASE_URL } from "@/lib/api";
import {
  NotificationContext,
  type Notification,
  type NotificationContextValue,
} from "@/hooks/use-notifications";
import { queryKeys } from "@/hooks/queries";
import type { Property } from "@/lib/types";

const MAX_NOTIFICATIONS = 50;

// One global SSE subscription that drives BOTH the toast and the
// bell dropdown. State lives here in a single provider so the bell's
// unread count survives route changes.
export const NotificationProvider = ({ children }: { children: ReactNode }) => {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const seenRef = useRef<Set<string>>(new Set());

  const markAllRead = useCallback(() => {
    setNotifications((prev) =>
      prev.every((n) => n.readAt !== null)
        ? prev
        : prev.map((n) => (n.readAt ? n : { ...n, readAt: Date.now() })),
    );
  }, []);

  const clear = useCallback(() => setNotifications([]), []);

  useEffect(() => {
    const url = `${API_BASE_URL}/portfolio/events`;
    const es = new EventSource(url);

    es.addEventListener("fact_update", (raw: MessageEvent) => {
      try {
        const payload = JSON.parse(raw.data) as {
          event_id?: string;
          property_id?: string;
          summary?: string;
          facts_written?: number;
          category?: string;
        };
        if (!payload.event_id || seenRef.current.has(payload.event_id)) return;
        seenRef.current.add(payload.event_id);

        const properties = queryClient.getQueryData<Property[]>(queryKeys.properties) ?? [];
        const property = properties.find((p) => p.id === payload.property_id);
        const propertyName = property?.name ?? "Unrouted event";
        const propertyId = property?.id ?? payload.property_id ?? null;
        const written = payload.facts_written ?? 0;
        const description =
          payload.summary?.trim() ||
          (written > 0
            ? `${written} new fact${written === 1 ? "" : "s"} captured.`
            : "Event ingested — no fact change.");
        const category = payload.category ?? "event";

        const notification: Notification = {
          id: `${payload.event_id}-${Date.now()}`,
          eventId: payload.event_id,
          propertyId,
          propertyName,
          category,
          description,
          factsWritten: written,
          receivedAt: Date.now(),
          readAt: null,
        };

        setNotifications((prev) => [notification, ...prev].slice(0, MAX_NOTIFICATIONS));

        toast.custom(
          (id) => (
            <EventToastCard
              propertyName={propertyName}
              description={description}
              factsWritten={written}
              category={category}
              onView={
                propertyId
                  ? () => {
                      navigate(`/properties/${propertyId}`);
                      toast.dismiss(id);
                    }
                  : undefined
              }
              onDismiss={() => toast.dismiss(id)}
            />
          ),
          { duration: 7000 },
        );

        queryClient.invalidateQueries({ queryKey: queryKeys.properties });
        queryClient.invalidateQueries({ queryKey: queryKeys.portfolioSummary });
        queryClient.invalidateQueries({ queryKey: queryKeys.portfolioActivity });
      } catch {
        // Malformed payload — ignore. EventSource auto-reconnects.
      }
    });

    return () => {
      es.close();
    };
  }, [queryClient, navigate]);

  const value = useMemo<NotificationContextValue>(() => {
    const unreadCount = notifications.filter((n) => n.readAt === null).length;
    return { notifications, unreadCount, markAllRead, clear };
  }, [notifications, markAllRead, clear]);

  return <NotificationContext.Provider value={value}>{children}</NotificationContext.Provider>;
};

// ---------------------------------------------------------------------------
// Toast card — kept here so the provider owns both surfaces.

const EventToastCard = ({
  propertyName,
  description,
  factsWritten,
  category,
  onView,
  onDismiss,
}: {
  propertyName: string;
  description: string;
  factsWritten: number;
  category: string;
  onView?: () => void;
  onDismiss: () => void;
}) => (
  <div className="w-[380px] max-w-[calc(100vw-2rem)] rounded-2xl border border-border bg-card shadow-card overflow-hidden animate-fade-in">
    <div className="flex gap-3 p-4">
      <div className="w-9 h-9 shrink-0 rounded-xl bg-success/15 ring-1 ring-success/20 flex items-center justify-center">
        <Sparkles className="w-4 h-4 text-success" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline justify-between gap-2">
          <span className="text-[10.5px] font-mono uppercase tracking-[0.12em] text-muted-foreground">
            New event
          </span>
          <span className="text-[10.5px] font-mono text-muted-foreground/80 shrink-0">
            {category}
          </span>
        </div>
        <div className="mt-0.5 text-[14px] font-semibold tracking-tight truncate">
          {propertyName}
        </div>
        <p className="mt-1 text-[13px] leading-snug text-muted-foreground line-clamp-2">
          {description}
        </p>
        <div className="mt-3 flex items-center justify-between gap-3">
          <span className="text-[11.5px] font-mono text-muted-foreground">
            {factsWritten > 0
              ? `+${factsWritten} fact${factsWritten === 1 ? "" : "s"}`
              : "no fact change"}
          </span>
          <div className="flex items-center gap-1">
            {onView && (
              <button
                onClick={onView}
                className="inline-flex items-center gap-1 text-[12px] px-2.5 py-1 rounded-lg bg-foreground text-background hover:opacity-90 transition-opacity"
              >
                View
                <ArrowUpRight className="w-3 h-3" />
              </button>
            )}
            <button
              onClick={onDismiss}
              aria-label="Dismiss"
              className="w-7 h-7 rounded-lg flex items-center justify-center text-muted-foreground hover:bg-foreground/[0.05] transition-colors"
            >
              <X className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      </div>
    </div>
    <div className="h-[3px] w-full bg-gradient-to-r from-success/60 via-success/30 to-transparent" />
  </div>
);
