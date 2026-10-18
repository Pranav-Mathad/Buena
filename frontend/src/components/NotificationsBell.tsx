import { useNavigate } from "react-router-dom";
import { ArrowUpRight, Bell, Check, Inbox, Sparkles } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { useNotifications, type Notification } from "@/hooks/use-notifications";

const formatRelative = (ts: number): string => {
  const diffMs = Date.now() - ts;
  if (diffMs < 5_000) return "just now";
  const seconds = Math.floor(diffMs / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return new Date(ts).toLocaleDateString("de-DE");
};

export const NotificationsBell = () => {
  const { notifications, unreadCount, markAllRead, clear } = useNotifications();
  const navigate = useNavigate();

  return (
    <Popover
      onOpenChange={(open) => {
        if (open && unreadCount > 0) markAllRead();
      }}
    >
      <PopoverTrigger asChild>
        <button
          aria-label={`Notifications${unreadCount > 0 ? ` (${unreadCount} unread)` : ""}`}
          className="relative w-9 h-9 rounded-lg flex items-center justify-center hover:bg-foreground/[0.04] transition-colors"
        >
          <Bell className="w-4 h-4" />
          {unreadCount > 0 && (
            <span className="absolute -top-0.5 -right-0.5 min-w-[18px] h-[18px] px-1 rounded-full bg-success text-background text-[10.5px] font-mono font-semibold flex items-center justify-center ring-2 ring-background animate-pulse-soft">
              {unreadCount > 9 ? "9+" : unreadCount}
            </span>
          )}
        </button>
      </PopoverTrigger>
      <PopoverContent
        align="end"
        sideOffset={10}
        className="w-[400px] max-w-[calc(100vw-2rem)] p-0 rounded-2xl border border-border bg-card shadow-card overflow-hidden"
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <div className="flex items-center gap-2">
            <span className="text-[14px] font-semibold tracking-tight">Notifications</span>
            <span className="text-[11.5px] font-mono text-muted-foreground">
              {notifications.length === 0 ? "no events yet" : `${notifications.length} recent`}
            </span>
          </div>
          {notifications.length > 0 && (
            <button
              onClick={clear}
              className="text-[12px] text-muted-foreground hover:text-foreground transition-colors"
            >
              Clear
            </button>
          )}
        </div>

        {notifications.length === 0 ? (
          <EmptyState />
        ) : (
          <div className="max-h-[440px] overflow-y-auto">
            {notifications.map((n) => (
              <NotificationRow
                key={n.id}
                notification={n}
                onView={n.propertyId ? () => navigate(`/properties/${n.propertyId}`) : undefined}
              />
            ))}
          </div>
        )}

        <div className="px-4 py-2.5 border-t border-border bg-subdued flex items-center justify-between">
          <span className="font-mono text-[11px] text-muted-foreground">live · portfolio-wide</span>
          {unreadCount === 0 && notifications.length > 0 && (
            <span className="inline-flex items-center gap-1 text-[11px] text-muted-foreground">
              <Check className="w-3 h-3" /> all read
            </span>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
};

const NotificationRow = ({
  notification,
  onView,
}: {
  notification: Notification;
  onView?: () => void;
}) => (
  <div
    className={`group flex gap-3 px-4 py-3 border-b border-border last:border-0 transition-colors ${
      notification.readAt === null
        ? "bg-success/[0.04] hover:bg-success/[0.08]"
        : "hover:bg-foreground/[0.02]"
    }`}
  >
    <div className="w-8 h-8 shrink-0 rounded-lg bg-success/15 ring-1 ring-success/20 flex items-center justify-center">
      <Sparkles className="w-3.5 h-3.5 text-success" />
    </div>
    <div className="flex-1 min-w-0">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[13px] font-semibold tracking-tight truncate">
          {notification.propertyName}
        </span>
        <span className="text-[11px] font-mono text-muted-foreground shrink-0">
          {formatRelative(notification.receivedAt)}
        </span>
      </div>
      <p className="text-[12.5px] text-muted-foreground mt-0.5 leading-snug line-clamp-2">
        {notification.description}
      </p>
      <div className="mt-2 flex items-center justify-between gap-2">
        <span className="font-mono text-[10.5px] text-muted-foreground">
          {notification.category}
          {" · "}
          {notification.factsWritten > 0
            ? `+${notification.factsWritten} fact${notification.factsWritten === 1 ? "" : "s"}`
            : "no fact change"}
        </span>
        {onView && (
          <button
            onClick={onView}
            className="inline-flex items-center gap-1 text-[11.5px] text-foreground/80 hover:text-foreground"
          >
            Open <ArrowUpRight className="w-3 h-3" />
          </button>
        )}
      </div>
    </div>
  </div>
);

const EmptyState = () => (
  <div className="px-6 py-10 flex flex-col items-center text-center">
    <div className="w-10 h-10 rounded-xl bg-foreground/[0.04] flex items-center justify-center">
      <Inbox className="w-4 h-4 text-muted-foreground" />
    </div>
    <p className="mt-3 text-[13px] text-foreground">No notifications yet</p>
    <p className="mt-1 text-[12px] text-muted-foreground max-w-[280px] leading-relaxed">
      Anything the pipeline ingests will pop here: emails, invoices, bank events, simulator
      advances.
    </p>
  </div>
);
