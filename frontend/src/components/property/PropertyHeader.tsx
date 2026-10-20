import { Building2, FileText } from "lucide-react";
import type { Property } from "@/lib/types";
import { StatusPill, Swatch } from "@/components/ui-bits";
import type { LiveStatus } from "@/hooks/use-live-property-updates";
import { cn } from "@/lib/utils";

/** Name, address, counts, the live-update badge and the property file button. */
export const PropertyHeader = ({
  property,
  liveStatus,
  pulse,
  onOpenMarkdown,
}: {
  property: Property;
  liveStatus: LiveStatus;
  pulse: number;
  onOpenMarkdown: () => void;
}) => (
  <div className="mt-6 flex items-start gap-4">
    <Swatch Icon={Building2} className="!w-11 !h-11 rounded-xl mt-0.5" />
    <div className="min-w-0">
      <h1 className="display text-[clamp(26px,3.4vw,34px)]">{property.name}</h1>
      <p className="text-[15px] text-muted-foreground mt-1.5">{property.address}</p>
      <div className="mt-4 flex flex-wrap items-center gap-2">
        <StatusPill variant="neutral">{property.stats.events} events</StatusPill>
        <StatusPill variant="neutral">{property.stats.facts} facts</StatusPill>
        <span
          key={`pulse-${pulse}`}
          className={cn(
            "pill transition-colors duration-500",
            pulse > 0 ? "bg-success/15 border-success/40" : "bg-card",
          )}
          title={
            liveStatus === "live"
              ? "Subscribed to live updates"
              : liveStatus === "offline"
                ? "SSE connection lost, retrying"
                : "Connecting…"
          }
        >
          <span
            className={cn(
              "dot",
              liveStatus === "live"
                ? "bg-success animate-pulse-soft"
                : liveStatus === "offline"
                  ? "bg-warning"
                  : "bg-muted-foreground/50",
            )}
          />
          <span className="font-mono text-[12px]">
            {liveStatus === "live"
              ? pulse > 0
                ? `+${pulse} live update${pulse === 1 ? "" : "s"}`
                : "live"
              : liveStatus}
          </span>
        </span>
        <button
          onClick={onOpenMarkdown}
          className="pill bg-card hover:border-foreground/30 transition-colors"
          title="Preview, download or edit the markdown file for this property"
        >
          <FileText className="w-3.5 h-3.5" />
          <span>Property file</span>
        </button>
      </div>
    </div>
  </div>
);
