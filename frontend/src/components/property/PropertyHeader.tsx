import { Building2, FileText } from "lucide-react";
import type { Property } from "@/lib/types";
import { StatusPill, Swatch } from "@/components/ui-bits";
import type { LiveStatus } from "@/hooks/use-live-property-updates";

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
    <Swatch Icon={Building2} className="!w-10 !h-10" />
    <div>
      <h1 className="display text-[32px] tracking-tight">{property.name}</h1>
      <p className="text-[15px] text-muted-foreground mt-1">{property.address}</p>
      <div className="mt-4 flex flex-wrap items-center gap-2">
        <StatusPill variant="neutral">{property.stats.events} events</StatusPill>
        <StatusPill variant="neutral">{property.stats.facts} facts</StatusPill>
        <span
          key={`pulse-${pulse}`}
          className={`pill text-[12px] flex items-center gap-1.5 transition-colors duration-500 ${
            pulse > 0 ? "bg-success/15 border-success/40" : "bg-card"
          }`}
          title={
            liveStatus === "live"
              ? "Subscribed to live updates"
              : liveStatus === "offline"
                ? "SSE connection lost — retrying"
                : "Connecting…"
          }
        >
          <span
            className={`dot ${
              liveStatus === "live"
                ? "bg-success animate-pulse-soft"
                : liveStatus === "offline"
                  ? "bg-warning"
                  : "bg-muted-foreground/50"
            }`}
          />
          <span className="font-mono">
            {liveStatus === "live"
              ? pulse > 0
                ? `+${pulse} live update${pulse === 1 ? "" : "s"}`
                : "live"
              : liveStatus}
          </span>
        </span>
        <button
          onClick={onOpenMarkdown}
          className="pill text-[12px] flex items-center gap-1.5 hover:border-foreground/30 transition-colors bg-card"
          title="Preview, download or edit the markdown file for this property"
        >
          <FileText className="w-3.5 h-3.5" />
          <span>Property file</span>
        </button>
      </div>
    </div>
  </div>
);
