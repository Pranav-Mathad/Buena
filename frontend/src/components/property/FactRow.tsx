import { useState } from "react";
import { ChevronDown, ExternalLink, Pencil } from "lucide-react";
import { relativeTime } from "@/lib/api";
import type { Fact } from "@/lib/types";
import type { SourceModalSource } from "@/components/SourceModal";
import { cn } from "@/lib/utils";

const Tooltip = ({ children, label }: { children: React.ReactNode; label: string }) => (
  <span className="relative group/tt">
    {children}
    <span className="pointer-events-none absolute left-1/2 -translate-x-1/2 -top-9 whitespace-nowrap rounded-md bg-foreground text-background text-[11px] font-mono px-2 py-1 opacity-0 group-hover/tt:opacity-100 transition-opacity z-20">
      {label}
    </span>
  </span>
);

const TRUNCATE_THRESHOLD = 240;

export const FactRow = ({
  fact,
  urgent,
  onOpenSource,
  onEdit,
}: {
  fact: Fact;
  urgent?: boolean;
  onOpenSource: (s: SourceModalSource) => void;
  onEdit?: (f: Fact) => void;
}) => {
  const [expanded, setExpanded] = useState(false);
  const isUrgent = urgent || fact.urgent;
  const isLong = (fact.value?.length ?? 0) > TRUNCATE_THRESHOLD;
  const editable = !!fact.factId && !!onEdit;
  const isOperatorEdit = fact.humanEdited || fact.source === "operator";

  return (
    <div
      className={cn(
        "group relative py-3.5 px-4 -mx-4 rounded-xl border-b border-border last:border-0 transition-colors hover:bg-foreground/[0.02]",
        isUrgent && "bg-warning/[0.05]",
      )}
    >
      {isUrgent && (
        <span className="absolute left-0 top-4 bottom-4 w-[3px] rounded-full bg-warning" />
      )}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2 min-w-0">
          <span
            className={cn(
              "font-mono text-[10px] uppercase tracking-[0.12em] px-1.5 py-[3px] rounded-md leading-none",
              isOperatorEdit
                ? "bg-success/10 text-success ring-1 ring-success/30"
                : "bg-foreground/[0.05] text-muted-foreground",
            )}
          >
            {fact.source}
          </span>
          <Tooltip label={fact.dateIso ?? fact.date}>
            <span className="font-mono text-[11.5px] text-muted-foreground cursor-default">
              {fact.dateIso ? relativeTime(fact.dateIso) : fact.date}
            </span>
          </Tooltip>
          {isUrgent && (
            <span className="font-mono text-[10px] uppercase tracking-[0.12em] text-warning px-1.5 py-[3px] rounded-md bg-warning/10 ring-1 ring-warning/30 leading-none">
              urgent
            </span>
          )}
          {isOperatorEdit && fact.editedBy && (
            <Tooltip label={`Edited by ${fact.editedBy}`}>
              <span className="font-mono text-[10.5px] text-success cursor-default">
                ✓ human-edited
              </span>
            </Tooltip>
          )}
        </div>
        <div className="flex items-center gap-3">
          {editable && (
            <Tooltip label="Edit this fact (overrides extractor proposals)">
              <button
                type="button"
                onClick={() => onEdit?.(fact)}
                className="inline-flex items-center gap-1 text-[11.5px] font-mono text-muted-foreground hover:text-foreground transition-colors opacity-0 group-hover:opacity-100 focus-visible:opacity-100"
              >
                <Pencil className="w-3 h-3" />
                edit
              </button>
            </Tooltip>
          )}
          <Tooltip label="Open original event">
            <button
              type="button"
              onClick={() =>
                onOpenSource({
                  channel: fact.source,
                  date: fact.date,
                  snippet: fact.value,
                  eventId: fact.eventId,
                })
              }
              className="inline-flex items-center gap-1 text-[11.5px] font-mono text-muted-foreground hover:text-foreground transition-colors"
            >
              source
              <ExternalLink className="w-3 h-3" />
            </button>
          </Tooltip>
        </div>
      </div>

      <p
        className={cn(
          "mt-2 text-[14.5px] leading-relaxed whitespace-pre-line",
          !expanded && isLong && "line-clamp-3",
        )}
      >
        {fact.value}
      </p>

      {isLong && (
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="mt-2 inline-flex items-center gap-1 text-[12px] text-muted-foreground hover:text-foreground transition-colors"
        >
          {expanded ? "Show less" : "Read more"}
          <ChevronDown className={cn("w-3 h-3 transition-transform", expanded && "rotate-180")} />
        </button>
      )}

      {fact.reason && (
        <p className="mt-2 text-[12.5px] italic text-muted-foreground leading-snug">
          {fact.reason}
        </p>
      )}
    </div>
  );
};
