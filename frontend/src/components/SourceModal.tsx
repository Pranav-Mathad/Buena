import { useQuery } from "@tanstack/react-query";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import { fetchEventRaw } from "@/lib/api";

// TODO(phase-12): reconcile two divergent source shapes (Fact.source: string
// vs Q&A sources: {id, channel, date, snippet}) when wiring to backend.
// Each call site adapts to this normalized shape for now.
export type SourceModalSource = {
  channel: string;
  date: string;
  snippet: string;
  // When present, the modal fetches the original event body from
  // ``GET /events/<id>/raw`` and replaces the snippet preview with the
  // full redacted content. Mock fact rows leave this undefined and the
  // modal stays in snippet-only mode.
  eventId?: string;
};

export const SourceModal = ({
  source,
  onOpenChange,
}: {
  source: SourceModalSource | null;
  onOpenChange: (open: boolean) => void;
}) => {
  const eventId = source?.eventId;

  const { data, isLoading, isError } = useQuery({
    queryKey: ["event-raw", eventId],
    queryFn: () => fetchEventRaw(eventId!),
    enabled: !!eventId,
    staleTime: 60_000,
  });

  return (
    <Dialog open={source !== null} onOpenChange={onOpenChange}>
      <DialogContent className="bg-card border-border max-w-xl sm:rounded-2xl">
        <DialogTitle className="sr-only">Source detail</DialogTitle>
        <DialogDescription className="sr-only">
          Source content for this fact or citation.
        </DialogDescription>
        {source && (
          <div className="space-y-4">
            <div className="font-mono text-[12px] text-muted-foreground">
              {source.channel} · {source.date}
              {eventId && (
                <span className="ml-2 opacity-60">
                  event {eventId.slice(0, 8)}…
                </span>
              )}
            </div>
            {eventId && isLoading && (
              <p className="text-[14px] text-muted-foreground italic">
                loading original event…
              </p>
            )}
            {eventId && isError && (
              <>
                <p className="text-[15px] leading-relaxed whitespace-pre-line">
                  {source.snippet}
                </p>
                <p className="text-[12px] italic text-warning">
                  Could not load original event — showing extracted snippet.
                </p>
              </>
            )}
            {eventId && data && (
              <pre className="text-[13px] leading-relaxed whitespace-pre-wrap font-mono bg-subdued rounded-lg p-4 border border-border max-h-[60vh] overflow-y-auto">
                {data}
              </pre>
            )}
            {!eventId && (
              <>
                <p className="text-[15px] leading-relaxed whitespace-pre-line">
                  {source.snippet}
                </p>
                <p className="text-[12px] italic text-muted-foreground border-t border-border pt-4">
                  In production, this would link to the original email, PDF, or bank record.
                </p>
              </>
            )}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
};
