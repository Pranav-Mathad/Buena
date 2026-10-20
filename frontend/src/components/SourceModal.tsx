import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import { useEventRaw } from "@/hooks/queries";

// Facts and Ask citations describe their source differently (a label string
// versus {id, channel, date, snippet}); each call site adapts to this shape.
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

  const { data, isLoading, isError } = useEventRaw(eventId);

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
              {eventId && <span className="ml-2 opacity-60">event {eventId.slice(0, 8)}…</span>}
            </div>
            {eventId && isLoading && (
              <p className="text-[14px] text-muted-foreground italic">loading original event…</p>
            )}
            {eventId && isError && (
              <>
                <p className="text-[15px] leading-relaxed whitespace-pre-line">{source.snippet}</p>
                <p className="text-[12px] italic text-warning">
                  Could not load the original event; showing the extracted snippet.
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
                <p className="text-[15px] leading-relaxed whitespace-pre-line">{source.snippet}</p>
                <p className="text-[12px] italic text-muted-foreground border-t border-border pt-4">
                  Imported from master data. There is no source document to open.
                </p>
              </>
            )}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
};
