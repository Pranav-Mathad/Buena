import { ClipboardList } from "lucide-react";
import type { Fact, Section } from "@/lib/types";
import type { SourceModalSource } from "@/components/SourceModal";
import { cn } from "@/lib/utils";
import { FactRow } from "./FactRow";

/** Facts imported from master data: no event behind them, "Label: value" text. */
const isMasterData = (s: Section) =>
  s.facts.length > 0 && s.facts.every((f) => f.source === "stammdaten" && !f.eventId);

const LABELLED = /^([^:\n]{1,40}):\s+(.+)$/s;

function splitLabel(value: string): [string | null, string] {
  const m = value.match(LABELLED);
  return m ? [m[1], m[2]] : [null, value];
}

/**
 * The property file's sections. Uncertain sections (open questions for a
 * human) and context sections (building and WEG activity) are styled apart
 * and tagged with `data-section-kind`. Master data renders as a two-column
 * sheet; everything else as one row per fact. Facts in uncertain sections
 * have no fact row to edit, so they get no edit button.
 */
export const SectionList = ({
  sections,
  onOpenSource,
  onEdit,
}: {
  sections: Section[];
  onOpenSource: (s: SourceModalSource) => void;
  onEdit: (fact: Fact, sectionTitle: string) => void;
}) => (
  <div className="space-y-10">
    {sections.map((s) => {
      const isContext = s.isContext;
      const isUncertain = s.isUncertain;
      const sheet = !isUncertain && !isContext && isMasterData(s);
      return (
        <section
          key={s.title}
          data-section-kind={isUncertain ? "uncertain" : isContext ? "context" : "facts"}
          className={cn(
            isUncertain && "rounded-2xl border border-warning/30 bg-warning/[0.04] p-5 sm:p-6",
          )}
        >
          {isContext && <hr className="border-border mb-6" />}
          <div className="flex items-center gap-3">
            {isUncertain && <ClipboardList className="w-4 h-4 text-warning" />}
            <h2
              className={cn(
                "font-semibold tracking-tight",
                isContext ? "text-[17px] text-muted-foreground" : "text-[20px]",
              )}
            >
              {s.title}
            </h2>
            {s.facts.length > 0 && (
              <span className="font-mono text-[11px] text-muted-foreground px-2 py-0.5 rounded-full border border-border bg-card">
                {s.facts.length}
              </span>
            )}
            {sheet && (
              <span className="ml-auto font-mono text-[11px] text-muted-foreground">
                from master data
              </span>
            )}
          </div>
          {sheet ? (
            <MasterDataSheet facts={s.facts} />
          ) : (
            <div className="mt-3 space-y-0.5">
              {s.facts.map((f, i) => (
                <FactRow
                  key={f.factId ?? i}
                  fact={f}
                  onOpenSource={onOpenSource}
                  onEdit={s.isUncertain ? undefined : (fact) => onEdit(fact, s.title)}
                />
              ))}
            </div>
          )}
        </section>
      );
    })}
  </div>
);

/** Master data as a two-column label/value sheet, filled column by column. */
const MasterDataSheet = ({ facts }: { facts: Fact[] }) => {
  const rows = Math.ceil(facts.length / 2);
  return (
    <dl
      className="mt-4 grid grid-cols-1 sm:grid-cols-2 sm:grid-flow-col gap-x-10 rounded-2xl border border-border bg-card px-5 py-1 shadow-card"
      style={{ gridTemplateRows: `repeat(${rows}, auto)` }}
    >
      {facts.map((f, i) => {
        const [label, value] = splitLabel(f.value);
        return (
          <div
            key={i}
            className={cn(
              "flex items-baseline justify-between gap-6 py-2.5 border-b border-border/80",
              i === facts.length - 1 && "border-0",
              i === rows - 1 && "sm:border-0",
            )}
          >
            {label ? (
              <>
                <dt className="text-[13px] text-muted-foreground shrink-0">{label}</dt>
                <dd className="text-[14px] font-medium text-right min-w-0 break-words">{value}</dd>
              </>
            ) : (
              <dd className="text-[14px] font-medium min-w-0 break-words">{value}</dd>
            )}
          </div>
        );
      })}
    </dl>
  );
};
