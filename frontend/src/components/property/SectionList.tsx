import { ClipboardList } from "lucide-react";
import type { Fact, Section } from "@/lib/types";
import type { SourceModalSource } from "@/components/SourceModal";
import { FactRow } from "./FactRow";

/**
 * The property file's sections. Uncertain sections (open questions for a
 * human) and context sections (building and WEG activity) are styled apart
 * and tagged with `data-section-kind`. Facts in uncertain sections have no
 * fact row to edit, so they get no edit button.
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
      return (
        <section
          key={s.title}
          data-section-kind={isUncertain ? "uncertain" : isContext ? "context" : "facts"}
          className={isUncertain ? "bg-subdued rounded-2xl p-6 border border-border" : ""}
        >
          {isContext && <hr className="border-border mb-6" />}
          <div className="flex items-center gap-3">
            {isUncertain && <ClipboardList className="w-4 h-4 text-muted-foreground" />}
            <h2 className={`font-semibold tracking-tight ${isContext ? "text-[18px] text-muted-foreground" : "text-[22px]"}`}>
              {s.title}
            </h2>
            {s.facts.length > 0 && (
              <span className="font-mono text-[11.5px] text-muted-foreground px-2 py-0.5 rounded-full border border-border bg-card">
                {s.facts.length}
              </span>
            )}
          </div>
          <div className="mt-4 space-y-1">
            {s.facts.map((f, i) => (
              <FactRow
                key={f.factId ?? i}
                fact={f}
                onOpenSource={onOpenSource}
                onEdit={
                  s.isUncertain
                    ? undefined
                    : (fact) => onEdit(fact, s.title)
                }
              />
            ))}
          </div>
        </section>
      );
    })}
  </div>
);
