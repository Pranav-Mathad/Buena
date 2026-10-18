import { Lightbulb } from "lucide-react";
import { Card, PrimaryButton, Swatch } from "@/components/ui-bits";

export const BriefingBanner = ({
  onGenerate,
  generating,
}: {
  onGenerate: () => void;
  generating: boolean;
}) => (
  <Card className="relative overflow-hidden p-6">
    <div className="absolute inset-0 bg-gradient-to-r from-warning/[0.05] via-transparent to-success/[0.05] animate-pulse-subtle pointer-events-none" />
    <div className="relative flex flex-wrap items-center justify-between gap-6">
      <div className="flex items-start gap-4 min-w-0">
        <Swatch Icon={Lightbulb} className="mt-0.5" />
        <div className="min-w-0">
          <h2 className="text-[18px] font-semibold tracking-tight">
            60-second briefing for a new property manager
          </h2>
          <p className="text-[13px] text-muted-foreground mt-1 leading-relaxed">
            Reads the property file, surfaces what's urgent, what's stable, and what's missing — in
            the manager's language.
          </p>
        </div>
      </div>
      <PrimaryButton onClick={onGenerate} disabled={generating} className="px-5 py-2.5 shrink-0">
        {generating ? (
          <>
            <span className="dot bg-primary-foreground animate-pulse-soft" />
            Generating...
          </>
        ) : (
          "Generate briefing"
        )}
      </PrimaryButton>
    </div>
  </Card>
);
