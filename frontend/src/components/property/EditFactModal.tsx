import { useState } from "react";
import { Pencil, X } from "lucide-react";
import { editFact } from "@/lib/api";
import type { Fact } from "@/lib/types";
import { PrimaryButton, SecondaryButton, Swatch } from "@/components/ui-bits";

export type EditFactState = {
  fact: Fact;
  section: string;
};

export const EditFactModal = ({
  state,
  propertyId,
  onClose,
  onSaved,
}: {
  state: EditFactState;
  propertyId: string;
  onClose: () => void;
  onSaved: () => void;
}) => {
  const [value, setValue] = useState(state.fact.value);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const trimmed = value.trim();
  const unchanged = trimmed === state.fact.value.trim();
  const empty = trimmed.length === 0;
  const canSave = !saving && !unchanged && !empty;

  const handleSave = async () => {
    if (!canSave || !state.fact.factId) return;
    setSaving(true);
    setError(null);
    try {
      await editFact(propertyId, state.fact.factId, trimmed);
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : "edit failed");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 animate-fade-in">
      <div className="absolute inset-0 bg-foreground/30 backdrop-blur-[2px]" onClick={onClose} />
      <div className="relative bg-card border border-border rounded-2xl max-w-xl w-full p-7 shadow-card">
        <div className="flex items-start justify-between mb-1">
          <div className="flex items-center gap-3">
            <Swatch Icon={Pencil} />
            <div>
              <h2 className="text-[20px] font-semibold tracking-tight">Edit fact</h2>
              <p className="font-mono text-[11px] text-muted-foreground uppercase tracking-wider mt-0.5">
                {state.section} · {state.fact.source}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="w-9 h-9 rounded-lg flex items-center justify-center hover:bg-foreground/[0.04]">
            <X className="w-4 h-4" />
          </button>
        </div>
        <p className="mt-4 text-[12.5px] text-muted-foreground leading-relaxed">
          Saving marks this fact as <span className="font-mono">human-edited</span>. Future
          extractor proposals against the same field will be skipped — the
          operator becomes the source of truth until you change it again.
        </p>
        <textarea
          value={value}
          onChange={(e) => setValue(e.target.value)}
          autoFocus
          rows={5}
          className="mt-4 w-full rounded-lg border border-border bg-background px-3 py-2.5 text-[14px] leading-snug focus:outline-none focus:ring-2 focus:ring-foreground/20 resize-y"
        />
        {error && (
          <p className="mt-3 text-[12.5px] font-mono text-warning leading-relaxed">
            {error}
          </p>
        )}
        <div className="mt-5 pt-4 border-t border-border flex items-center justify-between">
          <span className="font-mono text-[11px] text-muted-foreground">
            {unchanged ? "no changes" : empty ? "value cannot be empty" : `${trimmed.length} chars`}
          </span>
          <div className="flex items-center gap-2">
            <SecondaryButton onClick={onClose} disabled={saving}>
              Cancel
            </SecondaryButton>
            <PrimaryButton onClick={handleSave} disabled={!canSave}>
              {saving ? (
                <>
                  <span className="dot bg-primary-foreground animate-pulse-soft" />
                  Saving…
                </>
              ) : (
                "Save edit"
              )}
            </PrimaryButton>
          </div>
        </div>
      </div>
    </div>
  );
};
