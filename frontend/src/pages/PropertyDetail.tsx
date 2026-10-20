import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { ChevronLeft } from "lucide-react";
import { useProperties, usePropertyFile, queryKeys } from "@/hooks/queries";
import { useBriefing } from "@/hooks/use-briefing";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { useLivePropertyUpdates } from "@/hooks/use-live-property-updates";
import { Card } from "@/components/ui-bits";
import { AskPanel } from "@/components/AskPanel";
import { MarkdownModal } from "@/components/MarkdownModal";
import { SourceModal, type SourceModalSource } from "@/components/SourceModal";
import { BriefingBanner } from "@/components/property/BriefingBanner";
import { BriefingModal } from "@/components/property/BriefingModal";
import { EditFactModal, type EditFactState } from "@/components/property/EditFactModal";
import { PropertyFilePanel } from "@/components/property/PropertyFilePanel";
import { PropertyHeader } from "@/components/property/PropertyHeader";

const PropertyDetail = () => {
  const { id } = useParams();
  const [markdownOpen, setMarkdownOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"file" | "ask">("file");
  const [openSource, setOpenSource] = useState<SourceModalSource | null>(null);
  const [editing, setEditing] = useState<EditFactState | null>(null);
  const queryClient = useQueryClient();
  const { liveStatus, pulse } = useLivePropertyUpdates(id);
  const briefing = useBriefing(id);

  // Shares the cache with the portfolio listing, so it resolves instantly
  // when navigating in from there.
  const { data: livePropertiesRaw } = useProperties();
  const properties = livePropertiesRaw ?? [];

  // The structured property file. A failed request shows an error card; an
  // empty file shows the "no facts yet" state.
  const { data: liveFile, isError: fileError } = usePropertyFile(id);

  const propertyMeta = properties.find((p) => p.id === id);
  useDocumentTitle(propertyMeta?.name ?? "Property");

  // An empty file (no sections) means extraction has not written any facts
  // for this property yet; the panel says so instead of showing other data.
  const sections = liveFile && liveFile.sections.length > 0 ? liveFile.sections : null;

  // Briefing and Ask are offered for any property with at least one event:
  // with sparse context the answer says so, which is more useful than hiding
  // the feature.
  const hasContent = (propertyMeta?.stats?.events ?? 0) > 0;

  if (!propertyMeta) {
    const loading = livePropertiesRaw === undefined;
    return (
      <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 py-12 animate-fade-in">
        <Link
          to="/"
          className="inline-flex items-center gap-1 text-[13px] text-muted-foreground hover:text-foreground transition-colors"
        >
          <ChevronLeft className="w-4 h-4" /> Portfolio
        </Link>
        <Card className="mt-8 p-10 text-center">
          {loading ? (
            <p className="text-muted-foreground text-[14px] font-mono">loading property…</p>
          ) : (
            <>
              <h2 className="text-[20px] font-semibold">Property not found</h2>
              <p className="text-[14px] text-muted-foreground mt-2">
                {id ? `No property with id ${id} in the backend.` : "No property id in the URL."}
              </p>
            </>
          )}
        </Card>
      </div>
    );
  }

  return (
    <div className="max-w-[1400px] mx-auto px-5 sm:px-6 lg:px-10 py-8 lg:py-10 animate-fade-in">
      <Link
        to="/"
        className="inline-flex items-center gap-1 text-[13px] text-muted-foreground hover:text-foreground transition-colors"
      >
        <ChevronLeft className="w-4 h-4" /> Portfolio
      </Link>

      <PropertyHeader
        property={propertyMeta}
        liveStatus={liveStatus}
        pulse={pulse}
        onOpenMarkdown={() => setMarkdownOpen(true)}
      />

      {hasContent && (
        <div className="mt-8">
          <BriefingBanner onGenerate={briefing.start} generating={briefing.generating} />
          {briefing.error && (
            <p className="mt-2 text-[12px] font-mono text-warning">
              briefing failed: {briefing.error}
            </p>
          )}
        </div>
      )}

      {/* Mobile tabs */}
      <div className="mt-8 lg:hidden flex gap-1 border-b border-border">
        {(["file", "ask"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setActiveTab(t)}
            className={`px-4 py-2 text-[14px] border-b-2 -mb-px transition-colors ${
              activeTab === t
                ? "border-foreground text-foreground"
                : "border-transparent text-muted-foreground"
            }`}
          >
            {t === "file" ? "Property file" : "Ask"}
          </button>
        ))}
      </div>

      <div className="mt-10 grid grid-cols-1 lg:grid-cols-5 gap-10 xl:gap-14">
        {/* Property file: 60% on desktop */}
        <div className={`lg:col-span-3 ${activeTab === "ask" ? "hidden lg:block" : ""}`}>
          <PropertyFilePanel
            fileError={fileError}
            sections={sections}
            property={propertyMeta}
            onOpenSource={setOpenSource}
            onEdit={(fact, section) => setEditing({ fact, section })}
          />
        </div>

        {/* Ask: 40% on desktop, sticky */}
        <div className={`lg:col-span-2 ${activeTab === "file" ? "hidden lg:block" : ""}`}>
          <div className="lg:sticky lg:top-24">
            <Card className="p-6">
              <AskPanel unavailable={!hasContent} propertyId={id} />
            </Card>
          </div>
        </div>
      </div>

      {briefing.open && (
        <BriefingModal
          items={briefing.items}
          latencyMs={briefing.latencyMs}
          source={briefing.source}
          onClose={briefing.close}
          onRegenerate={briefing.regenerate}
        />
      )}
      {id && (
        <MarkdownModal
          propertyId={id}
          propertyName={propertyMeta.name}
          open={markdownOpen}
          onClose={() => setMarkdownOpen(false)}
        />
      )}
      <SourceModal source={openSource} onOpenChange={(open) => !open && setOpenSource(null)} />
      {editing && id && (
        <EditFactModal
          state={editing}
          propertyId={id}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null);
            queryClient.invalidateQueries({ queryKey: queryKeys.propertyFile(id) });
            queryClient.invalidateQueries({ queryKey: queryKeys.properties });
            queryClient.invalidateQueries({ queryKey: queryKeys.propertyMarkdown(id) });
          }}
        />
      )}
    </div>
  );
};

export default PropertyDetail;
