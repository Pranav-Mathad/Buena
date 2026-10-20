import type { Fact, Property, Section } from "@/lib/types";
import type { SourceModalSource } from "@/components/SourceModal";
import { Card } from "@/components/ui-bits";
import { SectionList } from "./SectionList";

/**
 * The property file column: an error card when the request failed, the
 * sections when there are any, otherwise a "no facts yet" card.
 */
export const PropertyFilePanel = ({
  fileError,
  sections,
  property,
  onOpenSource,
  onEdit,
}: {
  fileError: boolean;
  sections: Section[] | null;
  property: Property;
  onOpenSource: (s: SourceModalSource) => void;
  onEdit: (fact: Fact, sectionTitle: string) => void;
}) =>
  fileError ? (
    <Card className="p-8">
      <h2 className="text-[22px] font-semibold tracking-tight">Could not load the property file</h2>
      <p className="text-[15px] text-muted-foreground mt-3 leading-relaxed">
        The request to the API failed. Check that the backend is running and reload the page.
      </p>
    </Card>
  ) : sections ? (
    <SectionList sections={sections} onOpenSource={onOpenSource} onEdit={onEdit} />
  ) : (
    <Card className="p-8">
      <h2 className="text-[22px] font-semibold tracking-tight">No facts yet</h2>
      <p className="text-[15px] text-muted-foreground mt-3 leading-relaxed">
        {property.stats.events === 0
          ? "No events have been received for this property yet."
          : `${property.stats.events} event${property.stats.events === 1 ? " has" : "s have"} been received for this property, but no facts have been extracted from ${property.stats.events === 1 ? "it" : "them"} yet.`}{" "}
        This page updates on its own as facts are written.
      </p>
    </Card>
  );
