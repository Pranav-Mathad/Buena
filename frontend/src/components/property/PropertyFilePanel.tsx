import { Link } from "react-router-dom";
import type { Fact, Property, Section } from "@/lib/types";
import type { SourceModalSource } from "@/components/SourceModal";
import { Card } from "@/components/ui-bits";
import { SectionList } from "./SectionList";

/**
 * The property file column: an error card when the request failed, the
 * sections when there are any, otherwise a "no facts yet" card that links
 * to the featured property.
 */
export const PropertyFilePanel = ({
  fileError,
  sections,
  property,
  featured,
  onOpenSource,
  onEdit,
}: {
  fileError: boolean;
  sections: Section[] | null;
  property: Property;
  featured: Property | undefined;
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
    <Card className="p-8 space-y-6">
      <div>
        <h2 className="text-[22px] font-semibold tracking-tight">No facts yet</h2>
        <p className="text-[15px] text-muted-foreground mt-3 leading-relaxed">
          Keystone has {property.stats.events} events for this property but has not written any
          facts from them. Facts appear here as the worker extracts them; the page updates on its
          own when it does.
        </p>
      </div>
      {featured && featured.id !== property.id && (
        <div className="text-[13px] text-muted-foreground border-t border-border pt-5">
          <Link
            to={`/properties/${featured.id}`}
            className="underline underline-offset-2 hover:text-foreground"
          >
            Open the featured property: {featured.name}
          </Link>
        </div>
      )}
    </Card>
  );
