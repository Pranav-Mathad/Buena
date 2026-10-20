import { screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import PropertyDetail from "@/pages/PropertyDetail";
import { mockFetch, propertyDTO, renderWithProviders } from "@/test/utils";

afterEach(() => vi.unstubAllGlobals());

const fact = (value: string, factId: string | null) => ({
  value,
  source: "email",
  date: "23 Apr",
  date_iso: "2026-04-23",
  confidence: 0.9,
  urgent: false,
  reason: null,
  section: "maintenance",
  field: "x",
  event_id: "e-1",
  fact_id: factId,
  human_edited: false,
  edited_by: null,
});

describe("PropertyDetail", () => {
  it("renders every section and marks uncertain and context sections", async () => {
    mockFetch({
      "GET /properties": [propertyDTO()],
      "GET /properties/p-1/file": {
        id: "p-1",
        name: "Musterstraße 26 WE 29",
        address: "Musterstraße 26, 10115 Berlin",
        sections: [
          {
            title: "Maintenance",
            is_uncertain: false,
            is_context: false,
            facts: [fact("Leak in kitchen", "f-1")],
          },
          {
            title: "Needs review",
            is_uncertain: true,
            is_context: false,
            facts: [fact("Rent may change", null)],
          },
          {
            title: "WEG Context",
            is_uncertain: false,
            is_context: true,
            facts: [fact("Hausgeld paid", null)],
          },
        ],
      },
    });

    renderWithProviders(<PropertyDetail />, { route: "/properties/p-1", path: "/properties/:id" });

    const maintenance = (await screen.findByRole("heading", { name: "Maintenance" })).closest(
      "section",
    )!;
    const review = screen.getByRole("heading", { name: "Needs review" }).closest("section")!;
    const context = screen.getByRole("heading", { name: "WEG Context" }).closest("section")!;

    expect(maintenance).toHaveAttribute("data-section-kind", "facts");
    expect(review).toHaveAttribute("data-section-kind", "uncertain");
    expect(context).toHaveAttribute("data-section-kind", "context");
    expect(within(maintenance).getByText("Leak in kitchen")).toBeInTheDocument();
    // Extracted facts can be corrected by an operator; open uncertainties cannot.
    expect(within(maintenance).getByRole("button", { name: /edit/ })).toBeInTheDocument();
    expect(within(review).queryByRole("button", { name: /edit/ })).not.toBeInTheDocument();
  });
});
