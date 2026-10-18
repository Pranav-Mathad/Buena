import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { SectionList } from "@/components/property/SectionList";
import type { Fact, Section } from "@/lib/types";

const fact = (value: string, factId?: string): Fact => ({
  value,
  source: "email",
  date: "2026-04-23",
  eventId: "e-1",
  factId,
});

const sections: Section[] = [
  { title: "Maintenance", facts: [fact("Leak in kitchen", "f-1")] },
  { title: "Needs review", isUncertain: true, facts: [fact("Rent may change")] },
];

describe("SectionList", () => {
  it("passes the edited fact and its section title to onEdit", () => {
    const onEdit = vi.fn();
    render(<SectionList sections={sections} onOpenSource={vi.fn()} onEdit={onEdit} />);

    const maintenance = screen.getByRole("heading", { name: "Maintenance" }).closest("section")!;
    fireEvent.click(within(maintenance).getByRole("button", { name: /edit/ }));

    expect(onEdit).toHaveBeenCalledWith(sections[0].facts[0], "Maintenance");
  });

  it("opens the source of a fact with its event id", () => {
    const onOpenSource = vi.fn();
    render(<SectionList sections={sections} onOpenSource={onOpenSource} onEdit={vi.fn()} />);

    const review = screen.getByRole("heading", { name: "Needs review" }).closest("section")!;
    expect(within(review).queryByRole("button", { name: /edit/ })).not.toBeInTheDocument();
    fireEvent.click(within(review).getByRole("button", { name: /source/ }));

    expect(onOpenSource).toHaveBeenCalledWith({
      channel: "email",
      date: "2026-04-23",
      snippet: "Rent may change",
      eventId: "e-1",
    });
  });
});
