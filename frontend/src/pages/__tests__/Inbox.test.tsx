import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import Inbox from "@/pages/Inbox";
import { mockFetch, renderWithProviders } from "@/test/utils";

afterEach(() => vi.unstubAllGlobals());

const signal = {
  id: "s-1",
  property_id: "p-1",
  property_name: "Musterstraße 26 WE 29",
  type: "recurring_maintenance",
  severity: "urgent",
  message: "Third heating complaint in four months.",
  status: "pending",
  created_at: "2026-04-23T08:00:00Z",
};

describe("Inbox", () => {
  it("approves a signal through the API and refreshes the list", async () => {
    let pending = [signal];
    const fetchMock = mockFetch({
      "GET /signals": () => pending,
      "GET /admin/uncertainties": [],
      "GET /admin/rejected": [],
      "POST /signals/s-1/approve": () => {
        pending = [];
        return { signal_id: "s-1", status: "resolved", outbox_id: "o-1" };
      },
    });

    renderWithProviders(<Inbox />);
    await userEvent.click(await screen.findByRole("button", { name: "Approve" }));

    expect(fetchMock).toHaveBeenCalledWith(
      "http://api.test/signals/s-1/approve",
      expect.objectContaining({ method: "POST" }),
    );
    expect(await screen.findByText(/No pending signals/)).toBeInTheDocument();
  });
});
