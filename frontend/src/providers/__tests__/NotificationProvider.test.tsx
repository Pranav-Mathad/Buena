import { act, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { NotificationsBell } from "@/components/NotificationsBell";
import { NotificationProvider } from "@/providers/NotificationProvider";
import { MockEventSource } from "@/test/setup";
import { renderWithProviders } from "@/test/utils";

describe("NotificationProvider", () => {
  it("turns server-sent fact updates into unread notifications, once per event", async () => {
    renderWithProviders(
      <NotificationProvider>
        <NotificationsBell />
      </NotificationProvider>,
    );

    const stream = MockEventSource.instances.find((es) => es.url.endsWith("/portfolio/events"));
    expect(stream?.url).toBe("http://api.test/portfolio/events");
    expect(screen.getByRole("button", { name: "Notifications" })).toBeInTheDocument();

    const update = { event_id: "e-1", property_id: "p-1", summary: "Heating reported", facts_written: 1 };
    act(() => {
      stream!.emit("fact_update", update);
      stream!.emit("fact_update", update);
    });

    expect(await screen.findByRole("button", { name: "Notifications (1 unread)" })).toBeInTheDocument();
  });
});
