import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import DraftReply from "@/pages/DraftReply";
import { mockFetch, propertyDTO, renderWithProviders } from "@/test/utils";

afterEach(() => vi.unstubAllGlobals());

describe("DraftReply", () => {
  it("says drafting needs an LLM key when the API answers 503", async () => {
    mockFetch({
      "GET /properties": [propertyDTO()],
      "POST /draft-reply": new Response(
        JSON.stringify({ detail: "Reply drafting is unavailable: No LLM provider is configured" }),
        { status: 503 },
      ),
    });
    renderWithProviders(<DraftReply />);

    await userEvent.click(screen.getByRole("button", { name: "Heating outage (new)" }));
    await userEvent.click(screen.getByRole("button", { name: "Draft reply" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Reply drafting needs an LLM provider key");
    expect(alert).toHaveTextContent("No LLM provider is configured");
    expect(alert).not.toHaveTextContent("unreachable");
  });
});
