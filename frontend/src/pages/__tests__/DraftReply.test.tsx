import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import DraftReply from "@/pages/DraftReply";
import { parseRawMessage } from "@/lib/message";
import { mockFetch, propertyDTO, renderWithProviders } from "@/test/utils";

afterEach(() => vi.unstubAllGlobals());

const emptyInbox = { rows: [], total_inbox_size: 0, relevant_count: 0, keyword: null };

describe("DraftReply", () => {
  it("says drafting needs a language model when the API answers 503", async () => {
    mockFetch({
      "GET /properties": [propertyDTO()],
      "GET /properties/p-1/inbox": emptyInbox,
      "POST /draft-reply": new Response(
        JSON.stringify({ detail: "Reply drafting is unavailable: No LLM provider is configured" }),
        { status: 503 },
      ),
    });
    renderWithProviders(<DraftReply />);

    await userEvent.type(screen.getByRole("textbox", { name: "Body" }), "Die Heizung ist kalt.");
    await userEvent.click(screen.getByRole("button", { name: "Draft reply" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Reply drafting needs a language model");
    expect(alert).toHaveTextContent("No LLM provider is configured");
    expect(alert).not.toHaveTextContent("could not be reached");
  });

  it("fills the form from a recent message of the property", async () => {
    mockFetch({
      "GET /properties": [propertyDTO()],
      "GET /properties/p-1/inbox": {
        ...emptyInbox,
        total_inbox_size: 1,
        rows: [
          {
            event_id: "e-1",
            date: "2026-10-14",
            sender: "Laura Schmitz <laura.schmitz@example.de>",
            subject: "Lärm im Treppenhaus",
            preview: "Guten Tag,",
            relevant: false,
          },
        ],
      },
      "GET /events/e-1/raw":
        "From: Laura Schmitz <laura.schmitz@example.de>\nSubject: Lärm im Treppenhaus\n\nGuten Tag,\n\nseit einer Woche gibt es nachts Lärm.",
    });
    renderWithProviders(<DraftReply />);

    await userEvent.click(await screen.findByRole("button", { name: /Lärm im Treppenhaus/ }));

    expect(screen.getByRole("textbox", { name: "Subject" })).toHaveValue("Lärm im Treppenhaus");
    expect(screen.getByRole("textbox", { name: "Body" })).toHaveValue(
      "Guten Tag,\n\nseit einer Woche gibt es nachts Lärm.",
    );
  });
});

describe("parseRawMessage", () => {
  it("treats text without headers as the body", () => {
    expect(parseRawMessage("Hallo, die Tür klemmt.")).toEqual({
      subject: "",
      body: "Hallo, die Tür klemmt.",
    });
  });
});
