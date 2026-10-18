import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AskPanel } from "@/components/AskPanel";
import { mockFetch, propertyDTO, renderWithProviders } from "@/test/utils";

afterEach(() => vi.unstubAllGlobals());

const source = { id: "e-1", channel: "email", date: "2026-04-23", snippet: "Wasser läuft seit gestern" };

const askReply = (overrides: Record<string, unknown>) => ({
  answer: null,
  confidence: "high",
  reasoning: "",
  sources: [],
  partial_context: [],
  elapsed_ms: 1200,
  retrieved_count: 4,
  model: "test-model",
  ...overrides,
});

async function ask(reply: Record<string, unknown>) {
  const fetchMock = mockFetch({
    "GET /properties": [propertyDTO()],
    "POST /properties/p-1/ask": askReply(reply),
  });
  renderWithProviders(<AskPanel propertyId="p-1" />);
  await userEvent.type(screen.getByRole("textbox"), "What is going on?");
  await userEvent.click(screen.getByRole("button", { name: "Ask" }));
  return fetchMock;
}

describe("AskPanel", () => {
  it("shows an answer with its sources", async () => {
    const fetchMock = await ask({ status: "answered", answer: "Water leak since Tuesday.", sources: [source] });

    expect(await screen.findByText("Water leak since Tuesday.")).toBeInTheDocument();
    expect(screen.getByText("answered")).toBeInTheDocument();
    expect(screen.getByText("Wasser läuft seit gestern")).toBeInTheDocument();
    const [, init] = fetchMock.mock.calls.find(([url]) => String(url).endsWith("/ask"))!;
    expect(JSON.parse(String(init?.body))).toEqual({ question: "What is going on?" });
  });

  it("says it does not know, with the partial context it found", async () => {
    await ask({
      status: "insufficient_context",
      confidence: "insufficient context",
      reasoning: "No message mentions a renewal.",
      partial_context: [source],
    });

    expect(await screen.findByText("I don't know")).toBeInTheDocument();
    expect(screen.getByText("No message mentions a renewal.")).toBeInTheDocument();
    expect(screen.getByText("Wasser läuft seit gestern")).toBeInTheDocument();
  });

  it("refuses questions about data Keystone does not hold", async () => {
    await ask({
      status: "out_of_scope",
      confidence: "out of scope",
      reasoning: "Bank details live in the ERP.",
    });

    const label = await screen.findByText("out of scope");
    expect(label).toBeInTheDocument();
    expect(screen.getByText("Bank details live in the ERP.")).toBeInTheDocument();
    expect(within(document.body).queryByText("answered")).not.toBeInTheDocument();
  });
});
