import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, NetworkError, fetchProperties, fetchPropertyFile } from "./api";
import { mockFetch, propertyDTO } from "@/test/utils";

afterEach(() => vi.unstubAllGlobals());

describe("API client", () => {
  it("maps property summaries to the UI shape and keeps the featured flag", async () => {
    mockFetch({
      "GET /properties": [
        propertyDTO(),
        propertyDTO({ id: "p-2", is_hero: false, needs_review: 0 }),
      ],
    });

    const [featured, other] = await fetchProperties();

    expect(featured).toMatchObject({
      id: "p-1",
      stats: { events: 12, facts: 5, needsReview: 1 },
      openIssues: 2,
      isHero: true,
    });
    expect(other.isHero).toBe(false);
  });

  it("maps a property file, flags uncertain/context sections and strips markdown", async () => {
    mockFetch({
      "GET /properties/p-1/file": {
        id: "p-1",
        name: "WE 29",
        address: "Musterstraße 26",
        sections: [
          {
            title: "Maintenance",
            is_uncertain: false,
            is_context: false,
            facts: [
              {
                value: "**Leak** in the `kitchen`",
                source: "email",
                date: "23 Apr",
                date_iso: "2026-04-23",
                confidence: 0.9,
                urgent: true,
                reason: null,
                section: "maintenance",
                field: "open_water_damage",
                event_id: "e-1",
                fact_id: "f-1",
                human_edited: false,
                edited_by: null,
              },
            ],
          },
          { title: "Needs review", is_uncertain: true, is_context: false, facts: [] },
        ],
      },
    });

    const file = await fetchPropertyFile("p-1");

    expect(file.sections[0].facts[0]).toMatchObject({
      value: "Leak in the kitchen",
      urgent: true,
      eventId: "e-1",
      factId: "f-1",
    });
    expect(file.sections[0].isUncertain).toBeUndefined();
    expect(file.sections[1].isUncertain).toBe(true);
  });

  it("raises ApiError with the server's detail on a non-2xx response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () => new Response(JSON.stringify({ detail: "Ask is unavailable" }), { status: 503 }),
      ),
    );

    const error = await fetchProperties().catch((e: unknown) => e);

    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 503, detail: "Ask is unavailable", path: "/properties" });
  });

  it("raises NetworkError when the API cannot be reached", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Promise.reject(new TypeError("Failed to fetch"))),
    );

    await expect(fetchProperties()).rejects.toBeInstanceOf(NetworkError);
  });
});
