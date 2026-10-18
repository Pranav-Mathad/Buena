import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useBriefing } from "@/hooks/use-briefing";
import { mockFetch } from "@/test/utils";

afterEach(() => vi.unstubAllGlobals());

describe("useBriefing", () => {
  it("opens the modal with the parsed briefing when the fetch succeeds", async () => {
    mockFetch({
      "GET /admin/properties/p-1/onboarding": {
        property_id: "p-1",
        markdown: "## Urgent\n- Heating fails twice a month",
        cached: true,
      },
    });
    const { result } = renderHook(() => useBriefing("p-1"));

    await act(() => result.current.start());

    expect(result.current.open).toBe(true);
    expect(result.current.cached).toBe(true);
    expect(result.current.error).toBeNull();
    expect(result.current.items.some((it) => it.type === "bullet")).toBe(true);
  });

  it("stays closed and reports the error when the fetch fails", async () => {
    mockFetch({});
    const { result } = renderHook(() => useBriefing("p-1"));

    await act(() => result.current.start());

    expect(result.current.open).toBe(false);
    expect(result.current.generating).toBe(false);
    expect(result.current.error).toMatch(/unrouted|404/);
  });
});
