import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { queryKeys } from "@/hooks/queries";
import { useLivePropertyUpdates } from "@/hooks/use-live-property-updates";
import { MockEventSource } from "@/test/setup";

function setup() {
  const queryClient = new QueryClient();
  const invalidate = vi.spyOn(queryClient, "invalidateQueries");
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const hook = renderHook(() => useLivePropertyUpdates("p-1"), { wrapper });
  const stream = MockEventSource.instances.find((es) => es.url.endsWith("/properties/p-1/events"));
  return { hook, stream: stream!, invalidate };
}

describe("useLivePropertyUpdates", () => {
  it("goes live on hello and refetches the file on each fact update", () => {
    const { hook, stream, invalidate } = setup();
    expect(stream.url).toBe("http://api.test/properties/p-1/events");
    expect(hook.result.current).toEqual({ liveStatus: "connecting", pulse: 0 });

    act(() => stream.emit("hello", {}));
    expect(hook.result.current.liveStatus).toBe("live");

    act(() => stream.emit("fact_update", { event_id: "e-1", facts_written: 2 }));
    expect(hook.result.current.pulse).toBe(1);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.propertyFile("p-1") });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.properties });
  });

  it("ignores malformed payloads and reports a dropped connection", () => {
    const { hook, stream, invalidate } = setup();

    act(() => stream.emitRaw("fact_update", "{not json"));
    expect(hook.result.current.pulse).toBe(0);
    expect(invalidate).not.toHaveBeenCalled();

    act(() => stream.onerror?.());
    expect(hook.result.current.liveStatus).toBe("offline");
  });
});
