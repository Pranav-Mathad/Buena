import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { vi } from "vitest";

/** Render `ui` inside a fresh QueryClient and a router at `route`. */
export function renderWithProviders(
  ui: ReactElement,
  { route = "/", path }: { route?: string; path?: string } = {},
) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter
        initialEntries={[route]}
        future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
      >
        {path ? (
          <Routes>
            <Route path={path} element={ui} />
          </Routes>
        ) : (
          ui
        )}
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

type Handler = (init: RequestInit | undefined) => unknown;

/**
 * Replace global fetch with routes keyed by "METHOD /path" (query string
 * ignored). Unrouted requests fail the test with a 404. A route may return a
 * Response to control the status. Returns the mock so tests can assert on calls.
 */
export function mockFetch(routes: Record<string, Handler | unknown>) {
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const key = `${(init?.method ?? "GET").toUpperCase()} ${url.pathname}`;
    if (!(key in routes)) {
      return new Response(JSON.stringify({ detail: `unrouted ${key}` }), { status: 404 });
    }
    const route = routes[key];
    const body = typeof route === "function" ? (route as Handler)(init) : route;
    if (body instanceof Response) return body;
    return new Response(typeof body === "string" ? body : JSON.stringify(body), { status: 200 });
  });
  vi.stubGlobal("fetch", fn);
  return fn;
}

export const propertyDTO = (overrides: Record<string, unknown> = {}) => ({
  id: "p-1",
  name: "Musterstraße 26 WE 29",
  address: "Musterstraße 26, 10115 Berlin",
  owner_name: null,
  building_year_built: 1928,
  events: 12,
  facts: 5,
  needs_review: 1,
  open_issues: 2,
  is_hero: true,
  ...overrides,
});
