import "@testing-library/jest-dom";

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => {},
  }),
});

// EventSource is not implemented in jsdom. Tests drive it through
// MockEventSource.instances and emit().
type Listener = (event: MessageEvent) => void;

export class MockEventSource {
  static instances: MockEventSource[] = [];
  readonly url: string;
  onerror: (() => void) | null = null;
  private listeners = new Map<string, Listener[]>();

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: Listener) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  emit(type: string, data: unknown) {
    this.emitRaw(type, JSON.stringify(data));
  }

  /** Deliver `raw` as the event's data without encoding it. */
  emitRaw(type: string, raw: string) {
    const event = new MessageEvent(type, { data: raw });
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }

  close() {}
}

Object.defineProperty(globalThis, "EventSource", { writable: true, value: MockEventSource });

beforeEach(() => {
  MockEventSource.instances = [];
});
