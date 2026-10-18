import type { AskResult } from "./api";

// The shape the panel renders, adapted from the API result by adaptAskResult.
export type AskResponse =
  | {
      status: "answered";
      answer: string;
      confidence: string;
      sources: { id: string; channel: string; date: string; snippet: string }[];
      elapsed_ms: number;
      retrieved_count: number;
    }
  | {
      status: "insufficient_context";
      answer: null;
      confidence: string;
      reasoning: string;
      partial_context: { id: string; channel: string; date: string; snippet: string }[];
      elapsed_ms: number;
      retrieved_count: number;
    }
  | {
      status: "out_of_scope";
      answer: null;
      confidence: string;
      reasoning: string;
      elapsed_ms: number;
    };

export function adaptAskResult(r: AskResult): AskResponse {
  if (r.status === "answered") {
    return {
      status: "answered",
      answer: r.answer ?? "",
      confidence: r.confidence,
      sources: r.sources,
      elapsed_ms: r.elapsedMs,
      retrieved_count: r.retrievedCount,
    };
  }
  if (r.status === "out_of_scope") {
    return {
      status: "out_of_scope",
      answer: null,
      confidence: r.confidence,
      reasoning: r.reasoning,
      elapsed_ms: r.elapsedMs,
    };
  }
  return {
    status: "insufficient_context",
    answer: null,
    confidence: r.confidence,
    reasoning: r.reasoning,
    partial_context: r.partialContext,
    elapsed_ms: r.elapsedMs,
    retrieved_count: r.retrievedCount,
  };
}
