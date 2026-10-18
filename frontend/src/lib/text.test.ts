import { describe, expect, it } from "vitest";
import { parseBriefingMarkdown, stripInlineMarkdown } from "./text";

describe("stripInlineMarkdown", () => {
  it("removes emphasis, code, links and stray headings but keeps the words", () => {
    expect(
      stripInlineMarkdown("## Status\n**Heizung** ist _seit_ `gestern` kalt, siehe [Mail](https://x)"),
    ).toBe("Status\nHeizung ist seit gestern kalt, siehe Mail");
  });
});

describe("parseBriefingMarkdown", () => {
  it("splits headings, bullets and paragraphs, flags gaps and drops source links", () => {
    const items = parseBriefingMarkdown(
      [
        "## Key context",
        "",
        "- Water damage in the kitchen [source: abc](/events/abc/source)",
        "- Gap: no confirmation of the rent increase",
        "Lücken sind markiert.",
      ].join("\n"),
    );

    expect(items).toEqual([
      { type: "heading", text: "Key context" },
      { type: "bullet", text: "Water damage in the kitchen", hasGap: false },
      { type: "bullet", text: "Gap: no confirmation of the rent increase", hasGap: true },
      { type: "paragraph", text: "Lücken sind markiert." },
    ]);
  });
});
