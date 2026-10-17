// Cleans LLM-generated text for display in plain UI components.
//
// Pioneer/Claude sometimes emits markdown (`**bold**`, `[link](url)`,
// inline backticks, `## headings`) inside responses even when the
// prompt asks for plain text. This module is the single chokepoint
// that converts those artefacts into readable prose so callers don't
// have to remember to call it themselves.
//
// `stripInlineMarkdown` is intentionally conservative — it removes
// formatting markers but preserves the underlying text.

const PATTERNS: Array<[RegExp, string]> = [
  // [text](url)  →  text
  [/\[([^\]]+)\]\(([^)]+)\)/g, "$1"],
  // **bold**  →  bold
  [/\*\*([^*\n]+)\*\*/g, "$1"],
  // __bold__  →  bold
  [/__([^_\n]+)__/g, "$1"],
  // *italic*  →  italic   (must run after **bold**)
  [/(^|\W)\*([^*\n]+)\*(\W|$)/g, "$1$2$3"],
  // _italic_  →  italic
  [/(^|\W)_([^_\n]+)_(\W|$)/g, "$1$2$3"],
  // `code`  →  code
  [/`([^`\n]+)`/g, "$1"],
  // Stray ## / ### / #### headings inside a paragraph
  [/(^|\n)\s*#{1,6}\s+/g, "$1"],
];

export function stripInlineMarkdown(input: string | null | undefined): string {
  if (!input) return "";
  let out = input;
  for (const [pattern, replacement] of PATTERNS) {
    out = out.replace(pattern, replacement);
  }
  // Collapse runs of three+ spaces, trim trailing whitespace per line.
  return out
    .split("\n")
    .map((line) => line.replace(/[\t ]{2,}/g, " ").trimEnd())
    .join("\n");
}

export type BriefingItem =
  | { type: "heading"; text: string }
  | { type: "bullet"; text: string; hasGap: boolean }
  | { type: "paragraph"; text: string };

// Parse the onboarding markdown into structured items the modal can
// render without ever putting markdown markers on screen. Drops empty
// lines and `[source: ...]` deep-links — the renderer keeps source
// links in the property file, but the briefing prose reads cleaner
// without them.
export function parseBriefingMarkdown(markdown: string): BriefingItem[] {
  const items: BriefingItem[] = [];
  for (const raw of markdown.split("\n")) {
    const line = raw.replace(/\s*\[source:[^\]]*\]\([^)]+\)/g, "");
    const trimmed = line.trim();
    if (!trimmed) continue;

    const headingMatch = trimmed.match(/^#{1,6}\s+(.*)$/);
    if (headingMatch) {
      items.push({
        type: "heading",
        text: stripInlineMarkdown(headingMatch[1]),
      });
      continue;
    }

    const bulletMatch = trimmed.match(/^[-*]\s+(.*)$/);
    if (bulletMatch) {
      const text = stripInlineMarkdown(bulletMatch[1]);
      items.push({
        type: "bullet",
        text,
        hasGap: /Lücke:|Gap:/i.test(text),
      });
      continue;
    }

    items.push({ type: "paragraph", text: stripInlineMarkdown(trimmed) });
  }
  return items;
}
