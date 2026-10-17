import { useState, type ReactNode } from "react";
import { Download, X, FileText, ExternalLink } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { fetchPropertyMarkdown, type MarkdownDoc } from "@/lib/api";
import { stripInlineMarkdown } from "@/lib/text";
import { PrimaryButton, Swatch } from "./ui-bits";
import { SourceModal, type SourceModalSource } from "./SourceModal";

type MarkdownModalProps = {
  propertyId: string;
  propertyName: string;
  open: boolean;
  onClose: () => void;
};

// Preview-only modal for the canonical property markdown. The body
// is rendered live from facts + stammdaten + uncertainties on every
// open. Older entries collapse to one-liner pointers; clicking the
// "open detail" link on any compact pointer pops the SourceModal so
// the operator can read the original event without leaving the page.
// "Download" saves the verbatim markdown as a .md file.
export const MarkdownModal = ({
  propertyId,
  propertyName,
  open,
  onClose,
}: MarkdownModalProps) => {
  const [source, setSource] = useState<SourceModalSource | null>(null);

  const { data, isLoading, isError, refetch, error } = useQuery<MarkdownDoc>({
    queryKey: ["property-markdown", propertyId],
    queryFn: () => fetchPropertyMarkdown(propertyId),
    enabled: open,
    staleTime: 0,
    gcTime: 0,
  });

  if (!open) return null;

  const handleDownload = () => {
    if (!data) return;
    const blob = new Blob([data.body], {
      type: "text/markdown;charset=utf-8",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${slugify(propertyName)}.md`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 animate-fade-in">
      <div
        className="absolute inset-0 bg-foreground/30 backdrop-blur-[2px]"
        onClick={onClose}
      />
      <div className="relative bg-card border border-border rounded-2xl max-w-4xl w-full max-h-[88vh] flex flex-col overflow-hidden shadow-card">
        <div className="flex items-start justify-between p-6 pb-4 border-b border-border">
          <div className="flex items-center gap-3 min-w-0">
            <Swatch Icon={FileText} />
            <div className="min-w-0">
              <h2 className="text-[20px] font-semibold tracking-tight truncate">
                Property file (markdown)
              </h2>
              <p className="text-[13px] text-muted-foreground truncate">
                {propertyName}
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="w-9 h-9 rounded-lg flex items-center justify-center hover:bg-foreground/[0.04] shrink-0"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="px-6 py-3 border-b border-border flex items-center justify-between gap-3 flex-wrap">
          <span className="font-mono text-[11px] text-muted-foreground">
            Recent activity in detail · older entries collapse to pointers
          </span>
          <PrimaryButton onClick={handleDownload} disabled={!data}>
            <Download className="w-3.5 h-3.5" /> Download .md
          </PrimaryButton>
        </div>

        <div className="flex-1 overflow-y-auto bg-subdued p-6">
          {isLoading && (
            <div className="text-muted-foreground text-[14px]">
              Loading markdown…
            </div>
          )}
          {isError && (
            <div className="text-warning text-[14px]">
              Could not load markdown:{" "}
              {error instanceof Error ? error.message : "backend unreachable"}{" "}
              —{" "}
              <button onClick={() => refetch()} className="underline">
                retry
              </button>
            </div>
          )}
          {!isLoading && !isError && data && (
            <RenderedMarkdown body={data.body} onOpenSource={setSource} />
          )}
        </div>
      </div>

      <SourceModal
        source={source}
        onOpenChange={(o) => !o && setSource(null)}
      />
    </div>
  );
};

// ---------------------------------------------------------------------------
// Markdown → JSX renderer (focused subset)
//
// Handles the patterns the backend renderer actually emits:
//   #/##/### headings, _italic line_, ``- `` bullets, **bold**, *italic*,
//   `code`, and inline ``[label](url)`` links.
// Links pointing at ``/events/<uuid>/source`` render as a clickable
// "open detail" button that pops the SourceModal; other links render
// as plain underlined anchors.

const EVENT_LINK_RE =
  /\/events\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/source/i;

function RenderedMarkdown({
  body,
  onOpenSource,
}: {
  body: string;
  onOpenSource: (s: SourceModalSource) => void;
}) {
  const lines = body.split("\n");
  const blocks: ReactNode[] = [];
  let key = 0;

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const line = raw.trimEnd();

    if (!line.trim()) {
      blocks.push(<div key={key++} className="h-2" />);
      continue;
    }

    const headingMatch = line.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      const level = headingMatch[1].length;
      const text = headingMatch[2];
      blocks.push(renderHeading(text, level, key++, onOpenSource));
      continue;
    }

    const bulletMatch = line.match(/^\s*-\s+(.*)$/);
    if (bulletMatch) {
      blocks.push(
        <div key={key++} className="flex gap-2 items-baseline pl-1 py-1">
          <span className="text-muted-foreground select-none">•</span>
          <div className="flex-1 text-[14px] leading-relaxed">
            {renderInline(bulletMatch[1], onOpenSource)}
          </div>
        </div>,
      );
      continue;
    }

    // Whole-line italic paragraph: _text_
    const italicLineMatch = line.match(/^_([^_].*[^_])_$/);
    if (italicLineMatch) {
      blocks.push(
        <p
          key={key++}
          className="text-[13px] italic text-muted-foreground"
        >
          {renderInline(italicLineMatch[1], onOpenSource)}
        </p>,
      );
      continue;
    }

    blocks.push(
      <p key={key++} className="text-[14px] leading-relaxed">
        {renderInline(line, onOpenSource)}
      </p>,
    );
  }

  return <div className="space-y-1">{blocks}</div>;
}

function renderHeading(
  text: string,
  level: number,
  key: number,
  onOpenSource: (s: SourceModalSource) => void,
) {
  const sizing =
    level === 1
      ? "text-[20px] font-semibold tracking-tight mt-2"
      : level === 2
        ? "text-[17px] font-semibold tracking-tight mt-5 pt-3 border-t border-border first:border-0 first:pt-0 first:mt-0"
        : "text-[14px] font-semibold tracking-tight mt-4 text-muted-foreground uppercase tracking-wider";
  return (
    <div key={key} className={sizing}>
      {renderInline(text, onOpenSource)}
    </div>
  );
}

// Walk a single line and emit React nodes for each meaningful token.
// Order matters — links before bold, bold before italic, italic before
// code, so nested patterns peel off cleanly.
function renderInline(
  source: string,
  onOpenSource: (s: SourceModalSource) => void,
): ReactNode[] {
  const nodes: ReactNode[] = [];
  let cursor = 0;
  let nodeKey = 0;
  const linkRe = /\[([^\]]+)\]\(([^)]+)\)/g;
  let match: RegExpExecArray | null;
  while ((match = linkRe.exec(source))) {
    if (match.index > cursor) {
      nodes.push(
        ...renderEmphasis(source.slice(cursor, match.index), nodeKey),
      );
      nodeKey += 4;
    }
    const label = stripInlineMarkdown(match[1]);
    const url = match[2];
    const eventMatch = EVENT_LINK_RE.exec(url);
    if (eventMatch) {
      const eventId = eventMatch[1];
      nodes.push(
        <button
          key={`link-${nodeKey++}`}
          type="button"
          onClick={() =>
            onOpenSource({
              eventId,
              channel: "event",
              date: "",
              snippet: "Loading the original event…",
            })
          }
          className="inline-flex items-center gap-1 text-[13px] underline underline-offset-2 hover:text-foreground transition-colors text-muted-foreground"
        >
          {label}
          <ExternalLink className="w-3 h-3" />
        </button>,
      );
    } else {
      nodes.push(
        <a
          key={`anchor-${nodeKey++}`}
          href={url}
          target="_blank"
          rel="noreferrer"
          className="underline underline-offset-2 hover:text-foreground"
        >
          {label}
        </a>,
      );
    }
    cursor = match.index + match[0].length;
  }
  if (cursor < source.length) {
    nodes.push(...renderEmphasis(source.slice(cursor), nodeKey));
  }
  return nodes;
}

// Apply **bold**, *italic*, `code` styling within a non-link span.
function renderEmphasis(text: string, baseKey: number): ReactNode[] {
  let out = text;
  // Strip and convert in passes — we wrap in marker tokens then split.
  // Simpler approach: regex replace with HTML-like markers, then split.
  // Cheap & correct enough for the renderer's actual output shape.
  const tokens: ReactNode[] = [];
  const splitRe =
    /(\*\*[^*\n]+\*\*|`[^`\n]+`|(?:^|[^\w*])\*[^*\n]+\*(?=[^\w*]|$))/g;
  let lastIdx = 0;
  let m: RegExpExecArray | null;
  let key = baseKey;
  while ((m = splitRe.exec(out))) {
    if (m.index > lastIdx) {
      tokens.push(
        <span key={`t-${key++}`}>{out.slice(lastIdx, m.index)}</span>,
      );
    }
    const raw = m[1];
    if (raw.startsWith("**")) {
      tokens.push(
        <strong key={`b-${key++}`} className="font-semibold">
          {raw.slice(2, -2)}
        </strong>,
      );
    } else if (raw.startsWith("`")) {
      tokens.push(
        <code
          key={`c-${key++}`}
          className="font-mono text-[12.5px] px-1 py-0.5 rounded bg-foreground/[0.06]"
        >
          {raw.slice(1, -1)}
        </code>,
      );
    } else {
      // pattern includes a leading non-word char; preserve it
      const leadIdx = raw.indexOf("*");
      const lead = raw.slice(0, leadIdx);
      const inner = raw.slice(leadIdx + 1, -1);
      if (lead) {
        tokens.push(<span key={`l-${key++}`}>{lead}</span>);
      }
      tokens.push(
        <em key={`i-${key++}`} className="italic">
          {inner}
        </em>,
      );
    }
    lastIdx = m.index + raw.length;
  }
  if (lastIdx < out.length) {
    tokens.push(<span key={`t-${key++}`}>{out.slice(lastIdx)}</span>);
  }
  return tokens;
}

function slugify(name: string): string {
  return (
    name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/(^-|-$)+/g, "")
      .slice(0, 80) || "property"
  );
}
