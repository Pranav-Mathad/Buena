/** Splits a stored message into subject and body; text without headers is all body. */
export function parseRawMessage(raw: string): { subject: string; body: string } {
  const text = raw.replace(/\r\n/g, "\n").trim();
  const [head, ...rest] = text.split(/\n\n/);
  if (!/^(From|To|Subject|Date):/im.test(head)) return { subject: "", body: text };
  const subject = head.match(/^Subject:\s*(.*)$/im)?.[1]?.trim() ?? "";
  return { subject, body: rest.join("\n\n").trim() };
}
