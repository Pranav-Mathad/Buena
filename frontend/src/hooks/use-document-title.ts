import { useEffect } from "react";

const SITE = "Keystone";

/** Sets the browser tab title to "<title> · Keystone" while the page is mounted. */
export function useDocumentTitle(title: string | undefined) {
  useEffect(() => {
    document.title = title ? `${title} · ${SITE}` : SITE;
  }, [title]);
}
