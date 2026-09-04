// Counts the catalogs a deployment declares but does not actually have, for the
// badge on the admin link.
//
// The drift check deliberately never acts on what it finds -- ingesting a
// catalog is hours to days of work and stays an explicit decision. That makes
// it easy for drift to sit unnoticed, which is exactly what this badge is for.
import { useEffect, useState } from "react";
import { fetchCatalogStatus, type CatalogStatus } from "@/lib/adminApi";

/** How often to re-check. Drift changes on the order of hours, not seconds. */
const POLL_MS = 60_000;

export type CatalogIssues = {
  /** Catalogs that are not fully present. 0 renders no badge. */
  count: number;
  /** Human-readable breakdown, for the badge's title. */
  label: string;
};

/** Catalogs that are declared but not fully in the database. */
export function countIssues(catalogs: CatalogStatus[]): CatalogIssues {
  const missing = catalogs.filter((c) => c.health === "missing").length;
  // Partial is counted too, and is arguably the worse state: the collection
  // exists, so a crossmatch against it succeeds and quietly returns fewer
  // matches than it should rather than failing.
  const partial = catalogs.filter((c) => c.health === "partial").length;
  // An unknown slug cannot be fixed by clicking Ingest -- it needs a catalog
  // definition or a config correction -- but it is still something to address.
  const unknown = catalogs.filter((c) => c.health === "undeclared").length;

  const parts: string[] = [];
  if (missing) parts.push(`${missing} missing`);
  if (partial) parts.push(`${partial} partially ingested`);
  if (unknown) parts.push(`${unknown} with no definition`);

  return {
    count: missing + partial + unknown,
    label: parts.length ? `Catalogs: ${parts.join(", ")}` : "All declared catalogs present",
  };
}

export function useCatalogIssues(enabled: boolean): CatalogIssues {
  const [issues, setIssues] = useState<CatalogIssues>({ count: 0, label: "" });

  useEffect(() => {
    if (!enabled) {
      setIssues({ count: 0, label: "" });
      return;
    }
    let cancelled = false;
    let timer: number;

    async function poll() {
      try {
        const catalogs = await fetchCatalogStatus();
        if (!cancelled) setIssues(countIssues(catalogs));
      } catch {
        // A failed check must not render a misleading zero, so the previous
        // count stands until the next poll succeeds.
      }
      if (!cancelled) timer = window.setTimeout(poll, POLL_MS);
    }
    poll();

    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [enabled]);

  return issues;
}
