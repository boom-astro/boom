// Counts the catalogs a deployment declares but does not actually have, for the
// badge on the admin link.
//
// The drift check deliberately never acts on what it finds -- ingesting a
// catalog is hours to days of work and stays an explicit decision. That makes
// it easy for drift to sit unnoticed, which is exactly what this badge is for.
import { useEffect, useState } from "react";
import {
  fetchCatalogStatus,
  fetchEnrichmentStatus,
  type CatalogStatus,
  type EnrichmentDrift,
} from "@/lib/adminApi";

/** How often to re-check. Drift changes on the order of hours, not seconds. */
const POLL_MS = 60_000;

export type CatalogIssues = {
  /** Catalogs that are not fully present. 0 renders no badge. */
  count: number;
  /** Human-readable breakdown, for the badge's title. */
  label: string;
};

/** Counted per survey, not per stale set. */
export function countEnrichmentIssues(drift: EnrichmentDrift[]): {
  count: number;
  parts: string[];
} {
  const published = drift.filter((d) => d.current_set !== null);
  const unknown = published.filter((d) => d.drift_unknown);
  const stale = published.filter(
    (d) => !d.drift_unknown && (d.stale_sets.length > 0 || d.has_unstamped),
  );
  return {
    count: stale.length + unknown.length,
    parts: [
      ...stale.map((d) => `${d.survey.toUpperCase()} enrichment is stale`),
      ...unknown.map(
        (d) => `${d.survey.toUpperCase()} enrichment drift is unknown`,
      ),
    ],
  };
}

/** Catalogs the pipeline is configured to query but cannot fully read.
 *
 *  The table lists every catalog this release can ingest; most deployments
 *  will never ingest all of them, and an un-ingested catalog nobody
 *  crossmatches against is not a problem to fix. What counts is the gap
 *  between what crossmatch config asks the alert pipeline to query and what is
 *  actually in the database. */
export function countIssues(catalogs: CatalogStatus[]): CatalogIssues {
  const configured = catalogs.filter((c) => c.crossmatched);
  const missing = configured.filter((c) => c.health === "missing").length;
  // Partial is counted too, and is arguably the worse state: the collection
  // exists, so a crossmatch against it succeeds and quietly returns fewer
  // matches than it should rather than failing.
  //
  // "ingesting" is the same shortfall with a run working on it, and is not
  // counted. Counting it would light this badge for the hours an ingest takes,
  // every time somebody started one from the page the badge points at -- which
  // is how an operator learns to read past it, the same reason `external` is
  // left out below.
  const partial = configured.filter((c) => c.health === "partial").length;
  // An unknown slug cannot be fixed by clicking Ingest -- it needs a catalog
  // definition or a config correction -- but it is still something to address.
  // A collection built outside the ingest path is not: it has no definition on
  // purpose, so counting it would leave the badge permanently lit and the
  // operator reading past it.
  const unknown = catalogs.filter((c) => c.health === "undeclared").length;

  const parts: string[] = [];
  if (missing) parts.push(`${missing} missing`);
  if (partial) parts.push(`${partial} partially ingested`);
  if (unknown) parts.push(`${unknown} with no definition`);

  return {
    count: missing + partial + unknown,
    label: parts.length
      ? `Catalogs: ${parts.join(", ")}`
      : "Every crossmatched catalog is present",
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
        const [catalogs, drift] = await Promise.all([
          fetchCatalogStatus(),
          // Counted as no enrichment issues when it fails, so a newly missing
          // catalog still lights the badge: before this the rejection left the
          // whole previous count standing until the endpoint recovered.
          fetchEnrichmentStatus().catch(() => []),
        ]);
        if (!cancelled) {
          const catalogIssues = countIssues(catalogs);
          const enrichmentIssues = countEnrichmentIssues(drift);
          const parts = [
            ...(catalogIssues.count ? [catalogIssues.label] : []),
            ...enrichmentIssues.parts,
          ];
          setIssues({
            count: catalogIssues.count + enrichmentIssues.count,
            label: parts.length ? parts.join("; ") : "Nothing needs attention",
          });
        }
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
