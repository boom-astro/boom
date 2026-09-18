import { describe, expect, it } from "vitest";
import { countIssues } from "./use-catalog-issues";
import type { CatalogStatus, CatalogHealth } from "@/lib/adminApi";

function catalog(
  id: string,
  health: CatalogHealth,
  crossmatched = true,
): CatalogStatus {
  return {
    id,
    collection: id.toUpperCase(),
    title: id,
    health,
    chunks_done: 0,
    chunks_total: 0,
    n_records: 0,
    crossmatched,
  };
}

describe("countIssues", () => {
  it("shows nothing when every declared catalog is present", () => {
    const { count, label } = countIssues([catalog("ned-lvs", "present")]);
    expect(count).toBe(0);
    expect(label).toBe("Every crossmatched catalog is present");
  });

  it("counts a missing catalog", () => {
    const { count, label } = countIssues([
      catalog("ned-lvs", "missing"),
      catalog("2mass", "present"),
    ]);
    expect(count).toBe(1);
    expect(label).toContain("1 missing");
  });

  it("counts a partially ingested catalog", () => {
    // Arguably the worse state: the collection exists, so a crossmatch against
    // it succeeds and quietly returns fewer matches rather than failing.
    const { count, label } = countIssues([catalog("2mass", "partial")]);
    expect(count).toBe(1);
    expect(label).toContain("partially ingested");
  });

  it("counts a catalog with no definition", () => {
    // Not fixable by clicking Ingest, but still something to address.
    const { count, label } = countIssues([catalog("LSPSC", "undeclared")]);
    expect(count).toBe(1);
    expect(label).toContain("no definition");
  });

  it("breaks down a mixture in the label", () => {
    const { count, label } = countIssues([
      catalog("a", "missing"),
      catalog("b", "missing"),
      catalog("c", "partial"),
      catalog("d", "undeclared"),
      catalog("e", "present"),
    ]);
    expect(count).toBe(4);
    expect(label).toBe("Catalogs: 2 missing, 1 partially ingested, 1 with no definition");
  });

  it("ignores a catalog nothing crossmatches against", () => {
    // The table lists every catalog this release can ingest. Most deployments
    // never ingest all of them, and one the pipeline never queries is simply
    // available -- not a problem to fix.
    const { count, label } = countIssues([
      catalog("vsx", "missing", false),
      catalog("galex", "missing", false),
    ]);
    expect(count).toBe(0);
    expect(label).toBe("Every crossmatched catalog is present");
  });

  it("counts an undeclared name even when config does not crossmatch it", () => {
    // A slug with no definition is a config error either way: it cannot be
    // ingested and it will never match anything.
    expect(countIssues([catalog("LSPSCC", "undeclared", false)]).count).toBe(1);
  });

  it("is zero for a deployment that declares nothing", () => {
    expect(countIssues([]).count).toBe(0);
  });
});
