import { describe, expect, it } from "vitest";
import { countIssues } from "./use-catalog-issues";
import type { CatalogStatus, CatalogHealth } from "@/lib/adminApi";

function catalog(id: string, health: CatalogHealth): CatalogStatus {
  return {
    id,
    collection: id.toUpperCase(),
    title: id,
    health,
    chunks_done: 0,
    chunks_total: 0,
    n_records: 0,
  };
}

describe("countIssues", () => {
  it("shows nothing when every declared catalog is present", () => {
    const { count, label } = countIssues([catalog("ned-lvs", "present")]);
    expect(count).toBe(0);
    expect(label).toBe("All declared catalogs present");
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

  it("is zero for a deployment that declares nothing", () => {
    expect(countIssues([]).count).toBe(0);
  });
});
