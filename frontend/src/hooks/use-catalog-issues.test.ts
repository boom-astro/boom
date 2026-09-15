import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import {
  countEnrichmentIssues,
  countIssues,
  useCatalogIssues,
} from "./use-catalog-issues";
import * as adminApi from "@/lib/adminApi";
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

  it("does not count a collection that is built outside the ingest path", () => {
    // LSDR10 and TNS have no definition on purpose, and every deployment
    // crossmatches at least one of them. Counted, the badge would be lit on
    // every deployment forever, which is how a real issue goes unread.
    const { count, label } = countIssues([
      catalog("LSDR10", "external"),
      catalog("TNS", "external"),
      catalog("2mass", "present"),
    ]);
    expect(count).toBe(0);
    expect(label).toBe("Every crossmatched catalog is present");
  });

  it("is zero for a deployment that declares nothing", () => {
    expect(countIssues([]).count).toBe(0);
  });
});

describe("countEnrichmentIssues", () => {
  const clean = {
    survey: "ztf",
    current_set: 7,
    stale_sets: [],
    accepted_sets: [],
    has_unstamped: false,
    drift_unknown: false,
  };

  it("reports nothing when every alert is at the current set", () => {
    expect(countEnrichmentIssues([clean]).count).toBe(0);
  });

  it("counts a survey once, however many stale sets it has", () => {
    // An operator acts on "ZTF needs reprocessing", not on each set: one run
    // covers all of them.
    const { count, parts } = countEnrichmentIssues([
      {
        ...clean,
        stale_sets: [
          { id: 5, changed: ["btsbot"] },
          { id: 6, changed: ["sso"] },
        ],
      },
    ]);
    expect(count).toBe(1);
    expect(parts).toEqual(["ZTF enrichment is stale"]);
  });

  it("counts alerts that predate stamping as drift", () => {
    // They carry no set, so what produced them cannot be established.
    expect(countEnrichmentIssues([{ ...clean, has_unstamped: true }]).count).toBe(1);
  });

  it("does not count a survey with no published set", () => {
    // No worker has published one, so nothing can be shown to be stale and
    // there is no action the badge could point at. The table still says so.
    expect(
      countEnrichmentIssues([
        { ...clean, current_set: null, has_unstamped: true },
      ]).count,
    ).toBe(0);
  });

  it("counts a survey whose drift could not be determined", () => {
    // The query timed out, so stale_sets is empty for want of an answer. A
    // zero here would read as "nothing is stale", which is the one thing that
    // is not known.
    const { count, parts } = countEnrichmentIssues([
      { ...clean, drift_unknown: true },
    ]);
    expect(count).toBe(1);
    expect(parts).toEqual(["ZTF enrichment drift is unknown"]);
  });

  it("does not count a set an operator accepted", () => {
    // Accepted sets are absent from stale_sets, so the survey reads clean.
    expect(
      countEnrichmentIssues([{ ...clean, accepted_sets: [6] }]).count,
    ).toBe(0);
  });
});

describe("countIssues and an ingest in flight", () => {
  it("does not count a catalog a run is working on", () => {
    // Counting it would light the badge for the hours an ingest takes, every
    // time one was started from the page the badge points at.
    const { count, label } = countIssues([catalog("ned-lvs", "ingesting")]);
    expect(count).toBe(0);
    expect(label).toBe("Every crossmatched catalog is present");
  });

  it("still counts one that was left half done", () => {
    // Same shortfall, nothing working on it: this is the one to act on.
    const { count, label } = countIssues([catalog("ned-lvs", "partial")]);
    expect(count).toBe(1);
    expect(label).toContain("1 partially ingested");
  });
});

describe("useCatalogIssues", () => {
  let status: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    // `shouldAdvanceTime` so testing-library's `waitFor`, which polls on real
    // timers, still makes progress while the hook's own minute-long interval
    // is under our control.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    status = vi.spyOn(adminApi, "fetchCatalogStatus");
    // The hook reads enrichment drift in the same `Promise.all`, so leaving it
    // unstubbed makes every poll reject and the count read a misleading zero.
    vi.spyOn(adminApi, "fetchEnrichmentStatus").mockResolvedValue([]);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("does not poll at all for a non-admin", async () => {
    // Every caller of this hook is in the sidebar, which renders for everybody.
    // A non-admin polling an admin-only route would be 403 every minute.
    status.mockResolvedValue([catalog("ned-lvs", "missing")]);

    const { result } = renderHook(() => useCatalogIssues(false));

    expect(status).not.toHaveBeenCalled();
    expect(result.current.count).toBe(0);
  });

  it("clears the count when it stops being enabled", async () => {
    // An admin whose status is revoked mid-session must not be left with a
    // badge from the last poll they were allowed to make.
    status.mockResolvedValue([catalog("ned-lvs", "missing")]);
    const { result, rerender } = renderHook(
      ({ enabled }) => useCatalogIssues(enabled),
      { initialProps: { enabled: true } },
    );
    await waitFor(() => expect(result.current.count).toBe(1));

    rerender({ enabled: false });

    expect(result.current.count).toBe(0);
    expect(result.current.label).toBe("");
  });

  it("keeps the previous count when a poll fails", async () => {
    // Deliberate: a failed check must not render a misleading zero, which on
    // this badge reads as "every crossmatched catalog is present".
    status.mockResolvedValueOnce([catalog("ned-lvs", "missing")]);
    const { result } = renderHook(() => useCatalogIssues(true));
    await waitFor(() => expect(result.current.count).toBe(1));

    status.mockRejectedValueOnce(new Error("gone"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000);
    });

    expect(result.current.count).toBe(1);
    expect(result.current.label).toContain("1 missing");
  });

  it("re-checks on its own interval", async () => {
    status.mockResolvedValue([catalog("ned-lvs", "missing")]);
    const { result } = renderHook(() => useCatalogIssues(true));
    await waitFor(() => expect(status).toHaveBeenCalledTimes(1));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000);
    });
    expect(status).toHaveBeenCalledTimes(2);

    // Drift changes on the order of hours, so the interval is a minute and not
    // a second: three minutes is three checks, not a hundred and eighty.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(120_000);
    });
    expect(status).toHaveBeenCalledTimes(4);
    expect(result.current.count).toBe(1);
  });

  it("stops polling once unmounted", async () => {
    status.mockResolvedValue([catalog("ned-lvs", "missing")]);
    const { unmount } = renderHook(() => useCatalogIssues(true));
    await waitFor(() => expect(status).toHaveBeenCalledTimes(1));

    unmount();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(180_000);
    });

    expect(status).toHaveBeenCalledTimes(1);
  });
});
