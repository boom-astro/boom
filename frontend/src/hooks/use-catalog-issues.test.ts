import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { countIssues, useCatalogIssues } from "./use-catalog-issues";
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
