// The admin page: tabs for task runs, catalogs, enrichment, and users. It owns
// the poll that feeds the first three and the run panel any of them can open;
// each tab's views are in components/admin/.
//
// Catalog ingestion is deliberately only reachable from here and the API --
// there is no binary to run over SSH. See docs/task-system.md for why: these
// runs take hours, so they have to survive a deploy, report their logs while
// running, and be cancelable, and they have to leave a record of who ran what.
import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  ACTIVE_STATUSES,
  acceptEnrichmentSet,
  conflictingRun,
  fetchCatalogExports,
  fetchCatalogStatus,
  fetchEnrichmentStatus,
  fetchTasks,
  fetchTaskTypes,
  submitCatalogIngest,
  submitTask,
  unacceptEnrichmentSet,
  type CatalogExport,
  type CatalogStatus,
  type EnrichmentDrift,
  type Task,
  type TaskStatus,
  type TaskType,
} from "@/lib/adminApi";
import { countEnrichmentIssues, countIssues } from "@/hooks/use-catalog-issues";
import { CatalogsTable, ExportsTable, ReingestDialog } from "@/components/admin/catalogs";
import { EnrichmentDriftTable } from "@/components/admin/enrichment";
import {
  ACTIVE_FETCHED,
  CancelRunDialog,
  HISTORY_MAX,
  HISTORY_PAGE,
  NewRunDialog,
  POLL_MS,
  ResubmitDialog,
  RunDetail,
  RunsTable,
  type HistoryFilter,
} from "@/components/admin/tasks";
import { AdminUsers } from "@/components/admin/users";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

/** Consecutive failed polls before the page stops and says so. */
const MAX_POLL_FAILURES = 5;

const TABS = ["tasks", "catalogs", "enrichment", "users"] as const;
type Tab = (typeof TABS)[number];

const FINISHED_STATUSES: TaskStatus[] = ["succeeded", "failed", "canceled"];

export default function Admin() {
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedTab = searchParams.get("tab");
  const tab: Tab = TABS.includes(requestedTab as Tab) ? (requestedTab as Tab) : "tasks";
  const setTab = (next: string) =>
    setSearchParams(
      (params) => {
        params.set("tab", next);
        return params;
      },
      { replace: true },
    );

  const [catalogs, setCatalogs] = useState<CatalogStatus[]>([]);
  const [active, setActive] = useState<Task[]>([]);
  const [history, setHistory] = useState<Task[]>([]);
  const [historyFilter, setHistoryFilter] = useState<HistoryFilter>("all");
  const [historyLimit, setHistoryLimit] = useState(HISTORY_PAGE);
  const [taskTypes, setTaskTypes] = useState<TaskType[]>([]);
  const [enrichment, setEnrichment] = useState<EnrichmentDrift[]>([]);
  const [exports, setExports] = useState<CatalogExport[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [openedWith, setOpenedWith] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  // Two kinds of failure, kept apart because they have different lifetimes. A
  // load failure describes the page and the next successful poll replaces it;
  // an action failure describes something the operator just did, and must
  // stay until they have read it.
  const [loadError, setLoadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  // Consecutive failed polls. The page gives up rather than backing off
  // forever: one that has quietly stopped updating looks exactly like an idle
  // one, which on a page whose whole job is showing what is happening now is
  // the worst of the three states it could be in.
  const failures = useRef(0);
  const [attempt, setAttempt] = useState(0);
  const [reingesting, setReingesting] = useState<CatalogStatus | null>(null);
  const [confirmCancel, setConfirmCancel] = useState<Task | null>(null);
  const [resubmitting, setResubmitting] = useState<Task | null>(null);
  const [newRun, setNewRun] = useState(false);
  // The history query a response must match to be shown, so a slow poll for
  // the previous filter cannot overwrite the current one.
  const historyKey = `${historyFilter}:${historyLimit}`;
  const latestHistoryKey = useRef(historyKey);
  useEffect(() => {
    latestHistoryKey.current = historyKey;
  }, [historyKey]);

  // Returns the active runs it fetched, which the poll needs before React has
  // re-rendered with the new state.
  const refresh = useCallback(async (): Promise<Task[]> => {
    const key = `${historyFilter}:${historyLimit}`;
    try {
      const [status, inFlight, recent, types, drift, exported] = await Promise.all([
        fetchCatalogStatus(),
        fetchTasks({ statuses: ACTIVE_STATUSES, limit: ACTIVE_FETCHED }),
        // In-progress runs come from the fetch above, so history is finished
        // runs only.
        historyFilter === "active"
          ? Promise.resolve([])
          : fetchTasks({
              statuses: historyFilter === "all" ? FINISHED_STATUSES : [historyFilter],
              limit: historyLimit,
            }),
        fetchTaskTypes(),
        // Caught here, not with the others: the drift query is the one call
        // that scans, so it fails on its own schedule and must not cost the
        // operator every other table. `null` leaves the last reading standing
        // rather than rendering as "nothing is stale".
        fetchEnrichmentStatus().catch(() => null),
        fetchCatalogExports(),
      ]);
      setCatalogs(status);
      setActive(inFlight);
      if (latestHistoryKey.current === key) setHistory(recent);
      setTaskTypes(types);
      if (drift) setEnrichment(drift);
      setExports(exported);
      setLoadError(null);
      failures.current = 0;
      return inFlight;
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e));
      failures.current += 1;
      return [];
    } finally {
      setLoaded(true);
    }
  }, [historyFilter, historyLimit]);

  useEffect(() => {
    let cancelled = false;
    let timer: number;
    async function tick() {
      const inFlight = await refresh();
      if (cancelled) return;
      // Stop after enough consecutive failures rather than retrying forever on
      // an API that is refusing. Picked back up by the Retry button below.
      if (failures.current >= MAX_POLL_FAILURES) return;
      // Poll slowly while idle, so an open admin page is not hitting the API
      // every few seconds forever.
      timer = window.setTimeout(tick, inFlight.length > 0 ? POLL_MS : POLL_MS * 5);
    }
    tick();
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [refresh, attempt]);

  const stalled = loadError !== null && failures.current >= MAX_POLL_FAILURES;

  function retry() {
    failures.current = 0;
    setAttempt((n) => n + 1);
  }

  function openRun(runId: string, why: string | null = null) {
    setSelected(runId);
    setOpenedWith(why);
  }

  // The newest in-flight run per catalog, so the table can show it.
  const runsByCatalog = new Map<string, Task>();
  for (const run of active) {
    const catalog = typeof run.params.catalog === "string" ? run.params.catalog : null;
    if (catalog && !runsByCatalog.has(catalog)) runsByCatalog.set(catalog, run);
  }

  const catalogIssues = countIssues(catalogs).count;
  const enrichmentIssues = countEnrichmentIssues(enrichment).count;

  function onQueued(run: Task, why?: string) {
    setResubmitting(null);
    setNewRun(false);
    openRun(run._id, why ?? null);
    void refresh();
  }

  const onReprocessStale = (survey: string) =>
    start(
      survey,
      () =>
        submitTask("enrich_reprocess", {
          survey,
          selection: { kind: "stale" },
        }),
      `${survey} enrichment is already being reprocessed. Showing that run.`,
    );

  async function onAcceptSet(setId: number, reason: string) {
    try {
      await acceptEnrichmentSet(setId, reason);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  }

  async function onUnacceptSet(setId: number) {
    try {
      await unacceptEnrichmentSet(setId);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  }

  /** Submit, and treat an equivalent run already in flight as the answer. */
  async function start(key: string, submit: () => Promise<Task>, already: string) {
    setBusy(key);
    setActionError(null);
    try {
      const run = await submit();
      openRun(run._id);
      await refresh();
    } catch (e) {
      const existing = conflictingRun(e);
      // The server returns the run that is already in flight rather than a
      // bare error, so the client can jump straight to watching it.
      if (!existing) {
        setActionError(e instanceof Error ? e.message : String(e));
        return;
      }
      openRun(existing._id, already);
      await refresh();
    } finally {
      setBusy(null);
    }
  }

  const onIngest = (catalogId: string) =>
    start(
      catalogId,
      () => submitCatalogIngest(catalogId),
      `${catalogId} is already being ingested. Showing that run.`,
    );

  function confirmReingest(catalog: CatalogStatus) {
    // `drop_existing`, which is the only thing a re-ingest of a complete
    // catalog can usefully mean.
    void start(
      catalog.id,
      () => submitCatalogIngest(catalog.id, true),
      `${catalog.id} is already being ingested. Showing that run.`,
    );
  }

  return (
    <div className="px-4 lg:px-6">
      <div className="max-w-5xl mx-auto">
        <h1 className="text-2xl font-bold mb-2">Admin</h1>
        {/* Above the tabs, and not cleared by the next poll: this is the answer
            to something the operator just asked for. */}
        {actionError && (
          <div className="mb-4 rounded-lg border border-destructive/50 p-3">
            <p className="text-sm text-destructive">{actionError}</p>
          </div>
        )}
        {stalled && (
          <div className="mb-4 flex items-center justify-between gap-4 rounded-lg border p-3">
            <p className="text-sm text-muted-foreground">
              This page has stopped refreshing after {MAX_POLL_FAILURES} failed attempts, so what
              it shows may be out of date.
            </p>
            <Button variant="outline" size="sm" onClick={retry}>
              Retry
            </Button>
          </div>
        )}

        <Tabs value={tab} onValueChange={setTab}>
          <TabsList className="mb-4">
            <TabsTrigger value="tasks">
              Tasks
              {active.length > 0 && (
                <Badge variant="secondary" className="ml-1 px-1.5">
                  {active.length}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="catalogs">
              Catalogs
              {catalogIssues > 0 && (
                <Badge variant="destructive" className="ml-1 px-1.5">
                  {catalogIssues}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="enrichment">
              Enrichment
              {enrichmentIssues > 0 && (
                <Badge variant="destructive" className="ml-1 px-1.5">
                  {enrichmentIssues}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="users">Users</TabsTrigger>
          </TabsList>

          <TabsContent value="tasks">
            {!loaded ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : (
              <RunsTable
                active={active}
                finished={history}
                filter={historyFilter}
                onFilter={(f) => {
                  setHistoryFilter(f);
                  setHistoryLimit(HISTORY_PAGE);
                }}
                limit={historyLimit}
                onShowMore={() =>
                  setHistoryLimit((n) => Math.min(n + HISTORY_PAGE * 2, HISTORY_MAX))
                }
                onSelect={(id) => openRun(id)}
                onCancel={setConfirmCancel}
                onResubmit={setResubmitting}
                onNewRun={() => setNewRun(true)}
                error={loadError}
              />
            )}
          </TabsContent>

          <TabsContent value="catalogs">
            {!loaded ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : (
              <>
                <CatalogsTable
                  catalogs={catalogs}
                  runsByCatalog={runsByCatalog}
                  onIngest={onIngest}
                  onReingest={setReingesting}
                  onSelect={(id) => openRun(id)}
                  busy={busy}
                  error={loadError}
                />
                <ExportsTable exports={exports} />
              </>
            )}
          </TabsContent>

          <TabsContent value="enrichment">
            {!loaded ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : (
              <EnrichmentDriftTable
                drift={enrichment}
                onReprocess={onReprocessStale}
                onAccept={onAcceptSet}
                onUnaccept={onUnacceptSet}
                busy={busy}
              />
            )}
          </TabsContent>

          <TabsContent value="users">
            <AdminUsers />
          </TabsContent>
        </Tabs>
      </div>

      <Sheet open={selected !== null} onOpenChange={(open) => !open && setSelected(null)}>
        <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
          {selected && (
            <RunDetail
              runId={selected}
              openedWith={openedWith}
              onResubmit={setResubmitting}
            />
          )}
        </SheetContent>
      </Sheet>

      <CancelRunDialog
        run={confirmCancel}
        onCanceled={() => void refresh()}
        onClose={() => setConfirmCancel(null)}
      />

      <ResubmitDialog
        run={resubmitting}
        taskType={taskTypes.find((t) => t.id === resubmitting?.task_type)}
        onClose={() => setResubmitting(null)}
        onQueued={onQueued}
      />

      <NewRunDialog
        open={newRun}
        types={taskTypes}
        onClose={() => setNewRun(false)}
        onQueued={onQueued}
      />

      <ReingestDialog
        catalog={reingesting}
        onConfirm={confirmReingest}
        onClose={() => setReingesting(null)}
      />
    </div>
  );
}
