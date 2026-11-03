// The admin page: tabs for task runs, catalog and enrichment state, and users.
// It owns the poll that feeds the first two, the run panel either can open, and
// the Catalogs & enrichment tab. Task views are in components/admin/tasks.tsx
// and user management in components/admin/users.tsx.
//
// Catalog ingestion is deliberately only reachable from here and the API --
// there is no binary to run over SSH. See docs/task-system.md for why: these
// runs take hours, so they have to survive a deploy, report their logs while
// running, and be cancellable, and they have to leave a record of who ran what.
import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  ACTIVE_STATUSES,
  cancelTaskRun,
  acceptEnrichmentSet,
  conflictingRun,
  fetchCatalogExports,
  fetchCatalogStatus,
  fetchEnrichmentStatus,
  fetchTasks,
  fetchTaskTypes,
  isActive,
  submitCatalogIngest,
  submitTask,
  unacceptEnrichmentSet,
  type CatalogHealth,
  type CatalogExport,
  type CatalogStatus,
  type EnrichmentDrift,
  type Task,
  type TaskType,
} from "@/lib/adminApi";
import { countEnrichmentIssues, countIssues } from "@/hooks/use-catalog-issues";
import {
  ACTIVE_FETCHED,
  ActiveRuns,
  CancelRunDialog,
  HISTORY_MAX,
  HISTORY_PAGE,
  POLL_MS,
  ResubmitDialog,
  RunDetail,
  RunsTable,
  TaskCatalog,
  type HistoryFilter,
} from "@/components/admin/tasks";
import { AdminUsers } from "@/components/admin/users";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

/** Consecutive failed polls before the page stops and says so. */
const MAX_POLL_FAILURES = 5;

const TABS = ["tasks", "data", "users"] as const;
type Tab = (typeof TABS)[number];

function healthLabel(health: CatalogHealth): { text: string; variant: "default" | "secondary" | "destructive" | "outline" } {
  switch (health) {
    case "present":
      return { text: "Present", variant: "default" };
    case "missing":
      return { text: "Missing", variant: "destructive" };
    // Partly ingested is worse than absent: a crossmatch against it returns
    // fewer matches rather than failing, so it is called out separately.
    case "partial":
      return { text: "Partial", variant: "destructive" };
    // Also partly ingested, but because a run is part way through it. Not a
    // problem to fix, and not counted on the badge.
    case "ingesting":
      return { text: "Ingesting", variant: "secondary" };
    case "undeclared":
      return { text: "Unknown slug", variant: "outline" };
    // Not a problem to fix: the collection is real and populated by something
    // other than the ingest. The row's title says what.
    case "external":
      return { text: "Built elsewhere", variant: "secondary" };
  }
}

function CatalogsTable({
  catalogs,
  runsByCatalog,
  onIngest,
  onReingest,
  onSelect,
  busy,
  error,
}: {
  catalogs: CatalogStatus[];
  runsByCatalog: Map<string, Task>;
  onIngest: (id: string) => void;
  onReingest: (catalog: CatalogStatus) => void;
  onSelect: (runId: string) => void;
  busy: string | null;
  error: string | null;
}) {
  return (
    <section className="mb-8">
      <h2 className="text-lg font-semibold mb-1">Catalogs</h2>
      {error && <p className="text-sm text-destructive mb-3">{error}</p>}
      {catalogs.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {/* A read that failed and a release with no catalogs both leave this
              list empty, and they call for opposite responses -- one is a thing
              to go and fix, the other is nothing at all. */}
          {error
            ? "Catalog status could not be read, so nothing is known about what is ingested."
            : "This release defines no catalogs."}
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="py-2 pr-4 font-medium">Catalog</th>
                <th className="py-2 pr-4 font-medium">Collection</th>
                <th className="py-2 pr-4 font-medium">State</th>
                <th className="py-2 pr-4 font-medium">Crossmatch</th>
                <th className="py-2 pr-4 font-medium">Chunks</th>
                <th className="py-2 pr-4 font-medium">Records</th>
                <th className="py-2 font-medium" />
              </tr>
            </thead>
            <tbody>
              {catalogs.map((catalog) => {
                const label = healthLabel(catalog.health);
                const run = runsByCatalog.get(catalog.id);
                const running = run && isActive(run);
                return (
                  <tr key={catalog.id} className="border-b last:border-0">
                    <td className="py-2 pr-4">
                      <div className="font-medium">{catalog.id}</div>
                      {catalog.title && (
                        <div className="text-xs text-muted-foreground">{catalog.title}</div>
                      )}
                    </td>
                    <td className="py-2 pr-4 font-mono text-xs">{catalog.collection ?? "—"}</td>
                    <td className="py-2 pr-4">
                      <Badge variant={label.variant}>{label.text}</Badge>
                    </td>
                    <td className="py-2 pr-4">
                      {/* Whether the alert pipeline queries this catalog. An
                          un-ingested catalog nobody crossmatches against is
                          simply available; a configured one that is missing
                          returns zero matches on every alert, silently. */}
                      {catalog.crossmatched ? (
                        <Badge variant="outline">in use</Badge>
                      ) : (
                        <span className="text-xs text-muted-foreground">not configured</span>
                      )}
                    </td>
                    <td className="py-2 pr-4 tabular-nums">
                      {catalog.chunks_total > 0
                        ? `${catalog.chunks_done}/${catalog.chunks_total}`
                        : "—"}
                    </td>
                    <td className="py-2 pr-4 tabular-nums">
                      {catalog.n_records > 0 ? catalog.n_records.toLocaleString() : "—"}
                    </td>
                    <td className="py-2">
                      {running ? (
                        <Button variant="outline" size="sm" onClick={() => onSelect(run!._id)}>
                          {run!.status === "queued" ? "Queued…" : "Running…"}
                        </Button>
                      ) : // Neither an unknown slug nor a collection built
                      // elsewhere has anything to ingest, and offering a
                      // button would be misleading. Neither has one that is
                      // being ingested right now by a run too old to be in the
                      // page of runs this view fetched -- the server would
                      // refuse the submit anyway, but the button should not
                      // invite it.
                      catalog.health === "undeclared" ||
                        catalog.health === "external" ? null : catalog.health ===
                        "ingesting" ? (
                        <span className="text-xs text-muted-foreground">in progress</span>
                      ) : catalog.health === "present" ? (
                        // A present catalog has every chunk, so resuming would
                        // do nothing: the only re-ingest that means anything
                        // drops the collection and rebuilds it. That is
                        // destructive, so it goes through a confirmation --
                        // which is also what the task form demands of a
                        // destructive task, and this row used to skip.
                        <Button
                          variant="outline"
                          size="sm"
                          disabled={busy === catalog.id}
                          onClick={() => onReingest(catalog)}
                        >
                          {busy === catalog.id ? "Starting…" : "Re-ingest"}
                        </Button>
                      ) : (
                        <Button
                          size="sm"
                          disabled={busy === catalog.id}
                          onClick={() => onIngest(catalog.id)}
                        >
                          {busy === catalog.id
                            ? "Starting…"
                            : catalog.health === "partial"
                              ? "Resume ingest"
                              : "Ingest"}
                        </Button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

/** Reports enrichment drift and never acts on it: re-enriching takes days. */
function EnrichmentDriftTable({
  drift,
  onReprocess,
  onAccept,
  onUnaccept,
  busy,
}: {
  drift: EnrichmentDrift[];
  onReprocess: (survey: string) => void;
  onAccept: (setId: number, reason: string) => void;
  onUnaccept: (setId: number) => void;
  busy: string | null;
}) {
  const [accepting, setAccepting] = useState<number | null>(null);
  const [reason, setReason] = useState("");

  if (drift.length === 0) return null;

  return (
    <section className="mb-8">
      <h2 className="text-lg font-semibold mb-1">Enrichment</h2>
      <p className="text-sm text-muted-foreground mb-3">
        Enrichment runs once, when an alert is ingested. After a model or a
        derivation changes, alerts already in the database hold values the
        current code would not produce.
      </p>

      {drift.map((survey) => {
        const unknown = survey.current_set === null;
        const timedOut = !unknown && survey.drift_unknown;
        const clean =
          !unknown &&
          !timedOut &&
          survey.stale_sets.length === 0 &&
          !survey.has_unstamped;
        return (
          <div key={survey.survey} className="border rounded-lg p-3 mb-3">
            <div className="flex items-center justify-between gap-4 mb-2">
              <div>
                <span className="font-medium uppercase">{survey.survey}</span>{" "}
                <span className="text-xs text-muted-foreground font-mono">
                  {unknown ? "no set published" : `current set ${survey.current_set}`}
                </span>
              </div>
              {unknown ? (
                <Badge variant="outline">unknown</Badge>
              ) : timedOut ? (
                <Badge variant="outline">not known</Badge>
              ) : clean ? (
                <Badge>up to date</Badge>
              ) : (
                <Button
                  size="sm"
                  disabled={busy === survey.survey}
                  onClick={() => onReprocess(survey.survey)}
                >
                  {busy === survey.survey ? "Starting…" : "Reprocess stale"}
                </Button>
              )}
            </div>

            {unknown ? (
              <p className="text-xs text-muted-foreground">
                No enrichment worker has published a set for this survey yet, so
                there is nothing to compare against. It appears once a worker
                starts.
              </p>
            ) : timedOut ? (
              <p className="text-xs text-muted-foreground">
                The drift query ran out of time, so staleness is not known. That
                is expected until the <code>enrichment_set</code> index finishes
                building on this collection.
              </p>
            ) : clean ? (
              <p className="text-xs text-muted-foreground">
                Every alert was enriched by the current set.
              </p>
            ) : (
              <table className="w-full text-sm">
                <tbody>
                  {survey.has_unstamped && (
                    <tr className="border-b">
                      <td className="py-2 pr-4">
                        <div className="font-medium">Enriched before stamping</div>
                        <div className="text-xs text-muted-foreground">
                          These carry no set at all, so what produced them cannot
                          be established. Reprocessing is the only way to make
                          them attributable.
                        </div>
                      </td>
                      <td className="py-2 w-32 text-right text-xs text-muted-foreground">
                        no set
                      </td>
                    </tr>
                  )}
                  {survey.stale_sets.map((stale) => (
                    <tr key={stale.id} className="border-b last:border-0">
                      <td className="py-2 pr-4">
                        <div className="font-medium">Set {stale.id}</div>
                        <div className="text-xs text-muted-foreground">
                          {stale.changed.length > 0
                            ? `differs by: ${stale.changed.join(", ")}`
                            : "no longer in the registry, so what changed is unknown"}
                        </div>
                        {accepting === stale.id && (
                          <div className="mt-2 flex items-center gap-2">
                            <Input
                              autoFocus
                              aria-label={`Why is set ${stale.id} acceptable?`}
                              placeholder="Why is this acceptable?"
                              value={reason}
                              onChange={(e) => setReason(e.target.value)}
                              className="h-8 text-xs"
                            />
                            <Button
                              size="sm"
                              disabled={!reason.trim()}
                              onClick={() => {
                                onAccept(stale.id, reason.trim());
                                setAccepting(null);
                                setReason("");
                              }}
                            >
                              Accept
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => setAccepting(null)}
                            >
                              Cancel
                            </Button>
                          </div>
                        )}
                      </td>
                      <td className="py-2 w-32 text-right">
                        {accepting !== stale.id && (
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setAccepting(stale.id)}
                          >
                            Accept
                          </Button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {survey.accepted_sets.length > 0 && (
              <p className="text-xs text-muted-foreground mt-2">
                Accepted (not reported as drift, and skipped by a reprocess):{" "}
                {survey.accepted_sets.map((id, i) => (
                  <span key={id}>
                    {i > 0 && ", "}
                    set {id}{" "}
                    <button
                      className="underline hover:no-underline"
                      onClick={() => onUnaccept(id)}
                    >
                      undo
                    </button>
                  </span>
                ))}
                . Accepting records the decision against the set; no alert is
                rewritten, so each still says what actually produced it.
              </p>
            )}
          </div>
        );
      })}
    </section>
  );
}

function bytes(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(0)} kB`;
  return `${n} B`;
}

/** A curl command, not a download button: chunks are too big to buffer in the tab. */
function ExportsTable({ exports }: { exports: CatalogExport[] }) {
  if (exports.length === 0) return null;
  const origin = window.location.origin;
  return (
    <section className="mb-8">
      <h2 className="text-lg font-semibold mb-1">Catalog exports</h2>
      <p className="text-sm text-muted-foreground mb-3">
        Written by the <code>export_catalog</code> task, for catalogs BOOM cannot
        fetch again. Download these, publish them somewhere durable, then put the
        URL in the catalog's boompy module so every deployment can ingest it.
      </p>
      {exports.map((exp) => (
        <div key={exp.collection} className="border rounded-lg p-3 mb-3">
          <div className="flex items-center justify-between gap-4 mb-2">
            <span className="font-medium">{exp.collection}</span>
            <span className="text-xs text-muted-foreground font-mono">
              {exp.manifest?.rows?.toLocaleString() ?? "?"} rows
              {exp.manifest?.source_database ? ` · from ${exp.manifest.source_database}` : ""}
              {exp.manifest?.code_version?.git_sha
                ? ` · ${exp.manifest.code_version.git_sha.slice(0, 8)}`
                : ""}
            </span>
          </div>
          <table className="w-full text-sm">
            <tbody>
              {exp.files.map((f) => (
                <tr key={f.name} className="border-b last:border-0">
                  <td className="py-1 pr-4 font-mono text-xs">{f.name}</td>
                  <td className="py-1 pr-4 tabular-nums text-right w-24">{bytes(f.bytes)}</td>
                  <td className="py-1">
                    <code className="text-[11px] text-muted-foreground break-all">
                      curl -H "Authorization: Bearer $TOKEN" -O{" "}
                      {`${origin}/api/catalogs/exports/${exp.collection}/${f.name}`}
                    </code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </section>
  );
}

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
  const [notice, setNotice] = useState<string | null>(null);
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
        fetchTasks({
          statuses: historyFilter === "all" ? undefined : [historyFilter],
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

  const catalogIssues = countIssues(catalogs).count + countEnrichmentIssues(enrichment).count;

  async function onCancelRun(run: Task) {
    setActionError(null);
    try {
      // A running task stops at its next safe point, so this reports what the
      // server says it did rather than implying the run has stopped.
      setNotice(await cancelTaskRun(run._id));
      await refresh();
    } catch (e) {
      setNotice(null);
      setActionError(e instanceof Error ? e.message : String(e));
    }
  }

  function onResubmitted(run: Task, why?: string) {
    setResubmitting(null);
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
    setNotice(null);
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

  const onSubmitTask = (taskType: string, params: Record<string, unknown>) =>
    start(
      taskType,
      () => submitTask(taskType, params),
      `An equivalent ${taskType} run is already in flight. Showing that one.`,
    );

  const onIngest = (catalogId: string) =>
    start(
      catalogId,
      () => submitCatalogIngest(catalogId),
      `${catalogId} is already being ingested. Showing that run.`,
    );

  function confirmReingest() {
    const catalog = reingesting;
    setReingesting(null);
    if (!catalog) return;
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
        <p className="text-sm text-muted-foreground mb-6">
          Run and watch tasks, check catalog and enrichment state, and manage users.
        </p>

        {/* Above the tabs, and not cleared by the next poll: this is the answer
            to something the operator just asked for. */}
        {actionError && (
          <div className="mb-4 rounded-lg border border-destructive/50 p-3">
            <p className="text-sm text-destructive">{actionError}</p>
          </div>
        )}
        {notice && !actionError && (
          <p className="mb-4 text-sm text-muted-foreground">{notice}</p>
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
            <TabsTrigger value="data">
              Catalogs &amp; enrichment
              {catalogIssues > 0 && (
                <Badge variant="destructive" className="ml-1 px-1.5">
                  {catalogIssues}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="users">Users</TabsTrigger>
          </TabsList>

          <TabsContent value="tasks">
            {!loaded ? (
              <p className="text-sm text-muted-foreground">Loading…</p>
            ) : (
              <>
                <ActiveRuns runs={active} onSelect={(id) => openRun(id)} onCancel={setConfirmCancel} />
                <RunsTable
                  runs={history}
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
                  onResubmit={setResubmitting}
                  error={loadError}
                />
                <TaskCatalog types={taskTypes} onSubmit={onSubmitTask} busy={busy} />
              </>
            )}
          </TabsContent>

          <TabsContent value="data">
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
                <EnrichmentDriftTable
                  drift={enrichment}
                  onReprocess={onReprocessStale}
                  onAccept={onAcceptSet}
                  onUnaccept={onUnacceptSet}
                  busy={busy}
                />
                <ExportsTable exports={exports} />
              </>
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
        onConfirm={(run) => void onCancelRun(run)}
        onClose={() => setConfirmCancel(null)}
      />

      <ResubmitDialog
        run={resubmitting}
        taskType={taskTypes.find((t) => t.id === resubmitting?.task_type)}
        onClose={() => setResubmitting(null)}
        onQueued={onResubmitted}
      />

      <Dialog
        open={reingesting !== null}
        onOpenChange={(open) => !open && setReingesting(null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Re-ingest {reingesting?.id}?</DialogTitle>
            <DialogDescription>
              {reingesting?.collection ?? "The collection"} is dropped and
              rebuilt from the archive, which takes hours.
              {reingesting?.crossmatched
                ? " The alert pipeline queries this catalog, and until the ingest finishes it will match against whatever has been inserted so far rather than failing — so alerts processed during the run will be under-matched."
                : " Nothing crossmatches against it, so the alert pipeline is unaffected."}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setReingesting(null)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={confirmReingest}>
              Drop and re-ingest
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
