// A page for administrative work like managing users, ingesting new catalogs,
// kicking off reprocessing, etc.
//
// Catalog ingestion is deliberately only reachable from here and the API --
// there is no binary to run over SSH. See docs/task-system.md for why: these
// runs take hours, so they have to survive a deploy, report their logs while
// running, and be cancellable, and they have to leave a record of who ran what.
import { useCallback, useEffect, useRef, useState } from "react";
import {
  cancelTaskRun,
  acceptEnrichmentSet,
  fetchCatalogStatus,
  fetchEnrichmentStatus,
  fetchTaskLogs,
  fetchTaskRuns,
  fetchTaskTypes,
  isActive,
  submitCatalogIngest,
  submitTask,
  unacceptEnrichmentSet,
  type CatalogHealth,
  type CatalogStatus,
  type EnrichmentDrift,
  type TaskLogLine,
  type TaskRun,
  type TaskType,
} from "@/lib/adminApi";
import { TaskForm } from "@/components/task-form";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

/** How often to re-poll while anything is still running. */
const POLL_MS = 3000;

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
    case "undeclared":
      return { text: "Unknown slug", variant: "outline" };
  }
}

function statusVariant(status: TaskRun["status"]): "default" | "secondary" | "destructive" | "outline" {
  if (status === "succeeded") return "default";
  if (status === "failed") return "destructive";
  if (status === "running") return "secondary";
  return "outline";
}

function formatTime(seconds?: number | null): string {
  if (!seconds) return "—";
  return new Date(seconds * 1000).toLocaleString();
}

function CatalogsTable({
  catalogs,
  runsByCatalog,
  onIngest,
  onSelect,
  busy,
  error,
}: {
  catalogs: CatalogStatus[];
  runsByCatalog: Map<string, TaskRun>;
  onIngest: (id: string) => void;
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
          No catalogs declared for this deployment.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="py-2 pr-4 font-medium">Catalog</th>
                <th className="py-2 pr-4 font-medium">Collection</th>
                <th className="py-2 pr-4 font-medium">State</th>
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
                      ) : (
                        // An unknown slug is a config typo; there is nothing to
                        // ingest and offering a button would be misleading.
                        catalog.health !== "undeclared" &&
                        catalog.health !== "present" && (
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
                        )
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

/**
 * Runs that are queued or running, surfaced above everything else.
 *
 * These are the rows an admin is actually here to watch: a catalog ingest runs
 * for hours, and having to hunt for it in the history is exactly the friction
 * the admin page exists to remove.
 */
function ActiveRuns({ runs, onSelect, onCancel }: {
  runs: TaskRun[];
  onSelect: (id: string) => void;
  onCancel: (id: string) => void;
}) {
  const active = runs.filter(isActive);
  if (active.length === 0) return null;

  return (
    <section className="mb-8">
      <h2 className="text-lg font-semibold mb-1">In progress</h2>
      <p className="text-sm text-muted-foreground mb-3">
        {active.length} {active.length === 1 ? "run is" : "runs are"} active. These survive a
        deploy — a run whose worker goes away is picked up again and resumes.
      </p>
      <div className="space-y-3">
        {active.map((run) => {
          const pct =
            run.progress.total > 0
              ? Math.round((run.progress.done / run.progress.total) * 100)
              : null;
          const catalog = typeof run.params.catalog === "string" ? run.params.catalog : null;
          return (
            <div key={run._id} className="border rounded-lg p-3">
              <div className="flex items-start justify-between gap-4 mb-2">
                <div className="min-w-0">
                  <div className="font-medium">
                    {run.task_type}
                    {catalog && <span className="font-mono text-sm"> · {catalog}</span>}{" "}
                    <Badge variant={statusVariant(run.status)}>{run.status}</Badge>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    started by {run.actor.username}
                    {run.started_at && ` · running since ${formatTime(run.started_at)}`}
                    {run.attempts > 1 && ` · attempt ${run.attempts} (resumed)`}
                  </div>
                </div>
                <div className="flex gap-2 shrink-0">
                  <Button variant="outline" size="sm" onClick={() => onSelect(run._id)}>
                    Logs
                  </Button>
                  <Button variant="destructive" size="sm" onClick={() => onCancel(run._id)}>
                    Cancel
                  </Button>
                </div>
              </div>
              {run.progress.total > 0 ? (
                <>
                  <div className="flex justify-between text-xs text-muted-foreground mb-1">
                    <span>{run.progress.message || "working"}</span>
                    <span className="tabular-nums">
                      {run.progress.done}/{run.progress.total}
                      {pct !== null && ` (${pct}%)`}
                    </span>
                  </div>
                  <div className="h-2 w-full rounded bg-muted overflow-hidden">
                    <div
                      className="h-full bg-primary transition-all"
                      style={{ width: `${pct ?? 0}%` }}
                    />
                  </div>
                </>
              ) : (
                // Before the first chunk lands there is nothing to divide by --
                // the run is downloading, which can take a while on its own.
                <p className="text-xs text-muted-foreground">
                  {run.status === "queued"
                    ? "Waiting for a worker."
                    : "Starting — fetching the first chunk."}
                </p>
              )}
            </div>
          );
        })}
      </div>
    </section>
  );
}

function RunDetail({ runId, onClose }: { runId: string; onClose: () => void }) {
  const [run, setRun] = useState<TaskRun | null>(null);
  const [lines, setLines] = useState<TaskLogLine[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);
  // Tail cursor: the server returns only chunks after this, so a long-running
  // ingest is not re-fetched in full on every poll.
  const lastSeq = useRef<number | undefined>(undefined);
  const logEnd = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    lastSeq.current = undefined;
    setLines([]);

    async function poll() {
      try {
        const [runs, chunks] = await Promise.all([
          fetchTaskRuns(),
          fetchTaskLogs(runId, lastSeq.current),
        ]);
        if (cancelled) return;
        const current = runs.find((r) => r._id === runId) ?? null;
        setRun(current);
        if (chunks.length > 0) {
          lastSeq.current = Math.max(...chunks.map((c) => c.seq));
          setLines((prev) => [...prev, ...chunks.flatMap((c) => c.lines)]);
        }
        setError(null);
        // Stop polling once the run is over; the logs are complete at that point.
        if (current && !isActive(current)) return;
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
      if (!cancelled) timer = window.setTimeout(poll, POLL_MS);
    }

    let timer = window.setTimeout(poll, 0);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [runId]);

  useEffect(() => {
    logEnd.current?.scrollIntoView({ block: "end" });
  }, [lines.length]);

  async function onCancel() {
    setCanceling(true);
    try {
      await cancelTaskRun(runId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setCanceling(false);
    }
  }

  const progress = run?.progress;
  const pct =
    progress && progress.total > 0 ? Math.round((progress.done / progress.total) * 100) : null;

  return (
    <section className="mb-8 border rounded-lg p-4">
      <div className="flex items-start justify-between gap-4 mb-3">
        <div>
          <h2 className="text-lg font-semibold">
            {run?.task_type ?? "Run"}{" "}
            {run && <Badge variant={statusVariant(run.status)}>{run.status}</Badge>}
          </h2>
          <p className="text-xs text-muted-foreground font-mono">{runId}</p>
          {run && (
            <p className="text-xs text-muted-foreground mt-1">
              started by {run.actor.username} · requested {formatTime(run.requested_at)}
              {run.attempts > 1 && ` · attempt ${run.attempts}`}
            </p>
          )}
        </div>
        <div className="flex gap-2">
          {run && isActive(run) && (
            <Button variant="destructive" size="sm" disabled={canceling} onClick={onCancel}>
              {canceling ? "Canceling…" : "Cancel"}
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={onClose}>
            Close
          </Button>
        </div>
      </div>

      {run?.attempts && run.attempts > 1 ? (
        <p className="text-xs text-muted-foreground mb-2">
          This run was picked up again after its worker went away — a deploy or a restart. It
          resumes from the last completed chunk rather than starting over.
        </p>
      ) : null}

      {progress && progress.total > 0 && (
        <div className="mb-3">
          <div className="flex justify-between text-xs text-muted-foreground mb-1">
            <span>{progress.message || "working"}</span>
            <span className="tabular-nums">
              {progress.done}/{progress.total}
              {pct !== null && ` (${pct}%)`}
            </span>
          </div>
          <div className="h-2 w-full rounded bg-muted overflow-hidden">
            <div className="h-full bg-primary transition-all" style={{ width: `${pct ?? 0}%` }} />
          </div>
        </div>
      )}

      {run?.error && <p className="text-sm text-destructive mb-3">{run.error}</p>}
      {error && <p className="text-sm text-destructive mb-3">{error}</p>}

      <div className="max-h-80 overflow-y-auto rounded bg-muted/50 p-3 font-mono text-xs">
        {lines.length === 0 ? (
          <p className="text-muted-foreground">No log output yet.</p>
        ) : (
          lines.map((line, i) => (
            <div
              key={i}
              className={
                line.level === "error"
                  ? "text-destructive"
                  : line.level === "warn"
                    ? "text-amber-600 dark:text-amber-500"
                    : ""
              }
            >
              <span className="text-muted-foreground">
                {new Date(line.ts * 1000).toLocaleTimeString()}{" "}
              </span>
              {line.message}
            </div>
          ))
        )}
        <div ref={logEnd} />
      </div>
    </section>
  );
}

/**
 * Which enrichment produced the alerts already in the database.
 *
 * The enrichment counterpart of the catalogs table: it reports drift and never
 * acts on it, because re-enriching an archive is days of work.
 */
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
        // No published set means no enrichment worker has started since
        // stamping was deployed. Nothing can be called stale against a set
        // that does not exist, so the row says that instead of claiming the
        // archive is clean.
        const unknown = survey.current_set === null;
        const clean =
          !unknown && survey.stale_sets.length === 0 && !survey.has_unstamped;
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

/**
 * Every task this release can run, with a form built from its schema.
 *
 * Catalog ingests have their own row-level button above, because starting one
 * from the drift table needs no parameters. This is for everything else.
 */
function TaskCatalogue({
  types,
  onSubmit,
  busy,
}: {
  types: TaskType[];
  onSubmit: (taskType: string, params: Record<string, unknown>) => void;
  busy: string | null;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const task = types.find((t) => t.id === selected) ?? null;

  return (
    <section className="mb-8">
      <h2 className="text-lg font-semibold mb-1">Run a task</h2>
      <p className="text-sm text-muted-foreground mb-3">
        Data-mutating work runs here rather than over SSH: every run records who
        started it, with which parameters, under which release.
      </p>

      {task && (
        <TaskForm
          task={task}
          busy={busy === task.id}
          onCancel={() => setSelected(null)}
          onSubmit={(params) => onSubmit(task.id, params)}
        />
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <tbody>
            {types.map((type) => (
              <tr key={type.id} className="border-b last:border-0">
                <td className="py-2 pr-4">
                  <div className="font-medium">
                    {type.title}{" "}
                    {type.destructive && (
                      <Badge variant="destructive">can destroy data</Badge>
                    )}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {type.description}
                  </div>
                  <div className="text-xs text-muted-foreground font-mono mt-0.5">
                    {type.id}
                  </div>
                </td>
                <td className="py-2 w-24 text-right">
                  <Button
                    variant={selected === type.id ? "secondary" : "outline"}
                    size="sm"
                    onClick={() =>
                      setSelected(selected === type.id ? null : type.id)
                    }
                  >
                    {selected === type.id ? "Close" : "Configure"}
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function RunsTable({ runs, onSelect }: { runs: TaskRun[]; onSelect: (id: string) => void }) {
  return (
    <section>
      <h2 className="text-lg font-semibold mb-1">Recent task runs</h2>
      {runs.length === 0 ? (
        <p className="text-sm text-muted-foreground">Nothing has been run yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="py-2 pr-4 font-medium">Task</th>
                <th className="py-2 pr-4 font-medium">Status</th>
                <th className="py-2 pr-4 font-medium">Started by</th>
                <th className="py-2 pr-4 font-medium">Requested</th>
                <th className="py-2 font-medium" />
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr key={run._id} className="border-b last:border-0">
                  <td className="py-2 pr-4">
                    <div className="font-medium">{run.task_type}</div>
                    <div className="text-xs text-muted-foreground font-mono">
                      {typeof run.params.catalog === "string" ? String(run.params.catalog) : ""}
                    </div>
                  </td>
                  <td className="py-2 pr-4">
                    <Badge variant={statusVariant(run.status)}>{run.status}</Badge>
                  </td>
                  <td className="py-2 pr-4">{run.actor.username}</td>
                  <td className="py-2 pr-4 text-xs">{formatTime(run.requested_at)}</td>
                  <td className="py-2">
                    <Button variant="ghost" size="sm" onClick={() => onSelect(run._id)}>
                      View
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default function Admin() {
  const [catalogs, setCatalogs] = useState<CatalogStatus[]>([]);
  const [runs, setRuns] = useState<TaskRun[]>([]);
  const [taskTypes, setTaskTypes] = useState<TaskType[]>([]);
  const [enrichment, setEnrichment] = useState<EnrichmentDrift[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [status, recent, types, drift] = await Promise.all([
        fetchCatalogStatus(),
        fetchTaskRuns(),
        fetchTaskTypes(),
        fetchEnrichmentStatus(),
      ]);
      setCatalogs(status);
      setRuns(recent);
      setTaskTypes(types);
      setEnrichment(drift);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoaded(true);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    let timer: number;
    async function tick() {
      await refresh();
      if (cancelled) return;
      // Keep polling only while something is in flight, so an idle admin page
      // is not hitting the API every few seconds forever.
      const active = runs.some(isActive);
      timer = window.setTimeout(tick, active ? POLL_MS : POLL_MS * 5);
    }
    tick();
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refresh]);

  // The newest run per catalog, so the table can show one in flight.
  const runsByCatalog = new Map<string, TaskRun>();
  for (const run of runs) {
    const catalog = typeof run.params.catalog === "string" ? run.params.catalog : null;
    if (catalog && !runsByCatalog.has(catalog)) runsByCatalog.set(catalog, run);
  }

  async function onCancelRun(runId: string) {
    try {
      await cancelTaskRun(runId);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function onReprocessStale(survey: string) {
    await onSubmitTaskFor(survey, "enrich_reprocess", {
      survey,
      selection: { kind: "stale" },
    });
  }

  async function onSubmitTaskFor(
    busyKey: string,
    taskType: string,
    params: Record<string, unknown>,
  ) {
    setBusy(busyKey);
    try {
      const run = await submitTask(taskType, params);
      setSelected(run._id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  async function onAcceptSet(setId: number, reason: string) {
    try {
      await acceptEnrichmentSet(setId, reason);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function onUnacceptSet(setId: number) {
    try {
      await unacceptEnrichmentSet(setId);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function onSubmitTask(taskType: string, params: Record<string, unknown>) {
    setBusy(taskType);
    try {
      const run = await submitTask(taskType, params);
      setSelected(run._id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  async function onIngest(catalogId: string) {
    setBusy(catalogId);
    try {
      const run = await submitCatalogIngest(catalogId);
      setSelected(run._id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="px-4 lg:px-6">
      <div className="max-w-5xl mx-auto">
        <h1 className="text-2xl font-bold mb-2">Admin</h1>
        <p className="text-sm text-muted-foreground mb-6">
          This page is for administrative tasks like managing users, ingesting new catalogs, and
          kicking off reprocessing.
        </p>

        {!loaded ? (
          <p className="text-sm text-muted-foreground">Loading…</p>
        ) : (
          <>
            <ActiveRuns runs={runs} onSelect={setSelected} onCancel={onCancelRun} />
            <CatalogsTable
              catalogs={catalogs}
              runsByCatalog={runsByCatalog}
              onIngest={onIngest}
              onSelect={setSelected}
              busy={busy}
              error={error}
            />
            {selected && <RunDetail runId={selected} onClose={() => setSelected(null)} />}
            <EnrichmentDriftTable
              drift={enrichment}
              onReprocess={onReprocessStale}
              onAccept={onAcceptSet}
              onUnaccept={onUnacceptSet}
              busy={busy}
            />
            <TaskCatalogue types={taskTypes} onSubmit={onSubmitTask} busy={busy} />
            <RunsTable runs={runs} onSelect={setSelected} />
          </>
        )}
      </div>
    </div>
  );
}
