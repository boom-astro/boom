// A page for administrative work like managing users, ingesting new catalogs,
// kicking off reprocessing, etc.
//
// Catalog ingestion is deliberately only reachable from here and the API --
// there is no binary to run over SSH. See docs/task-system.md for why: these
// runs take hours, so they have to survive a deploy, report their logs while
// running, and be cancellable, and they have to leave a record of who ran what.
import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  ApiRequestError,
  cancelTaskRun,
  acceptEnrichmentSet,
  fetchCatalogExports,
  fetchCatalogStatus,
  fetchEnrichmentStatus,
  fetchTaskLogs,
  fetchTaskRun,
  fetchTasks,
  fetchTaskTypes,
  isActive,
  resubmitTaskRun,
  submitCatalogIngest,
  submitTask,
  unacceptEnrichmentSet,
  type CatalogHealth,
  type CatalogExport,
  type CatalogStatus,
  type EnrichmentDrift,
  type TaskLogLine,
  type Task,
  type TaskStatus,
  type TaskType,
} from "@/lib/adminApi";
import { countEnrichmentIssues, countIssues } from "@/hooks/use-catalog-issues";
import { AdminUsers } from "@/components/admin-users";
import { TaskForm } from "@/components/task-form";
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
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

/** How often to re-poll while anything is still running. */
const POLL_MS = 3000;
/** Consecutive failed polls before the page stops and says so. */
const MAX_POLL_FAILURES = 5;
/** Queued and running runs are fetched on their own, so none drops out of view
 *  behind newer history. */
const ACTIVE_STATUSES: TaskStatus[] = ["queued", "running"];
const ACTIVE_FETCHED = 200;
/** History page size, and the most "Show more" will ask the API for. */
const HISTORY_PAGE = 25;
const HISTORY_MAX = 500;

const TABS = ["tasks", "data", "users"] as const;
type Tab = (typeof TABS)[number];

type HistoryFilter = "all" | "succeeded" | "failed" | "canceled";
const HISTORY_FILTERS: { value: HistoryFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
  { value: "canceled", label: "Canceled" },
];


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

function statusVariant(status: Task["status"]): "default" | "secondary" | "destructive" | "outline" {
  if (status === "succeeded") return "default";
  if (status === "failed") return "destructive";
  if (status === "running") return "secondary";
  return "outline";
}

function formatTime(seconds?: number | null): string {
  // Not `!seconds`: 0 is a real timestamp, and it turns up on exactly the
  // records written before a field existed -- which is when rendering it as
  // "not set" hides the thing worth noticing.
  if (seconds === undefined || seconds === null) return "—";
  return new Date(seconds * 1000).toLocaleString();
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

/** Who submitted a run. Usernames come from an email's local part, so the
 *  email is what identifies the person. */
function actorLabel(actor: Task["actor"]): string {
  return actor.email || actor.username;
}

function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

/** How long a run took, or has been running so far. */
function runDuration(run: Task): string {
  if (run.started_at === undefined || run.started_at === null) return "—";
  const end = run.finished_at ?? (isActive(run) ? Date.now() / 1000 : null);
  return end === null ? "—" : formatDuration(end - run.started_at);
}

/** Parameters on one line, for a table cell. */
function paramSummary(params: Record<string, unknown>): string {
  return Object.entries(params)
    .filter(([, v]) => v !== null && v !== undefined)
    .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : String(v)}`)
    .join(" · ");
}

/** What a confirmation is about: who started the run, and with what. */
function RunSummary({ run }: { run: Task }) {
  return (
    <div className="min-w-0 rounded bg-muted/50 p-3 text-xs">
      <div className="text-muted-foreground mb-1">
        started by {actorLabel(run.actor)} · requested {formatTime(run.requested_at)}
      </div>
      <pre className="font-mono whitespace-pre-wrap break-all">
        {JSON.stringify(run.params, null, 2)}
      </pre>
    </div>
  );
}

/** Canceling sits one click from a list of runs, so it asks first. */
function CancelRunDialog({
  run,
  onConfirm,
  onClose,
}: {
  run: Task | null;
  onConfirm: (run: Task) => void;
  onClose: () => void;
}) {
  return (
    <Dialog open={run !== null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Cancel this {run?.task_type} run?</DialogTitle>
          <DialogDescription>
            {run?.status === "running"
              ? "It stops at its next safe point; work already done is kept."
              : "It has not started, so it is removed from the queue."}{" "}
            It can be run again afterward with the same parameters.
          </DialogDescription>
        </DialogHeader>
        {run && <RunSummary run={run} />}
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Keep it
          </Button>
          <Button
            variant="destructive"
            onClick={() => {
              if (run) onConfirm(run);
              onClose();
            }}
          >
            Cancel run
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

/**
 * Confirms running a finished run again with the same parameters.
 *
 * Owns its error rather than reporting it to the page, because it is opened
 * from the run panel, which covers the page's messages.
 */
function ResubmitDialog({
  run,
  taskType,
  onClose,
  onQueued,
}: {
  run: Task | null;
  taskType: TaskType | undefined;
  onClose: () => void;
  onQueued: (run: Task, notice?: string) => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const runId = run?._id;

  useEffect(() => {
    setBusy(false);
    setError(null);
  }, [runId]);

  async function confirm() {
    if (!run) return;
    setBusy(true);
    setError(null);
    try {
      onQueued(await resubmitTaskRun(run._id));
    } catch (e) {
      const existing = conflictingRun(e);
      if (existing) {
        onQueued(existing, `An equivalent ${run.task_type} run is already in flight. Showing that one.`);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <Dialog open={run !== null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Run {run?.task_type} again?</DialogTitle>
          <DialogDescription>
            A new run is queued with the same parameters as this {run?.status} one, recorded as
            started by you.
          </DialogDescription>
        </DialogHeader>
        {run && <RunSummary run={run} />}
        {taskType?.destructive && (
          <p className="text-sm text-destructive">
            This task type can destroy data, depending on its parameters.
          </p>
        )}
        {error && <p className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Close
          </Button>
          <Button
            variant={taskType?.destructive ? "destructive" : "default"}
            disabled={busy}
            onClick={confirm}
          >
            {busy ? "Submitting…" : "Run again"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ProgressBar({ progress }: { progress: Task["progress"] }) {
  const pct = progress.total > 0 ? Math.round((progress.done / progress.total) * 100) : 0;
  return (
    <>
      <div className="flex justify-between text-xs text-muted-foreground mb-1">
        <span>{progress.message || "working"}</span>
        <span className="tabular-nums">
          {progress.done}/{progress.total} ({pct}%)
        </span>
      </div>
      <div className="h-2 w-full rounded bg-muted overflow-hidden">
        <div className="h-full bg-primary transition-all" style={{ width: `${pct}%` }} />
      </div>
    </>
  );
}

/**
 * Runs that are queued or running, at the top of the Tasks tab.
 *
 * These are the rows an admin is actually here to watch: a catalog ingest runs
 * for hours, and having to hunt for it in the history is exactly the friction
 * the admin page exists to remove.
 */
function ActiveRuns({ runs, onSelect, onCancel }: {
  runs: Task[];
  onSelect: (id: string) => void;
  onCancel: (run: Task) => void;
}) {
  return (
    <section className="mb-8">
      <h2 className="text-lg font-semibold mb-1">In progress</h2>
      {runs.length === 0 ? (
        <p className="text-sm text-muted-foreground">Nothing is queued or running.</p>
      ) : (
        <>
          <p className="text-sm text-muted-foreground mb-3">
            {runs.length} {runs.length === 1 ? "run is" : "runs are"} active. These survive a
            deploy — a run whose worker goes away is picked up again and resumes.
          </p>
          <div className="space-y-3">
            {runs.map((run) => (
              <div key={run._id} className="border rounded-lg p-3">
                <div className="flex items-start justify-between gap-4 mb-2">
                  <div className="min-w-0">
                    <div className="font-medium">
                      {run.task_type}{" "}
                      <Badge variant={statusVariant(run.status)}>{run.status}</Badge>{" "}
                      {run.cancel_requested && <Badge variant="outline">stopping</Badge>}
                    </div>
                    <div
                      className="text-xs text-muted-foreground font-mono truncate"
                      title={paramSummary(run.params)}
                    >
                      {paramSummary(run.params)}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      started by {actorLabel(run.actor)}
                      {run.started_at && ` · running for ${runDuration(run)}`}
                      {run.attempts > 1 && ` · attempt ${run.attempts} (resumed)`}
                    </div>
                  </div>
                  <div className="flex gap-2 shrink-0">
                    <Button variant="outline" size="sm" onClick={() => onSelect(run._id)}>
                      Logs
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={run.cancel_requested}
                      onClick={() => onCancel(run)}
                    >
                      {run.cancel_requested ? "Stopping…" : "Cancel…"}
                    </Button>
                  </div>
                </div>
                {run.progress.total > 0 ? (
                  <ProgressBar progress={run.progress} />
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
            ))}
          </div>
        </>
      )}
    </section>
  );
}

/** One run's status and log tail, shown in a side panel over any tab. */
function RunDetail({
  runId,
  openedWith,
  onResubmit,
}: {
  runId: string;
  /** Why the panel opened on this run, e.g. it was already in flight. */
  openedWith: string | null;
  onResubmit: (run: Task) => void;
}) {
  const [run, setRun] = useState<Task | null>(null);
  const [lines, setLines] = useState<TaskLogLine[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);
  const [confirmCancel, setConfirmCancel] = useState<Task | null>(null);
  // Tail cursor: the server returns only chunks after this, so a long-running
  // ingest is not re-fetched in full on every poll.
  const lastSeq = useRef<number | undefined>(undefined);
  const logBox = useRef<HTMLDivElement | null>(null);
  // Whether the operator is reading the end of the log. Scrolling up is a
  // deliberate act and new output must not undo it.
  const following = useRef(true);

  useEffect(() => {
    let cancelled = false;
    lastSeq.current = undefined;
    following.current = true;
    // Everything the previous run left behind goes, not just its log, or the
    // panel shows the last run's header until the next poll returns.
    setLines([]);
    setRun(null);
    setError(null);
    setNotice(null);
    setCanceling(false);

    async function poll() {
      try {
        // Read by id, so a run that has dropped out of the lists is still found.
        const [current, chunks] = await Promise.all([
          fetchTaskRun(runId),
          fetchTaskLogs(runId, lastSeq.current),
        ]);
        if (cancelled) return;
        setRun(current);
        if (chunks.length > 0) {
          lastSeq.current = Math.max(...chunks.map((c) => c.seq));
          setLines((prev) => [...prev, ...chunks.flatMap((c) => c.lines)]);
        }
        setError(null);
        // Stop polling once the run is over; the logs are complete at that point.
        if (!isActive(current)) return;
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : String(e));
        // A run the API does not have is not going to appear, so there is
        // nothing left to poll for. Anything else may well be transient.
        if (e instanceof ApiRequestError && e.status === 404) return;
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
    const box = logBox.current;
    // Scroll the log box, not the page, so new output does not move the panel.
    if (box && following.current) box.scrollTop = box.scrollHeight;
  }, [lines.length]);

  async function cancel() {
    setCanceling(true);
    try {
      // A running task stops at its next safe point rather than being killed,
      // so the button cannot report a completed cancellation. The server says
      // which of the two happened and that is what gets shown.
      setNotice(await cancelTaskRun(runId));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setCanceling(false);
    }
  }

  const stopping = run?.cancel_requested && isActive(run);

  return (
    <>
      <SheetHeader className="pr-10">
        <SheetTitle className="flex flex-wrap items-center gap-2">
          {run?.task_type ?? "Run"}
          {run && <Badge variant={statusVariant(run.status)}>{run.status}</Badge>}
          {stopping && <Badge variant="outline">stopping</Badge>}
        </SheetTitle>
        <SheetDescription className="font-mono text-xs">{runId}</SheetDescription>
      </SheetHeader>

      <div className="flex flex-col gap-3 px-4 pb-4">
        {run && (
          <div className="text-xs text-muted-foreground space-y-0.5">
            <div>
              started by {actorLabel(run.actor)} · requested {formatTime(run.requested_at)}
            </div>
            <div>
              {run.started_at ? `began ${formatTime(run.started_at)}` : "not started"}
              {run.finished_at && ` · finished ${formatTime(run.finished_at)}`}
              {run.started_at && ` · ${runDuration(run)}`}
              {run.attempts > 1 && ` · attempt ${run.attempts}`}
            </div>
            {run.resubmitted_from && (
              <div>
                run again from <span className="font-mono">{run.resubmitted_from}</span>
              </div>
            )}
          </div>
        )}

        {run && (
          <div className="flex gap-2">
            {isActive(run) ? (
              <Button
                variant="outline"
                size="sm"
                disabled={canceling || run.cancel_requested}
                onClick={() => setConfirmCancel(run)}
              >
                {canceling ? "Canceling…" : run.cancel_requested ? "Cancel requested" : "Cancel…"}
              </Button>
            ) : (
              <Button variant="outline" size="sm" onClick={() => onResubmit(run)}>
                Run again…
              </Button>
            )}
          </div>
        )}

        {run && Object.keys(run.params).length > 0 && (
          <pre className="rounded bg-muted/50 p-3 font-mono text-xs whitespace-pre-wrap break-all">
            {JSON.stringify(run.params, null, 2)}
          </pre>
        )}

        {run?.attempts && run.attempts > 1 ? (
          <p className="text-xs text-muted-foreground">
            This run was picked up again after its worker went away — a deploy or a restart. It
            resumes from the last completed chunk rather than starting over.
          </p>
        ) : null}

        {run && run.progress.total > 0 && (
          <div>
            <ProgressBar progress={run.progress} />
          </div>
        )}

        {openedWith && <p className="text-sm text-muted-foreground">{openedWith}</p>}
        {notice && <p className="text-sm text-muted-foreground">{notice}</p>}
        {run?.error && <p className="text-sm text-destructive">{run.error}</p>}
        {error && <p className="text-sm text-destructive">{error}</p>}

        <div
          ref={logBox}
          // Following is given up the moment the operator scrolls away from the
          // end, and taken back when they return to it.
          onScroll={(e) => {
            const box = e.currentTarget;
            following.current = box.scrollHeight - box.scrollTop - box.clientHeight < 24;
          }}
          className="min-h-40 max-h-[60vh] overflow-y-auto rounded bg-muted/50 p-3 font-mono text-xs"
        >
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
        </div>
      </div>

      <CancelRunDialog
        run={confirmCancel}
        onConfirm={() => void cancel()}
        onClose={() => setConfirmCancel(null)}
      />
    </>
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

/**
 * Every task this release can run, with a form built from its schema.
 *
 * Catalog ingests have their own row-level button above, because starting one
 * from the drift table needs no parameters. This is for everything else.
 */
function TaskCatalog({
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
        Data-mutating work runs here rather than over SSH: every task records who
        started it, with which parameters, under which release.
      </p>

      {task && (
        // Keyed by task, so switching between two tasks starts a fresh form
        // rather than carrying the previous one's state across -- including the
        // "I understand this can destroy data" box, which would otherwise
        // arrive already checked on the next task.
        <TaskForm
          key={task.id}
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

function RunsTable({
  runs,
  filter,
  onFilter,
  limit,
  onShowMore,
  onSelect,
  onResubmit,
  error,
}: {
  runs: Task[];
  filter: HistoryFilter;
  onFilter: (filter: HistoryFilter) => void;
  limit: number;
  onShowMore: () => void;
  onSelect: (id: string) => void;
  onResubmit: (run: Task) => void;
  error: string | null;
}) {
  return (
    <section className="mb-8">
      <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
        <h2 className="text-lg font-semibold">Task runs</h2>
        <div className="flex gap-1" role="group" aria-label="Filter runs by status">
          {HISTORY_FILTERS.map((f) => (
            <Button
              key={f.value}
              size="sm"
              variant={filter === f.value ? "secondary" : "ghost"}
              aria-pressed={filter === f.value}
              onClick={() => onFilter(f.value)}
            >
              {f.label}
            </Button>
          ))}
        </div>
      </div>
      {runs.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {error
            ? "Task runs could not be read."
            : filter === "all"
              ? "Nothing has been run yet."
              : `No ${filter} runs.`}
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="py-2 pr-4 font-medium">Task</th>
                <th className="py-2 pr-4 font-medium">Status</th>
                <th className="py-2 pr-4 font-medium">Started by</th>
                <th className="py-2 pr-4 font-medium">Requested</th>
                <th className="py-2 pr-4 font-medium">Duration</th>
                <th className="py-2 font-medium" />
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => {
                const params = paramSummary(run.params);
                return (
                  <tr key={run._id} className="border-b last:border-0">
                    <td className="py-2 pr-4 max-w-xs">
                      <div className="font-medium">{run.task_type}</div>
                      {params && (
                        <div
                          className="text-xs text-muted-foreground font-mono truncate"
                          title={params}
                        >
                          {params}
                        </div>
                      )}
                    </td>
                    <td className="py-2 pr-4">
                      <Badge variant={statusVariant(run.status)}>{run.status}</Badge>
                    </td>
                    <td className="py-2 pr-4 text-xs">{actorLabel(run.actor)}</td>
                    <td className="py-2 pr-4 text-xs">{formatTime(run.requested_at)}</td>
                    <td className="py-2 pr-4 text-xs tabular-nums">{runDuration(run)}</td>
                    <td className="py-2 whitespace-nowrap text-right">
                      {(run.status === "canceled" || run.status === "failed") && (
                        <Button variant="ghost" size="sm" onClick={() => onResubmit(run)}>
                          Run again…
                        </Button>
                      )}
                      <Button variant="ghost" size="sm" onClick={() => onSelect(run._id)}>
                        View
                      </Button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {runs.length >= limit && limit < HISTORY_MAX && (
            <div className="mt-2">
              <Button variant="outline" size="sm" onClick={onShowMore}>
                Show more
              </Button>
            </div>
          )}
        </div>
      )}
    </section>
  );
}

/** The run a single-flight conflict carries, if that is what this failure is. */
function conflictingRun(e: unknown): Task | null {
  if (!(e instanceof ApiRequestError) || e.status !== 409) return null;
  const run = e.data;
  return run && typeof run === "object" && "_id" in run ? (run as Task) : null;
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
