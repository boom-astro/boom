// Task runs for the admin page's Tasks tab: what is queued or running, the
// history of finished runs, one run's logs, and starting a task from its
// schema. Each run records who started it and with which parameters; see
// docs/task-system.md.
import { useEffect, useRef, useState } from "react";
import {
  ApiRequestError,
  cancelTaskRun,
  conflictingRun,
  fetchTaskLogs,
  fetchTaskRun,
  isActive,
  resubmitTaskRun,
  type TaskLogLine,
  type Task,
  type TaskType,
} from "@/lib/adminApi";
import { TaskForm } from "@/components/task-form";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";

/** How often to re-poll while anything is still running. */
export const POLL_MS = 3000;
/** Queued and running runs are fetched on their own, so none drops out of view
 *  behind newer history. */
export const ACTIVE_FETCHED = 200;
/** History page size, and the most "Show more" will ask the API for. */
export const HISTORY_PAGE = 25;
export const HISTORY_MAX = 500;

export type HistoryFilter = "all" | "succeeded" | "failed" | "canceled";
const HISTORY_FILTERS: { value: HistoryFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
  { value: "canceled", label: "Canceled" },
];

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
export function CancelRunDialog({
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
export function ResubmitDialog({
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
export function ActiveRuns({ runs, onSelect, onCancel }: {
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
export function RunDetail({
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

/**
 * Every task this release can run, with a form built from its schema.
 *
 * Catalog ingests have their own row-level button above, because starting one
 * from the drift table needs no parameters. This is for everything else.
 */
export function TaskCatalog({
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

export function RunsTable({
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
