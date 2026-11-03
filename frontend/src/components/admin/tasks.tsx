// Task runs for the admin page's Tasks tab: one table of runs with what is
// queued or running at the top, one run's logs, and starting a task from its
// schema. Each run records who started it and with which parameters; see
// docs/task-system.md.
import { useEffect, useRef, useState } from "react";
import { IconArrowLeft } from "@tabler/icons-react";
import { toast } from "sonner";
import {
  ApiRequestError,
  cancelTaskRun,
  conflictingRun,
  fetchTaskLogs,
  fetchTaskRun,
  isActive,
  resubmitTaskRun,
  submitTask,
  type TaskLogLine,
  type TaskStatus,
  type Task,
  type TaskType,
} from "@/lib/adminApi";
import { TaskForm } from "@/components/task-form";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

/** How often to re-poll while anything is still running. */
export const POLL_MS = 3000;
/** Queued and running runs are fetched on their own, so none drops out of view
 *  behind newer history. */
export const ACTIVE_FETCHED = 200;
/** History page size, and the most "Show more" will ask the API for. */
export const HISTORY_PAGE = 25;
export const HISTORY_MAX = 500;

const ADD_TASK_TYPE_DOCS =
  "https://github.com/boom-astro/boom/blob/main/docs/task-system.md#adding-a-task-type";

export type HistoryFilter = "all" | "active" | "succeeded" | "failed" | "canceled";
const HISTORY_FILTERS: { value: HistoryFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "active", label: "In progress" },
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
      <pre className="max-h-64 overflow-y-auto font-mono whitespace-pre-wrap break-all">
        {JSON.stringify(run.params, null, 2)}
      </pre>
    </div>
  );
}

function cancelOutcome(status: TaskStatus | null): string {
  switch (status) {
    case "running":
    case null:
      return "Cancel requested. The run stops at its next safe point.";
    case "canceled":
    case "queued":
      return "Run canceled.";
    default:
      return `That run had already ${status}, so there was nothing to cancel.`;
  }
}

/** Canceling sits one click from a list of runs, so it asks first. */
export function CancelRunDialog({
  run,
  onCanceled,
  onClose,
}: {
  run: Task | null;
  onCanceled?: () => void;
  onClose: () => void;
}) {
  async function confirm(target: Task) {
    onClose();
    try {
      toast(cancelOutcome(await cancelTaskRun(target._id)));
      onCanceled?.();
    } catch (e) {
      toast.error(`Could not cancel the run: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

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
            onClick={() => run && void confirm(run)}
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
      <div className="flex justify-between gap-2 text-xs text-muted-foreground mb-1">
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
                disabled={run.cancel_requested}
                onClick={() => setConfirmCancel(run)}
              >
                {run.cancel_requested ? "Cancel requested" : "Cancel…"}
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
        // Re-read now rather than at the next poll, so the button reflects it.
        onCanceled={() => void fetchTaskRun(runId).then(setRun, () => {})}
        onClose={() => setConfirmCancel(null)}
      />
    </>
  );
}

/** Picks a task type, then configures and submits it. Owns its error, since it
 *  covers the page's messages. */
export function NewRunDialog({
  open,
  types,
  onClose,
  onQueued,
}: {
  open: boolean;
  types: TaskType[];
  onClose: () => void;
  onQueued: (run: Task, notice?: string) => void;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const task = types.find((t) => t.id === selected) ?? null;

  useEffect(() => {
    if (!open) return;
    setSelected(null);
    setBusy(false);
    setError(null);
  }, [open]);

  async function submit(taskType: string, params: Record<string, unknown>) {
    setBusy(true);
    setError(null);
    try {
      onQueued(await submitTask(taskType, params));
    } catch (e) {
      const existing = conflictingRun(e);
      if (existing) {
        onQueued(existing, `An equivalent ${taskType} run is already in flight. Showing that one.`);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={(next) => !next && onClose()}>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          {task && (
            <Button
              variant="ghost"
              size="sm"
              className="-ml-2 w-fit text-muted-foreground"
              onClick={() => {
                setSelected(null);
                setError(null);
              }}
            >
              <IconArrowLeft />
              All tasks
            </Button>
          )}
          <DialogTitle>{task ? task.title : "Run a task"}</DialogTitle>
          <DialogDescription>
            {task ? (
              task.description
            ) : (
              <>
                Missing one? See{" "}
                <a
                  className="underline hover:no-underline"
                  href={ADD_TASK_TYPE_DOCS}
                  target="_blank"
                  rel="noreferrer"
                >
                  adding a task type
                </a>
                .
              </>
            )}
          </DialogDescription>
        </DialogHeader>
        {task ? (
          // Keyed by task, so the "I understand this can destroy data" box
          // does not arrive already checked on the next task.
          <TaskForm
            key={task.id}
            task={task}
            busy={busy}
            onCancel={onClose}
            onSubmit={(params) => void submit(task.id, params)}
          />
        ) : (
          <div className="rounded-md border">
            {types.map((type) => (
              <button
                key={type.id}
                type="button"
                className="block w-full border-b px-3 py-2 text-left last:border-0 hover:bg-muted/50"
                onClick={() => setSelected(type.id)}
              >
                <div className="text-sm font-medium">
                  {type.title}{" "}
                  {type.destructive && <Badge variant="destructive">can destroy data</Badge>}
                </div>
                <div className="text-xs text-muted-foreground">{type.description}</div>
                <div className="text-xs text-muted-foreground font-mono">{type.id}</div>
              </button>
            ))}
          </div>
        )}
        {error && <p className="text-sm text-destructive">{error}</p>}
      </DialogContent>
    </Dialog>
  );
}

/** In-progress runs pinned above finished ones, each group newest first. */
export function RunsTable({
  active,
  finished,
  filter,
  onFilter,
  limit,
  onShowMore,
  onSelect,
  onCancel,
  onResubmit,
  onNewRun,
  error,
}: {
  active: Task[];
  /** Null while the current filter's runs are being fetched. */
  finished: Task[] | null;
  filter: HistoryFilter;
  onFilter: (filter: HistoryFilter) => void;
  limit: number;
  onShowMore: () => void;
  onSelect: (id: string) => void;
  onCancel: (run: Task) => void;
  onResubmit: (run: Task) => void;
  onNewRun: () => void;
  error: string | null;
}) {
  const showActive = filter === "all" || filter === "active";
  const shownFinished = filter === "active" ? [] : (finished ?? []);
  // The two lists are read separately, so a run that finished between the reads
  // is in both; its finished copy is the newer one.
  const finishedIds = new Set(shownFinished.map((run) => run._id));
  const rows = [
    ...(showActive ? active.filter((run) => !finishedIds.has(run._id)) : []),
    ...shownFinished,
  ];
  const emptyText = error
    ? "Task runs could not be read"
    : finished === null && filter !== "active"
      ? "Loading…"
      : filter === "all"
      ? "Nothing has been run yet"
      : filter === "active"
        ? "Nothing is queued or running"
        : `No ${filter} runs`;

  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>Tasks</CardTitle>
        <CardAction>
          <Button size="sm" onClick={onNewRun}>
            Run task…
          </Button>
        </CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex flex-wrap gap-1" role="group" aria-label="Filter runs by status">
          {HISTORY_FILTERS.map((f) => (
            <Button
              key={f.value}
              size="sm"
              variant={filter === f.value ? "secondary" : "ghost"}
              aria-pressed={filter === f.value}
              onClick={() => onFilter(f.value)}
            >
              {f.label}
              {f.value === "active" && active.length > 0 && (
                <Badge variant="secondary" className="ml-1 px-1.5">
                  {active.length}
                </Badge>
              )}
            </Button>
          ))}
        </div>

        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Task</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Started by</TableHead>
                <TableHead>Requested</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-muted-foreground">
                    {emptyText}
                  </TableCell>
                </TableRow>
              ) : (
                rows.map((run) => {
                  const params = paramSummary(run.params);
                  const running = isActive(run);
                  return (
                    <TableRow key={run._id}>
                      <TableCell className="max-w-56">
                        <div className="font-medium">{run.task_type}</div>
                        {params && (
                          <div
                            className="text-xs text-muted-foreground font-mono truncate"
                            title={params}
                          >
                            {params}
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="min-w-32">
                        <div className="flex flex-wrap gap-1">
                          <Badge variant={statusVariant(run.status)}>{run.status}</Badge>
                          {running && run.cancel_requested && (
                            <Badge variant="outline">stopping</Badge>
                          )}
                        </div>
                        {running && run.progress.total > 0 && (
                          <div className="mt-1">
                            <ProgressBar progress={run.progress} />
                          </div>
                        )}
                        {run.attempts > 1 && (
                          <div className="text-xs text-muted-foreground">
                            attempt {run.attempts}
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground">
                        {actorLabel(run.actor)}
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground">
                        <div>{formatTime(run.requested_at)}</div>
                        {run.started_at && (
                          <div className="text-xs tabular-nums">
                            {running ? "running for" : "took"} {runDuration(run)}
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="text-right whitespace-nowrap">
                        {running ? (
                          <Button
                            variant="ghost"
                            size="sm"
                            disabled={run.cancel_requested}
                            onClick={() => onCancel(run)}
                          >
                            {run.cancel_requested ? "Stopping…" : "Cancel…"}
                          </Button>
                        ) : (
                          (run.status === "canceled" || run.status === "failed") && (
                            <Button variant="ghost" size="sm" onClick={() => onResubmit(run)}>
                              Run again…
                            </Button>
                          )
                        )}
                        <Button variant="ghost" size="sm" onClick={() => onSelect(run._id)}>
                          {running ? "Logs" : "View"}
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </div>

        <div className="flex items-center justify-between text-sm text-muted-foreground">
          <span>
            {rows.length} {rows.length === 1 ? "run" : "runs"}
          </span>
          {filter !== "active" && shownFinished.length >= limit && limit < HISTORY_MAX && (
            <Button variant="outline" size="sm" onClick={onShowMore}>
              Show more
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
