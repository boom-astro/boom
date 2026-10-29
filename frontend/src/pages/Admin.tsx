// A page for administrative work like managing users, ingesting new catalogs,
// kicking off reprocessing, etc.
//
// Catalog ingestion is deliberately only reachable from here and the API --
// there is no binary to run over SSH. See docs/task-system.md for why: these
// runs take hours, so they have to survive a deploy, report their logs while
// running, and be cancellable, and they have to leave a record of who ran what.
import { useCallback, useEffect, useRef, useState } from "react";
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
  submitCatalogIngest,
  submitTask,
  unacceptEnrichmentSet,
  type CatalogHealth,
  type CatalogExport,
  type CatalogStatus,
  type EnrichmentDrift,
  type TaskLogLine,
  type Task,
  type TaskType,
} from "@/lib/adminApi";
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

/** How often to re-poll while anything is still running. */
const POLL_MS = 3000;
/** Consecutive failed polls before the page stops and says so. */
const MAX_POLL_FAILURES = 5;
/**
 * Runs fetched per poll.
 *
 * Larger than what the table shows, because the "In progress" section is
 * filtered out of the same list and the API has no status filter: at a page of
 * 25 a long ingest dropped out of it as soon as 25 newer runs existed, and the
 * catalog row went back to offering Ingest for a catalog already being
 * ingested. The server's single-flight check is what actually prevents the
 * second run; this is so the page stops inviting it.
 */
const RUNS_FETCHED = 100;
/** Runs listed under "Recent task runs". */
const RUNS_SHOWN = 25;

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

/**
 * Runs that are queued or running, surfaced above everything else.
 *
 * These are the rows an admin is actually here to watch: a catalog ingest runs
 * for hours, and having to hunt for it in the history is exactly the friction
 * the admin page exists to remove.
 */
function ActiveRuns({ runs, onSelect, onCancel }: {
  runs: Task[];
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
  const [run, setRun] = useState<Task | null>(null);
  const [lines, setLines] = useState<TaskLogLine[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);
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
    // Everything the previous run left behind goes, not just its log. Opening a
    // second run from the table used to show the first one's type, status,
    // actor and error until the next poll came back -- and if that poll failed,
    // for as long as the panel stayed open.
    setLines([]);
    setRun(null);
    setError(null);
    setNotice(null);
    setCanceling(false);

    async function poll() {
      try {
        // Read by id. Looking for the run in the most recent page meant a run
        // that had dropped off that page was simply never found: the header
        // stayed blank, nothing ever satisfied `isActive`, and the poll ran
        // until the panel was closed.
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
    // The log box is scrolled, not the page. `scrollIntoView` moved the whole
    // document to bring the log's end into view, which on a page this long
    // yanked it away from whatever was being read -- every three seconds,
    // because a running ingest appends a line that often.
    if (box && following.current) box.scrollTop = box.scrollHeight;
  }, [lines.length]);

  async function onCancel() {
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

  const progress = run?.progress;
  const pct =
    progress && progress.total > 0 ? Math.round((progress.done / progress.total) * 100) : null;
  const stopping = run?.cancel_requested && isActive(run);

  return (
    <section className="mb-8 border rounded-lg p-4">
      <div className="flex items-start justify-between gap-4 mb-3">
        <div>
          <h2 className="text-lg font-semibold">
            {run?.task_type ?? "Run"}{" "}
            {run && <Badge variant={statusVariant(run.status)}>{run.status}</Badge>}{" "}
            {stopping && <Badge variant="outline">stopping</Badge>}
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
            <Button
              variant="destructive"
              size="sm"
              disabled={canceling || run.cancel_requested}
              onClick={onCancel}
            >
              {canceling ? "Canceling…" : run.cancel_requested ? "Cancel requested" : "Cancel"}
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

      {notice && <p className="text-sm text-muted-foreground mb-3">{notice}</p>}
      {run?.error && <p className="text-sm text-destructive mb-3">{run.error}</p>}
      {error && <p className="text-sm text-destructive mb-3">{error}</p>}

      <div
        ref={logBox}
        // Following is given up the moment the operator scrolls away from the
        // end, and taken back when they return to it.
        onScroll={(e) => {
          const box = e.currentTarget;
          following.current = box.scrollHeight - box.scrollTop - box.clientHeight < 24;
        }}
        className="max-h-80 overflow-y-auto rounded bg-muted/50 p-3 font-mono text-xs"
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
  onSelect,
  error,
}: {
  runs: Task[];
  onSelect: (id: string) => void;
  error: string | null;
}) {
  const shown = runs.slice(0, RUNS_SHOWN);
  return (
    <section>
      <h2 className="text-lg font-semibold mb-1">Recent task runs</h2>
      {runs.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {error ? "Task runs could not be read." : "Nothing has been run yet."}
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
                <th className="py-2 font-medium" />
              </tr>
            </thead>
            <tbody>
              {shown.map((run) => (
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
          {runs.length > shown.length && (
            <p className="text-xs text-muted-foreground mt-2">
              Showing the {shown.length} most recent of {runs.length} fetched. Older runs are in{" "}
              <code>/docs</code> under <code>GET /tasks</code>.
            </p>
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
  const [catalogs, setCatalogs] = useState<CatalogStatus[]>([]);
  const [runs, setTasks] = useState<Task[]>([]);
  const [taskTypes, setTaskTypes] = useState<TaskType[]>([]);
  const [enrichment, setEnrichment] = useState<EnrichmentDrift[]>([]);
  const [exports, setExports] = useState<CatalogExport[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  // Two kinds of failure, kept apart because they have different lifetimes. A
  // load failure describes the page and the next successful poll replaces it;
  // an action failure describes something the operator just did, and clearing
  // it on the next poll is how a 400 naming the parameter it rejected used to
  // disappear three seconds later, before anyone could read it.
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

  // Returns the runs it fetched. The poll below needs them before React has
  // re-rendered with the new state, and reading `runs` there instead would read
  // the value captured when the effect was created -- which, with an empty
  // dependency list, is the initial empty array forever.
  const refresh = useCallback(async (): Promise<Task[]> => {
    try {
      const [status, recent, types, drift, exported] = await Promise.all([
        fetchCatalogStatus(),
        fetchTasks(undefined, RUNS_FETCHED),
        fetchTaskTypes(),
        // Caught here, not with the others: the drift query is the one call
        // that scans, so it fails on its own schedule, and a 500 from it used
        // to reject the whole batch -- costing the operator the catalogs table,
        // the runs table and a running reprocess's log tail. `null` leaves the
        // last reading standing rather than substituting an empty list, which
        // would render as "nothing is stale".
        fetchEnrichmentStatus().catch(() => null),
        fetchCatalogExports(),
      ]);
      setCatalogs(status);
      setTasks(recent);
      setTaskTypes(types);
      if (drift) setEnrichment(drift);
      setExports(exported);
      setLoadError(null);
      failures.current = 0;
      return recent;
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e));
      failures.current += 1;
      return [];
    } finally {
      setLoaded(true);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    let timer: number;
    async function tick() {
      const recent = await refresh();
      if (cancelled) return;
      // Stop after enough consecutive failures rather than retrying forever on
      // an API that is refusing. Picked back up by the Retry button below.
      if (failures.current >= MAX_POLL_FAILURES) return;
      // Keep polling only while something is in flight, so an idle admin page
      // is not hitting the API every few seconds forever.
      const active = recent.some(isActive);
      timer = window.setTimeout(tick, active ? POLL_MS : POLL_MS * 5);
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

  // The newest run per catalog, so the table can show one in flight.
  const runsByCatalog = new Map<string, Task>();
  for (const run of runs) {
    const catalog = typeof run.params.catalog === "string" ? run.params.catalog : null;
    if (catalog && !runsByCatalog.has(catalog)) runsByCatalog.set(catalog, run);
  }

  async function onCancelRun(runId: string) {
    setActionError(null);
    try {
      // A running task stops at its next safe point, so this reports what the
      // server says it did rather than implying the run has stopped.
      setNotice(await cancelTaskRun(runId));
      await refresh();
    } catch (e) {
      setNotice(null);
      setActionError(e instanceof Error ? e.message : String(e));
    }
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
      setSelected(run._id);
      await refresh();
    } catch (e) {
      const existing = conflictingRun(e);
      // The server returns the run that is already in flight rather than a
      // bare error, specifically so the client can jump straight to watching
      // it. Discarding that and reporting a conflict left the operator to go
      // and find a run the page was already holding.
      if (!existing) {
        setActionError(e instanceof Error ? e.message : String(e));
        return;
      }
      setSelected(existing._id);
      setNotice(already);
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
          This page is for administrative tasks like managing users, ingesting new catalogs, and
          kicking off reprocessing.
        </p>

        <AdminUsers />

        {/* Above everything, and not cleared by the next poll: this is the
            answer to something the operator just asked for, and it is the one
            message on the page they are actually waiting on. */}
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

        {!loaded ? (
          <p className="text-sm text-muted-foreground">Loading…</p>
        ) : (
          <>
            <ActiveRuns runs={runs} onSelect={setSelected} onCancel={onCancelRun} />
            <CatalogsTable
              catalogs={catalogs}
              runsByCatalog={runsByCatalog}
              onIngest={onIngest}
              onReingest={setReingesting}
              onSelect={setSelected}
              busy={busy}
              error={loadError}
            />
            <ExportsTable exports={exports} />
            {selected && <RunDetail runId={selected} onClose={() => setSelected(null)} />}
            <EnrichmentDriftTable
              drift={enrichment}
              onReprocess={onReprocessStale}
              onAccept={onAcceptSet}
              onUnaccept={onUnacceptSet}
              busy={busy}
            />
            <TaskCatalog types={taskTypes} onSubmit={onSubmitTask} busy={busy} />
            <RunsTable runs={runs} onSelect={setSelected} error={loadError} />
          </>
        )}
      </div>

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
