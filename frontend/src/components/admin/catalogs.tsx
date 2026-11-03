// Catalogs for the admin page's Catalogs tab: which archival catalogs are
// ingested, starting or resuming an ingest, and exports of catalogs BOOM cannot
// fetch again. Ingests run as tasks; see docs/catalogs.md.
import {
  isActive,
  type CatalogExport,
  type CatalogHealth,
  type CatalogStatus,
  type Task,
} from "@/lib/adminApi";
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

export function CatalogsTable({
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

function bytes(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(0)} kB`;
  return `${n} B`;
}

/** A curl command, not a download button: chunks are too big to buffer in the tab. */
export function ExportsTable({ exports }: { exports: CatalogExport[] }) {
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


/** Re-ingesting a present catalog drops it first, so it asks. */
export function ReingestDialog({
  catalog,
  onConfirm,
  onClose,
}: {
  catalog: CatalogStatus | null;
  onConfirm: (catalog: CatalogStatus) => void;
  onClose: () => void;
}) {
  return (
    <Dialog open={catalog !== null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Re-ingest {catalog?.id}?</DialogTitle>
          <DialogDescription>
            {catalog?.collection ?? "The collection"} is dropped and
            rebuilt from the archive, which takes hours.
            {catalog?.crossmatched
              ? " The alert pipeline queries this catalog, and until the ingest finishes it will match against whatever has been inserted so far rather than failing — so alerts processed during the run will be under-matched."
              : " Nothing crossmatches against it, so the alert pipeline is unaffected."}
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="destructive"
            onClick={() => {
              if (catalog) onConfirm(catalog);
              onClose();
            }}
          >
            Drop and re-ingest
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
