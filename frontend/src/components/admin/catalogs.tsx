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
  Card,
  CardContent,
  CardDescription,
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const STATE_VARIANT: Record<CatalogHealth, "secondary" | "destructive" | "outline"> = {
  present: "outline",
  missing: "destructive",
  partial: "destructive",
  ingesting: "secondary",
  undeclared: "outline",
  external: "outline",
};

const STATE_TINT: Partial<Record<CatalogHealth, string>> = {
  present: "border-transparent bg-emerald-600 text-white dark:bg-emerald-600/60",
  partial: "border-transparent bg-amber-600 text-white dark:bg-amber-600/60",
  external: "text-muted-foreground",
};

function healthText(health: CatalogHealth): string {
  switch (health) {
    case "present":
      return "Present";
    case "missing":
      return "Missing";
    // Partly ingested is worse than absent: a crossmatch against it returns
    // fewer matches rather than failing, so it is called out separately.
    case "partial":
      return "Partial";
    case "ingesting":
      return "Ingesting";
    case "undeclared":
      return "Unknown slug";
    // Not a problem to fix: the collection is real and populated by something
    // other than the ingest. The row's title says what.
    case "external":
      return "Built elsewhere";
  }
}

function Chunks({ catalog, health }: { catalog: CatalogStatus; health: CatalogHealth }) {
  const { chunks_done: done, chunks_total: total } = catalog;
  if (total === 0) return <span className="text-muted-foreground">—</span>;
  if ((health !== "partial" && health !== "ingesting") || done >= total) {
    return (
      <span className="text-muted-foreground">
        {done}/{total}
      </span>
    );
  }
  return (
    <div className="flex w-28 flex-col gap-1">
      <div className="h-1.5 overflow-hidden rounded-full bg-muted">
        <div className="h-full bg-primary" style={{ width: `${(done / total) * 100}%` }} />
      </div>
      <span className="text-xs text-muted-foreground">
        {done}/{total}
      </span>
    </div>
  );
}

export function CatalogsTable({
  catalogs,
  runsByCatalog,
  onIngest,
  onReingest,
  busy,
  error,
}: {
  catalogs: CatalogStatus[];
  runsByCatalog: Map<string, Task>;
  onIngest: (id: string) => void;
  onReingest: (catalog: CatalogStatus) => void;
  busy: string | null;
  error: string | null;
}) {
  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>Catalogs</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {error && <p className="text-sm text-destructive">{error}</p>}
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Catalog</TableHead>
                <TableHead>Collection</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Crossmatch</TableHead>
                <TableHead>Chunks</TableHead>
                <TableHead className="text-right">Records</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {catalogs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-24 text-center text-muted-foreground">
                    {/* A read that failed and a release with no catalogs both
                        leave this list empty, and they call for opposite
                        responses -- one is a thing to go and fix, the other is
                        nothing at all. */}
                    {error
                      ? "Catalog status could not be read, so nothing is known about what is ingested"
                      : "This release defines no catalogs"}
                  </TableCell>
                </TableRow>
              ) : (
                catalogs.map((catalog) => {
                  const run = runsByCatalog.get(catalog.id);
                  const running = run !== undefined && isActive(run);
                  const health: CatalogHealth = running ? "ingesting" : catalog.health;
                  const starting = busy === catalog.id;
                  return (
                    <TableRow key={catalog.id}>
                      <TableCell className="max-w-64 whitespace-normal">
                        <div className="font-medium">{catalog.id}</div>
                        {catalog.title && (
                          <div className="text-xs text-muted-foreground">{catalog.title}</div>
                        )}
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {catalog.collection ?? "—"}
                      </TableCell>
                      <TableCell>
                        <Badge variant={STATE_VARIANT[health]} className={STATE_TINT[health]}>
                          {running && run.status === "queued" ? "Queued" : healthText(health)}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-sm">
                        {/* Whether the alert pipeline queries this catalog. An
                            un-ingested catalog nobody crossmatches against is
                            simply available; a configured one that is missing
                            returns zero matches on every alert, silently. */}
                        {catalog.crossmatched ? (
                          "in use"
                        ) : (
                          <span className="text-muted-foreground">not configured</span>
                        )}
                      </TableCell>
                      <TableCell className="text-sm tabular-nums">
                        <Chunks catalog={catalog} health={health} />
                      </TableCell>
                      <TableCell className="text-right text-sm tabular-nums">
                        {catalog.n_records > 0 ? (
                          catalog.n_records.toLocaleString()
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        {health === "missing" || health === "partial" ? (
                          <Button size="sm" disabled={starting} onClick={() => onIngest(catalog.id)}>
                            {starting
                              ? "Starting…"
                              : health === "partial"
                                ? "Resume ingest"
                                : "Ingest"}
                          </Button>
                        ) : health === "present" ? (
                          // A present catalog has every chunk, so resuming would
                          // do nothing: the only re-ingest that means anything
                          // drops the collection and rebuilds it. That is
                          // destructive, so it goes through a confirmation.
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={starting}
                            onClick={() => onReingest(catalog)}
                          >
                            {starting ? "Starting…" : "Re-ingest"}
                          </Button>
                        ) : null}
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
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
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>Catalog exports</CardTitle>
        <CardDescription>
          Written by the <code>export_catalog</code> task, for catalogs BOOM cannot fetch
          again. Download these, publish them somewhere durable, then put the URL in the
          catalog's boompy module so every deployment can ingest it.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {exports.map((exp) => (
          <div key={exp.collection} className="flex flex-col gap-2">
            <div className="flex items-center justify-between gap-4">
              <span className="font-medium">{exp.collection}</span>
              <span className="text-xs text-muted-foreground font-mono">
                {exp.manifest?.rows?.toLocaleString() ?? "?"} rows
                {exp.manifest?.source_database ? ` · from ${exp.manifest.source_database}` : ""}
                {exp.manifest?.code_version?.git_sha
                  ? ` · ${exp.manifest.code_version.git_sha.slice(0, 8)}`
                  : ""}
              </span>
            </div>
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>File</TableHead>
                    <TableHead className="text-right">Size</TableHead>
                    <TableHead>Download</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {exp.files.map((f) => (
                    <TableRow key={f.name}>
                      <TableCell className="font-mono text-xs">{f.name}</TableCell>
                      <TableCell className="text-sm text-muted-foreground tabular-nums text-right">
                        {bytes(f.bytes)}
                      </TableCell>
                      <TableCell className="whitespace-normal">
                        <code className="text-[11px] text-muted-foreground break-all">
                          curl -H "Authorization: Bearer $TOKEN" -O{" "}
                          {`${origin}/api/catalogs/exports/${exp.collection}/${f.name}`}
                        </code>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
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
