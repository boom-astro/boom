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
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>Catalogs</CardTitle>
        <CardDescription>
          Every catalog this release can ingest, and whether the alert pipeline crossmatches
          against it.
        </CardDescription>
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
                <TableHead>Records</TableHead>
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
                  const label = healthLabel(catalog.health);
                  const run = runsByCatalog.get(catalog.id);
                  const running = run && isActive(run);
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
                        <Badge variant={label.variant}>{label.text}</Badge>
                      </TableCell>
                      <TableCell>
                        {/* Whether the alert pipeline queries this catalog. An
                            un-ingested catalog nobody crossmatches against is
                            simply available; a configured one that is missing
                            returns zero matches on every alert, silently. */}
                        {catalog.crossmatched ? (
                          <Badge variant="outline">in use</Badge>
                        ) : (
                          <span className="text-sm text-muted-foreground">not configured</span>
                        )}
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground tabular-nums">
                        {catalog.chunks_total > 0
                          ? `${catalog.chunks_done}/${catalog.chunks_total}`
                          : "—"}
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground tabular-nums">
                        {catalog.n_records > 0 ? catalog.n_records.toLocaleString() : "—"}
                      </TableCell>
                      <TableCell className="text-right">
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
