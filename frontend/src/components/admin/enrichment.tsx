// Enrichment for the admin page's Enrichment tab: which enrichment set each
// survey's alerts were produced by, what the current set contains, and
// reprocessing or accepting the stale ones. See docs/alert-processing.md.
import { useState } from "react";
import type { EnrichmentDrift, EnrichmentSet } from "@/lib/adminApi";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

/** The models, by file hash, and derivation versions that make up a set. */
function SetContents({ set }: { set: EnrichmentSet }) {
  return (
    <div className="flex flex-col gap-3">
      <p className="text-xs text-muted-foreground">
        First seen {new Date(set.first_seen * 1000).toLocaleString()} · fingerprint{" "}
        <span className="font-mono" title={set.fingerprint}>
          {set.fingerprint.slice(0, 12)}
        </span>
      </p>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Model</TableHead>
              <TableHead>Version</TableHead>
              <TableHead>SHA-256</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {Object.entries(set.models).map(([field, model]) => (
              <TableRow key={field}>
                <TableCell className="font-mono text-xs">{field}</TableCell>
                <TableCell>{model.name}</TableCell>
                <TableCell
                  className="font-mono text-xs text-muted-foreground"
                  title={model.sha256}
                >
                  {model.sha256.slice(0, 12)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Derivation</TableHead>
              <TableHead>Version</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {Object.entries(set.derivations).map(([name, version]) => (
              <TableRow key={name}>
                <TableCell className="font-mono text-xs">{name}</TableCell>
                <TableCell className="tabular-nums">{version}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

/** Reports enrichment drift and never acts on it: re-enriching takes days. */
export function EnrichmentDriftTable({
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

  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>Enrichment</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-6">
        {drift.length === 0 && (
          <div className="rounded-md border h-24 flex items-center justify-center text-sm text-muted-foreground">
            No survey reports enrichment
          </div>
        )}

        {drift.map((survey) => {
          const unknown = survey.current_set === null;
          const timedOut = !unknown && survey.drift_unknown;
          const clean =
            !unknown &&
            !timedOut &&
            survey.stale_sets.length === 0 &&
            !survey.has_unstamped;
          return (
            <div key={survey.survey} className="flex flex-col gap-3">
              <div className="flex items-center justify-between gap-4">
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
                  No alert is stale.
                </p>
              ) : (
                <div className="rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Stale set</TableHead>
                        <TableHead />
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {survey.has_unstamped && (
                        <TableRow>
                          <TableCell className="whitespace-normal">
                            <div className="font-medium">Enriched before stamping</div>
                            <div className="text-xs text-muted-foreground">
                              Below a derivation version declared wrong.
                            </div>
                          </TableCell>
                          <TableCell className="w-32 text-right text-sm text-muted-foreground">
                            no set
                          </TableCell>
                        </TableRow>
                      )}
                      {survey.stale_sets.map((stale) => (
                        <TableRow key={stale.id}>
                          <TableCell className="whitespace-normal">
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
                          </TableCell>
                          <TableCell className="w-32 text-right">
                            {accepting !== stale.id && (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => setAccepting(stale.id)}
                              >
                                Accept
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}

              {survey.current && (
                <Accordion type="single" collapsible>
                  <AccordionItem value="current">
                    <AccordionTrigger className="py-2 text-xs">
                      What current set {survey.current._id} contains
                    </AccordionTrigger>
                    <AccordionContent>
                      <SetContents set={survey.current} />
                    </AccordionContent>
                  </AccordionItem>
                </Accordion>
              )}

              {survey.accepted_sets.length > 0 && (
                <p className="text-xs text-muted-foreground">
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
      </CardContent>
    </Card>
  );
}
