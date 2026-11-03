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
import { Input } from "@/components/ui/input";

/** The models, by file hash, and derivation versions that make up a set. */
function SetContents({ set }: { set: EnrichmentSet }) {
  return (
    <div className="space-y-3 text-xs">
      <p className="text-muted-foreground">
        First seen {new Date(set.first_seen * 1000).toLocaleString()} · fingerprint{" "}
        <span className="font-mono" title={set.fingerprint}>
          {set.fingerprint.slice(0, 12)}
        </span>
      </p>
      <table className="w-full">
        <thead>
          <tr className="border-b text-left text-muted-foreground">
            <th className="py-1 pr-4 font-medium">Model</th>
            <th className="py-1 pr-4 font-medium">Version</th>
            <th className="py-1 font-medium">SHA-256</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(set.models).map(([field, model]) => (
            <tr key={field} className="border-b last:border-0">
              <td className="py-1 pr-4 font-mono">{field}</td>
              <td className="py-1 pr-4">{model.name}</td>
              <td className="py-1 font-mono text-muted-foreground" title={model.sha256}>
                {model.sha256.slice(0, 12)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <table className="w-full">
        <thead>
          <tr className="border-b text-left text-muted-foreground">
            <th className="py-1 pr-4 font-medium">Derivation</th>
            <th className="py-1 font-medium">Version</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(set.derivations).map(([name, version]) => (
            <tr key={name} className="border-b last:border-0">
              <td className="py-1 pr-4 font-mono">{name}</td>
              <td className="py-1 tabular-nums">{version}</td>
            </tr>
          ))}
        </tbody>
      </table>
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
    <section className="mb-8">
      <h2 className="text-lg font-semibold mb-1">Enrichment</h2>
      <p className="text-sm text-muted-foreground mb-3">
        Enrichment runs once, when an alert is ingested. After a model or a
        derivation changes, alerts already in the database hold values the
        current code would not produce.
      </p>
      {drift.length === 0 && (
        <p className="text-sm text-muted-foreground">No survey reports enrichment.</p>
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

            {survey.current && (
              <Accordion type="single" collapsible className="mt-2">
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
