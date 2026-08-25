import { useMemo, useState } from 'react';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { Accordion, AccordionItem, AccordionTrigger, AccordionContent } from '@/components/ui/accordion';
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Info } from 'lucide-react';
import useAppStore from '@/lib/store';

function prettyKey(k: string) {
  return k
    .replace(/_/g, ' ')
    .replace(/\b(ra|dec|jd|mjd|mag|sig|id|objname)\b/gi, s => s.toUpperCase())
    .replace(/(^|\s)\w/g, c => c.toUpperCase());
}

export default function CrossmatchCard() {
  const [helpDialogOpen, setHelpDialogOpen] = useState(false);
  const current = useAppStore(state => state.currentSource) as { data?: { cross_matches?: Record<string, unknown[]> } } | null;

  const crossMatches = current?.data?.cross_matches ?? {};

  const { catalogs, firstNonEmpty } = useMemo(() => {
    const entries = Object.entries(crossMatches || {});
    entries.sort((a, b) => {
      const aHas = Array.isArray(a[1]) && a[1].length > 0;
      const bHas = Array.isArray(b[1]) && b[1].length > 0;
      if (aHas !== bHas) return aHas ? -1 : 1; // catalogs with matches first
      return a[0].localeCompare(b[0]); // alphabetically within each group
    });
    const first = entries.find(([, matches]) => Array.isArray(matches) && matches.length > 0)?.[0];
    return { catalogs: entries, firstNonEmpty: first };
  }, [crossMatches]);

  if (!current) return null;

  return (
    <Card className="@container/card col-span-1 @xl/main:col-span-2 @5xl/main:col-span-3">
      <CardContent>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <CardTitle className="text-lg">Cross-matches</CardTitle>
            <button 
              onClick={() => setHelpDialogOpen(true)} 
              title="Widget information"
              className="p-1 rounded hover:bg-slate-100 dark:hover:bg-slate-700"
            >
              <Info className="w-4 h-4 text-gray-500 dark:text-gray-400" />
            </button>
          </div>
        </div>
        {catalogs.length === 0 && (
          <div className="text-sm text-gray-500">No cross-matches available for this object.</div>
        )}

        {catalogs.length > 0 && (
          <Accordion type="single" collapsible className="w-full" defaultValue={firstNonEmpty || undefined}>
            {catalogs.map(([cat, matches]) => {
              const list = Array.isArray(matches) ? matches : [];
              const disabled = list.length === 0;

              // gather columns dynamically (union of keys), exclude `coordinates`
              const colSet = new Set<string>();
              list.forEach((m: unknown) => {
                if (!m || typeof m !== 'object') return;
                const obj = m as Record<string, unknown>;
                Object.keys(obj).forEach(k => { if (k !== 'coordinates') colSet.add(k); });
              });
              // ensure separation_arcsec is visible
              if (colSet.has('separation_arcsec') === false) colSet.add('separation_arcsec');

              // convert to array and try to order important keys first
              const preferredOrder = ['ra','dec','RA','DEC','objname','name','separation_arcsec'];
              const cols = Array.from(colSet).sort((a,b) => {
                const ai = preferredOrder.indexOf(a);
                const bi = preferredOrder.indexOf(b);
                if (ai !== -1 || bi !== -1) {
                  if (ai === -1) return 1;
                  if (bi === -1) return -1;
                  return ai - bi;
                }
                return a.localeCompare(b);
              });

              return (
                <AccordionItem key={cat} value={cat} className={`rounded-md ${disabled ? 'opacity-40' : ''}`}>
                  <AccordionTrigger disabled={disabled} className="font-medium">
                    <div className="flex w-full items-center gap-3">
                      <div className="font-semibold">{cat}</div>
                      <Badge variant="outline" className="text-xs">{list.length} match{list.length !== 1 ? 'es' : ''}</Badge>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    {disabled ? (
                      <div className="text-sm text-gray-500">No matches in this catalog.</div>
                    ) : (
                      <div className="overflow-x-auto">
                        <Table className="min-w-[600px]">
                          <TableHeader>
                            <tr>
                              {cols.map(c => (
                                <TableHead key={c}>{prettyKey(c)}</TableHead>
                              ))}
                            </tr>
                          </TableHeader>
                          <TableBody>
                            {list.map((rowRaw: unknown, idx: number) => (
                              <TableRow key={idx}>
                                {cols.map(col => {
                                  const row = (rowRaw ?? {}) as Record<string, unknown>;
                                  const v = row[col];
                                  const formatNum = (num: number) => {
                                    if (!Number.isFinite(num)) return null;
                                    // Limit to at most 5 decimal places, trim trailing zeros
                                    const s = num.toFixed(5).replace(/\.0+$|(?<=\.[0-9]*?)0+$/,'');
                                    return s;
                                  };

                                  if (col === 'separation_arcsec') {
                                    const n = typeof v === 'number' ? v : (v ? Number(v) : NaN);
                                    const s = formatNum(n);
                                    return (
                                      <TableCell key={col}>
                                        {s ? `${s}″` : '—'}
                                      </TableCell>
                                    );
                                  }
                                  if (v === null || v === undefined) return <TableCell key={col}>—</TableCell>;
                                  if (typeof v === 'number') return <TableCell key={col}>{formatNum(v)}</TableCell>;
                                  if (typeof v === 'string') {
                                    // if string contains a numeric value, format it
                                    const maybeNum = Number(v);
                                    if (Number.isFinite(maybeNum)) {
                                      const s = formatNum(maybeNum);
                                      return <TableCell key={col}>{s ?? v}</TableCell>;
                                    }
                                    return <TableCell key={col}>{v}</TableCell>;
                                  }
                                  // fallback for objects/arrays
                                  return <TableCell key={col}><pre className="whitespace-pre-wrap">{JSON.stringify(v)}</pre></TableCell>;
                                })}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    )}
                  </AccordionContent>
                </AccordionItem>
              );
            })}
          </Accordion>
        )}
      </CardContent>

      {/* Help Dialog */}
      <Dialog open={helpDialogOpen} onOpenChange={setHelpDialogOpen}>
        <DialogContent className="w-[min(1000px,95vw)] max-w-none sm:!max-w-none max-h-[90vh] overflow-auto">
          <DialogHeader>
            <DialogTitle className="text-xl">Understanding Cross-matches</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 text-sm">
            <div>
              <h3 className="font-semibold mb-2">What This Widget Shows</h3>
              <p className="text-gray-600 dark:text-gray-300">
                This widget displays cross-matches between the survey's object and sources in various astronomical catalogs. 
                Each catalog contains a list of known astronomical objects, and the cross-match shows which catalog entries 
                are positionally coincident with the detected object, ordered by increasing angular separation.
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Catalog Availability</h3>
              <p className="text-gray-600 dark:text-gray-300 mb-2">
                The catalogs available for cross-matching depend on the survey from which the object originates. Different surveys 
                have pre-computed cross-matches with different sets of reference catalogs, depending on how the broker is configured.
                <br/><span className="font-semibold">If you think other catalogs would be useful, please contact the administrators!</span>
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Pre-computed Matches</h3>
              <p className="text-gray-600 dark:text-gray-300">
                Cross-matches are <span className="font-semibold">pre-computed once when the object is first detected</span> (except for catalogs that change over time).
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Reading the Tables</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">Catalog Name:</span>
                  <span>The name of the reference catalog (e.g., Gaia DR3, 2MASS, PS1, etc.).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Match Count:</span>
                  <span>The number of sources found in that catalog within the search radius.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Separation:</span>
                  <span>The angular distance (in arcseconds) between the survey's object and the catalog source.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Additional Columns:</span>
                  <span>Varies by catalog.</span>
                </div>
              </div>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Interpreting Matches</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">No matches:</span>
                  <span>The survey's object is not in that catalog or lies outside the survey's coverage area.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Small separations (&lt;1"):</span>
                  <span>Likely the same source, especially if the coordinates are from precise astrometric catalogs like Gaia.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Larger separations (1-5"):</span>
                  <span>Possible associations, but could also be chance coincidences (depending on the catalog).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Multiple matches:</span>
                  <span>The closest match is usually the most likely candidate, but consider all nearby sources when available.</span>
                </div>
              </div>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </Card>
  );
}
