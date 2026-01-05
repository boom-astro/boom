import { useMemo } from 'react';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { Accordion, AccordionItem, AccordionTrigger, AccordionContent } from '@/components/ui/accordion';
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import useAppStore from '@/lib/store';

function prettyKey(k: string) {
  return k
    .replace(/_/g, ' ')
    .replace(/\b(ra|dec|jd|mjd|mag|sig|id|objname)\b/gi, s => s.toUpperCase())
    .replace(/(^|\s)\w/g, c => c.toUpperCase());
}

export default function CrossmatchCard() {
  const current = useAppStore(state => state.currentSource) as { data?: { cross_matches?: Record<string, unknown[]> } } | null;

  const crossMatches = current?.data?.cross_matches ?? {};

  const catalogs = useMemo(() => {
    // produce sorted array of [catalogName, matches[]] with catalogs having matches first
    const entries = Object.entries(crossMatches || {});
    entries.sort((a, b) => {
      const al = Array.isArray(a[1]) ? a[1].length : 0;
      const bl = Array.isArray(b[1]) ? b[1].length : 0;
      if (al !== bl) return bl - al; // more matches first
      return a[0].localeCompare(b[0]);
    });
    return entries;
  }, [crossMatches]);

  if (!current) return null;

  return (
    <Card className="@container/card col-span-2 row-span-2">
      <CardContent>
        <div className="flex items-center justify-between">
            <CardTitle className="text-lg">Cross-matches</CardTitle>
        </div>
        {catalogs.length === 0 && (
          <div className="text-sm text-gray-500">No cross-matches available for this object.</div>
        )}

        {catalogs.length > 0 && (
          <Accordion type="single" collapsible className="w-full">
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
                    <div className="flex items-center justify-between w-full">
                      <div className="flex items-center gap-3">
                        <div className="font-semibold">{cat}</div>
                      </div>
                      <div className="flex items-center gap-3">
                        <Badge variant="outline" className="text-xs">{list.length}</Badge>
                      </div>
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
    </Card>
  );
}
