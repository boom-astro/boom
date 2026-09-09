import { useState } from 'react';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Info } from 'lucide-react';

// Output of the FLARE light-curve classifier, written by the enrichment worker
// under the alert's `flare` field. The card is not rendered when the field is absent.
export type FlareResult = {
  p_sn_ia?: number;
  p_sn_cc?: number;
  p_slsn?: number;
  p_agn?: number;
  p_tde?: number;
  p_cv?: number;
  label?: string;
  set?: string[];
  alpha?: number;
  credibility?: number;
  confidence?: number;
  energy?: number;
  energy_percentile?: number | null;
  p_anomaly?: number;
  base_rate?: number;
  likelihood_ratio?: number;
  novelty_p?: number | null;
  argmax_excluded?: boolean;
  p_values?: Record<string, number>;
  coverage_measured?: number | null;
  coverage_n?: number | null;
  p_value_floor?: number | null;
  n_context_present?: number;
  model?: string;
};

type ProbKey = 'p_sn_ia' | 'p_sn_cc' | 'p_slsn' | 'p_agn' | 'p_tde' | 'p_cv';

const CLASSES: Array<{ key: string; name: string; prob: ProbKey }> = [
  { key: 'SN_Ia', name: 'SN Ia', prob: 'p_sn_ia' },
  { key: 'SN_CC', name: 'SN CC', prob: 'p_sn_cc' },
  { key: 'SLSN', name: 'SLSN', prob: 'p_slsn' },
  { key: 'AGN', name: 'AGN', prob: 'p_agn' },
  { key: 'TDE', name: 'TDE', prob: 'p_tde' },
  { key: 'CV', name: 'CV', prob: 'p_cv' },
];

const classLabel = (key: string) => CLASSES.find((c) => c.key === key)?.name ?? key.replace('_', ' ');
const pct = (x: number | undefined | null, digits = 1) =>
  x === undefined || x === null ? '—' : `${(x * 100).toFixed(digits)}%`;
const num = (x: number | undefined | null, digits = 3) =>
  x === undefined || x === null ? '—' : x.toFixed(digits);

export default function FlareCard({ alert }: { alert: unknown }) {
  const [helpDialogOpen, setHelpDialogOpen] = useState(false);
  const flare = (alert as { flare?: FlareResult } | null)?.flare;
  if (!flare) return null;

  const set = flare.set ?? null;
  const coverage = flare.alpha === undefined ? null : Math.round((1 - flare.alpha) * 100);
  const setLabel = coverage === null ? 'prediction set' : `${coverage}% prediction set`;
  const alphaLabel = flare.alpha === undefined ? 'the alpha level' : `${flare.alpha}`;
  const noveltyP = flare.novelty_p ?? null;
  const percentile = flare.energy_percentile ?? null;
  // The novelty p-value is the fraction of known-class objects at least this extreme,
  // so the object is an anomaly at X% false-alarm rate when novelty_p <= X/100.
  // Report the strictest level it passes, if any.
  const farLevel = noveltyP === null ? null : [0.01, 0.05, 0.1].find((far) => noveltyP <= far) ?? null;
  const needsReview = farLevel === 0.01 || !!flare.argmax_excluded;
  const anomalyText =
    noveltyP === null
      ? '—'
      : farLevel !== null
        ? `Yes, at ${Math.round(farLevel * 100)}% FAR`
        : 'No';

  return (
    <Card className="@container/card col-span-1 @xl/main:col-span-2">
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <CardTitle className="text-lg">FLARE</CardTitle>
            <button
              onClick={() => setHelpDialogOpen(true)}
              title="Widget information"
              className="p-1 rounded hover:bg-slate-100 dark:hover:bg-slate-700"
            >
              <Info className="w-4 h-4 text-gray-500 dark:text-gray-400" />
            </button>
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            {flare.model && (
              <Badge variant="outline" className="text-xs" title={flare.model}>
                {flare.model.replace(/\s+v[\d.]+.*$/, '')}
              </Badge>
            )}
            {needsReview && (
              <Badge variant="outline" className="text-xs font-semibold">Review</Badge>
            )}
          </div>
        </div>

        <div className="text-sm">
          <span className="font-medium">
            {set === null ? '—' : set.length ? `{${set.map(classLabel).join(', ')}}` : 'No class'}
          </span>
          {coverage !== null && <span className="text-muted-foreground"> at {coverage}% coverage</span>}
        </div>

        <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
          <span className="text-muted-foreground">Anomaly</span>
          <span className="font-medium">
            {anomalyText}
            {farLevel === null && percentile !== null && (
              <span className="text-muted-foreground">
                {' '}(percentile {percentile >= 99.5 ? percentile.toFixed(1) : percentile.toFixed(0)} of the known-class stream)
              </span>
            )}
          </span>

          <span className="text-muted-foreground">Credibility</span>
          <span className="font-medium">{num(flare.credibility)}</span>

          <span className="text-muted-foreground">Confidence</span>
          <span className="font-medium">{num(flare.confidence)}</span>

          <span className="text-muted-foreground">P(anomaly)</span>
          <span className="font-medium">
            {pct(flare.p_anomaly)}
            {flare.base_rate !== undefined && (
              <span className="text-muted-foreground"> at a {pct(flare.base_rate)} base rate</span>
            )}
          </span>

          <span className="text-muted-foreground">Novelty p-value</span>
          <span className="font-medium">{num(flare.novelty_p)}</span>

          <span className="text-muted-foreground">Catalog columns</span>
          <span className="font-medium">
            {flare.n_context_present !== undefined ? `${flare.n_context_present} of 30` : '—'}
          </span>
        </div>

        <div className="rounded-md border overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Class</TableHead>
                <TableHead className="text-right">Probability</TableHead>
                <TableHead className="text-right">p-value</TableHead>
                <TableHead className="text-right">In set</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {CLASSES.map(({ key, name, prob }) => {
                const inSet = set?.includes(key) ?? false;
                return (
                  <TableRow key={key} className={inSet ? 'font-medium' : ''}>
                    <TableCell>{name}</TableCell>
                    <TableCell className="text-right">{pct(flare[prob])}</TableCell>
                    <TableCell className="text-right">{num(flare.p_values?.[key])}</TableCell>
                    <TableCell className="text-right">{inSet ? 'Yes' : '—'}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      </CardContent>

      {/* Help Dialog */}
      <Dialog open={helpDialogOpen} onOpenChange={setHelpDialogOpen}>
        <DialogContent className="w-[min(1000px,95vw)] max-w-none sm:!max-w-none max-h-[90vh] overflow-auto">
          <DialogHeader>
            <DialogTitle className="text-xl">Understanding FLARE</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 text-sm">
            <div>
              <h3 className="font-semibold mb-2">What This Widget Shows</h3>
              <p className="text-gray-600 dark:text-gray-300">
                FLARE classifies the object from its ZTF g and r light curve and the broker's catalog cross-matches
                (host galaxy, photometric redshift, Gaia, WISE and Pan-STARRS) into six classes: SN Ia, SN CC, SLSN,
                AGN, TDE and CV. It also reports how well the object fits any of these classes.
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Reading the Summary</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">Prediction set:</span>
                  <span>
                    The headline result: the classes that cannot be ruled out at the stated coverage level. One class is a
                    confident call; several classes mean the photometry alone does not separate them; no class means the
                    object fits none of them.
                  </span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Anomaly:</span>
                  <span>
                    Whether the object is flagged as out of taxonomy, at the strictest false-alarm rate it passes (1%, 5% or
                    10%). "No" means at least 10% of known-class objects are this unusual; the percentile then shows how
                    far the object sits in the known-class anomaly-energy distribution.
                  </span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Credibility:</span>
                  <span>How typical the object is of its best class (the largest p-value, between 0 and 1).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Confidence:</span>
                  <span>How clearly the best class is separated from the runner-up (one minus the second-largest p-value).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">P(anomaly):</span>
                  <span>The probability that the object belongs to none of the six classes, given the stated base rate of such objects.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Novelty p-value:</span>
                  <span>The fraction of known-class objects that are at least as unusual as this one. Values below 0.01 are flagged for review.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Catalog columns:</span>
                  <span>How many of the 30 cross-match features were available. Missing catalogs lower the reliability of the result.</span>
                </div>
              </div>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Reading the Table</h3>
              <p className="text-gray-600 dark:text-gray-300">
                Each row gives the class probability and its conformal p-value. A class is in the {setLabel} when
                its p-value exceeds {alphaLabel}, so the set has a guaranteed coverage per class rather than a single best guess.
                A set with several classes means the photometry alone does not separate them; an empty set means no class fits.
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">When to Review</h3>
              <p className="text-gray-600 dark:text-gray-300">
                The "Review" badge appears when the most probable class is excluded from its own prediction set, or when
                the object is an anomaly at 1% FAR. Both indicate an object that does not resemble the training classes
                and may deserve a spectrum.
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Reference</h3>
              <p className="text-gray-600 dark:text-gray-300">
                Sasli et al., "From anomaly to classification: a physics-informed pipeline for discovering astronomical transients".
              </p>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </Card>
  );
}
