import { useState } from 'react';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { Info } from 'lucide-react';

/**
 * FLARE — the light-curve classifier's card.
 *
 * Answers one question per alert: what is it, and does it deserve a spectrum?
 * Three statements lead: the conformal prediction set with its coverage
 * guarantee, how much to trust it (credibility with its floor), and whether
 * the object sits outside the taxonomy (novelty p-value, likelihood ratio,
 * P(anomaly) at an explicit base rate). Class probabilities and p-values sit
 * below, folded, for whoever wants to check.
 *
 * Data: the alert's `flare` field, written by the enrichment worker when the
 * `flare` feature is enabled. Absent field → the card is not rendered.
 */

export type FlareResult = {
  p_sn_ia?: number; p_sn_cc?: number; p_slsn?: number; p_agn?: number; p_tde?: number; p_cv?: number;
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

const CLASSES: Array<[string, string]> = [
  ['SN_Ia', 'SN Ia'], ['SN_CC', 'SN CC'], ['SLSN', 'SLSN'], ['AGN', 'AGN'], ['TDE', 'TDE'], ['CV', 'CV'],
];
const nice = (k: string) => CLASSES.find(([key]) => key === k)?.[1] ?? k.replace('_', ' ');
const pct = (x: number | undefined | null, d = 0) => (x === undefined || x === null ? '–' : `${(x * 100).toFixed(d)}%`);
const num = (x: number | undefined | null, d = 3) => (x === undefined || x === null ? '–' : x.toFixed(d));

export default function FlareCard({ alert }: { alert: unknown }) {
  const [helpOpen, setHelpOpen] = useState(false);
  const [detailsOpen, setDetailsOpen] = useState(false);
  const fl = (alert as { flare?: FlareResult } | null)?.flare;
  if (!fl) return null;

  const set = fl.set ?? [];
  const alpha = fl.alpha ?? 0.1;
  const coverage = Math.round((1 - alpha) * 100);
  const probs: Record<string, number | undefined> = {
    SN_Ia: fl.p_sn_ia, SN_CC: fl.p_sn_cc, SLSN: fl.p_slsn, AGN: fl.p_agn, TDE: fl.p_tde, CV: fl.p_cv,
  };
  const novel = fl.novelty_p !== undefined && fl.novelty_p !== null && fl.novelty_p <= 0.01;
  const review = novel || !!fl.argmax_excluded;
  const oneIn = fl.novelty_p ? Math.round(1 / fl.novelty_p) : null;
  const energyPct = fl.energy_percentile ?? null;

  const verdict = fl.argmax_excluded
    ? `The most likely class, ${nice(fl.label ?? '')}, is not in its own ${coverage}% set: the light curve does not look like a typical member of any trained class.`
    : novel
      ? `Consistent with ${set.map(nice).join(' or ')}, but more extreme than 99% of known-class objects in the anomaly space.`
      : set.length === 1
        ? `A ${nice(set[0])} at the ${coverage}% level.`
        : set.length === 0
          ? 'No class is consistent with this light curve at the chosen level.'
          : `Photometry alone cannot separate ${set.map(nice).join(' from ')}: a candidate set, not a wrong label.`;

  return (
    <Card className="@container/card col-span-1 @xl/main:col-span-2">
      <CardContent className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <CardTitle className="text-lg">FLARE</CardTitle>
            <span className="text-sm text-gray-500 dark:text-gray-400">photometric classification</span>
            <button onClick={() => setHelpOpen(true)} title="What this card shows"
              className="p-1 rounded hover:bg-slate-100 dark:hover:bg-slate-700">
              <Info className="w-4 h-4 text-gray-500 dark:text-gray-400" />
            </button>
          </div>
          <div className="flex items-center gap-2">
            {fl.model && <Badge variant="outline" className="font-mono text-[11px]">{fl.model}</Badge>}
            {review
              ? <Badge className="bg-amber-500/15 text-amber-700 dark:text-amber-300 border border-amber-500/40">review queue</Badge>
              : <Badge variant="secondary">routine</Badge>}
          </div>
        </div>

        {/* 1 · what it is */}
        <div>
          <div className="text-[11px] uppercase tracking-wide text-gray-500 dark:text-gray-400">prediction set</div>
          <div className="flex flex-wrap items-baseline gap-2 mt-1">
            <div className="font-mono text-2xl font-semibold">
              {set.length ? `{${set.map(nice).join(', ')}}` : '∅'}
            </div>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="text-sm text-gray-600 dark:text-gray-300 tabular-nums">
                  {coverage}% coverage ± {Math.round(alpha * 100)}%
                </span>
              </TooltipTrigger>
              <TooltipContent className="max-w-xs">
                Class-conditional conformal set at α = {alpha}: for each class, the true class is inside the set
                for at least {coverage}% of its members{fl.coverage_measured !== undefined && fl.coverage_measured !== null
                  ? `; measured out of fold for ${nice(fl.label ?? '')}: ${pct(fl.coverage_measured, 1)}${fl.coverage_n ? ` (n = ${fl.coverage_n})` : ''}`
                  : ''}.
              </TooltipContent>
            </Tooltip>
          </div>
        </div>

        {/* 2 · how much to trust it  ·  3 · is it new */}
        <div className="grid grid-cols-1 @md/card:grid-cols-2 gap-3">
          <div className="rounded-md border border-border/60 p-3">
            <div className="text-[11px] uppercase tracking-wide text-gray-500 dark:text-gray-400">how typical of its best class</div>
            <div className="flex items-baseline gap-2 mt-1">
              <span className="font-mono text-xl tabular-nums">{num(fl.credibility)}</span>
              <span className="text-xs text-gray-500 dark:text-gray-400">credibility, max p-value</span>
            </div>
            <div className="text-xs text-gray-600 dark:text-gray-300 mt-1 tabular-nums">
              confidence {num(fl.confidence)} · argmax {nice(fl.label ?? '')} at p = {num(fl.p_values?.[fl.label ?? ''])}
              {fl.p_value_floor !== undefined && fl.p_value_floor !== null && (
                <> · floor {num(fl.p_value_floor)}</>
              )}
            </div>
          </div>

          <div className={`rounded-md border p-3 ${novel ? 'border-amber-500/50 bg-amber-500/5' : 'border-border/60'}`}>
            <div className="text-[11px] uppercase tracking-wide text-gray-500 dark:text-gray-400">is it outside the taxonomy?</div>
            <div className="flex items-baseline gap-2 mt-1">
              <span className="font-mono text-xl tabular-nums">{oneIn ? `1 in ${oneIn.toLocaleString()}` : '–'}</span>
              <span className="text-xs text-gray-500 dark:text-gray-400">known objects are this extreme</span>
            </div>
            <div className="text-xs text-gray-600 dark:text-gray-300 mt-1 tabular-nums">
              likelihood ratio {fl.likelihood_ratio !== undefined ? `${fl.likelihood_ratio.toFixed(1)}×` : '–'} (prior-free)
              {' · '}P(anomaly) {pct(fl.p_anomaly, 1)} at a {pct(fl.base_rate, 1)} base rate
            </div>
          </div>
        </div>

        {/* energy gauge: percentile against the known-class stream, mark at the 1% false-alarm budget */}
        <div>
          <div className="flex justify-between text-[11px] text-gray-500 dark:text-gray-400">
            <span>anomaly energy, percentile against the known-class stream</span>
            <span className="tabular-nums">{energyPct !== null ? energyPct.toFixed(1) : '–'}</span>
          </div>
          <div className="relative h-2 mt-1 rounded bg-slate-200 dark:bg-slate-700 overflow-hidden">
            <div className={`absolute inset-y-0 left-0 ${novel ? 'bg-amber-500' : 'bg-teal-600 dark:bg-teal-400'}`}
              style={{ width: `${Math.max(0, Math.min(100, energyPct ?? 0))}%` }} />
            <div className="absolute inset-y-0 w-0.5 bg-red-500" style={{ left: '99%' }} title="1% false-alarm budget" />
          </div>
        </div>

        <p className={`text-sm ${review ? 'text-amber-700 dark:text-amber-300' : 'text-gray-700 dark:text-gray-200'}`}>{verdict}</p>

        {/* details, folded */}
        <button onClick={() => setDetailsOpen(v => !v)}
          className="self-start text-xs text-gray-500 dark:text-gray-400 hover:underline">
          {detailsOpen ? 'hide' : 'show'} class probabilities and p-values
        </button>
        {detailsOpen && (
          <div className="grid grid-cols-[auto_1fr_auto_auto] gap-x-3 gap-y-1 text-xs tabular-nums items-center">
            <span className="text-gray-500 dark:text-gray-400">class</span>
            <span className="text-gray-500 dark:text-gray-400">p-value (set = p &gt; α)</span>
            <span className="text-gray-500 dark:text-gray-400 text-right">p</span>
            <span className="text-gray-500 dark:text-gray-400 text-right">P(class)</span>
            {CLASSES.map(([key, name]) => {
              const p = fl.p_values?.[key] ?? 0;
              const inSet = set.includes(key);
              return (
                <div key={key} className="contents">
                  <span className={inSet ? 'font-semibold' : ''}>{name}</span>
                  <div className="h-1.5 rounded bg-slate-200 dark:bg-slate-700 overflow-hidden">
                    <div className={`h-full ${inSet ? 'bg-teal-600 dark:bg-teal-400' : 'bg-slate-400 dark:bg-slate-500'}`}
                      style={{ width: `${Math.min(100, p * 100)}%` }} />
                  </div>
                  <span className="text-right">{num(p)}</span>
                  <span className="text-right text-gray-600 dark:text-gray-300">{num(probs[key])}</span>
                </div>
              );
            })}
            {fl.n_context_present !== undefined && (
              <span className="col-span-4 text-gray-500 dark:text-gray-400 pt-1">
                {fl.n_context_present} of 30 catalogue columns available for this alert
              </span>
            )}
          </div>
        )}
      </CardContent>

      <Dialog open={helpOpen} onOpenChange={setHelpOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader><DialogTitle>FLARE</DialogTitle></DialogHeader>
          <div className="space-y-3 text-sm">
            <p>FLARE classifies the alert from its ZTF g/r photometry and the broker's catalogue crossmatches
              (host, photo-z, Gaia, WISE, PS1), with gradient-boosted trees on 190 physics features.</p>
            <p><b>Prediction set.</b> A class-conditional conformal set: at α = 0.10 every class is inside its
              set for at least 90% of its members, out of fold. A set with two classes is an honest hand-off to
              spectroscopy, not a wrong label; an argmax excluded from its own set is the strongest flag for review.</p>
            <p><b>Credibility</b> is the largest conformal p-value: how typical the object is of its best class.
              <b> Confidence</b> is one minus the second-largest p-value. The floor 1/(n+1) is set by the number of
              calibration members of that class.</p>
            <p><b>Outside the taxonomy.</b> The anomaly energy is −logsumexp of the classifier's logits in a
              dedicated feature space. The novelty p-value is the fraction of known-class objects at least this
              extreme and needs no prior; P(anomaly) turns the likelihood ratio into a probability at the stated
              base rate, which is the benchmark's own and far richer than a raw alert stream.</p>
            <p className="text-gray-500 dark:text-gray-400">Sasli et al., "From anomaly to classification: a
              physics-informed pipeline for discovering astronomical transients".</p>
          </div>
        </DialogContent>
      </Dialog>
    </Card>
  );
}
