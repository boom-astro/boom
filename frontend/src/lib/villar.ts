import type { VillarFit } from "@/lib/api";

const ZP = 23.9;
const PHASE_MIN = -50;
const PHASE_MAX = 100;
const N_SAMPLES = 500;
const FIT_BANDS = ["g", "r"];
const PARAM_NAMES = ["A", "beta", "gamma", "t_0", "tau_rise", "tau_fall"] as const;

export type VillarCurve = { band: string; points: { jd: number; mag: number }[] };

type BandParams = { A: number; beta: number; gamma: number; t_0: number; tau_rise: number; tau_fall: number };

function villarFlux(phase: number, { A, beta, gamma, t_0, tau_rise, tau_fall }: BandParams): number {
  const p = Math.max(phase - t_0, -50 * tau_rise);
  const sigmoid = A / (1 + Math.exp(-p / tau_rise));
  if (p <= gamma) return sigmoid * (1 - beta * p);
  return sigmoid * (1 - beta * gamma) * Math.exp(-(p - gamma) / tau_fall);
}

function bandParams(fit: VillarFit, band: string): BandParams | null {
  const entries = PARAM_NAMES.map(name => [name, fit.params[`${name}_ZTF_${band}`]] as const);
  if (!entries.every(([, value]) => Number.isFinite(value))) return null;
  const params = Object.fromEntries(entries) as BandParams;
  return { ...params, A: 10 ** ((ZP - params.A) / 2.5) };
}

export function villarCurves(fit: VillarFit): VillarCurve[] {
  const tPeak = fit.t_peak;
  if (tPeak === null || !Number.isFinite(tPeak)) return [];
  return FIT_BANDS.flatMap(band => {
    const params = bandParams(fit, band);
    if (!params) return [];
    const points = Array.from({ length: N_SAMPLES }, (_, i) => {
      const phase = PHASE_MIN + (i / (N_SAMPLES - 1)) * (PHASE_MAX - PHASE_MIN);
      const flux = villarFlux(phase, params);
      return { jd: tPeak + phase, mag: flux > 0 ? ZP - 2.5 * Math.log10(flux) : NaN };
    });
    return [{ band, points }];
  });
}
