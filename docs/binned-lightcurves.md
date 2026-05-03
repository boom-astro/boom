# Binned light curves

Per-source aggregation of photometric points into per-band median+error bins.
Supersedes the raw `prv_candidates`/`fp_hists` arrays as the canonical input
to classifiers, light-curve fitting, and aggregation queries; raw arrays are
trimmed to a configurable hot-recent retention window after each bin write.

This document describes the schema and the math. The cadence state machine
that decides *which* window each source's bins are computed at is documented
separately (forthcoming, with the `cadence` module).

## Why

At Argus rates (≈ 100M alerts/night), retaining unbinned per-frame photometry
for every source becomes both a storage and a query problem. The proposal
identifies four observations:

1. Most downstream consumers (BTSbot, ACAI, AppleCiDEr, SCoPe, NMMA, GP fits,
   parametric fits, user filters) operate on summary statistics over
   time-windowed photometry, not on raw individual frames.
2. Source classes have very different cadence requirements: a kilonova at
   first light wants every frame, a Type Ia past peak wants nightly, an RR
   Lyrae wants per-period sampling, a YSO wants monthly summary.
3. ZTF/Argus carry both detection-pipeline-triggered points
   (`prv_candidates`) and forced-photometry at known positions (`fp_hists`).
   None of the downstream consumers we have inspected distinguish them at the
   input layer; both are weighted by error and treated as photometry.
4. Argus is the canonical long-term store for periodic-variable light curves.
   BOOM does not need to retain decimated raw photometry at long baselines.

The binner computes a class-cadence-aware median+error summary that meets
points 1 and 2, unifies points 3 (with a per-bin provenance tag), and elides
storage at point 4 (periodic variables get only the hot-recent retention
window in BOOM, no time-bins).

## Schema

A new field on each `<survey>_alerts_aux` document:

```jsonc
binned_lightcurve: {
  "g": [BinnedPoint, ...],
  "r": [BinnedPoint, ...],
  "i": [BinnedPoint, ...],
  // ... one array per band the source has been observed in
}
```

Each `BinnedPoint` (defined in `src/utils/binning.rs::BinnedPoint`):

| field         | type           | meaning                                                                                                  |
| ------------- | -------------- | -------------------------------------------------------------------------------------------------------- |
| `jd`          | `f64`          | Median JD of contributing flux points (weighted-mean for n=2); median JD of upper limits if upper-only.  |
| `n`           | `i32`          | Number of finite-flux points (excludes upper limits).                                                    |
| `flux_med`    | `Option<f64>`  | Inverse-variance-weighted central flux. `null` for upper-limit-only bins.                                |
| `flux_err`    | `f64`          | Robust SEM (see below). For upper-limit-only bins, the deepest input limit.                              |
| `flux_mad`    | `f64`          | MAD of contributing fluxes; `0.0` when `n < 2`.                                                          |
| `has_nondet`  | `bool`         | Whether the bin window contained any upper-limit points.                                                 |
| `src_kinds`   | `[SrcKind]`    | Distinct provenance kinds that contributed: subset of `["psf", "fp", "upper"]`, in canonical order.      |
| `window_start_jd` | `f64`      | Start JD of the bin's source window. Together with `band`, the bin's stable identity key (see below).    |
| `window_days` | `f64`          | Width of the bin window in days. Lets consumers reason about the cadence at which each bin was computed. |
| `band`        | `Band`         | Band of this bin.                                                                                        |

`BinnedPoint` arrays are append-only across tier changes: when a source's
cadence tier changes, the binner begins writing finer bins forward and older
coarser bins remain in place. The `window_days` field lets consumers
reconstruct the cadence at any historical jd.

Within an active window, the binner is *idempotent* on `(band,
window_start_jd)`: re-running over the same window (e.g., `--date` backfill
after late-arriving forced photometry) replaces the bin in place rather than
appending a near-duplicate with a drifted median `jd`. This keeps the array
clean under reprocessing while preserving the append-only-across-tier-changes
invariant.

Magnitudes are not stored on the bin. Downstream consumers convert from
`flux_med` using the survey's appropriate flux-to-magnitude zero point
(documented per survey in `src/utils/lightcurves.rs`). Storing magnitudes on
the bin would force the binner to know which survey it's binning for and
would duplicate information already derivable from `flux_med`.

## Math

Implemented in `src/utils/binning.rs`. All arithmetic is `f64` for numerical
stability even when the upstream input is `f32`.

### Input filtering

Per `(objectId, band, window)`, the binner partitions input points:

- Out-of-window: silently skipped.
- `flux_err` non-finite or non-positive: dropped.
- For non-upper points, `flux` non-finite: dropped.
- Upper points: `flux` is `None` by definition; `flux_err` carries the
  equivalent flux limit.

If the window contains no surviving points (no flux measurements *and* no
upper limits), no bin is written.

### Central flux

| n      | central                                                            |
| ------ | ------------------------------------------------------------------ |
| 1      | the single point's flux                                            |
| 2      | inverse-variance weighted mean (a 2-point median is degenerate)    |
| ≥ 3    | inverse-variance weighted median (robust to outliers in the tail)  |

The crossover at `n ≥ 3` reflects that a 3-point median already provides
useful outlier rejection while a 2-point median is just one of the two
inputs. For `n = 2`, the weighted mean is the maximum-likelihood estimate
under Gaussian noise and avoids the arbitrary tie-break a median would
require.

### Error

```
flux_err = max( weighted_mean_err, 1.4826 · MAD / √n )
```

where `weighted_mean_err = √(1 / Σ wᵢ)` and `wᵢ = 1 / σᵢ²`. The lower bound
from the inputs prevents catastrophically small errors when the spread
happens to be small by chance (e.g., two points with the same flux give
`MAD = 0`; the lower bound takes over).

For `n = 1`, `flux_err` is the input point's `flux_err`. For upper-only
bins, `flux_err` is the deepest input limit (smallest input `flux_err`
among upper points), and `flux_mad = 0`.

### Provenance tag

Each bin carries `src_kinds: Vec<SrcKind>` — the distinct kinds among its
contributing points, in canonical order `[Psf, Fp, Upper]`. Consumers that
want detection-only bins filter on `src_kinds == [Psf]`; consumers that
want every bin (the common case) ignore the field.

## Consumer expectations

Confirmed against:

- **villar-pso** (`PhotometryMag { time, mag, mag_err, band }`,
  inverse-variance weighting via `merge_close_times`): unified
  representation, no provenance distinction. Conversion from `BinnedPoint`
  to `PhotometryMag` lives in `villar-pso`, not BOOM.
- **AppleCider TEMPO** (training matrix
  `[dt, dt_prev, band_id, logflux, logflux_err, ...]`): unified, no
  provenance distinction. AppleCider's preprocessing does its own 12-hour
  internal merge — bins coarser than 12 h during a transient's active phase
  lose information. The transient-ladder `H` tier `h_cadence_hours` for
  explosive/SN-tagged sources is correspondingly set to ≤ 12 h so the
  binned representation matches AppleCider's expected granularity.
- **SCoPe**: decimates with `removeHighCadence(cadence_minutes=30.0)` then
  runs LS/CE/AOV/FPW period-finding. SCoPe does not consume
  `binned_lightcurve` for periodic variables — those source classes have
  no time-bins in this PR; SCoPe consumes raw photometry from the
  hot-recent retention window or, in production, periodically pulls from
  Argus to re-fit periods.
- **BTSbot, ACAI**: per-alert classifiers consuming `(metadata vector,
  image cutout)`. They do not read the binned light curve at all.
