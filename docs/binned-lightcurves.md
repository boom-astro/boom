# Binned light curves

Per-source aggregation of photometric points into per-band median+error bins.
Supersedes the raw `prv_candidates`/`fp_hists` arrays as the canonical input
to classifiers, light-curve fitting, and aggregation queries; raw arrays are
trimmed to a configurable hot-recent retention window after each bin write.

This document describes the schema and the math. The cadence state machine
that decides *which* window each source's bins are computed at is documented
separately (forthcoming, with the `cadence` module).

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
