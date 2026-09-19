# Villar light-curve fits

Every ZTF alert that BOOM enriches on a GPU gets an analytic transient
light-curve fit attached to it, under the `villar_fit` key. This page explains
what the fitted parameters mean, what units they are in, and how to use them —
whether you are writing a BOOM filter, querying the alert database, or
reconstructing the model curve yourself.

The model is the piecewise analytic supernova light-curve function introduced by
[Villar et al. (2019)](https://ui.adsabs.harvard.edu/abs/2019ApJ...884...83V/abstract).
BOOM fits it jointly in ZTF *g* and *r* with a GPU particle-swarm optimizer
([`villar-pso`](https://github.com/frenbox/villar-pso)), minimizing a
band-balanced chi-squared with Gaussian priors.

## Quick reference

| Field | Meaning | Unit |
| --- | --- | --- |
| `villar_fit.A_ZTF_r` / `A_ZTF_g` | Amplitude | µJy |
| `villar_fit.beta_ZTF_r` / `beta_ZTF_g` | Plateau slope, as a fraction of `A` | day⁻¹ |
| `villar_fit.gamma_ZTF_r` / `gamma_ZTF_g` | Plateau duration | days |
| `villar_fit.t_0_ZTF_r` / `t_0_ZTF_g` | Reference (rise) time | days from peak |
| `villar_fit.tau_rise_ZTF_r` / `tau_rise_ZTF_g` | Rise e-folding time | days |
| `villar_fit.tau_fall_ZTF_r` / `tau_fall_ZTF_g` | Decline e-folding time | days |
| `villar_fit.extra_sigma_ZTF_r` / `extra_sigma_ZTF_g` | Fitted excess scatter | µJy |
| `villar_fit.reduced_chi2` | Goodness of fit | dimensionless |
| `villar_fit.peak_flux` | Normalization scale used by the fit | µJy |

Sixteen fields in total: seven parameters × two bands, plus a fit statistic and
the normalization scale. Every field is a double, and **every field is `NaN`
when the fit did not run or did not succeed** — see
[When the fields are NaN](#when-the-fields-are-nan).

Fits are stored **per alert**, not per object. A well-observed object has one
`villar_fit` per `candid`, each computed from the light curve as it stood at
that alert's epoch. The most recent `candid` has the most complete light curve
and therefore the most trustworthy fit.

## The model

Let `t` be time in days and let `t_peak` be the epoch of the brightest *r*-band
detection in the light curve. The fit works in **phase**:

```
phase = t - t_peak
p     = max(phase - t_0, -50 * tau_rise)
```

The model flux is then, per band:

```
                       A
sigmoid_term  =  ─────────────────
                 1 + exp(-p / tau_rise)

F(p)  =  sigmoid_term * (1 - beta * p)                                   if p <= gamma
      =  sigmoid_term * (1 - beta * gamma) * exp(-(p - gamma) / tau_fall) if p >  gamma
```

In words: a sigmoid rise on a timescale `tau_rise`, then a linearly declining
plateau of duration `gamma`, then an exponential tail with e-folding time
`tau_fall`. `t_0` sets where the rise sits relative to the *r*-band peak; at
`p = 0` the flux is exactly `A/2`.

Two deliberate differences from the paper's Equation 1:

- **No baseline term.** Villar et al. include a constant `c`. ZTF alert
  photometry is difference-image photometry, already baseline-subtracted, so
  BOOM fits without it.
- **Reparametrized slope.** The paper writes the plateau as `A + β(t - t_0)`
  with `β` in flux/day. BOOM factors the amplitude out, `A(1 - β·p)`, so its
  `beta` is a *fractional* decline rate. The flux slope in the paper's sense is
  `-A * beta`.

The *g*-band is fitted as an offset from *r* in the optimizer's internal space,
but **the stored values are absolute per band**. `gamma_ZTF_g` is the *g*-band
plateau duration, not a difference.

### Flux convention

Magnitudes are converted to flux with a zeropoint of 23.9, so all flux-valued
parameters are in **microjanskys**:

```
F_uJy = 10 ** ((23.9 - mag) / 2.5)
mag   = 23.9 - 2.5 * log10(F_uJy)
```

### Physical validity constraints

Parameter sets that produce unphysical curves are rejected outright by the
optimizer, so a successful fit always satisfies all three of:

```
gamma * beta                                   <= 1
exp(-gamma / tau_rise) * (tau_fall/tau_rise - 1) <= 1
beta * tau_fall + beta * gamma                 <= 1
```

## Parameters in detail

### `A` — amplitude (µJy)

Scale of the light curve. With the sigmoid saturated and `beta` small, the
plateau flux tends to `A`. Convert to a magnitude with the formula above.

The *g*/*r* amplitude ratio `A_ZTF_g / A_ZTF_r` is a color proxy:
`-2.5 * log10(A_ZTF_g / A_ZTF_r)` is roughly the *g − r* color at plateau.

### `beta` — plateau slope

How fast the plateau declines, as a fraction of `A` per day, in day⁻¹. Positive
`beta` means fading, negative means still brightening through the plateau.

Because it is a *fractional* rate, `beta` is already comparable across objects
of any brightness. If you want the slope in flux units — the `β` of the paper's
Equation 1 — it is `-A * beta`, in µJy/day.

### `gamma` — plateau duration (days)

Length of the linear-decline phase, measured from `t_0`. Short `gamma` with a
short `tau_fall` looks like a fast transient; long `gamma` is the signature of a
Type IIP-like plateau. Bounded to roughly 6–100 days in *r*.

### `t_0` — reference time (days from peak)

Where the sigmoid rise is centered, in days relative to the brightest *r*-band
epoch. **It is not an absolute JD or MJD** — it is already a phase. Negative
values (the usual case; the prior is centered at −12 days) put the rise before
peak. To get an absolute epoch, add the JD of the brightest *r*-band point in
the light curve.

`t_0` is the closest thing the model has to an explosion-time proxy, but it is
the half-maximum point of the rise, not first light.

### `tau_rise` — rise timescale (days)

E-folding time of the sigmoid rise. Small values (< 1 day) mean an abrupt
turn-on; a fast-riser search is essentially a cut on this parameter.

### `tau_fall` — decline timescale (days)

E-folding time of the exponential tail after the plateau. This is the parameter
that separates fast-evolving transients from slowly fading, radioactively
powered supernovae.

### `extra_sigma` — excess scatter (µJy)

A white-noise term fitted alongside the shape parameters and added in quadrature
to the reported photometric errors:

```
sigma_total^2 = flux_err^2 + extra_sigma^2
```

It absorbs both real intrinsic variability and underestimated uncertainties.
A large `extra_sigma` relative to `A` means the model is not describing the data
well even if `reduced_chi2` looks acceptable — the fit bought its chi-squared by
inflating the errors. **Check `extra_sigma / A` alongside `reduced_chi2`.**

### `reduced_chi2`

Chi-squared per degree of freedom of the best fit, computed on peak-normalized
flux with `extra_sigma` included in the denominator, with
`dof = max(n_points - 14, 1)`.

Because `extra_sigma` is free, this statistic is pulled towards ~1 by
construction. Treat it as a relative ranking tool — "which of these fits is
worse" — rather than an absolute, calibrated goodness-of-fit probability. Values
well above a few indicate the model genuinely failed.

### `peak_flux`

The largest flux, in either band, among the points the fit actually saw — the
scale everything was normalized by before fitting. It is stored because it is
not otherwise recoverable from the alert document alone: reproducing it means
replaying the preprocessing over that object's `ZTF_alerts_aux` photometry,
filtered to `jd <= ` the alert's own `jd`.

Divide `A` or `extra_sigma` by it to recover the normalized (dimensionless)
values the optimizer worked in, which is what you want when comparing fit
*shapes* across objects of very different brightness.

## Parameter bounds

The optimizer searches inside hard bounds, so stored values are always within
these ranges. A parameter sitting exactly on a bound is a red flag: the fit
wanted to go further and could not.

| Parameter | `ZTF_r` range | `ZTF_g` envelope |
| --- | --- | --- |
| `A` (× peak flux) | 0.631 – 1.413 | 0.316 – 2.239 |
| `beta` (× peak flux) | −0.01 – 0.03 | −0.03 – 0.05 |
| `gamma` (days) | 6.31 – 100 | 1.0 – 316 |
| `t_0` (days) | −50 – 30 | −55 – 33 |
| `tau_rise` (days) | 0.501 – 10.0 | 0.050 – 31.6 |
| `tau_fall` (days) | 6.31 – 158 | 1.0 – 316 |
| `extra_sigma` (× peak flux) | 0.00316 – 0.501 | 0.00079 – 1.995 |

The *r*-band ranges are exact. The *g*-band column is an **envelope**: *g* is
searched as a bounded offset from *r*, so reaching a *g* extreme also requires
*r* to be near its own extreme. The bounds were calibrated against reference
fits and carry roughly 2σ of margin, which is why they are tighter than a naive
"any physical supernova" range would be.

## Preprocessing

The fit does not see the raw alert history. Before fitting, `villar-pso`:

1. Keeps only *g* and *r* points; drops anything with non-finite or
   non-positive flux.
2. Converts magnitudes to flux with zeropoint 23.9.
3. Sets phase zero at the **brightest *r*-band point**.
4. Merges points within 0.04 days of each other, per band, by inverse-variance
   weighted average.
5. Truncates to phase ∈ [−50, +100] days.
6. Requires **more than two surviving points in each of *g* and *r***, or the
   fit is skipped.
7. Normalizes all fluxes by the global peak flux; parameters are converted back
   to physical units afterwards.

Step 5 is why `t_0` has a −50 day floor, and step 3 is why `t_0` is usually
negative.

## When the fields are NaN

All sixteen fields are written as `NaN` — never omitted — whenever a fit is not
produced, so consumers always see one schema. This happens when:

- The light curve has ≤ 2 points in *g* or in *r* after preprocessing. This is
  the common case by far: most alerts are early, single-band, or sparse.
- There is no *r*-band data at all.
- Peak flux is non-positive.
- GPU batch fitting fails for the whole batch.

`villar_fit` is written **only when BOOM runs with GPU enrichment enabled**
(built with the `gpu` feature and a CUDA or Metal device configured — see
[gpu.md](gpu.md)). On a CPU-only deployment the key is absent entirely, which is
different from being `NaN`.

In MongoDB, `NaN` compares false against every range predicate, so a query like
`{"villar_fit.reduced_chi2": {"$lt": 3}}` already excludes unfitted alerts. To
check explicitly, use `{"villar_fit.A_ZTF_r": {"$gte": 0}}`, which is false for
`NaN`; `$exists` will **not** work, because the field is present.

## Using the fits

### Querying

Well-fitted transients:

```js
db.ZTF_alerts.find({
  "villar_fit.reduced_chi2": { $lt: 3 }
})
```

Fast risers with a slow decline — the classic superluminous-supernova corner:

```js
db.ZTF_alerts.find({
  "villar_fit.reduced_chi2":     { $lt: 3 },
  "villar_fit.tau_rise_ZTF_r":   { $lt: 3 },
  "villar_fit.tau_fall_ZTF_r":   { $gt: 40 }
})
```

Long-plateau, IIP-like candidates:

```js
db.ZTF_alerts.find({
  "villar_fit.reduced_chi2":  { $lt: 3 },
  "villar_fit.gamma_ZTF_r":   { $gt: 50 }
})
```

Blue transients, via the amplitude ratio (needs `$expr`, since it compares two
fields):

```js
db.ZTF_alerts.find({
  $expr: { $gt: ["$villar_fit.A_ZTF_g", { $multiply: ["$villar_fit.A_ZTF_r", 1.3] }] }
})
```

Because fits are per-alert, add your own `objectId` grouping (or a "latest
candid" stage) if you want one fit per object rather than one per alert.

### Reconstructing the model curve

```python
import numpy as np

ZP = 23.9

def villar_flux(phase, A, beta, gamma, t_0, tau_rise, tau_fall):
    """Model flux in uJy, for one band."""
    p = np.maximum(np.asarray(phase, float) - t_0, -50.0 * tau_rise)
    sigmoid = A / (1.0 + np.exp(-p / tau_rise))
    plateau = sigmoid * (1.0 - beta * p)
    tail    = sigmoid * (1.0 - beta * gamma) * np.exp(-(p - gamma) / tau_fall)
    return np.where(p <= gamma, plateau, tail)


def model_lightcurve(villar_fit, band="ZTF_r", phases=None):
    """Evaluate one band of a stored villar_fit over a phase grid."""
    if phases is None:
        phases = np.linspace(-50.0, 100.0, 500)

    return phases, villar_flux(
        phases,
        A=villar_fit[f"A_{band}"],
        beta=villar_fit[f"beta_{band}"],
        gamma=villar_fit[f"gamma_{band}"],
        t_0=villar_fit[f"t_0_{band}"],
        tau_rise=villar_fit[f"tau_rise_{band}"],
        tau_fall=villar_fit[f"tau_fall_{band}"],
    )
```

To overlay this on observed photometry, plot the data against
`jd - jd_of_brightest_r_band_point` so both are on the same phase axis, and
convert the model flux to magnitudes with `23.9 - 2.5 * log10(F)`.

### Derived quantities

A few combinations are more useful than the raw parameters:

| Quantity | Expression | Why |
| --- | --- | --- |
| Plateau slope, flux units | `-A * beta` | The paper's `β`, in µJy/day |
| Peak magnitude | `23.9 - 2.5*log10(A)` | Comparable across objects |
| Color at plateau | `-2.5*log10(A_ZTF_g / A_ZTF_r)` | Approximate *g − r* |
| Rise/fall asymmetry | `tau_fall / tau_rise` | Separates fast transients |
| Fractional scatter | `extra_sigma / A` | Sanity check on `reduced_chi2` |
| Total duration proxy | `gamma + tau_fall` | Rough event timescale |

## Caveats

Worth knowing before you build science on these numbers.

**`reduced_chi2` uses a truncated point set.** Internally the fitter pads each
band to a common length and then evaluates the cost over the first `n_points`
entries of a band-sorted array. When the *r*-band needs padding, that window
includes some zero-weight padded entries and correspondingly excludes the
latest *g*-band points. The padded entries carry an error of 1000 in normalized
flux and contribute essentially nothing, but a handful of late *g* points can
be left out of both the fit and the reported chi-squared.

**`dof` is clamped at 1.** With 14 free parameters, any light curve with fewer
than 15 points gets `dof = 1`, so `reduced_chi2` is then just the total
chi-squared. Compare fits with similar numbers of points.

**`extra_sigma` can mask a bad fit.** Because it is free, the optimizer can
drive `reduced_chi2` towards 1 by inflating the errors. Always read
`extra_sigma / A` alongside it.

**Priors are ZTF-calibrated and fairly tight.** The bounds above come from
reference fits on ZTF supernovae. Genuinely exotic transients can be pushed
against a bound rather than fitted; check for railed parameters.

**One fit per alert, not per object.** Early alerts are fitted on partial light
curves and will disagree with later ones. Use the latest `candid` unless you
specifically want the historical view.

**Only *g* and *r*.** ZTF *i*-band points are discarded before fitting.

## Source

| What | Where |
| --- | --- |
| Model, priors, PSO, preprocessing | [`villar-pso`](https://github.com/frenbox/villar-pso) |
| Batch fitting and Mongo writes | [`src/enrichment/ztf.rs`](../src/enrichment/ztf.rs) |
| GPU context setup | [`src/enrichment/models/mod.rs`](../src/enrichment/models/mod.rs) |

## References

- Villar, V. A., Berger, E., Miller, G., et al. 2019, *Supernova Photometric
  Classification Pipelines Trained on Spectroscopically Classified Supernovae
  from the Pan-STARRS1 Medium-deep Survey*, ApJ, 884, 83.
  [doi:10.3847/1538-4357/ab418c](https://doi.org/10.3847/1538-4357/ab418c) ·
  [arXiv:1905.07422](https://arxiv.org/abs/1905.07422)
