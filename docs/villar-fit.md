# Villar light-curve fits

Every ZTF alert that BOOM enriches on a GPU gets an analytic transient
light-curve fit attached to it, under the `villar_fit` key. This page explains
what the fitted parameters mean, what units they are in, and how to use them —
whether you are writing a BOOM filter, querying the alert database, or
reconstructing the model curve yourself.

The model is the piecewise analytic supernova light-curve function introduced by
[Villar et al. (2019)](https://ui.adsabs.harvard.edu/abs/2019ApJ...884...83V/abstract).
BOOM fits it jointly in *g* and *r* with a GPU particle-swarm optimizer
([`villar-pso`](https://github.com/frenbox/villar-pso)), minimizing a
band-balanced Gaussian negative log-likelihood with Gaussian priors, then
refining the swarm's best point with a short Nelder-Mead polish. The fit runs in
flux; the two flux-valued parameters are reported on the AB magnitude scale.

The light curve it fits is the ZTF photometry (detections and forced
photometry) up to the alert, plus the *g* and *r* photometry of the matched
LSST object when there is one. Points from both surveys go into the same two
bands, so the parameters keep their `ZTF_` names even when LSST points were
used. DECam points are not used.

## Quick reference

| Field | Meaning | Unit |
| --- | --- | --- |
| `villar_fit.A_ZTF_r` / `A_ZTF_g` | Amplitude | AB mag |
| `villar_fit.beta_ZTF_r` / `beta_ZTF_g` | Plateau slope, as a fraction of `A` | day⁻¹ |
| `villar_fit.gamma_ZTF_r` / `gamma_ZTF_g` | Plateau duration | days |
| `villar_fit.t_0_ZTF_r` / `t_0_ZTF_g` | Reference (rise) time | days from peak |
| `villar_fit.tau_rise_ZTF_r` / `tau_rise_ZTF_g` | Rise e-folding time | days |
| `villar_fit.tau_fall_ZTF_r` / `tau_fall_ZTF_g` | Decline e-folding time | days |
| `villar_fit.extra_sigma_ZTF_r` / `extra_sigma_ZTF_g` | Fitted excess scatter, as a magnitude error at `A` | mag |
| `villar_fit.reduced_chi2` | Goodness of fit | dimensionless |
| `villar_fit.peak_mag` | Normalization scale used by the fit | AB mag |
| `villar_fit.t_peak` | Epoch of the brightest *r*-band point, where phase is zero | JD |

Seventeen fields in total: seven parameters × two bands, plus a fit statistic,
the normalization scale and the phase reference. Every field is a double, and
**every field is `NaN` when the fit did not run or did not succeed** — see
[When the fields are NaN](#when-the-fields-are-nan).

Fits are stored **per alert**, not per object. A well-observed object has one
`villar_fit` per `candid`, each computed from the light curve as it stood at
that alert's epoch. The most recent `candid` has the most complete light curve
and therefore the most trustworthy fit.

## The model

Let `t` be time in days and let `t_peak` be the epoch of the brightest *r*-band
point the fit saw, stored as `villar_fit.t_peak` (JD; absent on fits stored
before it was added). The fit works in **phase**:

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

The model is linear in flux, so `A` here is a flux. The stored `A` is an AB
magnitude; convert it first (see [Flux and magnitude convention](#flux-and-magnitude-convention)).

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

### Flux and magnitude convention

The fit works in flux. Magnitudes are converted with a zeropoint of 23.9, so
the internal flux scale is in **microjanskys**:

```
F_uJy = 10 ** ((23.9 - mag) / 2.5)
mag   = 23.9 - 2.5 * log10(F_uJy)
```

The two flux-valued parameters are converted back for storage. `A` and
`peak_mag` are plain AB magnitudes. `extra_sigma` follows the photometric
`mag_err` convention: the flux scatter expressed as a magnitude error at the
amplitude `A`.

```
A           = 23.9 - 2.5 * log10(A_uJy)
extra_sigma = 2.5 / ln(10) * extra_sigma_uJy / A_uJy    # ≈ 1.0857 * extra_sigma_uJy / A_uJy
peak_mag    = 23.9 - 2.5 * log10(peak_flux_uJy)
```

and back:

```
A_uJy           = 10 ** ((23.9 - A) / 2.5)
extra_sigma_uJy = extra_sigma * A_uJy / 1.0857
```

The other five parameters are in days or day⁻¹ and do not depend on the flux
scale.

### Physical validity constraints

Parameter sets that produce unphysical curves are rejected outright by the
optimizer, so a successful fit always satisfies all three of:

```
gamma * beta                                   <= 1
exp(-gamma / tau_rise) * (tau_fall/tau_rise - 1) <= 1
beta * tau_fall + beta * gamma                 <= 1
```

## Parameters in detail

### `A` — amplitude (AB mag)

Scale of the light curve. With the sigmoid saturated and `beta` small, the
plateau flux tends to `A`, so `A` is roughly the plateau magnitude and is
directly comparable across objects. Lower is brighter.

The amplitude difference is a color proxy: `A_ZTF_g - A_ZTF_r` is roughly the
*g − r* color at plateau.

### `beta` — plateau slope

How fast the plateau declines, as a fraction of `A` per day, in day⁻¹. Positive
`beta` means fading, negative means still brightening through the plateau.

Because it is a *fractional* rate, `beta` is already comparable across objects
of any brightness. If you want the slope in flux units — the `β` of the paper's
Equation 1 — it is `-A_uJy * beta`, in µJy/day, with `A` converted to µJy first.

### `gamma` — plateau duration (days)

Length of the linear-decline phase, measured from `t_0`. Short `gamma` with a
short `tau_fall` looks like a fast transient; long `gamma` is the signature of a
Type IIP-like plateau. Bounded to roughly 6–100 days in *r*.

### `t_0` — reference time (days from peak)

Where the sigmoid rise is centered, in days relative to the brightest *r*-band
epoch. **It is not an absolute JD or MJD** — it is already a phase. Negative
values (the usual case; the prior is centered at −12 days) put the rise before
peak. To get an absolute epoch, add `t_peak`.

`t_0` is the closest thing the model has to an explosion-time proxy, but it is
the half-maximum point of the rise, not first light.

### `tau_rise` — rise timescale (days)

E-folding time of the sigmoid rise. Small values (< 1 day) mean an abrupt
turn-on; a fast-riser search is essentially a cut on this parameter.

### `tau_fall` — decline timescale (days)

E-folding time of the exponential tail after the plateau. This is the parameter
that separates fast-evolving transients from slowly fading, radioactively
powered supernovae.

### `extra_sigma` — excess scatter (mag)

A white-noise term fitted alongside the shape parameters and added in quadrature
to the reported photometric errors, in flux:

```
sigma_total^2 = flux_err^2 + extra_sigma_uJy^2
```

It absorbs both real intrinsic variability and underestimated uncertainties.
The stored value is that scatter as a magnitude error at the amplitude, so it
is already relative to `A`: `0.05` is a scatter of about 5% of the amplitude.

The likelihood includes the Gaussian normalization term `ln sigma_total^2`, so
widening the errors has a cost and `extra_sigma` grows only as far as the
residuals call for. A large `extra_sigma` still means the model is not
describing the data well even if `reduced_chi2` looks acceptable — the fit
traded residuals for scatter. **Check `extra_sigma` alongside
`reduced_chi2`.**

### `reduced_chi2`

Chi-squared per degree of freedom of the best fit, computed on peak-normalized
flux with `extra_sigma` included in the denominator, with
`dof = max(n_points - 14, 1)`.

When the model cannot follow the data, the fit grows `extra_sigma` until the
residuals are about the size of the total error, so this statistic sits near 1
even for a poor fit. Treat it as a relative ranking tool — "which of these fits
is worse" — rather than an absolute, calibrated goodness-of-fit probability.
Values well above a few indicate the model genuinely failed.

The optimizer minimizes the full likelihood, not this statistic, and with three
seeds keeps the one with the lowest likelihood cost. The stored fit is therefore
not necessarily the one with the lowest `reduced_chi2`.

### `peak_mag`

The AB magnitude of the largest flux, in either band, among the points the fit
actually saw — the scale everything was normalized by before fitting. It is
stored because it is not otherwise recoverable from the alert document alone:
reproducing it means replaying the preprocessing over that object's
`ZTF_alerts_aux` photometry, filtered to `jd <= ` the alert's own `jd`.

`A - peak_mag` is the amplitude relative to that scale — the normalized value
the optimizer worked in, as a magnitude — which is what you want when comparing
fit *shapes* across objects of very different brightness. `extra_sigma` needs
no such step; it is already relative to `A`.

## Parameter bounds

The optimizer searches inside hard bounds, so stored values are always within
these ranges. A parameter sitting exactly on a bound is a red flag: the fit
wanted to go further and could not.

| Parameter | `ZTF_r` range | `ZTF_g` envelope |
| --- | --- | --- |
| `A - peak_mag` (mag) | −0.375 – +0.5 | −0.875 – +1.25 |
| `beta` (day⁻¹) | −0.01 – 0.03 | −0.03 – 0.05 |
| `gamma` (days) | 6.31 – 100 | 1.0 – 316 |
| `t_0` (days) | −50 – 30 | −55 – 33 |
| `tau_rise` (days) | 0.501 – 10.0 | 0.050 – 31.6 |
| `tau_fall` (days) | 6.31 – 158 | 1.0 – 316 |
| `extra_sigma_uJy` (× peak flux) | 0.00316 – 0.501 | 0.00079 – 1.995 |

The *r*-band ranges are exact. The *g*-band column is an **envelope**: *g* is
searched as a bounded offset from *r*, so reaching a *g* extreme also requires
*r* to be near its own extreme. The bounds were calibrated against reference
fits and carry roughly 2σ of margin, which is why they are tighter than a naive
"any physical supernova" range would be.

The search bounds `A` as a fraction of peak flux (0.631 – 1.413 in *r*), which
is the magnitude range shown: up to 0.375 mag brighter or 0.5 mag fainter than
`peak_mag`. `extra_sigma` is bounded in flux, as a fraction of peak flux.
Its stored magnitude value also depends on `A`, so it has no fixed range of its
own; convert it back with the formula in
[Flux and magnitude convention](#flux-and-magnitude-convention) to check it
against a bound.

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
7. Normalizes all fluxes by the global peak flux; `A` and `extra_sigma` are
   converted to AB magnitudes afterwards.

Step 5 is why `t_0` has a −50 day floor, and step 3 is why `t_0` is usually
negative.

## When the fields are NaN

All seventeen fields are written as `NaN` — never omitted — whenever a fit is not
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

Bright enough for spectroscopic follow-up, brighter than 18.5 mag at plateau:

```js
db.ZTF_alerts.find({
  "villar_fit.reduced_chi2": { $lt: 3 },
  "villar_fit.A_ZTF_r":      { $lt: 18.5 }
})
```

Blue transients, *g − r* < −0.285 at plateau (a 30% *g*-band flux excess;
needs `$expr`, since it compares two fields):

```js
db.ZTF_alerts.find({
  $expr: { $lt: ["$villar_fit.A_ZTF_g", { $subtract: ["$villar_fit.A_ZTF_r", 0.285] }] }
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
    """Evaluate one band of a stored villar_fit over a phase grid, in AB mag."""
    if phases is None:
        phases = np.linspace(-50.0, 100.0, 500)

    flux = villar_flux(
        phases,
        A=10 ** ((ZP - villar_fit[f"A_{band}"]) / 2.5),  # stored A is AB mag
        beta=villar_fit[f"beta_{band}"],
        gamma=villar_fit[f"gamma_{band}"],
        t_0=villar_fit[f"t_0_{band}"],
        tau_rise=villar_fit[f"tau_rise_{band}"],
        tau_fall=villar_fit[f"tau_fall_{band}"],
    )
    # Non-positive model flux has no magnitude.
    with np.errstate(divide="ignore", invalid="ignore"):
        mag = np.where(flux > 0, ZP - 2.5 * np.log10(flux), np.nan)
    return phases, mag
```

To overlay this on observed photometry, plot the data against
`jd - villar_fit.t_peak` so both are on the same phase axis.

### Derived quantities

A few combinations are more useful than the raw parameters:

| Quantity | Expression | Why |
| --- | --- | --- |
| Plateau slope, flux units | `-beta * 10**((23.9 - A)/2.5)` | The paper's `β`, in µJy/day |
| Color at plateau | `A_ZTF_g - A_ZTF_r` | Approximate *g − r* |
| Amplitude vs. normalization | `A - peak_mag` | Brightness-independent shape |
| Rise/fall asymmetry | `tau_fall / tau_rise` | Separates fast transients |
| Fractional scatter | `extra_sigma / 1.0857` | Sanity check on `reduced_chi2` |
| Total duration proxy | `gamma + tau_fall` | Rough event timescale |

## Caveats

Worth knowing before you build science on these numbers.

**`dof` is clamped at 1.** With 14 free parameters, any light curve with fewer
than 15 points gets `dof = 1`, so `reduced_chi2` is then just the total
chi-squared. Compare fits with similar numbers of points.

**`extra_sigma` can mask a bad fit.** The likelihood charges for widening the
errors, but a model that cannot follow the data still trades residuals for
scatter, which keeps `reduced_chi2` near 1. Always read `extra_sigma` alongside
it.

**Priors are ZTF-calibrated and fairly tight.** The bounds above come from
reference fits on ZTF supernovae. Genuinely exotic transients can be pushed
against a bound rather than fitted; check for railed parameters.

**One fit per alert, not per object.** Early alerts are fitted on partial light
curves and will disagree with later ones. Use the latest `candid` unless you
specifically want the historical view.

**Only *g* and *r*.** ZTF *i*-band points and the other LSST bands are
discarded before fitting.

**ZTF and LSST are fitted as one survey.** Their *g* and *r* filters are close
but not identical, and the fit does not correct for the difference: a
systematic offset between the two surveys ends up in `extra_sigma`.

## Source

| What | Where |
| --- | --- |
| Model, priors, PSO, preprocessing | [`villar-pso`](https://github.com/frenbox/villar-pso) |
| Batch fitting and Mongo writes | [`src/enrichment/ztf.rs`](https://github.com/boom-astro/boom/blob/main/src/enrichment/ztf.rs) |
| GPU context setup | [`src/enrichment/models/mod.rs`](https://github.com/boom-astro/boom/blob/main/src/enrichment/models/mod.rs) |

## References

- Villar, V. A., Berger, E., Miller, G., et al. 2019, *Supernova Photometric
  Classification Pipelines Trained on Spectroscopically Classified Supernovae
  from the Pan-STARRS1 Medium-deep Survey*, ApJ, 884, 83.
  [doi:10.3847/1538-4357/ab418c](https://doi.org/10.3847/1538-4357/ab418c) ·
  [arXiv:1905.07422](https://arxiv.org/abs/1905.07422)
