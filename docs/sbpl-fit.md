# SBPL light-curve fits

Every ZTF alert that BOOM enriches on a GPU gets a smoothly broken power-law
(SBPL) light-curve fit attached to it, under the `sbpl_fit` key. This page
explains what the fitted parameters mean, what units they are in, and how to use
them — whether you are writing a BOOM filter, querying the alert database, or
reconstructing the model curve yourself.

The SBPL is the standard phenomenological model for power-law transients such
as GRB afterglows: a rising power law that turns over smoothly into a declining
one. BOOM fits it jointly across ZTF *g*, *r* and *i* with a GPU particle-swarm
optimizer followed by L-BFGS refinement
([`sbpl-pso`](https://github.com/frenbox/sbpl-pso)), constrained to a single
peak.

## Quick reference

| Field | Meaning | Unit |
| --- | --- | --- |
| `sbpl_fit.alpha1` | Power-law index before the break (rise) | dimensionless, ≥ 0 |
| `sbpl_fit.alpha2` | Power-law index after the break (decline) | dimensionless, ≤ 0 |
| `sbpl_fit.beta` | Spectral index, F<sub>ν</sub> ∝ ν<sup>β</sup> | dimensionless |
| `sbpl_fit.logd` | log₁₀ of the break smoothness `D` | dimensionless |
| `sbpl_fit.loga` | log₁₀ of the amplitude | log₁₀ µJy |
| `sbpl_fit.tb` | Break timescale, measured from `t0` | days |
| `sbpl_fit.t0` | Onset time | **JD** |
| `sbpl_fit.<param>_err` | Spread of `<param>` across optimizer restarts | as `<param>` |
| `sbpl_fit.reduced_chi2` | Goodness of fit, χ²/N | dimensionless |
| `sbpl_fit.n_obs` | Points the fit saw | integer |
| `sbpl_fit.n_bands` | Bands the fit saw | integer |

Seventeen fields in total: seven parameters, their seven spreads, a fit
statistic, and two counts. The fifteen doubles are **`NaN` when the fit did not
run or did not succeed**; the two counts are always set, so an unfitted alert
still says how much data it had. See [When the fields are NaN](#when-the-fields-are-nan).

Fits are stored **per alert**, not per object. A well-observed object has one
`sbpl_fit` per `candid`, each computed from the light curve as it stood at that
alert's epoch. The most recent `candid` has the most complete light curve and
therefore the most trustworthy fit.

## The model

Let `t` be time in JD and `ν` the effective frequency of the band. Then

```
tau = (t - t0) / tb
D   = 10 ** logd

F(t, ν) = 10**loga * (ν / 1e15 Hz)**beta * tau**alpha1
          * [0.5 * (1 + tau**(1/D))] ** ((alpha2 - alpha1) * D)      for t > t0
F(t, ν) = 0                                                          for t <= t0
```

Well before the break (`tau ≪ 1`) the flux rises as `tau**alpha1`; well after it
(`tau ≫ 1`) it declines as `tau**alpha2`. `D` sets how gradual the turnover is:
small `D` is a sharp corner, `D` near 1 a broad, rounded peak.

The time dependence is the same in every band and the spectrum is a single
power law, so **the model is achromatic**: the color is fixed by `beta` for the
whole light curve. A transient whose color evolves is fitted with a compromise
color.

### Flux and frequency convention

Magnitudes are converted to flux with a zeropoint of 23.9, so fluxes are in
**microjanskys**:

```
F_uJy = 10 ** ((23.9 - mag) / 2.5)
mag   = 23.9 - 2.5 * log10(F_uJy)
```

Each band is represented by one effective wavelength, `ν = c / λ`:

| Band | λ (Å) |
| --- | --- |
| *g* | 4770 |
| *r* | 6231 |
| *i* | 7625 |

### Peak shape

BOOM constrains the fit to `alpha1 >= 0` and `alpha2 <= 0`: the light curve
rises, peaks once, and declines. With both indices nonzero the peak falls at

```
t_peak = t0 + tb * (alpha1 / -alpha2) ** D
```

which is not in general `t0 + tb`. `alpha2 = 0` means no decline has been seen
yet, and `alpha1 = 0` means no rise has been seen, which is typical of an object
first detected after peak.

## Parameters in detail

### `alpha1`, `alpha2` — temporal indices

Logarithmic slopes of the rise and the decline, in `F ∝ (t - t0)**alpha`,
bounded to `0 ≤ alpha1 ≤ 10` and `-10 ≤ alpha2 ≤ 0`. Because they are measured
from `t0` rather than from peak, compare them only between fits whose `t0` is
constrained.

`alpha1` is the least constrained parameter. It is measured from `t0`, and
unless the rise is well sampled nothing in the data pins `t0` down, so a steeper
rise from a later onset fits as well as a gentle one from an earlier onset.
**`alpha1` sitting at its bound of 10 is common**, and means "the rise is not
constrained", not "the rise is steep".

### `beta` — spectral index

`F_ν ∝ ν**beta`. Larger `beta` is bluer. With the wavelengths above, the model
color is

```
g - r = -2.5 * beta * log10(6231 / 4770) ≈ -0.29 * beta
```

and it does not change with time.

### `logd` — break smoothness

log₁₀ of `D`, bounded to [−3, 0]. Close to −3 the break is effectively a corner
and `tb` is well defined; close to 0 the turnover is spread over a decade or
more in time.

### `loga` — amplitude (log₁₀ µJy)

At `t = t0 + tb` (`tau = 1`) the bracketed term is exactly 1, so `10**loga` is
the flux at the break time **at ν = 10¹⁵ Hz** (λ ≈ 2998 Å, bluer than any ZTF
band). The break-time flux in a ZTF band is

```
F_band(t0 + tb) = 10**loga * (ν_band / 1e15 Hz)**beta
```

### `tb` — break timescale (days)

The scale of `tau`, measured from `t0`, **not** an epoch. Add it to `t0` to get
the break epoch, and see [Peak shape](#peak-shape) for how the break relates to
the peak.

### `t0` — onset (JD)

The time before which the model flux is zero. **Unlike `villar_fit.t_0`, this is
an absolute JD, not a phase.** It is bounded to lie at or before the first point
the fit saw, and no more than two light-curve spans before it.

### `*_err` — restart spread

The standard deviation of each parameter across the optimizer's three
independent restarts. It measures whether the optimizer lands in the same place
each time, **not** a statistical uncertainty: zero means every restart agreed,
and a large spread in `t0`, `tb` or `alpha1` is the degeneracy described above.

### `reduced_chi2`

Chi-squared divided by the number of points, `N`, not by the degrees of
freedom. There is no fitted excess-scatter term, so the reported photometric
errors are taken at face value and the statistic is not pulled towards 1: a
large value can be a poor fit or underestimated errors.

### `n_obs`, `n_bands`

How many points, and how many of *g*, *r*, *i*, the fit had. Set whether or not
the fit ran.

## Parameter bounds

| Parameter | Range |
| --- | --- |
| `alpha1` | 0 – 10 |
| `alpha2` | −10 – 0 |
| `beta` | −5 – 5 |
| `logd` | −3 – 0 |
| `tb` (days) | `max(0.01·span, 0.01)` – `min(max(10·span, 500), 10000)` |
| `t0` (JD) | `t_first − 2·span` – `t_first` |

`span` is the time between the first and last point the fit saw, and `t_first`
the first point's JD. `loga` is searched over ±10 in peak-normalized flux and
converted to µJy afterwards. A parameter sitting on a bound means the fit wanted
to go further and could not.

## Preprocessing

The fit sees the alert's own ZTF photometry only:

1. Takes the object's alert photometry and forced photometry up to the alert's
   `jd`. Photometry from a matched LSST object is **not** included, unlike in
   `villar_fit`.
2. Keeps what the rest of enrichment counts as a detection: every alert
   detection, and forced photometry with SNR ≥ 3.
3. Drops negative-difference detections. `magpsf` is the magnitude of the
   absolute flux, so without this a source fading below its reference would
   read as brightening.
4. Keeps one point per band and epoch, preferring the alert's own measurement
   to forced photometry of the same exposure.
5. Converts magnitudes to µJy with zeropoint 23.9.
6. Requires **at least 7 points across at least 2 bands**, spanning a nonzero
   time, or the fit is skipped.
7. Normalizes fluxes by the brightest point for fitting; `loga` is converted
   back to physical units afterwards.

## When the fields are NaN

The fifteen doubles are written as `NaN` — never omitted — whenever a fit is not
produced, so consumers always see one schema. This happens when:

- The alert has fewer than 7 usable points, or only one band. This is the
  common case by far: most alerts are early or sparse.
- It has no usable *g*, *r* or *i* photometry at all (`n_obs` and `n_bands` are
  then 0).
- GPU batch fitting fails for the whole batch.

`sbpl_fit` is written **only when BOOM runs with GPU enrichment enabled** (built
with the `gpu` feature and a CUDA or Metal device configured — see
[gpu.md](gpu.md)). On a CPU-only deployment the key is absent entirely, which is
different from being `NaN`.

In MongoDB, `NaN` compares false against every range predicate, so a query like
`{"sbpl_fit.reduced_chi2": {"$lt": 3}}` already excludes unfitted alerts. To
check explicitly, use `{"sbpl_fit.reduced_chi2": {"$gte": 0}}`; `$exists` will
**not** work, because the field is present.

## Using the fits

### Querying

Well-fitted light curves:

```js
db.ZTF_alerts.find({
  "sbpl_fit.reduced_chi2": { $lt: 3 }
})
```

Fast decliners, with the rise actually constrained:

```js
db.ZTF_alerts.find({
  "sbpl_fit.reduced_chi2": { $lt: 3 },
  "sbpl_fit.alpha2":       { $lt: -3 },
  "sbpl_fit.alpha1":       { $lt: 9.9 }
})
```

Blue transients (`g − r < −0.2`, so `beta > 0.7`):

```js
db.ZTF_alerts.find({
  "sbpl_fit.reduced_chi2": { $lt: 3 },
  "sbpl_fit.beta":         { $gt: 0.7 }
})
```

Why an alert has no fit:

```js
db.ZTF_alerts.find(
  {
    "sbpl_fit.n_obs":        { $exists: true },
    "sbpl_fit.reduced_chi2": { $not: { $gte: 0 } }
  },
  { "sbpl_fit.n_obs": 1, "sbpl_fit.n_bands": 1 }
)
```

Because fits are per-alert, add your own `objectId` grouping (or a "latest
candid" stage) if you want one fit per object rather than one per alert.

### Reconstructing the model curve

```python
import numpy as np

C_ANGSTROM_PER_S = 2.99792458e18
WAVELENGTH = {"g": 4770.0, "r": 6231.0, "i": 7625.0}


def sbpl_flux(jd, sbpl_fit, band="r"):
    """Model flux in uJy for one ZTF band at the given JDs."""
    nu_scaled = C_ANGSTROM_PER_S / WAVELENGTH[band] / 1e15
    tau = (np.asarray(jd, float) - sbpl_fit["t0"]) / sbpl_fit["tb"]
    d = 10.0 ** sbpl_fit["logd"]
    a1, a2 = sbpl_fit["alpha1"], sbpl_fit["alpha2"]
    with np.errstate(invalid="ignore", divide="ignore"):
        flux = (
            10.0 ** sbpl_fit["loga"]
            * nu_scaled ** sbpl_fit["beta"]
            * tau**a1
            * (0.5 * (1.0 + tau ** (1.0 / d))) ** ((a2 - a1) * d)
        )
    return np.where(tau > 0, flux, 0.0)


def peak_jd(sbpl_fit):
    """Epoch of peak flux, or None when the fit has no rise or no decline."""
    a1, a2 = sbpl_fit["alpha1"], sbpl_fit["alpha2"]
    if not (a1 > 0 and a2 < 0):
        return None
    d = 10.0 ** sbpl_fit["logd"]
    return sbpl_fit["t0"] + sbpl_fit["tb"] * (a1 / -a2) ** d
```

Both work directly on the JD axis of the observed photometry. Convert the model
flux to magnitudes with `23.9 - 2.5 * log10(F)`.

### Derived quantities

| Quantity | Expression | Why |
| --- | --- | --- |
| Peak epoch (JD) | `t0 + tb * (alpha1 / -alpha2) ** 10**logd` | Needs `alpha1 > 0`, `alpha2 < 0` |
| Break epoch (JD) | `t0 + tb` | Where `loga` is defined |
| Color | `-0.29 * beta` | *g − r*, constant in this model |
| Break flux in a band | `10**loga * (ν_band / 1e15)**beta` | µJy |
| Rise constrained? | `alpha1 < 10` and small `t0_err` | See [`alpha1`](#alpha1-alpha2--temporal-indices) |

## Caveats

Worth knowing before you build science on these numbers.

**The rise is often unconstrained.** Without data on the early rise, `t0`,
`tb` and `alpha1` trade off against each other; `alpha1` at 10 and a large
`t0_err` are the symptoms. The decline (`alpha2`) and the color (`beta`) are
much better determined.

**A fit is not guaranteed to be the best one.** The optimizer runs three
restarts of 30 particles for 200 iterations each. In tests on synthetic
ZTF-like peaked light curves, about one fit in six ended with a chi-squared
noticeably worse than a far longer search found. Compare `reduced_chi2` across
fits rather than trusting any single one absolutely.

**Achromatic by construction.** One spectral index for the whole light curve.
Supernovae redden as they evolve; their `beta` is an average.

**One fit per alert, not per object.** Early alerts are fitted on partial light
curves and will disagree with later ones. Use the latest `candid` unless you
specifically want the historical view.

**ZTF only.** For objects also seen by LSST, only the ZTF photometry is fitted.

## Source

| What | Where |
| --- | --- |
| Model, bounds, PSO, L-BFGS refinement | [`sbpl-pso`](https://github.com/frenbox/sbpl-pso) |
| Photometry selection, batch fitting, Mongo writes | [`src/enrichment/ztf.rs`](../src/enrichment/ztf.rs) |
| GPU context setup | [`src/enrichment/models/mod.rs`](../src/enrichment/models/mod.rs) |
