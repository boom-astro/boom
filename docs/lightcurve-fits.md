# Arnett and temperature fits

Every ZTF alert that BOOM enriches gets two light-curve fits computed on the
CPU with [`lightcurve-fitting`](https://github.com/boom-astro/lightcurve-fitting):
an Arnett model per band under `arnett_fit`, and a blackbody temperature
evolution under `thermal_fit`. Like [`villar_fit`](villar-fit.md), they are
stored per alert, from the light curve as it stood at that alert's epoch, and
filters can use them.

Both fits use the light curve the Villar fit uses (ZTF detections and forced
photometry up to the alert, plus the photometry of the matched LSST object),
restricted to the points from 50 days before to 100 days after its brightest
point. Alerts enriched before these fits were added have neither key.

## `arnett_fit`

One sub-document per band, `g`, `r` and `i`, each fitted on its own in flux:

```
s    = softplus(t - t0)
F(t) = A * (f * exp(-s / 8.8) + (1 - f) * exp(-s / 111.3)) * (1 - exp(-(s / tau_m)^2))
```

The first factor is the heating from ⁵⁶Ni (8.8 day e-folding) and ⁵⁶Co
(111.3 days) decay, the second the photon diffusion out of the ejecta.

| Field | Meaning | Unit |
| --- | --- | --- |
| `t0` / `t0_err` | Explosion time | JD / days |
| `tau_m` / `tau_m_err` | Diffusion timescale | days |
| `ni_fraction` / `ni_fraction_err` | `f`, the share of the heating from ⁵⁶Ni | |
| `mag_chi2` | χ² per point, in magnitude | |
| `n_points` | Points fitted in that band | |

Errors come from a Laplace approximation at the best fit. A band with fewer
than 5 points in the window, or whose fit did not converge, is `null`.

## `thermal_fit`

The colors of every band against a Gaussian process of a reference band (the
bluest of `u`, `g` and `r` with at least 5 points) are fitted with a blackbody
whose temperature evolves as

```
log10 T(t) = log_temp_start + cooling_rate * (t - t_start)
```

where `t_start` is the first point in the window.

| Field | Meaning | Unit |
| --- | --- | --- |
| `log_temp_start` / `log_temp_start_err` | Temperature at `t_start` | log10 K |
| `log_temp_latest` | Temperature at the last color point | log10 K |
| `cooling_rate` / `cooling_rate_err` | Slope of `log10 T` | dex per day |
| `chi2` | χ² per color point | |
| `n_color_obs` | Color points fitted | |
| `n_bands_used` | Bands besides the reference one | |

The errors are the spread of two optimizer restarts, so only a rough guide.
Every field but the two counts is `null` without a reference band or with
fewer than 3 color points.

## In a filter

A `null` field matches no comparison, so alerts without a fit drop out:

```js
{ "$match": {
    "arnett_fit.r.tau_m": { "$gt": 10, "$lt": 30 },
    "arnett_fit.r.mag_chi2": { "$lt": 5 },
    "thermal_fit.log_temp_latest": { "$gt": 4 }
} }
```
