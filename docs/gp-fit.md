# Gaussian-process fits

Every ZTF alert that BOOM enriches gets a multi-band Gaussian-process fit
computed on the CPU with [`rust-gp`](https://github.com/frenbox/rust-gp), stored
under `gp_fit`. The GP is non-parametric: rather than a model of the light
curve, it measures the peak, its width and the rise and fade rates in each band.
Like [`villar_fit`](villar-fit.md), it is computed from the light curve as it
stood at that alert's epoch, and filters can use it. Alerts enriched before the
fit was added have no `gp_fit`.

## Input

The alert's own ZTF photometry up to its epoch: detections, forced photometry
and non-detections. A matched LSST object's photometry is left out.

- A detection below the reference (negative flux) is dropped.
- A detection on an image shallower than 19 mag is dropped, and magnitude
  errors are raised to at least 0.1 mag.
- One point is kept per exposure, the alert photometry over the forced one.
- Non-detections are kept only before the first and after the last detection,
  as a flux of 0 with the 5σ limit as its error.

Detections whose leave-one-out residual exceeds 5σ are clipped and the fit is
redone, as long as at least 5 detections remain.

## Stored fields

`gp_fit` is flat, one key per field and band, for `g`, `r` and `i`
(`peak_mag_r`):

| Field | Meaning | Unit |
| --- | --- | --- |
| `reduced_chi2` | χ² per degree of freedom of the detections left after clipping, the GP's effective number of parameters taken off | |
| `peak_jd_<band>` / `peak_jd_err_<band>` | Time of the peak | JD / days |
| `peak_mag_<band>` / `peak_mag_err_<band>` | Peak magnitude | AB mag |
| `fwhm_<band>` / `fwhm_err_<band>` | Time spent above half the peak flux | days |
| `rise_rate_<band>` / `rise_rate_err_<band>` | Mean rate from half maximum to the peak | mag/day |
| `fade_rate_<band>` / `fade_rate_err_<band>` | Mean rate from the peak to half maximum | mag/day |
| `n_detections_<band>` | Detections fitted in that band | |

Errors are half the 16th to 84th percentile range over 100 draws of the curve
from the posterior.

## When fields are null

Every key is always written, so a missing `gp_fit` means the alert was enriched
before the fit existed. The whole document is null when the light curve was not
fitted:

- fewer than 5 detections;
- detections spanning 500 days or more, which are persistent variables rather
  than transients;
- the fit failed.

In a fitted light curve, a band's values are null where it has no data (its
`n_detections` is then 0), or where its brightest point is its first or last
one, since the peak may then lie outside the data. The rise or fade of a band is null when its half-maximum crossing is
not within the band's data: the fit never extrapolates.

## In a filter

A null field matches no comparison, so alerts without a value drop out:

```js
{ "$match": {
    "gp_fit.rise_rate_r": { "$gt": 0.1 },
    "gp_fit.fwhm_r": { "$lt": 30 },
    "gp_fit.reduced_chi2": { "$lt": 3 }
} }
```
