//! Intra-night tracklet finding: group a night's unassociated detections into
//! sets consistent with a single source moving at a constant on-sky rate.

use std::collections::HashSet;

/// The night a Julian date falls in, as an integer.
///
/// JD rolls over at 12:00 UTC, which is the middle of the night for a site in
/// the Americas: a ZTF night running past that boundary would otherwise count
/// as two, and a single-night group would satisfy a two-night requirement.
pub fn night_of(jd: f64) -> i64 {
    (jd - 0.5).floor() as i64
}

/// One detection offered to the linker.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Detection {
    pub id: i64,
    pub jd: f64,
    /// Degrees.
    pub ra: f64,
    /// Degrees.
    pub dec: f64,
    /// Apparent magnitude, when the survey reported one.
    pub mag: Option<f64>,
    /// Uncertainty on `mag`, which sets how much of a difference is real.
    pub mag_err: Option<f64>,
    /// Filter as a single letter, for reporting.
    pub band: Option<char>,
}

/// Bounds a tracklet must satisfy to be believable.
#[derive(Debug, Clone)]
pub struct TrackletConfig {
    /// Below this a source is stationary, not a mover.
    pub min_rate_deg_per_day: f64,
    /// Above this the motion outruns what a night's cadence can link.
    pub max_rate_deg_per_day: f64,
    /// How far a detection may sit from the fitted line.
    pub max_rms_arcsec: f64,
    /// Search radius when attaching a detection to a seed's prediction.
    pub match_radius_arcsec: f64,
    pub min_detections: usize,
    /// Longest a tracklet may span, days. Also bounds the pair search radius,
    /// since two detections cannot be further apart than the fastest motion
    /// carries in this time.
    pub max_span_days: f64,
    /// Two detections in the same exposure cannot constrain a rate.
    pub min_pair_dt_days: f64,
    /// Shortest on-sky arc a pair may span, arcseconds. Below this the measured
    /// rate is dominated by astrometric error rather than by motion.
    pub min_arc_arcsec: f64,
    /// Reject a pair whose magnitudes differ by more than this many combined
    /// sigma. `None` disables the test, as does a survey reporting no errors.
    pub max_mag_sigma: Option<f64>,
    /// Brightness change a real object may show within the window regardless of
    /// measurement error, added in quadrature. Rotation is the bulk of it.
    pub intrinsic_mag_scatter: f64,
}

impl Default for TrackletConfig {
    fn default() -> Self {
        Self {
            // A main-belt asteroid at opposition moves ~0.2 deg/day; this keeps
            // slow movers while rejecting the stationary field.
            min_rate_deg_per_day: 0.02,
            // Matches the rate heliolinx's make_tracklets accepts: spurious pairs
            // grow as the square of this, and genuinely faster movers trail.
            max_rate_deg_per_day: 1.0,
            max_rms_arcsec: 1.5,
            match_radius_arcsec: 3.0,
            // Two, not three: a survey revisiting a field twice a night is the
            // nominal case, so requiring three discards most of the sky. Pairs
            // are the less pure for it, which the orbit fit downstream settles.
            min_detections: 2,
            // Three hours, not heliolinx's 1.5: ZTF revisits a field slowly
            // enough that the shorter window leaves roughly half the two-night
            // objects unreachable, and widening it costs nothing in purity.
            max_span_days: 3.0 / 24.0,
            // heliolinx's mintime, which it takes in hours, converted: 6 minutes.
            min_pair_dt_days: 0.1 / 24.0,
            // heliolinx's minarc: rejects the pairs a stationary star produces.
            min_arc_arcsec: 10.0,
            // Nearly free rather than powerful: it removes a couple of percent
            // of chance pairs at no measured cost in real ones. A magnitude
            // limited survey piles most detections up where the errors are
            // large, and there the test rightly abstains.
            max_mag_sigma: Some(5.0),
            // Typical rotational amplitude over an hour. Without it the test
            // would reject real pairs hardest where the photometry is best.
            intrinsic_mag_scatter: 0.15,
        }
    }
}

/// A set of detections fitted by constant motion through the tangent plane.
#[derive(Debug, Clone)]
pub struct Tracklet {
    pub ids: Vec<i64>,
    /// Epoch the reference position is quoted at, the mean of the detections.
    pub jd_ref: f64,
    pub ra_ref: f64,
    pub dec_ref: f64,
    /// Great-circle rate along RA, degrees/day, already including cos(dec).
    pub ra_rate_deg_per_day: f64,
    pub dec_rate_deg_per_day: f64,
    pub rms_arcsec: f64,
    /// Centre of the tangent plane the rates are expressed in.
    ra_center: f64,
    dec_center: f64,
    /// Plane position at `jd_ref`, degrees.
    xi0: f64,
    eta0: f64,
}

impl Tracklet {
    /// A tracklet stated directly as a position and a rate, for a caller that
    /// already knows the motion rather than fitting it.
    #[allow(clippy::too_many_arguments)]
    pub fn from_motion(
        ids: Vec<i64>,
        jd_ref: f64,
        ra: f64,
        dec: f64,
        ra_rate_deg_per_day: f64,
        dec_rate_deg_per_day: f64,
        rms_arcsec: f64,
    ) -> Self {
        Self {
            ids,
            jd_ref,
            ra_ref: ra,
            dec_ref: dec,
            ra_rate_deg_per_day,
            dec_rate_deg_per_day,
            rms_arcsec,
            ra_center: ra,
            dec_center: dec,
            xi0: 0.0,
            eta0: 0.0,
        }
    }

    /// Total on-sky rate, degrees/day.
    pub fn rate_deg_per_day(&self) -> f64 {
        self.ra_rate_deg_per_day.hypot(self.dec_rate_deg_per_day)
    }
}

/// Great-circle separation in degrees.
pub fn angular_separation_deg(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
    let (r1, d1) = (ra1.to_radians(), dec1.to_radians());
    let (r2, d2) = (ra2.to_radians(), dec2.to_radians());
    let (sd, sr) = (((d2 - d1) / 2.0).sin(), ((r2 - r1) / 2.0).sin());
    let h = sd * sd + d1.cos() * d2.cos() * sr * sr;
    (2.0 * h.sqrt().clamp(-1.0, 1.0).asin()).to_degrees()
}

/// Gnomonic projection about (ra0, dec0), degrees. `None` on the far hemisphere.
pub fn tangent_plane(ra: f64, dec: f64, ra0: f64, dec0: f64) -> Option<(f64, f64)> {
    let (r, d) = (ra.to_radians(), dec.to_radians());
    let (r0, d0) = (ra0.to_radians(), dec0.to_radians());
    let cos_c = d0.sin() * d.sin() + d0.cos() * d.cos() * (r - r0).cos();
    if cos_c <= 1e-12 {
        return None;
    }
    let xi = d.cos() * (r - r0).sin() / cos_c;
    let eta = (d0.cos() * d.sin() - d0.sin() * d.cos() * (r - r0).cos()) / cos_c;
    Some((xi.to_degrees(), eta.to_degrees()))
}

/// Inverse of [`tangent_plane`], returning degrees.
fn from_tangent_plane(xi_deg: f64, eta_deg: f64, ra0: f64, dec0: f64) -> (f64, f64) {
    let (xi, eta) = (xi_deg.to_radians(), eta_deg.to_radians());
    let (r0, d0) = (ra0.to_radians(), dec0.to_radians());
    let rho = xi.hypot(eta);
    if rho < 1e-15 {
        return (ra0, dec0);
    }
    let c = rho.atan();
    let dec = (c.cos() * d0.sin() + eta * c.sin() * d0.cos() / rho).asin();
    let ra = r0 + (xi * c.sin()).atan2(rho * d0.cos() * c.cos() - eta * d0.sin() * c.sin());
    (ra.to_degrees().rem_euclid(360.0), dec.to_degrees())
}

/// Least-squares `v = a + b*t`. `None` when every epoch coincides.
fn fit_line(t: &[f64], v: &[f64]) -> Option<(f64, f64)> {
    let n = t.len() as f64;
    let (mt, mv) = (t.iter().sum::<f64>() / n, v.iter().sum::<f64>() / n);
    let mut num = 0.0;
    let mut den = 0.0;
    for (ti, vi) in t.iter().zip(v) {
        num += (ti - mt) * (vi - mv);
        den += (ti - mt) * (ti - mt);
    }
    if den <= 0.0 {
        return None;
    }
    let b = num / den;
    Some((mv - b * mt, b))
}

/// Whether two detections are too different in brightness to be one object.
///
/// Measured in combined sigma rather than magnitudes: a fixed cut is severe on
/// bright sources whose errors are millimagnitudes and vacuous on faint ones
/// whose errors approach the cut itself. Anything the photometry cannot speak
/// to -- a missing magnitude or error, or two different filters, where colour
/// makes the difference meaningless -- passes rather than being rejected.
fn photometry_disagrees(a: &Detection, b: &Detection, cfg: &TrackletConfig) -> bool {
    let Some(limit) = cfg.max_mag_sigma else {
        return false;
    };
    if a.band != b.band {
        return false;
    }
    let (Some(ma), Some(mb)) = (a.mag, b.mag) else {
        return false;
    };
    let (Some(sa), Some(sb)) = (a.mag_err, b.mag_err) else {
        return false;
    };
    let floor = cfg.intrinsic_mag_scatter;
    let sigma = (sa * sa + sb * sb + floor * floor).sqrt();
    sigma.is_finite() && sigma > 0.0 && (ma - mb).abs() / sigma > limit
}

/// Fit constant motion to a set of detections, or `None` if it does not hold.
fn fit_tracklet(dets: &[Detection], cfg: &TrackletConfig) -> Option<Tracklet> {
    if dets.len() < 2 {
        return None;
    }
    // Growth bounds each detection against the seed's midpoint, which two
    // detections either side of it can satisfy while spanning twice the window.
    let (first, last) = dets.iter().fold((f64::MAX, f64::MIN), |(lo, hi), d| {
        (lo.min(d.jd), hi.max(d.jd))
    });
    if last - first > cfg.max_span_days {
        return None;
    }
    let jd_ref = dets.iter().map(|d| d.jd).sum::<f64>() / dets.len() as f64;
    // Project about the first detection so the fit stays linear in the plane.
    let (ra0, dec0) = (dets[0].ra, dets[0].dec);

    let mut t = Vec::with_capacity(dets.len());
    let mut xs = Vec::with_capacity(dets.len());
    let mut ys = Vec::with_capacity(dets.len());
    for d in dets {
        let (xi, eta) = tangent_plane(d.ra, d.dec, ra0, dec0)?;
        t.push(d.jd - jd_ref);
        xs.push(xi);
        ys.push(eta);
    }

    let (x0, xr) = fit_line(&t, &xs)?;
    let (y0, yr) = fit_line(&t, &ys)?;

    let mut sq = 0.0;
    for i in 0..dets.len() {
        let (dx, dy) = (xs[i] - (x0 + xr * t[i]), ys[i] - (y0 + yr * t[i]));
        sq += dx * dx + dy * dy;
    }
    let rms_arcsec = (sq / dets.len() as f64).sqrt() * 3600.0;
    if rms_arcsec > cfg.max_rms_arcsec {
        return None;
    }

    let (ra_ref, dec_ref) = from_tangent_plane(x0, y0, ra0, dec0);
    let rate = xr.hypot(yr);
    if rate < cfg.min_rate_deg_per_day || rate > cfg.max_rate_deg_per_day {
        return None;
    }

    Some(Tracklet {
        ids: dets.iter().map(|d| d.id).collect(),
        jd_ref,
        ra_ref,
        dec_ref,
        ra_rate_deg_per_day: xr,
        dec_rate_deg_per_day: yr,
        rms_arcsec,
        ra_center: ra0,
        dec_center: dec0,
        xi0: x0,
        eta0: y0,
    })
}

/// Find every tracklet supported by `detections`, longest first.
///
/// Seeds on pairs that could be one mover, grows each seed by attaching
/// detections near its predicted position, then drops seeds whose detections
/// are all covered by a longer tracklet.
pub fn find_tracklets(detections: &[Detection], cfg: &TrackletConfig) -> Vec<Tracklet> {
    if detections.len() < cfg.min_detections.max(2) {
        return Vec::new();
    }

    // Sorted by declination so the pair search sweeps a thin band per detection.
    let mut order: Vec<usize> = (0..detections.len()).collect();
    order.sort_by(|&a, &b| {
        detections[a]
            .dec
            .partial_cmp(&detections[b].dec)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let max_sep = cfg.max_rate_deg_per_day * cfg.max_span_days;
    let match_radius = cfg.match_radius_arcsec / 3600.0;
    // Declinations in `order`, so growth can binary-search the same band the
    // pair search sweeps rather than rescanning every detection per seed.
    let sorted_decs: Vec<f64> = order.iter().map(|&k| detections[k].dec).collect();
    let mut candidates: Vec<Tracklet> = Vec::new();

    for (oi, &i) in order.iter().enumerate() {
        for &j in order[oi + 1..].iter() {
            let (a, b) = (&detections[i], &detections[j]);
            if b.dec - a.dec > max_sep {
                break;
            }
            let dt = (b.jd - a.jd).abs();
            if dt < cfg.min_pair_dt_days || dt > cfg.max_span_days {
                continue;
            }
            // Before the trigonometry, since it rejects a good share of pairs.
            if photometry_disagrees(a, b, cfg) {
                continue;
            }
            let sep = angular_separation_deg(a.ra, a.dec, b.ra, b.dec);
            if sep * 3600.0 < cfg.min_arc_arcsec {
                continue;
            }
            let rate = sep / dt;
            if rate < cfg.min_rate_deg_per_day || rate > cfg.max_rate_deg_per_day {
                continue;
            }

            let Some(seed) = fit_tracklet(&[*a, *b], cfg) else {
                continue;
            };
            let mut members = vec![*a, *b];
            // A member sits within `match_radius` of the seed line, which over
            // the window reaches at most `max_sep` from the pair in latitude.
            let margin = max_sep + match_radius;
            let lo = sorted_decs.partition_point(|&x| x < a.dec.min(b.dec) - margin);
            let hi = sorted_decs.partition_point(|&x| x <= a.dec.max(b.dec) + margin);
            for &k in &order[lo..hi] {
                let d = &detections[k];
                if k == i || k == j || (d.jd - seed.jd_ref).abs() > cfg.max_span_days {
                    continue;
                }
                if photometry_disagrees(a, d, cfg) {
                    continue;
                }
                let (pra, pdec) = predict(&seed, d.jd);
                if angular_separation_deg(d.ra, d.dec, pra, pdec) <= match_radius {
                    members.push(*d);
                }
            }
            members.sort_by(|p, q| p.jd.partial_cmp(&q.jd).unwrap_or(std::cmp::Ordering::Equal));
            members.dedup_by_key(|d| d.id);

            if members.len() < cfg.min_detections {
                continue;
            }
            if let Some(t) = fit_tracklet(&members, cfg) {
                candidates.push(t);
            }
        }
    }

    // Longest first, then tightest, so subset removal keeps the best version.
    candidates.sort_by(|a, b| {
        b.ids.len().cmp(&a.ids.len()).then(
            a.rms_arcsec
                .partial_cmp(&b.rms_arcsec)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });

    let mut kept: Vec<Tracklet> = Vec::new();
    // Membership sets alongside `kept`: the subset test is the hot loop here.
    let mut kept_ids: Vec<HashSet<i64>> = Vec::new();
    for c in candidates {
        let covered = kept_ids
            .iter()
            .any(|k| c.ids.iter().all(|id| k.contains(id)));
        if !covered {
            kept_ids.push(c.ids.iter().copied().collect());
            kept.push(c);
        }
    }
    kept
}

/// Where a tracklet puts its source at `jd`, degrees.
pub fn predict(tracklet: &Tracklet, jd: f64) -> (f64, f64) {
    let dt = jd - tracklet.jd_ref;
    from_tangent_plane(
        tracklet.xi0 + tracklet.ra_rate_deg_per_day * dt,
        tracklet.eta0 + tracklet.dec_rate_deg_per_day * dt,
        tracklet.ra_center,
        tracklet.dec_center,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Detections of one source moving at a constant rate from (ra0, dec0).
    fn mover(
        ra0: f64,
        dec0: f64,
        ra_rate: f64,
        dec_rate: f64,
        jds: &[f64],
        id0: i64,
    ) -> Vec<Detection> {
        jds.iter()
            .enumerate()
            .map(|(k, &jd)| {
                let dt = jd - jds[0];
                let (ra, dec) = from_tangent_plane(ra_rate * dt, dec_rate * dt, ra0, dec0);
                Detection {
                    id: id0 + k as i64,
                    jd,
                    ra,
                    dec,
                    mag: None,
                    mag_err: None,
                    band: None,
                }
            })
            .collect()
    }

    // Spaced so all four fit inside the pair window, as a real cadence does.
    const NIGHT: [f64; 4] = [2460000.700, 2460000.715, 2460000.730, 2460000.745];

    #[test]
    fn test_angular_separation_is_symmetric_and_scaled() {
        assert!((angular_separation_deg(10.0, 0.0, 11.0, 0.0) - 1.0).abs() < 1e-9);
        // A degree of RA at dec 60 subtends half a degree on the sky.
        assert!((angular_separation_deg(10.0, 60.0, 11.0, 60.0) - 0.5).abs() < 1e-3);
    }

    #[test]
    fn test_tangent_plane_round_trips() {
        let (xi, eta) = tangent_plane(31.2, -14.5, 31.0, -14.0).unwrap();
        let (ra, dec) = from_tangent_plane(xi, eta, 31.0, -14.0);
        assert!((ra - 31.2).abs() < 1e-9 && (dec + 14.5).abs() < 1e-9);
    }

    #[test]
    fn test_recovers_a_linear_mover() {
        let dets = mover(120.0, 20.0, 0.30, -0.10, &NIGHT, 1);
        let found = find_tracklets(&dets, &TrackletConfig::default());
        assert_eq!(found.len(), 1);
        let t = &found[0];
        assert_eq!(t.ids.len(), 4);
        assert!((t.ra_rate_deg_per_day - 0.30).abs() < 1e-6);
        assert!((t.dec_rate_deg_per_day + 0.10).abs() < 1e-6);
        assert!(t.rms_arcsec < 1e-3);
    }

    #[test]
    fn test_rejects_a_stationary_source() {
        let dets = mover(120.0, 20.0, 0.0, 0.0, &NIGHT, 1);
        assert!(find_tracklets(&dets, &TrackletConfig::default()).is_empty());
    }

    #[test]
    fn test_rejects_motion_faster_than_the_cadence_can_link() {
        let dets = mover(120.0, 20.0, 40.0, 0.0, &NIGHT, 1);
        assert!(find_tracklets(&dets, &TrackletConfig::default()).is_empty());
    }

    #[test]
    fn test_rejects_a_source_that_does_not_move_in_a_line() {
        let mut dets = mover(120.0, 20.0, 0.30, 0.0, &NIGHT, 1);
        // A tenth of a degree off the line dwarfs the 1.5 arcsec tolerance.
        dets[2].dec += 0.1;
        let found = find_tracklets(&dets, &TrackletConfig::default());
        assert!(found.iter().all(|t| !t.ids.contains(&3)));
    }

    #[test]
    fn test_separates_two_movers_in_one_field() {
        let mut dets = mover(120.0, 20.0, 0.30, -0.10, &NIGHT, 1);
        dets.extend(mover(120.05, 20.02, -0.25, 0.15, &NIGHT, 100));
        let found = find_tracklets(&dets, &TrackletConfig::default());
        // Both sources come out whole, and no multi-point tracklet mixes them.
        // Ids below 100 belong to one source and ids from 100 to the other.
        let whole: Vec<&Tracklet> = found.iter().filter(|t| t.ids.len() == 4).collect();
        assert_eq!(whole.len(), 2);
        assert!(whole.iter().any(|t| t.ids.iter().all(|&id| id < 100)));
        assert!(whole.iter().any(|t| t.ids.iter().all(|&id| id >= 100)));
        assert!(found
            .iter()
            .filter(|t| t.ids.len() > 2)
            .all(|t| t.ids.iter().all(|&id| id < 100) || t.ids.iter().all(|&id| id >= 100)));
    }

    /// A pair is a tracklet by default, since a survey visiting a field twice a
    /// night gives nothing longer.
    #[test]
    fn test_a_pair_is_a_tracklet_by_default() {
        // Far enough apart in time to clear the pair gate, which this is not about.
        let dets = mover(120.0, 20.0, 0.30, 0.0, &[NIGHT[0], NIGHT[3]], 1);
        assert_eq!(find_tracklets(&dets, &TrackletConfig::default()).len(), 1);

        let strict = TrackletConfig {
            min_detections: 3,
            ..TrackletConfig::default()
        };
        assert!(find_tracklets(&dets, &strict).is_empty());
    }

    #[test]
    fn test_ignores_detections_from_another_night() {
        let mut dets = mover(120.0, 20.0, 0.30, -0.10, &NIGHT, 1);
        dets.push(Detection {
            id: 99,
            jd: NIGHT[0] + 3.0,
            ra: 120.9,
            dec: 19.7,
            mag: None,
            mag_err: None,
            band: None,
        });
        let found = find_tracklets(&dets, &TrackletConfig::default());
        assert_eq!(found.len(), 1);
        assert!(!found[0].ids.contains(&99));
    }

    #[test]
    fn test_predict_returns_the_detected_positions() {
        let dets = mover(120.0, 20.0, 0.30, -0.10, &NIGHT, 1);
        let t = &find_tracklets(&dets, &TrackletConfig::default())[0];
        for d in &dets {
            let (ra, dec) = predict(t, d.jd);
            assert!(angular_separation_deg(ra, dec, d.ra, d.dec) * 3600.0 < 0.01);
        }
    }

    #[test]
    fn test_handles_the_ra_wrap() {
        // Close enough to 360 that the night's motion carries it past the wrap.
        let dets = mover(359.995, 5.0, 0.30, 0.0, &NIGHT, 1);
        assert!(dets.iter().any(|d| d.ra < 1.0) && dets.iter().any(|d| d.ra > 359.0));
        let found = find_tracklets(&dets, &TrackletConfig::default());
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].ids.len(), 4);
    }

    /// A Palomar night runs from about 02:00 to 14:00 UTC, straddling the JD
    /// rollover at 12:00, and must still read as one night.
    #[test]
    fn test_one_night_across_the_jd_rollover_is_one_night() {
        let evening: f64 = 2_461_272.6; // 02:24 UTC
        let morning: f64 = 2_461_273.05; // 13:12 UTC, past the rollover
        assert!(morning.floor() > evening.floor(), "the rollover is spanned");
        assert_eq!(night_of(evening), night_of(morning));
        // And a genuinely different night still separates.
        assert_ne!(night_of(evening), night_of(evening + 1.0));
    }

    /// Growth measures each detection against the seed's midpoint, so without a
    /// total-span bound a tracklet can reach twice `max_span_days`.
    #[test]
    fn test_a_tracklet_may_not_span_twice_the_window() {
        let cfg = TrackletConfig::default();
        let span = cfg.max_span_days;
        // Evenly spread over twice the window, so every detection is within
        // `span` of the midpoint while the set spans `2 * span`.
        let jds = [0.0, 0.5 * span, 1.5 * span, 2.0 * span].map(|d| 2_461_272.6 + d);
        let dets = mover(180.0, 20.0, 0.3, 0.05, &jds, 1);

        for t in find_tracklets(&dets, &cfg) {
            let times: Vec<f64> = t
                .ids
                .iter()
                .map(|id| dets.iter().find(|d| d.id == *id).unwrap().jd)
                .collect();
            let lo = times.iter().cloned().fold(f64::MAX, f64::min);
            let hi = times.iter().cloned().fold(f64::MIN, f64::max);
            assert!(
                hi - lo <= span + 1e-9,
                "tracklet spans {} days against a {span} day window",
                hi - lo
            );
        }
    }

    fn at_mag(mag: f64, err: f64, band: char) -> Detection {
        Detection {
            id: 1,
            jd: 2_461_272.6,
            ra: 180.0,
            dec: 20.0,
            mag: Some(mag),
            mag_err: Some(err),
            band: Some(band),
        }
    }

    /// One magnitude difference, two verdicts: near the limit it is consistent
    /// with noise, and on a well-measured pair it is not. A cut in magnitudes
    /// rather than in sigma would have to answer both cases the same way.
    #[test]
    fn test_the_same_difference_is_judged_by_the_errors() {
        let cfg = TrackletConfig::default();
        let gap = 1.2;
        let faint = (at_mag(20.5, 0.35, 'r'), at_mag(20.5 + gap, 0.40, 'r'));
        let bright = (at_mag(16.0, 0.01, 'r'), at_mag(16.0 + gap, 0.01, 'r'));
        assert!(!photometry_disagrees(&faint.0, &faint.1, &cfg));
        assert!(photometry_disagrees(&bright.0, &bright.1, &cfg));
    }

    /// Rotation moves a bright asteroid by a couple of tenths within the
    /// window, which a sigma-only test would call a five-sigma mismatch.
    #[test]
    fn test_rotation_is_not_a_mismatch() {
        let cfg = TrackletConfig::default();
        let a = at_mag(16.0, 0.01, 'r');
        let b = at_mag(16.2, 0.01, 'r');
        assert!(!photometry_disagrees(&a, &b, &cfg));

        let no_floor = TrackletConfig {
            intrinsic_mag_scatter: 0.0,
            ..TrackletConfig::default()
        };
        assert!(photometry_disagrees(&a, &b, &no_floor));
    }

    /// Anything the photometry cannot speak to has to pass.
    #[test]
    fn test_photometry_test_abstains_without_information() {
        let cfg = TrackletConfig::default();
        let bright = at_mag(16.0, 0.01, 'r');
        // Two filters: the difference is colour, not variability.
        assert!(!photometry_disagrees(
            &bright,
            &at_mag(18.0, 0.01, 'g'),
            &cfg
        ));
        // No uncertainty reported, so there is no scale to judge against.
        let mut no_err = at_mag(18.0, 0.01, 'r');
        no_err.mag_err = None;
        assert!(!photometry_disagrees(&bright, &no_err, &cfg));
        // And the test can be switched off outright.
        let off = TrackletConfig {
            max_mag_sigma: None,
            ..TrackletConfig::default()
        };
        assert!(!photometry_disagrees(
            &bright,
            &at_mag(18.0, 0.01, 'r'),
            &off
        ));
    }
}
