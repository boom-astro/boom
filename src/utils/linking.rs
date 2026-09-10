//! Intra-night tracklet finding: group a night's unassociated detections into
//! sets consistent with a single source moving at a constant on-sky rate.

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
    /// Guards against pairing across nights.
    pub max_span_days: f64,
    /// Two detections in the same exposure cannot constrain a rate.
    pub min_pair_dt_days: f64,
}

impl Default for TrackletConfig {
    fn default() -> Self {
        Self {
            // A main-belt asteroid at opposition moves ~0.2 deg/day; this keeps
            // slow movers while rejecting the stationary field.
            min_rate_deg_per_day: 0.02,
            // Fast enough for NEOs without pairing unrelated sources across the field.
            max_rate_deg_per_day: 5.0,
            max_rms_arcsec: 1.5,
            match_radius_arcsec: 3.0,
            min_detections: 3,
            max_span_days: 0.5,
            min_pair_dt_days: 30.0 / 1440.0,
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
fn tangent_plane(ra: f64, dec: f64, ra0: f64, dec0: f64) -> Option<(f64, f64)> {
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

/// Fit constant motion to a set of detections, or `None` if it does not hold.
fn fit_tracklet(dets: &[Detection], cfg: &TrackletConfig) -> Option<Tracklet> {
    if dets.len() < 2 {
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
            let sep = angular_separation_deg(a.ra, a.dec, b.ra, b.dec);
            let rate = sep / dt;
            if rate < cfg.min_rate_deg_per_day || rate > cfg.max_rate_deg_per_day {
                continue;
            }

            let Some(seed) = fit_tracklet(&[*a, *b], cfg) else {
                continue;
            };
            let mut members = vec![*a, *b];
            for (k, d) in detections.iter().enumerate() {
                if k == i || k == j || (d.jd - seed.jd_ref).abs() > cfg.max_span_days {
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
    for c in candidates {
        let covered = kept
            .iter()
            .any(|k| c.ids.iter().all(|id| k.ids.contains(id)));
        if !covered {
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
                    band: None,
                }
            })
            .collect()
    }

    const NIGHT: [f64; 4] = [2460000.70, 2460000.75, 2460000.80, 2460000.85];

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
        assert_eq!(found.len(), 2);
        assert!(found.iter().all(|t| t.ids.len() == 4));
        // Each tracklet draws from one source, so its ids share a decade.
        assert!(found
            .iter()
            .any(|t| t.ids.contains(&1) && !t.ids.contains(&100)));
        assert!(found
            .iter()
            .any(|t| t.ids.contains(&100) && !t.ids.contains(&1)));
    }

    #[test]
    fn test_honours_min_detections() {
        let dets = mover(120.0, 20.0, 0.30, 0.0, &NIGHT[..2], 1);
        let strict = TrackletConfig::default();
        assert!(find_tracklets(&dets, &strict).is_empty());
        let pairs_ok = TrackletConfig {
            min_detections: 2,
            ..TrackletConfig::default()
        };
        assert_eq!(find_tracklets(&dets, &pairs_ok).len(), 1);
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
        let dets = mover(359.98, 5.0, 0.30, 0.0, &NIGHT, 1);
        assert!(dets.iter().any(|d| d.ra < 1.0) && dets.iter().any(|d| d.ra > 359.0));
        let found = find_tracklets(&dets, &TrackletConfig::default());
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].ids.len(), 4);
    }
}
