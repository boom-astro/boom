//! Observing geometry for solar system objects: heliocentric distance,
//! topocentric distance and solar phase angle.
//!
//! Two-body propagation is deliberate. Geometry is far less sensitive to orbit
//! error than sky position is: an error large enough to move a predicted
//! position by 8" shifts these quantities by ~1e-4 au and ~0.003 deg. A
//! perturbed integrator would be needed to predict *where* an object is, not to
//! say how far away it was.

use std::f64::consts::PI;

/// Gaussian gravitational constant, rad/day.
const GAUSS_K: f64 = 0.017_202_098_95;
/// Obliquity is not applied: everything here stays in the ecliptic frame.
const KEPLER_TOLERANCE: f64 = 1e-12;
const KEPLER_MAX_ITER: usize = 64;

/// Heliocentric osculating elements at `epoch_jd`, as MPCORB distributes them.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrbitalElements {
    pub epoch_jd: f64,
    /// Semimajor axis, au.
    pub a: f64,
    /// Eccentricity.
    pub e: f64,
    /// Inclination, degrees.
    pub incl: f64,
    /// Longitude of the ascending node, degrees.
    pub node: f64,
    /// Argument of perihelion, degrees.
    pub peri: f64,
    /// Mean anomaly at `epoch_jd`, degrees.
    pub mean_anomaly: f64,
}

/// Observing geometry at a given instant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Geometry {
    /// Sun-to-object distance, au.
    pub helio_dist: f64,
    /// Observer-to-object distance, au.
    pub topo_dist: f64,
    /// Sun-object-observer angle, degrees.
    pub phase_angle: f64,
}

/// Solve `M = E - e sin E` for the eccentric anomaly, radians.
///
/// Newton-Raphson, starting from `M + e sin M`, which converges for all
/// elliptical eccentricities MPCORB carries.
pub fn solve_kepler(mean_anomaly: f64, e: f64) -> f64 {
    let m = mean_anomaly.rem_euclid(2.0 * PI);
    let mut ecc_anomaly = m + e * m.sin();
    for _ in 0..KEPLER_MAX_ITER {
        let delta = (ecc_anomaly - e * ecc_anomaly.sin() - m) / (1.0 - e * ecc_anomaly.cos());
        ecc_anomaly -= delta;
        if delta.abs() < KEPLER_TOLERANCE {
            break;
        }
    }
    ecc_anomaly
}

/// Heliocentric ecliptic position at `jd`, au.
pub fn heliocentric_position(elements: &OrbitalElements, jd: f64) -> [f64; 3] {
    let n = GAUSS_K / elements.a.powf(1.5);
    let m = elements.mean_anomaly.to_radians() + n * (jd - elements.epoch_jd);
    let ecc_anomaly = solve_kepler(m, elements.e);

    // Position in the orbital plane.
    let x_orb = elements.a * (ecc_anomaly.cos() - elements.e);
    let y_orb = elements.a * (1.0 - elements.e * elements.e).sqrt() * ecc_anomaly.sin();

    let (sin_peri, cos_peri) = elements.peri.to_radians().sin_cos();
    let (sin_node, cos_node) = elements.node.to_radians().sin_cos();
    let (sin_incl, cos_incl) = elements.incl.to_radians().sin_cos();

    // Rotate: argument of perihelion, inclination, longitude of node.
    let x_peri = x_orb * cos_peri - y_orb * sin_peri;
    let y_peri = x_orb * sin_peri + y_orb * cos_peri;

    [
        x_peri * cos_node - y_peri * sin_node * cos_incl,
        x_peri * sin_node + y_peri * cos_node * cos_incl,
        y_peri * sin_incl,
    ]
}

/// Earth's heliocentric ecliptic position at `jd`, au.
///
/// Low-precision solar theory (Meeus ch. 25), good to ~1e-4 au. That is three
/// orders of magnitude finer than this module needs.
pub fn earth_position(jd: f64) -> [f64; 3] {
    let t = (jd - 2_451_545.0) / 36_525.0;

    let mean_long = (280.464_66 + 36_000.769_83 * t + 0.000_303_2 * t * t).to_radians();
    let mean_anomaly = (357.529_11 + 35_999.050_29 * t - 0.000_153_7 * t * t).to_radians();
    let e = 0.016_708_634 - 0.000_042_037 * t - 0.000_000_126_7 * t * t;

    // Equation of the centre.
    let centre = ((1.914_602 - 0.004_817 * t - 0.000_014 * t * t) * mean_anomaly.sin()
        + (0.019_993 - 0.000_101 * t) * (2.0 * mean_anomaly).sin()
        + 0.000_289 * (3.0 * mean_anomaly).sin())
    .to_radians();

    // Meeus gives longitude referred to the mean equinox of date; MPCORB elements
    // are J2000, so precess back (0.01397 deg/yr).
    let true_long = mean_long + centre - (1.397 * t).to_radians();
    let true_anomaly = mean_anomaly + centre;
    let sun_dist = 1.000_001_018 * (1.0 - e * e) / (1.0 + e * true_anomaly.cos());

    // Earth is opposite the Sun as seen from the barycentre; ecliptic latitude
    // is negligible at this precision.
    [
        -sun_dist * true_long.cos(),
        -sun_dist * true_long.sin(),
        0.0,
    ]
}

/// Observing geometry for `elements` at `jd`.
pub fn geometry_at(elements: &OrbitalElements, jd: f64) -> Geometry {
    let helio = heliocentric_position(elements, jd);
    let earth = earth_position(jd);
    let topo = [
        helio[0] - earth[0],
        helio[1] - earth[1],
        helio[2] - earth[2],
    ];

    let helio_dist = norm(&helio);
    let topo_dist = norm(&topo);

    // Phase angle is the Sun-object-observer angle; the vectors from the object
    // to each are the negatives of these, so the dot product is unchanged.
    let cos_phase = (dot(&helio, &topo) / (helio_dist * topo_dist)).clamp(-1.0, 1.0);

    Geometry {
        helio_dist,
        topo_dist,
        phase_angle: cos_phase.acos().to_degrees(),
    }
}

fn dot(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn norm(v: &[f64; 3]) -> f64 {
    dot(v, v).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Roughly main-belt, low eccentricity and inclination.
    fn main_belt() -> OrbitalElements {
        OrbitalElements {
            epoch_jd: 2_461_000.5,
            a: 3.0,
            e: 0.05,
            incl: 5.0,
            node: 120.0,
            peri: 45.0,
            mean_anomaly: 10.0,
        }
    }

    #[test]
    fn test_kepler_solution_satisfies_its_own_equation() {
        for &e in &[0.0, 0.05, 0.3, 0.7, 0.95] {
            for step in 0..12 {
                let m = step as f64 * PI / 6.0;
                let ecc = solve_kepler(m, e);
                let residual = ecc - e * ecc.sin() - m.rem_euclid(2.0 * PI);
                assert!(residual.abs() < 1e-9, "e={e} M={m} residual={residual}");
            }
        }
    }

    #[test]
    fn test_circular_orbit_stays_at_its_semimajor_axis() {
        let circular = OrbitalElements {
            e: 0.0,
            ..main_belt()
        };
        for days in [0.0, 100.0, 1000.0, 5000.0] {
            let r = norm(&heliocentric_position(&circular, circular.epoch_jd + days));
            assert!((r - circular.a).abs() < 1e-9, "days={days} r={r}");
        }
    }

    #[test]
    fn test_eccentric_orbit_stays_between_perihelion_and_aphelion() {
        let el = main_belt();
        let (q, big_q) = (el.a * (1.0 - el.e), el.a * (1.0 + el.e));
        for days in 0..400 {
            let r = norm(&heliocentric_position(&el, el.epoch_jd + days as f64 * 5.0));
            assert!(
                r >= q - 1e-9 && r <= big_q + 1e-9,
                "r={r} not in [{q},{big_q}]"
            );
        }
    }

    #[test]
    fn test_earth_distance_stays_within_its_annual_range() {
        for days in 0..370 {
            let r = norm(&earth_position(2_461_000.5 + days as f64));
            assert!((0.982..=1.018).contains(&r), "day {days}: r={r}");
        }
    }

    // A full orbit returns to the same place.
    #[test]
    fn test_position_is_periodic_over_one_orbit() {
        let el = main_belt();
        let period_days = 2.0 * PI / (GAUSS_K / el.a.powf(1.5));
        let start = heliocentric_position(&el, el.epoch_jd);
        let after = heliocentric_position(&el, el.epoch_jd + period_days);
        for k in 0..3 {
            assert!((start[k] - after[k]).abs() < 1e-8, "axis {k}");
        }
    }

    #[test]
    fn test_geometry_is_physically_bounded() {
        let el = main_belt();
        for days in 0..365 {
            let g = geometry_at(&el, el.epoch_jd + days as f64);
            assert!(g.phase_angle >= 0.0 && g.phase_angle <= 180.0);
            // The observer is inside the object's orbit, so the object is never
            // closer than helio_dist minus Earth's distance, nor further than the sum.
            assert!(g.topo_dist > g.helio_dist - 1.02);
            assert!(g.topo_dist < g.helio_dist + 1.02);
        }
    }

    // For an outer object, phase angle is bounded by the geometry: sin(alpha_max)
    // = 1 au / helio_dist. A 3 au object cannot exceed ~19.5 deg.
    #[test]
    fn test_phase_angle_respects_the_outer_object_limit() {
        let el = main_belt();
        let limit = (1.017_f64 / (el.a * (1.0 - el.e))).asin().to_degrees();
        let mut seen_max: f64 = 0.0;
        for days in 0..2000 {
            let g = geometry_at(&el, el.epoch_jd + days as f64 * 2.0);
            seen_max = seen_max.max(g.phase_angle);
            assert!(
                g.phase_angle <= limit + 0.5,
                "phase {} > {}",
                g.phase_angle,
                limit
            );
        }
        assert!(
            seen_max > 5.0,
            "should sample a range of phase angles, saw {seen_max}"
        );
    }

    // At opposition the object is closest and the phase angle is smallest; both
    // extremes should coincide.
    #[test]
    fn test_opposition_minimises_distance_and_phase_together() {
        let el = main_belt();
        let mut best = (f64::MAX, f64::MAX, 0.0_f64);
        for days in 0..800 {
            let jd = el.epoch_jd + days as f64;
            let g = geometry_at(&el, jd);
            if g.topo_dist < best.0 {
                best = (g.topo_dist, g.phase_angle, jd);
            }
        }
        assert!(best.1 < 3.0, "phase at closest approach was {} deg", best.1);
    }
}

#[cfg(test)]
mod horizons_validation {
    use super::*;

    /// Elements as MPCORB distributes them at epoch K2669 (2026 June 9), checked
    /// against JPL Horizons at JD 2461272.5 (2026 Aug 20, geocentric).
    struct Case {
        name: &'static str,
        elements: OrbitalElements,
        helio_dist: f64,
        topo_dist: f64,
        phase_angle: f64,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "1 Ceres",
                elements: OrbitalElements {
                    epoch_jd: 2_461_200.5,
                    a: 2.7655526,
                    e: 0.0796923,
                    incl: 10.58803,
                    node: 80.24863,
                    peri: 73.29420,
                    mean_anomaly: 274.41935,
                },
                helio_dist: 2.706853365104,
                topo_dist: 3.16890538454643,
                phase_angle: 17.6824,
            },
            // Much higher inclination and eccentricity, so a frame or rotation
            // error that survived Ceres would show here.
            Case {
                name: "2 Pallas",
                elements: OrbitalElements {
                    epoch_jd: 2_461_200.5,
                    a: 2.7695590,
                    e: 0.2307001,
                    incl: 34.93279,
                    node: 172.88661,
                    peri: 310.96993,
                    mean_anomaly: 254.24963,
                },
                helio_dist: 2.915730582216,
                topo_dist: 2.22979772666357,
                phase_angle: 16.7833,
            },
        ]
    }

    /// Near-Earth asteroids, evaluated far from their elements' epoch.
    ///
    /// `cases()` above checks two main-belt objects 72 days out, which is the
    /// easy regime: near-circular orbits, weak perturbations, short lever arm.
    /// Two things could break outside it, and both matter in production, since
    /// MPCORB epochs move on roughly a six-month cadence while alerts arrive
    /// nightly. First, two-body propagation accumulates error with time from
    /// epoch. Second, NEOs are the objects planetary perturbations act on most.
    ///
    /// Reference values are JPL Horizons (geocentric, quantities 19/20/43);
    /// elements are the MPCORB records for the same epoch.
    ///
    /// Note what this does *not* cover: none of these windows contains a close
    /// planetary encounter, which is where a two-body model genuinely fails.
    /// Apophis in 2029 would need real perturbations.
    fn far_from_epoch_cases() -> Vec<(Case, f64)> {
        vec![
            (
                Case {
                    name: "433 Eros, 200 days before epoch",
                    elements: OrbitalElements {
                        epoch_jd: 2_461_200.5,
                        a: 1.4582437,
                        e: 0.2228780,
                        incl: 10.82855,
                        node: 304.26797,
                        peri: 178.91814,
                        mean_anomaly: 62.51145,
                    },
                    helio_dist: 1.298448154570,
                    topo_dist: 0.400169669522,
                    phase_angle: 33.1581,
                },
                2_461_000.5,
            ),
            (
                Case {
                    name: "433 Eros, 300 days after epoch",
                    elements: OrbitalElements {
                        epoch_jd: 2_461_200.5,
                        a: 1.4582437,
                        e: 0.2228780,
                        incl: 10.82855,
                        node: 304.26797,
                        peri: 178.91814,
                        mean_anomaly: 62.51145,
                    },
                    helio_dist: 1.700293597970,
                    topo_dist: 2.561134066572,
                    phase_angle: 14.0222,
                },
                2_461_500.5,
            ),
            (
                Case {
                    name: "99942 Apophis, 200 days before epoch",
                    elements: OrbitalElements {
                        epoch_jd: 2_461_200.5,
                        a: 0.9223592,
                        e: 0.1911492,
                        incl: 3.34100,
                        node: 203.89365,
                        peri: 126.67957,
                        mean_anomaly: 175.33040,
                    },
                    helio_dist: 0.824595526480,
                    topo_dist: 1.766171027568,
                    phase_angle: 14.2409,
                },
                2_461_000.5,
            ),
            (
                Case {
                    name: "99942 Apophis, 300 days after epoch",
                    elements: OrbitalElements {
                        epoch_jd: 2_461_200.5,
                        a: 0.9223592,
                        e: 0.1911492,
                        incl: 3.34100,
                        node: 203.89365,
                        peri: 126.67957,
                        mean_anomaly: 175.33040,
                    },
                    helio_dist: 1.080704644109,
                    topo_dist: 1.129927205505,
                    phase_angle: 53.7417,
                },
                2_461_500.5,
            ),
        ]
    }

    #[test]
    fn test_matches_horizons_far_from_epoch() {
        for (c, jd) in far_from_epoch_cases() {
            let g = geometry_at(&c.elements, jd);
            let d_helio = (g.helio_dist - c.helio_dist).abs();
            let d_topo = (g.topo_dist - c.topo_dist).abs();
            let d_phase = (g.phase_angle - c.phase_angle).abs();

            assert!(d_helio < 1e-3, "{}: helio off by {d_helio} au", c.name);
            assert!(d_topo < 1e-3, "{}: topo off by {d_topo} au", c.name);
            assert!(d_phase < 0.02, "{}: phase off by {d_phase} deg", c.name);
        }
    }

    #[test]
    fn test_matches_horizons() {
        for c in cases() {
            let g = geometry_at(&c.elements, 2_461_272.5);
            let d_helio = (g.helio_dist - c.helio_dist).abs();
            let d_topo = (g.topo_dist - c.topo_dist).abs();
            let d_phase = (g.phase_angle - c.phase_angle).abs();

            assert!(d_helio < 1e-4, "{}: helio off by {d_helio} au", c.name);
            assert!(d_topo < 1e-3, "{}: topo off by {d_topo} au", c.name);
            assert!(d_phase < 0.01, "{}: phase off by {d_phase} deg", c.name);
        }
    }

    // The tolerances above are what the science needs, expressed in magnitudes:
    // a 1e-3 au error at ~3 au is under a millimag in the distance scaling.
    #[test]
    fn test_distance_error_is_negligible_in_magnitudes() {
        for c in cases() {
            let g = geometry_at(&c.elements, 2_461_272.5);
            let ours = 5.0 * (g.helio_dist * g.topo_dist).log10();
            let truth = 5.0 * (c.helio_dist * c.topo_dist).log10();
            assert!(
                (ours - truth).abs() < 0.001,
                "{}: {} mag error in the distance term",
                c.name,
                (ours - truth).abs()
            );
        }
    }
}
