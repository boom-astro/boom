//! Observing geometry for solar system objects: heliocentric distance,
//! topocentric distance and solar phase angle.
//!
//! Two-body propagation is deliberate. Geometry is far less sensitive to orbit
//! error than sky position is: an error large enough to move a predicted
//! position by 8" shifts these quantities by ~1e-4 au and ~0.003 deg. A
//! perturbed integrator would be needed to predict *where* an object is, not to
//! say how far away it was.

use crate::utils::orbit_fit::C_AU_PER_DAY;
use std::f64::consts::PI;

/// Gaussian gravitational constant, rad/day.
const GAUSS_K: f64 = 0.017_202_098_95;
/// Obliquity is not applied: everything here stays in the ecliptic frame.
const KEPLER_TOLERANCE: f64 = 1e-12;
/// Where both the elliptical and hyperbolic solutions go numerically unusable.
pub(crate) const PARABOLIC_TOLERANCE: f64 = 1e-8;
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
    /// Mean anomaly at `epoch_jd`, degrees. Meaningless once `e >= 1`.
    pub mean_anomaly: f64,
    /// Perihelion distance, au. Authoritative: build it, do not field-update it.
    pub q: f64,
    /// Time of perihelion passage, JD.
    pub tp: f64,
}

impl OrbitalElements {
    /// Elliptical elements as MPCORB gives them, with `q` and `tp` derived.
    pub fn elliptical(
        epoch_jd: f64,
        a: f64,
        e: f64,
        incl: f64,
        node: f64,
        peri: f64,
        mean_anomaly: f64,
    ) -> Self {
        Self {
            epoch_jd,
            a,
            e,
            incl,
            node,
            peri,
            mean_anomaly,
            q: 0.0,
            tp: 0.0,
        }
        .with_perihelion()
    }

    /// Fill `q` and `tp` from an elliptical element set that lacks them.
    pub fn with_perihelion(mut self) -> Self {
        self.q = self.a * (1.0 - self.e);
        let n = GAUSS_K / self.a.powf(1.5);
        self.tp = self.epoch_jd - self.mean_anomaly.to_radians() / n;
        self
    }
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
    /// Degrees from perihelion in (-180, 180], negative inbound.
    pub true_anomaly: f64,
    /// Perihelion passage, JD. Per detection because a refreshed orbit moves it.
    pub perihelion_time: f64,
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

/// True anomaly at `jd`, radians, by Kepler, Barker or the hyperbolic analogue.
pub fn true_anomaly(elements: &OrbitalElements, jd: f64) -> f64 {
    let e = elements.e;
    let dt = jd - elements.tp;

    if (e - 1.0).abs() < PARABOLIC_TOLERANCE {
        // Barker's equation, solved by Cardano rather than iteration.
        let n = GAUSS_K * (2.0 / (elements.q.powi(3))).sqrt() / 2.0;
        let b = 3.0 * n * dt / 2.0;
        let w = (b + (b * b + 1.0).sqrt()).cbrt();
        return 2.0 * (w - 1.0 / w).atan();
    }

    if e < 1.0 {
        let a = elements.q / (1.0 - e);
        let m = GAUSS_K / a.powf(1.5) * dt;
        let ecc = solve_kepler(m, e);
        let half = ((1.0 + e) / (1.0 - e)).sqrt() * (ecc / 2.0).tan();
        return 2.0 * half.atan();
    }

    let a = elements.q / (e - 1.0);
    let m = GAUSS_K / a.powf(1.5) * dt;
    let h = solve_hyperbolic_kepler(m, e);
    let half = ((e + 1.0) / (e - 1.0)).sqrt() * (h / 2.0).tanh();
    2.0 * half.atan()
}

/// Solve `M = e sinh H - H` for the hyperbolic anomaly, radians.
fn solve_hyperbolic_kepler(m: f64, e: f64) -> f64 {
    let mut h = if m.abs() > 6.0 {
        m.signum() * (2.0 * m.abs() / e).ln()
    } else {
        m / (e - 1.0)
    };
    for _ in 0..KEPLER_MAX_ITER {
        let delta = (e * h.sinh() - h - m) / (e * h.cosh() - 1.0);
        h -= delta;
        if delta.abs() < KEPLER_TOLERANCE {
            break;
        }
    }
    h
}

/// Heliocentric ecliptic position at `jd`, au.
pub fn heliocentric_position(elements: &OrbitalElements, jd: f64) -> [f64; 3] {
    // Conic equation from the true anomaly, so one path serves every orbit type.
    let nu = true_anomaly(elements, jd);
    let r = elements.q * (1.0 + elements.e) / (1.0 + elements.e * nu.cos());
    let x_orb = r * nu.cos();
    let y_orb = r * nu.sin();

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

/// Obliquity of the ecliptic at J2000, degrees.
const OBLIQUITY_DEG: f64 = 23.439_281;
/// Constant since the leap second of 2017 January 1.
pub const TT_MINUS_UTC_DAYS: f64 = 69.184 / 86_400.0;
/// Earth's equatorial radius, au. The unit the MPC tabulates sites in.
const EARTH_RADIUS_AU: f64 = 4.263_521e-5;

/// An observing site, in the parallax constants the MPC publishes for it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Site {
    /// East longitude, degrees.
    pub longitude_deg: f64,
    /// Distance from the Earth's rotation axis, Earth radii.
    pub rho_cos_phi: f64,
    /// Distance from the equatorial plane, Earth radii. Negative in the south.
    pub rho_sin_phi: f64,
    pub astrometric_sigma_arcsec: f64,
    pub mpc_code: &'static str,
}

/// ZTF at Palomar, MPC code I41.
pub const ZTF: Site = Site {
    longitude_deg: 243.140_22,
    rho_cos_phi: 0.836_325,
    rho_sin_phi: 0.546_877,
    astrometric_sigma_arcsec: 0.25,
    mpc_code: "I41",
};

/// The Simonyi Survey Telescope at Rubin Observatory, MPC code X05.
pub const RUBIN: Site = Site {
    longitude_deg: 289.250_58,
    rho_cos_phi: 0.864_981,
    rho_sin_phi: -0.500_958,
    astrometric_sigma_arcsec: 0.09,
    mpc_code: "X05",
};

/// Catalina Sky Survey, the 0.7 m Schmidt on Mt Bigelow, MPC code 703.
pub const CATALINA: Site = Site {
    longitude_deg: 249.267_36,
    rho_cos_phi: 0.845_311,
    rho_sin_phi: 0.533_211,
    astrometric_sigma_arcsec: 1.97,
    mpc_code: "703",
};

/// Mt Lemmon Survey, the 1.5 m, MPC code G96. CSS's deepest survey telescope.
pub const MT_LEMMON: Site = Site {
    longitude_deg: 249.211_28,
    rho_cos_phi: 0.845_107,
    rho_sin_phi: 0.533_611,
    astrometric_sigma_arcsec: 0.48,
    mpc_code: "G96",
};

/// Steward Observatory, Mt Lemmon Station, MPC code I52. CSS follow-up.
pub const MT_LEMMON_STATION: Site = Site {
    longitude_deg: 249.211_08,
    rho_cos_phi: 0.845_109,
    rho_sin_phi: 0.533_609,
    astrometric_sigma_arcsec: 0.48,
    mpc_code: "I52",
};

/// Kitt Peak-Bok, the 2.3 m with 90Prime, MPC code V00.
pub const BOK: Site = Site {
    longitude_deg: 248.399_81,
    rho_cos_phi: 0.849_456,
    rho_sin_phi: 0.526_492,
    astrometric_sigma_arcsec: 0.24,
    mpc_code: "V00",
};

/// Catalina Sky Survey-Kuiper, the 1.5 m on Mt Bigelow, MPC code V06.
pub const KUIPER: Site = Site {
    longitude_deg: 249.267_45,
    rho_cos_phi: 0.845_313,
    rho_sin_phi: 0.533_209,
    astrometric_sigma_arcsec: 0.48,
    mpc_code: "V06",
};

/// Siding Spring Survey, MPC code E12. Southern, and the only CSS site there,
/// so its parallax differs from the Arizona ones in sign as well as size.
pub const SIDING_SPRING: Site = Site {
    longitude_deg: 149.064_2,
    rho_cos_phi: 0.855_63,
    rho_sin_phi: -0.516_21,
    astrometric_sigma_arcsec: 1.36,
    mpc_code: "E12",
};

pub const DECAM: Site = Site {
    longitude_deg: 289.193_58,
    rho_cos_phi: 0.865_572,
    rho_sin_phi: -0.499_793,
    astrometric_sigma_arcsec: 0.15,
    mpc_code: "W84",
};

pub const WINTER: Site = Site {
    longitude_deg: 243.137_46,
    rho_cos_phi: 0.836_357,
    rho_sin_phi: 0.546_831,
    astrometric_sigma_arcsec: 0.5,
    mpc_code: "675",
};

impl Site {
    /// The site an MPC observatory code names, for astrometry that says where it
    /// was taken rather than being assumed to come from one telescope.
    pub fn from_mpc_code(code: &str) -> Option<Self> {
        Some(match code {
            "I41" => ZTF,
            "X05" => RUBIN,
            "703" => CATALINA,
            "G96" => MT_LEMMON,
            "I52" => MT_LEMMON_STATION,
            "V00" => BOK,
            "V06" => KUIPER,
            "E12" => SIDING_SPRING,
            "W84" => DECAM,
            "675" => WINTER,
            _ => return None,
        })
    }
}

/// Heliocentric ecliptic position of an observing site at `jd` (UTC), au.
///
/// An observation is made from a point on a spinning Earth, not from its
/// centre. The offset is only an Earth radius, but against a main-belt asteroid
/// a couple of au away that is still several arcseconds -- comparable to the
/// error everything else in this module is trying to avoid.
pub fn observer_position(jd: f64, site: &Site) -> [f64; 3] {
    // Greenwich mean sidereal time, degrees. Good to well under a second of
    // time over the decades a survey archive spans.
    let d = jd - 2_451_545.0;
    let gmst = (280.460_618_37 + 360.985_647_366_29 * d).rem_euclid(360.0);
    let lst = (gmst + site.longitude_deg).to_radians();

    // Geocentric, equatorial, in au.
    let (x, y, z) = (
        site.rho_cos_phi * lst.cos() * EARTH_RADIUS_AU,
        site.rho_cos_phi * lst.sin() * EARTH_RADIUS_AU,
        site.rho_sin_phi * EARTH_RADIUS_AU,
    );
    // Equatorial to ecliptic, then onto the Earth's own position.
    let (s, c) = OBLIQUITY_DEG.to_radians().sin_cos();
    let earth = earth_position_utc(jd);
    [
        earth[0] + x,
        earth[1] + c * y + s * z,
        earth[2] - s * y + c * z,
    ]
}

/// Earth's heliocentric ecliptic position at `jd` (TT), au.
///
/// VSOP87A, within 4e-7 au of DE441 over 1990-2040.
pub fn earth_position(jd: f64) -> [f64; 3] {
    let eq = crate::utils::vsop87::earth_equatorial(jd);
    let (s, c) = OBLIQUITY_DEG.to_radians().sin_cos();
    [eq[0], c * eq[1] + s * eq[2], -s * eq[1] + c * eq[2]]
}

/// JPL's approximate elements for 1800-2050.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Planet {
    pub name: &'static str,
    elements: [f64; 6],
    rates: [f64; 6],
}

pub const JUPITER: Planet = Planet {
    name: "jupiter",
    elements: [
        5.202_887,
        0.048_386_24,
        1.304_396_95,
        34.396_440_51,
        14.728_479_83,
        100.473_909_09,
    ],
    rates: [
        -0.000_116_07,
        -0.000_132_53,
        -0.001_837_14,
        3_034.746_127_75,
        0.212_526_68,
        0.204_691_06,
    ],
};

pub const SATURN: Planet = Planet {
    name: "saturn",
    elements: [
        9.536_675_94,
        0.053_861_79,
        2.485_991_87,
        49.954_244_23,
        92.598_878_31,
        113.662_424_48,
    ],
    rates: [
        -0.001_250_6,
        -0.000_509_91,
        0.001_936_09,
        1_222.493_622_01,
        -0.418_972_16,
        -0.288_677_94,
    ],
};

pub const URANUS: Planet = Planet {
    name: "uranus",
    elements: [
        19.189_164_64,
        0.047_257_44,
        0.772_637_83,
        313.238_104_51,
        170.954_276_3,
        74.016_925_03,
    ],
    rates: [
        -0.001_961_76,
        -0.000_043_97,
        -0.002_429_39,
        428.482_027_85,
        0.408_052_81,
        0.042_405_89,
    ],
};

pub const NEPTUNE: Planet = Planet {
    name: "neptune",
    elements: [
        30.069_922_76,
        0.008_590_48,
        1.770_043_47,
        -55.120_029_69,
        44.964_762_27,
        131.784_225_74,
    ],
    rates: [
        0.000_262_91,
        0.000_051_05,
        0.000_353_72,
        218.459_453_25,
        -0.322_414_64,
        -0.005_086_64,
    ],
};

impl Planet {
    pub fn elements_at(&self, jd: f64) -> OrbitalElements {
        let t = (jd - 2_451_545.0) / 36_525.0;
        let [a, e, incl, long, peri_long, node]: [f64; 6] =
            std::array::from_fn(|k| self.elements[k] + self.rates[k] * t);
        OrbitalElements::elliptical(jd, a, e, incl, node, peri_long - node, long - peri_long)
    }
}

pub fn earth_position_utc(jd: f64) -> [f64; 3] {
    earth_position(jd + TT_MINUS_UTC_DAYS)
}

/// Observing geometry for `elements` at `jd` (UTC).
pub fn geometry_at(elements: &OrbitalElements, jd: f64, site: &Site) -> Geometry {
    let observer = observer_position(jd, site);
    let from_observer = |p: [f64; 3]| [p[0] - observer[0], p[1] - observer[1], p[2] - observer[2]];
    let tt = jd + TT_MINUS_UTC_DAYS;
    let emitted = tt - norm(&from_observer(heliocentric_position(elements, tt))) / C_AU_PER_DAY;
    let helio = heliocentric_position(elements, emitted);
    let topo = from_observer(helio);

    let helio_dist = norm(&helio);
    let topo_dist = norm(&topo);

    // Phase angle is the Sun-object-observer angle; the vectors from the object
    // to each are the negatives of these, so the dot product is unchanged.
    let cos_phase = (dot(&helio, &topo) / (helio_dist * topo_dist)).clamp(-1.0, 1.0);

    Geometry {
        helio_dist,
        topo_dist,
        true_anomaly: {
            let nu = true_anomaly(elements, emitted)
                .to_degrees()
                .rem_euclid(360.0);
            if nu > 180.0 {
                nu - 360.0
            } else {
                nu
            }
        },
        perihelion_time: elements.tp,
        phase_angle: cos_phase.acos().to_degrees(),
    }
}

pub(crate) fn dot(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

pub(crate) fn norm(v: &[f64; 3]) -> f64 {
    dot(v, v).sqrt()
}

#[cfg(test)]
mod tests_observer {
    use super::*;

    /// The offset is an Earth radius, and it points somewhere.
    #[test]
    fn site_sits_an_earth_radius_from_the_centre() {
        let jd = 2461286.8;
        let earth = earth_position_utc(jd);
        let obs = observer_position(jd, &ZTF);
        let d = ((obs[0] - earth[0]).powi(2)
            + (obs[1] - earth[1]).powi(2)
            + (obs[2] - earth[2]).powi(2))
        .sqrt();
        // Palomar is at 33 degrees north, so slightly inside the equatorial radius.
        assert!(
            (0.9 * EARTH_RADIUS_AU..=EARTH_RADIUS_AU).contains(&d),
            "offset {d} au against an Earth radius of {EARTH_RADIUS_AU}"
        );
    }

    /// Half a day apart the Earth has turned, so the site has moved.
    #[test]
    fn the_site_turns_with_the_earth() {
        let jd = 2461286.8;
        let a = observer_position(jd, &ZTF);
        let b = observer_position(jd + 0.5, &ZTF);
        let earth_a = earth_position_utc(jd);
        let earth_b = earth_position_utc(jd + 0.5);
        // Offsets relative to the Earth, so the orbit does not mask the spin.
        let oa = [a[0] - earth_a[0], a[1] - earth_a[1], a[2] - earth_a[2]];
        let ob = [b[0] - earth_b[0], b[1] - earth_b[1], b[2] - earth_b[2]];
        let moved =
            ((oa[0] - ob[0]).powi(2) + (oa[1] - ob[1]).powi(2) + (oa[2] - ob[2]).powi(2)).sqrt();
        assert!(
            moved > EARTH_RADIUS_AU,
            "site moved only {moved} au in half a day"
        );
    }

    /// Southern sites sit below the equatorial plane, northern ones above.
    #[test]
    fn hemispheres_are_the_right_way_up() {
        assert!(ZTF.rho_sin_phi > 0.0, "Palomar should be north");
        assert!(RUBIN.rho_sin_phi < 0.0, "Rubin should be south");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Perihelion distance is reached at the perihelion time, for every conic.
    #[test]
    fn test_each_conic_reaches_perihelion_at_tp() {
        let tp = 2_461_000.5;
        for e in [0.0, 0.5, 0.95, 1.0, 1.5, 3.0] {
            let el = OrbitalElements {
                epoch_jd: tp,
                a: if e < 1.0 { 2.0 / (1.0 - e) } else { 0.0 },
                e,
                incl: 10.0,
                node: 30.0,
                peri: 60.0,
                mean_anomaly: 0.0,
                q: 2.0,
                tp,
            };
            let r = norm(&heliocentric_position(&el, tp));
            assert!((r - 2.0).abs() < 1e-6, "e={e} gave r={r}");
            assert!(
                true_anomaly(&el, tp).abs() < 1e-6,
                "e={e} is not at perihelion"
            );
        }
    }

    /// A hyperbolic orbit only ever recedes, and never comes back.
    #[test]
    fn test_a_hyperbolic_orbit_recedes_forever() {
        let tp = 2_461_000.5;
        let el = OrbitalElements {
            epoch_jd: tp,
            a: 0.0,
            e: 2.5,
            incl: 20.0,
            node: 45.0,
            peri: 90.0,
            mean_anomaly: 0.0,
            q: 1.5,
            tp,
        };
        let mut previous = 0.0;
        for days in [0.0, 100.0, 500.0, 2000.0] {
            let r = norm(&heliocentric_position(&el, tp + days));
            assert!(r >= previous, "r went backwards at {days} days");
            previous = r;
        }
        assert!(previous > 10.0, "still at {previous} au after 2000 days");
    }

    /// Equal distance either side of perihelion; a wrong time sign breaks it.
    #[test]
    fn test_distance_is_symmetric_about_perihelion() {
        let tp = 2_461_000.5;
        for e in [0.7, 1.0, 1.4] {
            let el = OrbitalElements {
                epoch_jd: tp,
                a: if e < 1.0 { 1.2 / (1.0 - e) } else { 0.0 },
                e,
                incl: 5.0,
                node: 0.0,
                peri: 0.0,
                mean_anomaly: 0.0,
                q: 1.2,
                tp,
            };
            for days in [30.0, 200.0] {
                let before = norm(&heliocentric_position(&el, tp - days));
                let after = norm(&heliocentric_position(&el, tp + days));
                assert!(
                    (before - after).abs() < 1e-6,
                    "e={e} at {days} days: {before} vs {after}"
                );
            }
        }
    }

    /// a and M must agree with q and tp, since only the latter propagates.
    #[test]
    fn test_perihelion_derivation_round_trips() {
        let b = main_belt();
        assert!((b.q - b.a * (1.0 - b.e)).abs() < 1e-12);

        // Half an orbit past tp the object sits at aphelion.
        let period = 2.0 * PI / (GAUSS_K / b.a.powf(1.5));
        let r = norm(&heliocentric_position(&b, b.tp + period / 2.0));
        assert!(
            (r - b.a * (1.0 + b.e)).abs() < 1e-6,
            "aphelion was {r}, expected {}",
            b.a * (1.0 + b.e)
        );
    }

    /// Roughly main-belt, low eccentricity and inclination.
    fn main_belt() -> OrbitalElements {
        OrbitalElements::elliptical(2_461_000.5, 3.0, 0.05, 5.0, 120.0, 45.0, 10.0)
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
        let b = main_belt();
        // Built, not field-updated: overriding e alone would strand q and tp.
        let circular = OrbitalElements::elliptical(
            b.epoch_jd,
            b.a,
            0.0,
            b.incl,
            b.node,
            b.peri,
            b.mean_anomaly,
        );
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
            let g = geometry_at(&el, el.epoch_jd + days as f64, &ZTF);
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
            let g = geometry_at(&el, el.epoch_jd + days as f64 * 2.0, &ZTF);
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
            let g = geometry_at(&el, jd, &ZTF);
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
    /// against JPL Horizons at JD 2461272.5 UT (2026 Aug 20, geocentric).
    struct Case {
        name: &'static str,
        elements: OrbitalElements,
        helio_dist: f64,
        topo_dist: f64,
        phase_angle: f64,
        /// Astrometric RA/Dec, degrees: light-time corrected, no aberration,
        /// which is the convention MPC astrometry is reported in.
        ra: f64,
        dec: f64,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "1 Ceres",
                elements: OrbitalElements::elliptical(
                    2_461_200.5,
                    2.7655526,
                    0.0796923,
                    10.58803,
                    80.24863,
                    73.29420,
                    274.41935,
                ),
                helio_dist: 2.706853365104,
                topo_dist: 3.16890538454643,
                phase_angle: 17.6824,
                ra: 92.50594,
                dec: 22.52311,
            },
            // Much higher inclination and eccentricity, so a frame or rotation
            // error that survived Ceres would show here.
            Case {
                name: "2 Pallas",
                elements: OrbitalElements::elliptical(
                    2_461_200.5,
                    2.7695590,
                    0.2307001,
                    34.93279,
                    172.88661,
                    310.96993,
                    254.24963,
                ),
                helio_dist: 2.915730582216,
                topo_dist: 2.22979772666357,
                phase_angle: 16.7833,
                ra: 24.66921,
                dec: -1.81445,
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
    /// Reference values are JPL Horizons (geocentric, UT, quantities 19/20/43);
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
                    elements: OrbitalElements::elliptical(
                        2_461_200.5,
                        1.4582437,
                        0.2228780,
                        10.82855,
                        304.26797,
                        178.91814,
                        62.51145,
                    ),
                    helio_dist: 1.298448154570,
                    topo_dist: 0.400169669522,
                    phase_angle: 33.1581,
                    ra: 8.81256,
                    dec: 43.28246,
                },
                2_461_000.5,
            ),
            (
                Case {
                    name: "433 Eros, 300 days after epoch",
                    elements: OrbitalElements::elliptical(
                        2_461_200.5,
                        1.4582437,
                        0.2228780,
                        10.82855,
                        304.26797,
                        178.91814,
                        62.51145,
                    ),
                    helio_dist: 1.700293597970,
                    topo_dist: 2.561134066572,
                    phase_angle: 14.0222,
                    ra: 349.82626,
                    dec: -0.16259,
                },
                2_461_500.5,
            ),
            (
                Case {
                    name: "99942 Apophis, 200 days before epoch",
                    elements: OrbitalElements::elliptical(
                        2_461_200.5,
                        0.9223592,
                        0.1911492,
                        3.34100,
                        203.89365,
                        126.67957,
                        175.33040,
                    ),
                    helio_dist: 0.824595526480,
                    topo_dist: 1.766171027568,
                    phase_angle: 14.2409,
                    ra: 248.93646,
                    dec: -20.65265,
                },
                2_461_000.5,
            ),
            (
                Case {
                    name: "99942 Apophis, 300 days after epoch",
                    elements: OrbitalElements::elliptical(
                        2_461_200.5,
                        0.9223592,
                        0.1911492,
                        3.34100,
                        203.89365,
                        126.67957,
                        175.33040,
                    ),
                    helio_dist: 1.080704644109,
                    topo_dist: 1.129927205505,
                    phase_angle: 53.7417,
                    ra: 74.26769,
                    dec: 19.54714,
                },
                2_461_500.5,
            ),
        ]
    }

    #[test]
    fn test_matches_horizons_far_from_epoch() {
        for (c, jd) in far_from_epoch_cases() {
            let g = geometry_at(&c.elements, jd, &ZTF);
            let d_helio = (g.helio_dist - c.helio_dist).abs();
            let d_topo = (g.topo_dist - c.topo_dist).abs();
            let d_phase = (g.phase_angle - c.phase_angle).abs();

            assert!(d_helio < 1e-3, "{}: helio off by {d_helio} au", c.name);
            assert!(d_topo < 3e-4, "{}: topo off by {d_topo} au", c.name);
            assert!(d_phase < 0.02, "{}: phase off by {d_phase} deg", c.name);
        }
    }

    #[test]
    fn test_matches_horizons() {
        for c in cases() {
            let g = geometry_at(&c.elements, 2_461_272.5, &ZTF);
            let d_helio = (g.helio_dist - c.helio_dist).abs();
            let d_topo = (g.topo_dist - c.topo_dist).abs();
            let d_phase = (g.phase_angle - c.phase_angle).abs();

            assert!(d_helio < 1e-4, "{}: helio off by {d_helio} au", c.name);
            assert!(d_topo < 1e-4, "{}: topo off by {d_topo} au", c.name);
            assert!(d_phase < 0.01, "{}: phase off by {d_phase} deg", c.name);
        }
    }

    /// Against JPL Horizons (geocenter, ICRF equator, TDB), 1990 to 2041.
    #[test]
    fn test_earth_matches_horizons() {
        let (s, c) = OBLIQUITY_DEG.to_radians().sin_cos();
        for (jd, eq) in [
            (
                2_447_892.5,
                [-0.178_287_922_704, 0.887_231_821_652, 0.384_685_522_567],
            ),
            (
                2_451_545.0,
                [-0.177_135_099_273, 0.887_428_522_326, 0.384_742_899_088],
            ),
            (
                2_455_362.5,
                [-0.112_008_465_385, -0.926_256_114_347, -0.401_554_039_367],
            ),
            (
                2_458_928.5,
                [-0.995_844_117_444, 0.006_876_306_663, 0.002_985_547_201],
            ),
            (
                2_461_272.5,
                [0.845_273_411_844, -0.510_433_911_434, -0.221_265_991_387],
            ),
            (
                2_464_632.5,
                [0.783_457_740_640, 0.559_496_990_829, 0.242_507_278_738],
            ),
            (
                2_466_154.5,
                [-0.164_118_011_456, 0.889_552_170_674, 0.385_572_051_864],
            ),
        ] {
            let truth = [eq[0], c * eq[1] + s * eq[2], -s * eq[1] + c * eq[2]];
            let earth = earth_position(jd);
            let off = norm(&[
                earth[0] - truth[0],
                earth[1] - truth[1],
                earth[2] - truth[2],
            ]);
            assert!(off < 5e-7, "JD {jd}: {off:.2e} au from Horizons");
        }
    }

    /// Separation from the Horizons astrometric position, arcseconds.
    fn sky_error_arcsec(c: &Case, jd: f64) -> f64 {
        let (ra, dec) = crate::utils::identify::predict_radec(&c.elements, jd);
        crate::utils::linking::angular_separation_deg(ra, dec, c.ra, c.dec) * 3600.0
    }

    /// Sky position, which the distance and phase-angle checks above leave
    /// untested: both are nearly insensitive to the light-time correction, so
    /// they passed while predicted positions were out by tens of arcseconds.
    ///
    /// 3 arcsec separates the two models rather than merely passing: dropping
    /// the correction puts Ceres at 10.3 and Pallas at 12.4.
    #[test]
    fn test_matches_horizons_on_the_sky() {
        for c in cases() {
            let sep = sky_error_arcsec(&c, 2_461_272.5);
            assert!(sep < 3.0, "{}: {sep:.1} arcsec from Horizons", c.name);
        }
    }

    /// Far from epoch the error is two-body propagation, not the sky model, so
    /// the tolerance is set by how long the lever arm is rather than by optics.
    #[test]
    fn test_matches_horizons_on_the_sky_far_from_epoch() {
        for (c, jd) in far_from_epoch_cases() {
            let sep = sky_error_arcsec(&c, jd);
            assert!(sep < 60.0, "{}: {sep:.1} arcsec from Horizons", c.name);
        }
    }

    // The tolerances above are what the science needs, expressed in magnitudes:
    // a 1e-3 au error at ~3 au is under a millimag in the distance scaling.
    #[test]
    fn test_distance_error_is_negligible_in_magnitudes() {
        for c in cases() {
            let g = geometry_at(&c.elements, 2_461_272.5, &ZTF);
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

    /// The parallax constants are a point on the ellipsoid, so the two must
    /// square to very nearly one. A mistyped digit in a hand-copied site fails
    /// here rather than becoming a quiet astrometric bias.
    #[test]
    fn test_every_site_lies_on_the_earth() {
        for (code, site) in [
            ("I41", ZTF),
            ("X05", RUBIN),
            ("703", CATALINA),
            ("G96", MT_LEMMON),
            ("I52", MT_LEMMON_STATION),
            ("V00", BOK),
            ("V06", KUIPER),
            ("E12", SIDING_SPRING),
        ] {
            let r2 = site.rho_cos_phi * site.rho_cos_phi + site.rho_sin_phi * site.rho_sin_phi;
            assert!(
                (0.993..=1.0).contains(&r2),
                "{code}: rho_cos_phi^2 + rho_sin_phi^2 = {r2:.6}, not on the ellipsoid"
            );
            assert!(
                (0.0..360.0).contains(&site.longitude_deg),
                "{code}: longitude {} is not east-of-Greenwich degrees",
                site.longitude_deg
            );
            assert_eq!(
                Site::from_mpc_code(code),
                Some(site),
                "{code} resolves wrong"
            );
        }
        assert_eq!(Site::from_mpc_code("ZZZ"), None);
        // The only southern site, so its sign is the one worth stating.
        assert!(SIDING_SPRING.rho_sin_phi < 0.0, "E12 is in Australia");
    }

    /// Astrometry from two telescopes cannot be fitted as though it came from
    /// one. Palomar to Arizona is most of the residual gate at inner-belt
    /// distances, and Palomar to Siding Spring exceeds it outright -- so a
    /// cross-survey fit has to carry the site each observation came from.
    #[test]
    fn test_sites_disagree_by_more_than_the_residual_gate() {
        use crate::utils::heliolinc::test_orbits;
        use crate::utils::orbit_fit::predict_radec;

        const GATE_ARCSEC: f64 = 2.0;
        let epoch = 2460948.72339281;
        let offset = |r: f64, site: Site| -> Option<f64> {
            let (state, _) = test_orbits(0.0, 30.0, epoch, &[r]).into_iter().next()?;
            let (ra0, dec0) = predict_radec(&state, epoch, epoch, &ZTF)?;
            let (ra1, dec1) = predict_radec(&state, epoch, epoch, &site)?;
            Some(
                (((ra1 - ra0 + 540.0).rem_euclid(360.0) - 180.0) * dec0.to_radians().cos())
                    .hypot(dec1 - dec0)
                    * 3600.0,
            )
        };

        // Siding Spring against Palomar is a whole-Earth baseline.
        let southern = offset(2.5, SIDING_SPRING).expect("a trial orbit at 2.5 au");
        assert!(
            southern > GATE_ARCSEC,
            "E12 vs I41 at 2.5 au is {southern:.2}\", which should exceed the {GATE_ARCSEC}\" gate"
        );

        // Arizona against Palomar is smaller but not negligible: at inner-belt
        // distances it already eats most of the budget a fit has to spend.
        let inner = offset(1.5, MT_LEMMON).expect("a trial orbit at 1.5 au");
        assert!(
            inner > 0.5 * GATE_ARCSEC,
            "G96 vs I41 at 1.5 au is {inner:.2}\", small enough to ignore?"
        );

        // Parallax grows as the object nears, so NEOs -- what CSS is for -- are
        // worse still than any of these.
        let outer = offset(3.5, MT_LEMMON).expect("a trial orbit at 3.5 au");
        assert!(
            inner > outer,
            "the offset should grow as the object nears: {inner:.2}\" at 1.5 au vs {outer:.2}\" at 3.5 au"
        );
    }

    #[test]
    fn test_giant_planets_match_astropy() {
        use crate::utils::identify::predict_radec;
        use crate::utils::linking::angular_separation_deg;

        let jd = 2461337.75;
        for (planet, ra, dec) in [
            (JUPITER, 145.498, 14.498),
            (SATURN, 9.729, 1.252),
            (URANUS, 62.63, 20.9),
            (NEPTUNE, 2.289, -0.553),
        ] {
            let (pra, pdec) = predict_radec(&planet.elements_at(jd), jd);
            let sep = angular_separation_deg(pra, pdec, ra, dec);
            assert!(sep < 0.25, "{} is {sep:.3} deg off", planet.name);
        }
    }
}
