//! Shape-aware host-galaxy association.
//!
//! Associates a transient with its host galaxy using the directional light
//! radius (DLR) and a Bayesian posterior, rather than a fixed circular
//! aperture. Given a galaxy's semi-major axis `a`, axis ratio `b/a` and
//! position angle, the DLR is the galaxy's radius *along the direction of the
//! transient*, and the dimensionless `d_DLR = separation / DLR` says how many
//! galaxy-radii out the transient sits.
//!
//! This matters because a circular aperture is simultaneously too small for
//! large nearby galaxies (M31 spans degrees) and too large for dwarfs, and it
//! ignores inclination entirely. Doing the geometry here, in typed Rust, also
//! keeps it out of the Mongo aggregation language, where a null comparison
//! silently evaluates true and disables the cut.
//!
//! Entry point for the alert path: [`associate_from_xmatches`], which takes a
//! survey's cross-match results and returns the document stored under
//! `aux.host_galaxy`.

use std::collections::HashMap;

use mongodb::bson::Document;

/// Compares two floats within a tolerance, defaulting to 1e-9 absolute.
///
/// Test-only helper so the module stays dependency-free.
#[cfg(test)]
macro_rules! assert_close {
    ($a:expr, $b:expr) => {
        assert_close!($a, $b, epsilon = 1e-9)
    };
    ($a:expr, $b:expr, epsilon = $eps:expr) => {{
        let (a, b, eps): (f64, f64, f64) = ($a, $b, $eps);
        assert!(
            (a - b).abs() <= eps,
            "{} !~= {} (|delta| = {} > {})",
            a,
            b,
            (a - b).abs(),
            eps
        );
    }};
}

mod associate;
mod association;
mod catalog;
mod config;
mod dlr;
mod ellipse;
mod error;
mod likelihood;
mod prior;
mod types;

pub use associate::{associate_host, AssociationConfig, AssociationResult};
pub use association::{HostGalaxyAssociation, StoredHostCandidate};
pub use catalog::{collect_galaxies, galaxy_from_ls_dr10, galaxy_from_ned_lvs, LS_DR10, NED_LVS};
pub use config::HostGalaxyConfig;
pub use dlr::{compute_dlr, DlrResult};
pub use ellipse::Ellipse;
pub use error::HostError;
pub use types::{GalaxyCandidate, HostCandidate, Transient};

/// Run host association for one alert from its cross-match results.
///
/// Returns `None` when association is disabled, so callers can leave the field
/// off the aux document entirely rather than storing a misleading empty result.
/// When it is enabled but nothing matched, an empty association is returned so
/// the stored shape stays consistent.
pub fn associate_from_xmatches(
    ra: f64,
    dec: f64,
    xmatches: &HashMap<String, Vec<Document>>,
    config: &HostGalaxyConfig,
) -> Option<HostGalaxyAssociation> {
    if !config.enabled {
        return None;
    }

    let galaxies = collect_galaxies(xmatches, config);
    if galaxies.is_empty() {
        return Some(HostGalaxyAssociation::empty());
    }

    let transient = Transient::new(ra, dec);
    match associate_host(&transient, &galaxies, &config.association_config()) {
        Ok(result) => Some(HostGalaxyAssociation::from_result(&result)),
        // The only error is "no candidates", which the guard above rules out.
        Err(_) => Some(HostGalaxyAssociation::empty()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    fn enabled_config() -> HostGalaxyConfig {
        HostGalaxyConfig {
            enabled: true,
            ..Default::default()
        }
    }

    /// M100, a large face-on spiral: a = 222 arcsec.
    fn ngc4321() -> Document {
        doc! {
            "_id": "NGC 4321",
            "ra": 185.728_75,
            "dec": 15.822_3,
            "objtype": "G",
            "z": 0.005_24,
            "z_unc": 0.000_01,
            "diam": 444.0_f64,
            "diam_ba": 0.87_f64,
            "diam_pa": 30.0_f64,
            "dist_mpc": 16.8_f64,
        }
    }

    #[test]
    fn test_disabled_returns_none() {
        let mut xmatches = HashMap::new();
        xmatches.insert(NED_LVS.to_string(), vec![ngc4321()]);
        assert!(associate_from_xmatches(
            185.728_75,
            15.822_3,
            &xmatches,
            &HostGalaxyConfig::default()
        )
        .is_none());
    }

    #[test]
    fn test_end_to_end_from_crossmatch_documents() {
        // A transient 30 arcsec out sits deep inside this galaxy. The legacy
        // fixed-aperture path would only agree if its circle happened to be
        // wider than 30 arcsec.
        let mut xmatches = HashMap::new();
        xmatches.insert(NED_LVS.to_string(), vec![ngc4321()]);

        let assoc = associate_from_xmatches(
            185.728_75,
            15.822_3 + 30.0 / 3600.0,
            &xmatches,
            &enabled_config(),
        )
        .unwrap();

        let best = assoc.best_host.as_ref().unwrap();
        assert_eq!(best.objname.as_deref(), Some("NGC 4321"));
        assert_eq!(best.catalog.as_deref(), Some(NED_LVS));
        assert_close!(best.sep_arcsec, 30.0, epsilon = 0.01);
        assert!(best.d_dlr < 1.0, "d_DLR was {}", best.d_dlr);
        assert!(best.posterior > 0.5);
        // dist_mpc present, so the projected separation is available too.
        assert!(best.sep_kpc.unwrap() > 0.0);
        assert_close!(best.dist_mpc.unwrap(), 16.8);
    }

    #[test]
    fn test_no_matches_yields_empty_association() {
        let assoc =
            associate_from_xmatches(10.0, 20.0, &HashMap::new(), &enabled_config()).unwrap();
        assert!(assoc.best_host.is_none());
        assert_eq!(assoc.n_candidates_searched, 0);
        assert_close!(assoc.p_host_none, 1.0);
    }

    #[test]
    fn test_far_outside_every_galaxy_yields_no_host() {
        let mut xmatches = HashMap::new();
        xmatches.insert(NED_LVS.to_string(), vec![ngc4321()]);

        // A full degree away: well outside even this large galaxy.
        let assoc =
            associate_from_xmatches(185.728_75, 15.822_3 + 1.0, &xmatches, &enabled_config())
                .unwrap();

        assert!(assoc.best_host.is_none());
        assert_eq!(assoc.n_candidates_searched, 1);
        assert_eq!(assoc.n_candidates_after_dlr_cut, 0);
        assert_close!(assoc.p_host_none, 1.0);
    }

    #[test]
    fn test_m31_scale_host_is_found_far_from_centre() {
        // The case the fixed 100 arcsec aperture structurally cannot handle:
        // M31 spans degrees, so a transient half a degree out is still well
        // inside it.
        let m31 = doc! {
            "_id": "MESSIER 031",
            "ra": 10.684_7,
            "dec": 41.269_1,
            "objtype": "G",
            "z": -0.001_001_f64,
            "diam": 11_400.0_f64,   // ~3.2 deg major axis
            "diam_ba": 0.32_f64,
            "diam_pa": 35.0_f64,
            "dist_mpc": 0.79_f64,
        };
        let mut xmatches = HashMap::new();
        xmatches.insert(NED_LVS.to_string(), vec![m31]);

        // 0.4 deg north of centre = 1440 arcsec, far past any 100 arcsec circle.
        let assoc = associate_from_xmatches(10.684_7, 41.269_1 + 0.4, &xmatches, &enabled_config())
            .unwrap();

        let best = assoc.best_host.as_ref().unwrap();
        assert_eq!(best.objname.as_deref(), Some("MESSIER 031"));
        assert_close!(best.sep_arcsec, 1440.0, epsilon = 1.0);
        assert!(
            best.d_dlr < 5.0,
            "M31 should still be the host at 0.4 deg, d_DLR was {}",
            best.d_dlr
        );
    }
}
