//! The stored form of a host association.
//!
//! [`super::associate_host`] returns a rich result carrying the full galaxy
//! record for each candidate. What lands on the alert aux document is a
//! flattened view of that: the fields a filter actually cuts on, at the top
//! level of each candidate rather than nested behind `galaxy`.

use serde::{Deserialize, Serialize};

use super::associate::AssociationResult;

/// Arcsec per radian, for converting an angular separation to a projected
/// physical distance once a distance is known.
const ARCSEC_PER_RADIAN: f64 = 206_264.806_247_096_36;

/// One candidate host, flattened for storage and filtering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredHostCandidate {
    pub objname: Option<String>,
    pub catalog: Option<String>,
    pub objtype: Option<String>,
    pub ra: f64,
    pub dec: f64,
    /// Angular separation from the transient, arcsec.
    pub sep_arcsec: f64,
    /// Projected separation in kpc, when a distance is available.
    pub sep_kpc: Option<f64>,
    /// Galaxy light radius toward the transient, arcsec.
    pub dlr_arcsec: f64,
    /// Separation in units of the directional light radius. This is the
    /// quantity filters should cut on.
    pub d_dlr: f64,
    /// Rank by `d_dlr`, 1 = the galaxy the transient sits deepest inside.
    pub dlr_rank: u32,
    /// Normalised probability that this galaxy is the host.
    pub posterior: f64,
    pub z: Option<f64>,
    /// Adopted distance in Mpc; redshift-independent only when
    /// `dist_mpc_method` says so.
    pub dist_mpc: Option<f64>,
    pub dist_mpc_method: Option<String>,
    pub a_arcsec: f64,
    pub b_arcsec: f64,
    pub pa_deg: f64,
}

/// Host association for one object, as stored under `aux.host_galaxy`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostGalaxyAssociation {
    /// Highest-posterior candidate, duplicated out of `candidates` so filters
    /// and downstream consumers do not have to index into the array.
    pub best_host: Option<StoredHostCandidate>,
    pub candidates: Vec<StoredHostCandidate>,
    /// Galaxies supplied by the cross-match, before any shape or offset cut.
    pub n_candidates_searched: u32,
    /// Galaxies surviving the d_DLR cut.
    pub n_candidates_after_dlr_cut: u32,
    /// Probability that none of the candidates is the true host.
    pub p_host_none: f64,
}

impl HostGalaxyAssociation {
    /// Flatten an [`AssociationResult`] into the stored shape.
    pub fn from_result(result: &AssociationResult) -> Self {
        let candidates: Vec<StoredHostCandidate> = result
            .candidates
            .iter()
            .map(|c| {
                let dist_mpc = c.galaxy.dist_mpc;
                // theta[rad] * D gives the projected separation; Mpc -> kpc is
                // a factor of 1000.
                let sep_kpc = dist_mpc
                    .filter(|d| *d > 0.0)
                    .map(|d| c.separation_arcsec / ARCSEC_PER_RADIAN * d * 1000.0);

                StoredHostCandidate {
                    objname: c.galaxy.objname.clone(),
                    catalog: c.galaxy.catalog.clone(),
                    objtype: c.galaxy.objtype.clone(),
                    ra: c.galaxy.ra,
                    dec: c.galaxy.dec,
                    sep_arcsec: c.separation_arcsec,
                    sep_kpc,
                    dlr_arcsec: c.dlr,
                    d_dlr: c.fractional_offset,
                    dlr_rank: c.dlr_rank,
                    posterior: c.posterior,
                    z: c.galaxy.redshift,
                    dist_mpc,
                    dist_mpc_method: c.galaxy.dist_mpc_method.clone(),
                    a_arcsec: c.galaxy.a_arcsec,
                    b_arcsec: c.galaxy.b_arcsec,
                    pa_deg: c.galaxy.pa_deg,
                }
            })
            .collect();

        Self {
            // `candidates` is already sorted by descending posterior.
            best_host: candidates.first().cloned(),
            n_candidates_searched: result.n_considered as u32,
            n_candidates_after_dlr_cut: candidates.len() as u32,
            p_host_none: result.p_none,
            candidates,
        }
    }

    /// An association that found nothing, for objects with no galaxy
    /// cross-matches at all. Recorded rather than left absent so consumers see
    /// a consistent shape.
    pub fn empty() -> Self {
        Self {
            best_host: None,
            candidates: Vec::new(),
            n_candidates_searched: 0,
            n_candidates_after_dlr_cut: 0,
            p_host_none: 1.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::associate::{associate_host, AssociationConfig};
    use super::super::types::{GalaxyCandidate, Transient};
    use super::*;

    fn galaxy(dec_offset_arcsec: f64, dist_mpc: Option<f64>) -> GalaxyCandidate {
        GalaxyCandidate {
            ra: 180.0,
            dec: 45.0 + dec_offset_arcsec / 3600.0,
            a_arcsec: 20.0,
            b_arcsec: 10.0,
            pa_deg: 0.0,
            redshift: Some(0.004),
            redshift_err: Some(0.000_1),
            dist_mpc,
            dist_mpc_method: dist_mpc.map(|_| "zIndependent".to_string()),
            mag: None,
            mag_err: None,
            objtype: Some("G".to_string()),
            objname: Some("test-galaxy".to_string()),
            catalog: Some("NED_LVS".to_string()),
            shape_from_image: false,
        }
    }

    #[test]
    fn test_flattens_and_picks_best_host() {
        let transient = Transient::new(180.0, 45.0);
        let result = associate_host(
            &transient,
            &[galaxy(5.0, None), galaxy(15.0, None)],
            &AssociationConfig::default(),
        )
        .unwrap();

        let stored = HostGalaxyAssociation::from_result(&result);

        assert_eq!(stored.candidates.len(), 2);
        assert_eq!(stored.n_candidates_searched, 2);
        assert_eq!(stored.n_candidates_after_dlr_cut, 2);
        let best = stored.best_host.as_ref().unwrap();
        assert_eq!(best, &stored.candidates[0]);
        assert!(best.posterior >= stored.candidates[1].posterior);
        assert_eq!(best.dlr_rank, 1);
        assert_eq!(best.catalog.as_deref(), Some("NED_LVS"));
    }

    #[test]
    fn test_sep_kpc_computed_only_with_a_distance() {
        let transient = Transient::new(180.0, 45.0);

        let without = HostGalaxyAssociation::from_result(
            &associate_host(
                &transient,
                &[galaxy(5.0, None)],
                &AssociationConfig::default(),
            )
            .unwrap(),
        );
        assert!(without.best_host.unwrap().sep_kpc.is_none());

        // At 16.8 Mpc, 5 arcsec projects to 5/206264.8 * 16.8 * 1000 kpc.
        let with = HostGalaxyAssociation::from_result(
            &associate_host(
                &transient,
                &[galaxy(5.0, Some(16.8))],
                &AssociationConfig::default(),
            )
            .unwrap(),
        );
        let best = with.best_host.unwrap();
        assert_close!(
            best.sep_kpc.unwrap(),
            5.0 / ARCSEC_PER_RADIAN * 16.8 * 1000.0,
            epsilon = 1e-9
        );
        // Sanity: a few hundred pc, not kiloparsecs.
        assert!(best.sep_kpc.unwrap() < 1.0);
        // The provenance has to survive into storage, or a filter cannot tell
        // a real distance from a repackaged redshift.
        assert_eq!(best.dist_mpc_method.as_deref(), Some("zIndependent"));
    }

    #[test]
    fn test_empty_association_is_well_formed() {
        let empty = HostGalaxyAssociation::empty();
        assert!(empty.best_host.is_none());
        assert!(empty.candidates.is_empty());
        assert_close!(empty.p_host_none, 1.0);
    }

    #[test]
    fn test_bson_round_trip() {
        // The alert worker serialises this onto the aux document and the
        // enrichment pipeline reads it back, so both directions must hold.
        let transient = Transient::new(180.0, 45.0);
        let stored = HostGalaxyAssociation::from_result(
            &associate_host(
                &transient,
                &[galaxy(5.0, Some(16.8)), galaxy(15.0, None)],
                &AssociationConfig::default(),
            )
            .unwrap(),
        );

        let bson = mongodb::bson::to_bson(&stored).unwrap();
        let back: HostGalaxyAssociation = mongodb::bson::from_bson(bson).unwrap();
        assert_eq!(back, stored);

        let empty = HostGalaxyAssociation::empty();
        let bson = mongodb::bson::to_bson(&empty).unwrap();
        let back: HostGalaxyAssociation = mongodb::bson::from_bson(bson).unwrap();
        assert_eq!(back, empty);
    }

    #[test]
    fn test_no_surviving_candidates_reports_p_none_one() {
        let transient = Transient::new(180.0, 45.0);
        // 500 arcsec from a 20 arcsec galaxy is far beyond the cutoff.
        let result = associate_host(
            &transient,
            &[galaxy(500.0, None)],
            &AssociationConfig::default(),
        )
        .unwrap();

        let stored = HostGalaxyAssociation::from_result(&result);
        assert!(stored.best_host.is_none());
        assert_eq!(stored.n_candidates_searched, 1);
        assert_eq!(stored.n_candidates_after_dlr_cut, 0);
        assert_close!(stored.p_host_none, 1.0);
    }
}
