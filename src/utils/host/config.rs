use serde::{Deserialize, Serialize};

use super::associate::AssociationConfig;
use super::catalog::{LS_DR10, NED_LVS};

/// Configuration for host galaxy association, read from `config.yaml` under
/// the `host_galaxy` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HostGalaxyConfig {
    /// Off by default: association is only meaningful once a galaxy catalog
    /// with shapes has been ingested and added to the survey's crossmatch list.
    pub enabled: bool,
    /// Cross-match collection supplying NED-LVS diameters.
    pub ned_lvs_catalog: String,
    /// Cross-match collection supplying Legacy Survey Tractor shapes.
    pub ls_dr10_catalog: String,
    /// Largest d_DLR still admitted as a candidate. Deliberately looser than
    /// the cut a filter would apply, so the posterior is normalised over the
    /// full plausible set rather than a pre-truncated one.
    pub max_dlr: f64,
    /// Floor on the semi-minor axis, in arcsec, for degenerate shapes.
    pub min_axis_arcsec: f64,
    /// Most candidates to store per object.
    pub max_candidates: usize,
    /// Drop Legacy Survey rows typed as point sources; they have no galaxy
    /// extent and would otherwise contribute spurious tiny-DLR candidates.
    pub exclude_star_like: bool,
    /// The morphological `type` value marking a point source.
    pub star_type_value: String,
    /// Include the redshift term in the posterior when both the transient and
    /// the galaxy have one.
    pub use_redshift: bool,
}

impl Default for HostGalaxyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ned_lvs_catalog: NED_LVS.to_string(),
            ls_dr10_catalog: LS_DR10.to_string(),
            max_dlr: 5.0,
            min_axis_arcsec: 0.05,
            max_candidates: 10,
            exclude_star_like: true,
            star_type_value: "PSF".to_string(),
            use_redshift: true,
        }
    }
}

impl HostGalaxyConfig {
    /// Project the deployment-facing config onto the algorithm's own config,
    /// keeping the scoring core independent of BOOM's configuration types.
    pub fn association_config(&self) -> AssociationConfig {
        AssociationConfig {
            max_fractional_offset: self.max_dlr,
            min_b_arcsec: self.min_axis_arcsec,
            max_candidates: self.max_candidates,
            use_redshift: self.use_redshift,
            use_absmag: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_by_default() {
        assert!(!HostGalaxyConfig::default().enabled);
    }

    #[test]
    fn test_association_config_carries_the_cuts_over() {
        let config = HostGalaxyConfig {
            max_dlr: 4.0,
            max_candidates: 3,
            use_redshift: false,
            ..Default::default()
        };
        let association = config.association_config();
        assert_close!(association.max_fractional_offset, 4.0);
        assert_eq!(association.max_candidates, 3);
        assert!(!association.use_redshift);
    }

    #[test]
    fn test_partial_config_fills_in_defaults() {
        // `#[serde(default)]` on the struct means an operator can set one key
        // in config.yaml without having to restate the whole block.
        let config: HostGalaxyConfig =
            serde_json::from_str(r#"{"enabled": true, "max_dlr": 4.0}"#).unwrap();
        assert!(config.enabled);
        assert_close!(config.max_dlr, 4.0);
        assert_eq!(config.ned_lvs_catalog, NED_LVS);
        assert_eq!(config.max_candidates, 10);
        assert!(config.exclude_star_like);
    }
}
