use serde::{Deserialize, Serialize};

use super::associate::AssociationConfig;
use super::catalog::{LS_DR10, NED_LVS};

/// Host-galaxy association, read from `config.yaml` under `host_galaxy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HostGalaxyConfig {
    /// Off until a galaxy catalog with shapes is ingested and listed in the
    /// survey's crossmatch block.
    pub enabled: bool,
    /// Cross-match key supplying NED-LVS diameters.
    pub ned_lvs_catalog: String,
    /// Cross-match key supplying Legacy Survey Tractor shapes.
    pub ls_dr10_catalog: String,
    /// Largest d_DLR still admitted as a candidate, deliberately looser than the
    /// cut a filter would apply so the posterior normalises over the full set.
    pub max_dlr: f64,
    /// Floor on the semi-minor axis, arcsec, for degenerate shapes.
    pub min_axis_arcsec: f64,
    /// Axis ratio below which a shape is a fit failure and the row is dropped.
    pub min_axis_ratio: f64,
    /// Axis ratio a shape between `min_axis_ratio` and this is pinned to, which
    /// bounds the elongation without shrinking the object.
    pub pinned_axis_ratio: f64,
    pub max_candidates: usize,
    /// Drop Legacy Survey rows typed as point sources, which have no extent.
    pub exclude_star_like: bool,
    /// Legacy `type` values with no galaxy extent: `PSF` is a point source,
    /// `DUP` a Gaia duplicate carrying no shape.
    pub star_type_values: Vec<String>,
    /// NED-LVS `objtype` values that are not host galaxies: quasars, line
    /// systems, and lensed systems whose shape describes the lens.
    pub ned_lvs_excluded_objtypes: Vec<String>,
    /// Include the redshift term when both the transient and the galaxy have one.
    pub use_redshift: bool,
    /// Reject a REX row smaller than this, arcsec. The three REX cuts sit in
    /// sensitive parts of the parameter space, so they are configurable.
    pub rex_min_shape_r_arcsec: f64,
    /// Reject a REX row below this r-band signal-to-noise.
    pub rex_min_snr: f64,
    /// Reject a REX row with at least this fraction of its aperture flux from
    /// neighbours, which means it sits inside something larger.
    pub rex_max_fracflux: f64,
    /// Isophote the Legacy half-light radius is converted to, mag/arcsec^2.
    pub isophote_mag: f64,
}

impl Default for HostGalaxyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ned_lvs_catalog: NED_LVS.to_string(),
            ls_dr10_catalog: LS_DR10.to_string(),
            max_dlr: 5.0,
            min_axis_arcsec: 0.05,
            min_axis_ratio: 0.05,
            pinned_axis_ratio: 0.1,
            max_candidates: 10,
            exclude_star_like: true,
            star_type_values: vec!["PSF".to_string(), "DUP".to_string()],
            ned_lvs_excluded_objtypes: ["QSO", "AbLS", "EmLS", "EmObj", "Q_Lens", "G_Lens"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            use_redshift: true,
            rex_min_shape_r_arcsec: 0.3,
            rex_min_snr: 5.0,
            rex_max_fracflux: 0.5,
            isophote_mag: super::sersic::MU_25,
        }
    }
}

impl HostGalaxyConfig {
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
        let config: HostGalaxyConfig =
            serde_json::from_str(r#"{"enabled": true, "max_dlr": 4.0}"#).unwrap();
        assert!(config.enabled);
        assert_close!(config.max_dlr, 4.0);
        assert_eq!(config.ned_lvs_catalog, NED_LVS);
        assert_eq!(config.max_candidates, 10);
        assert!(config.exclude_star_like);
    }
}

#[cfg(test)]
mod config_file_tests {
    use super::HostGalaxyConfig;

    // A typo under `#[serde(default)]` silently keeps the default, so assert values.
    #[test]
    fn test_rex_and_isophote_knobs_parse_from_config_yaml() {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/config.yaml"
            )))
            .build()
            .expect("config.yaml loads");
        let parsed: HostGalaxyConfig = settings
            .get("host_galaxy")
            .expect("host_galaxy deserializes");

        assert_eq!(parsed.rex_min_shape_r_arcsec, 0.3);
        assert_eq!(parsed.rex_min_snr, 5.0);
        assert_eq!(parsed.rex_max_fracflux, 0.5);
        assert_eq!(parsed.isophote_mag, 25.0);
    }
}
