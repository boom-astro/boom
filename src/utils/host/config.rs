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
    /// Axis ratio below which a shape is taken as unphysical and the row is
    /// dropped rather than pinned.
    pub min_axis_ratio: f64,
    /// Axis ratio that a shape between `min_axis_ratio` and this is pinned to.
    /// An absolute floor on the minor axis alone would flatten genuinely small
    /// galaxies; a ratio floor bounds the elongation without touching size.
    pub pinned_axis_ratio: f64,
    /// Most candidates to store per object.
    pub max_candidates: usize,
    /// Drop Legacy Survey rows typed as point sources; they have no galaxy
    /// extent and would otherwise contribute spurious tiny-DLR candidates.
    pub exclude_star_like: bool,
    /// Morphological `type` values marking a row with no galaxy extent. `PSF`
    /// is a point source; `DUP` is a Gaia duplicate, which carries no shape.
    pub star_type_values: Vec<String>,
    /// NED-LVS `objtype` values that are not host galaxies: quasars, absorption
    /// and emission line systems, and lensed systems whose catalogued shape
    /// describes the lens rather than anything a transient sits in.
    pub ned_lvs_excluded_objtypes: Vec<String>,
    /// Include the redshift term in the posterior when both the transient and
    /// the galaxy have one.
    pub use_redshift: bool,
    /// Round-exponential rejection. REX is the Tractor model for marginally
    /// resolved sources, and at DECam seeing a small one is indistinguishable
    /// from a point source. These three cuts are in sensitive regions of the
    /// parameter space -- small changes move the results materially -- so they
    /// are configurable rather than hardcoded.
    ///
    /// Reject a REX row when it is smaller than this, in arcsec.
    pub rex_min_shape_r_arcsec: f64,
    /// Reject a REX row below this r-band signal-to-noise.
    pub rex_min_snr: f64,
    /// Reject a REX row with at least this fraction of its aperture flux coming
    /// from neighbours; such a source sits inside something larger.
    pub rex_max_fracflux: f64,
    /// Surface brightness of the isophote the Legacy half-light radius is
    /// converted to, mag/arcsec^2. 25 puts it on the same scale as NED-LVS D25.
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
            isophote_mag: crate::utils::host::sersic::MU_25,
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

#[cfg(test)]
mod config_file_tests {
    use super::HostGalaxyConfig;

    /// The knobs are only useful if the deployed config actually reaches them.
    /// `#[serde(default)]` means a typo in config.yaml silently keeps the
    /// default rather than failing, so assert the parsed values.
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
