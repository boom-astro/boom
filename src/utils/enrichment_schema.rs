//! Enrichment paths no Avro schema declares: dotted paths whose shape follows the
//! deployment's config and what matched, so only BOOM can enumerate them.

use crate::api::catalogs::WATCHLIST_PREFIX;
use crate::conf::CatalogXmatchConfig;
use crate::utils::enums::Survey;
use serde::Serialize;
use utoipa::ToSchema;

/// Villar fit parameters, in the order the fitter reports them.
pub const VILLAR_PARAMS: [&str; 7] = [
    "A",
    "beta",
    "gamma",
    "t_0",
    "tau_rise",
    "tau_fall",
    "extra_sigma",
];

/// Bands the fit is run per.
pub const VILLAR_BANDS: [&str; 2] = ["ZTF_r", "ZTF_g"];

/// Leaf, type, nullable, description; carried by `best_host` and every `candidates` entry.
const HOST_CANDIDATE_FIELDS: [(&str, &str, bool, &str); 19] = [
    ("objname", "string", true, "name of the galaxy"),
    ("catalog", "string", true, "catalog the row came from"),
    (
        "objtype",
        "string",
        true,
        "morphological type as the catalog reports it",
    ),
    (
        "size_is_isophotal",
        "bool",
        false,
        "whether the size is an isophotal diameter rather than a half-light radius",
    ),
    (
        "orientation_is_nominal",
        "bool",
        false,
        "true when the position angle is a placeholder, which makes d_dlr directionally meaningless",
    ),
    ("ra", "double", false, "right ascension, degrees"),
    ("dec", "double", false, "declination, degrees"),
    (
        "sep_arcsec",
        "double",
        false,
        "angular separation from the transient, arcsec",
    ),
    ("sep_kpc", "double", true, "projected separation, kpc"),
    (
        "dlr_arcsec",
        "double",
        false,
        "galaxy light radius toward the transient, arcsec",
    ),
    (
        "d_dlr",
        "double",
        false,
        "separation in units of dlr_arcsec, which is what filters cut on",
    ),
    (
        "dlr_rank",
        "int",
        false,
        "rank by d_dlr, 1 = the galaxy the transient sits deepest inside",
    ),
    (
        "posterior",
        "double",
        false,
        "normalised over all candidates considered, not just the stored ones",
    ),
    ("z", "double", true, "redshift"),
    (
        "dist_mpc",
        "double",
        true,
        "adopted distance, Mpc, redshift-independent only when dist_mpc_method says so",
    ),
    (
        "dist_mpc_method",
        "string",
        true,
        "how dist_mpc was obtained",
    ),
    ("a_arcsec", "double", false, "semi-major axis, arcsec"),
    ("b_arcsec", "double", false, "semi-minor axis, arcsec"),
    ("pa_deg", "double", false, "position angle, degrees"),
];

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct EnrichmentField {
    pub path: String,
    pub value_type: &'static str,
    /// Absent documents match no comparison, and `$exists` reports it.
    pub optional: bool,
    /// NaN fails every comparison while `$exists` still reports true.
    pub nan_possible: bool,
    pub description: String,
}

impl EnrichmentField {
    fn new(
        path: impl Into<String>,
        value_type: &'static str,
        optional: bool,
        nan_possible: bool,
        description: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            value_type,
            optional,
            nan_possible,
            description: description.into(),
        }
    }
}

/// A skipped fit writes every one of these as NaN rather than omitting them.
fn villar_fields() -> Vec<EnrichmentField> {
    let mut out = vec![
        EnrichmentField::new(
            "villar_fit.reduced_chi2",
            "double",
            true,
            true,
            "Reduced chi-square of the Villar fit; NaN when the fit was skipped.",
        ),
        EnrichmentField::new(
            "villar_fit.peak_flux",
            "double",
            true,
            true,
            "Peak flux of the fitted model; NaN when the fit was skipped.",
        ),
    ];
    for band in VILLAR_BANDS {
        for param in VILLAR_PARAMS {
            out.push(EnrichmentField::new(
                format!("villar_fit.{param}_{band}"),
                "double",
                true,
                true,
                format!("Villar parameter {param} in {band}; NaN when the fit was skipped."),
            ));
        }
    }
    out
}

/// Generated from each catalog's projection, so config alone decides the list.
fn cross_match_fields(crossmatch: &[CatalogXmatchConfig]) -> Vec<EnrichmentField> {
    let mut out = Vec::new();
    for catalog in crossmatch {
        // Watchlist matches land on the watchlist document, never on the alert.
        if catalog.catalog.starts_with(WATCHLIST_PREFIX) {
            continue;
        }
        out.push(EnrichmentField::new(
            format!("cross_matches.{}", catalog.catalog),
            "array",
            true,
            false,
            format!(
                "Rows from {} within {:.1} arcsec; empty array when nothing matched.",
                catalog.catalog,
                catalog.radius.to_degrees() * 3600.0
            ),
        ));
        let mut keys: Vec<&String> = catalog.projection.keys().collect();
        keys.sort();
        for key in keys {
            if key == "_id" {
                continue;
            }
            out.push(EnrichmentField::new(
                format!("cross_matches.{}.{}", catalog.catalog, key),
                "any",
                true,
                false,
                format!("{key} on a matched {} row.", catalog.catalog),
            ));
        }
    }
    out
}

/// Everything `HostGalaxyAssociation` stores, not just the offset filters cut on.
fn host_galaxy_fields() -> Vec<EnrichmentField> {
    let mut out = vec![
        EnrichmentField::new(
            "host_galaxy.best_host",
            "object",
            true,
            false,
            "Highest-posterior candidate, duplicated from candidates[0]; null when none passed the cuts.",
        ),
        EnrichmentField::new(
            "host_galaxy.candidates",
            "array",
            true,
            false,
            "Candidates that passed the d_DLR cut, best first.",
        ),
        EnrichmentField::new(
            "host_galaxy.n_candidates_searched",
            "int",
            true,
            false,
            "Galaxies the cross-match supplied, before any shape or offset cut.",
        ),
        EnrichmentField::new(
            "host_galaxy.n_candidates_after_dlr_cut",
            "int",
            true,
            false,
            "Galaxies surviving the d_DLR cut.",
        ),
        EnrichmentField::new(
            "host_galaxy.p_host_none",
            "double",
            true,
            false,
            "Posterior that none of the candidates is the host.",
        ),
    ];
    for parent in ["best_host", "candidates"] {
        for (leaf, value_type, nullable, what) in HOST_CANDIDATE_FIELDS {
            out.push(EnrichmentField::new(
                format!("host_galaxy.{parent}.{leaf}"),
                value_type,
                true,
                false,
                if nullable {
                    format!("{what}; null when the catalog did not give it.")
                } else {
                    format!("{what}.")
                },
            ));
        }
    }
    out
}

fn association_fields(survey: &Survey, host_galaxy_enabled: bool) -> Vec<EnrichmentField> {
    let mut out = Vec::new();
    if host_galaxy_enabled {
        out.extend(host_galaxy_fields());
    }
    if matches!(survey, Survey::Ztf) {
        out.push(EnrichmentField::new(
            "sso_history",
            "array",
            true,
            false,
            "Past alerts sharing this alert's solar system designation, oldest \
             first, within a year and inside the association radius.",
        ));
        for (leaf, value_type, nullable, what) in [
            (
                "designation",
                "string",
                false,
                "MPC designation the entry matched on",
            ),
            ("jd", "double", false, "epoch of the entry"),
            ("fid", "int", false, "filter id of the entry"),
            ("magpsf", "double", false, "PSF magnitude"),
            ("sigmapsf", "double", false, "uncertainty on magpsf"),
            ("ra", "double", false, "right ascension, degrees"),
            ("dec", "double", false, "declination, degrees"),
            (
                "predicted_mag",
                "double",
                true,
                "magnitude the ephemeris predicted",
            ),
            (
                "separation_arcsec",
                "double",
                true,
                "offset from the predicted position",
            ),
            ("helio_dist", "double", true, "heliocentric distance, au"),
            ("topo_dist", "double", true, "topocentric distance, au"),
            ("phase_angle", "double", true, "solar phase angle, degrees"),
        ] {
            out.push(EnrichmentField::new(
                format!("sso_history.{leaf}"),
                value_type,
                true,
                false,
                if nullable {
                    format!("{what}; null on entries enriched before geometry existed.")
                } else {
                    format!("{what}.")
                },
            ));
        }
    }
    out
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EnabledEnrichers {
    pub host_galaxy: bool,
    pub villar: bool,
}

/// Every field a filter can reference that the Avro packet does not describe.
pub fn enrichment_fields(
    survey: &Survey,
    crossmatch: &[CatalogXmatchConfig],
    enabled: EnabledEnrichers,
) -> Vec<EnrichmentField> {
    let mut out = Vec::new();
    if matches!(survey, Survey::Ztf) && enabled.villar {
        out.extend(villar_fields());
    }
    out.extend(cross_match_fields(crossmatch));
    out.extend(association_fields(survey, enabled.host_galaxy));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::host::{HostGalaxyAssociation, StoredHostCandidate};
    use std::collections::BTreeSet;

    fn xmatch_config(catalog: &str) -> CatalogXmatchConfig {
        CatalogXmatchConfig {
            catalog: catalog.to_string(),
            radius: crate::conf::arcsec_to_radians(300.0),
            projection: mongodb::bson::doc! { "_id": 1, "z": 1, "Diam": 1 },
            ..Default::default()
        }
    }

    #[test]
    fn test_villar_declares_every_field_the_fitter_writes() {
        let fields = villar_fields();
        assert_eq!(fields.len(), 2 + VILLAR_PARAMS.len() * VILLAR_BANDS.len());
        assert!(fields.iter().all(|f| f.nan_possible));
        for expected in [
            "villar_fit.reduced_chi2",
            "villar_fit.peak_flux",
            "villar_fit.A_ZTF_r",
            "villar_fit.extra_sigma_ZTF_g",
        ] {
            assert!(
                fields.iter().any(|f| f.path == expected),
                "{expected} is not declared"
            );
        }
    }

    #[cfg(feature = "gpu")]
    #[test]
    fn test_villar_names_match_the_fitter() {
        assert_eq!(VILLAR_PARAMS, villar_pso::PARAM_NAMES);
        assert_eq!(VILLAR_BANDS, villar_pso::FILTERS);
    }

    #[test]
    fn test_cross_matches_follow_the_configured_catalogs() {
        let fields = cross_match_fields(&[xmatch_config("NED")]);
        let paths: Vec<&str> = fields.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"cross_matches.NED"));
        assert!(paths.contains(&"cross_matches.NED.z"));
        assert!(paths.contains(&"cross_matches.NED.Diam"));
        assert!(!paths.contains(&"cross_matches.NED._id"));
    }

    #[test]
    fn test_watchlist_catalogs_are_not_advertised() {
        let fields = cross_match_fields(&[
            xmatch_config(&format!("{WATCHLIST_PREFIX}someone")),
            xmatch_config("NED"),
        ]);
        assert!(fields.iter().all(|f| !f.path.contains(WATCHLIST_PREFIX)));
        assert!(fields.iter().any(|f| f.path == "cross_matches.NED"));
    }

    fn written_paths(value: &serde_json::Value, prefix: &str, out: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let path = format!("{prefix}.{key}");
                    out.insert(path.clone());
                    written_paths(child, &path, out);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    written_paths(item, prefix, out);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn test_host_galaxy_declares_every_stored_field() {
        let candidate = StoredHostCandidate {
            objname: Some("NGC 1234".to_string()),
            catalog: Some("NED".to_string()),
            objtype: Some("G".to_string()),
            size_is_isophotal: true,
            orientation_is_nominal: false,
            ra: 1.0,
            dec: 2.0,
            sep_arcsec: 3.0,
            sep_kpc: Some(4.0),
            dlr_arcsec: 5.0,
            d_dlr: 0.5,
            dlr_rank: 1,
            posterior: 0.9,
            z: Some(0.05),
            dist_mpc: Some(200.0),
            dist_mpc_method: Some("z".to_string()),
            a_arcsec: 6.0,
            b_arcsec: 7.0,
            pa_deg: 8.0,
        };
        let association = HostGalaxyAssociation {
            best_host: Some(candidate.clone()),
            candidates: vec![candidate],
            n_candidates_searched: 3,
            n_candidates_after_dlr_cut: 1,
            p_host_none: 0.1,
        };

        let mut written = BTreeSet::new();
        written_paths(
            &serde_json::to_value(&association).unwrap(),
            "host_galaxy",
            &mut written,
        );
        let declared: BTreeSet<String> = host_galaxy_fields()
            .iter()
            .map(|f| f.path.clone())
            .collect();
        assert_eq!(declared, written);
    }

    #[test]
    fn test_host_galaxy_is_advertised_only_when_it_runs() {
        let off = enrichment_fields(&Survey::Ztf, &[], EnabledEnrichers::default());
        let on = enrichment_fields(
            &Survey::Ztf,
            &[],
            EnabledEnrichers {
                host_galaxy: true,
                ..Default::default()
            },
        );
        assert!(!off.iter().any(|f| f.path.starts_with("host_galaxy")));
        assert!(on.iter().any(|f| f.path == "host_galaxy.best_host.d_dlr"));
        assert_eq!(on.len(), off.len() + host_galaxy_fields().len());
    }

    #[test]
    fn test_host_galaxy_is_advertised_for_every_survey() {
        let enabled = EnabledEnrichers {
            host_galaxy: true,
            ..Default::default()
        };
        for survey in [Survey::Ztf, Survey::Lsst, Survey::Decam, Survey::Winter] {
            let fields = enrichment_fields(&survey, &[], enabled);
            assert!(
                fields
                    .iter()
                    .any(|f| f.path == "host_galaxy.best_host.d_dlr"),
                "{survey:?} does not advertise host_galaxy"
            );
        }
    }

    #[test]
    fn test_villar_is_advertised_only_for_ztf_on_gpu() {
        let gpu = EnabledEnrichers {
            villar: true,
            ..Default::default()
        };
        let ztf = enrichment_fields(&Survey::Ztf, &[], gpu);
        let lsst = enrichment_fields(&Survey::Lsst, &[], gpu);
        let cpu = enrichment_fields(&Survey::Ztf, &[], EnabledEnrichers::default());
        assert!(ztf.iter().any(|f| f.path.starts_with("villar_fit.")));
        assert!(!lsst.iter().any(|f| f.path.starts_with("villar_fit.")));
        assert!(!cpu.iter().any(|f| f.path.starts_with("villar_fit.")));
    }
}
