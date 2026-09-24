//! The enrichment paths no Avro schema declares.
//!
//! `ZtfAlertToFilter` and its siblings already cover the enrichment that lands in
//! a struct field: `properties`, `classifications`, the joined photometry arrays.
//! What is left is `$set` as dotted paths whose shape depends on config or on what
//! matched, so BOOM is the only place that can enumerate it.

use crate::conf::CatalogXmatchConfig;
use crate::utils::enums::Survey;
use serde::Serialize;
use utoipa::ToSchema;

/// Villar fit parameters, in the order the fitter reports them.
///
/// Declared here rather than read from `villar_pso`, which sits behind the
/// `gpu` feature that the API advertising these is not built with. The
/// enrichment worker's skipped-fit path reads these same two arrays, so the
/// advertised list and the written list cannot drift apart; a `gpu` build also
/// runs a test pinning them to the crate's own constants.
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

/// A field a filter can reference, and what its author has to know to use it.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct EnrichmentField {
    /// Dotted path exactly as a pipeline writes it.
    pub path: String,
    /// BSON type of the value, or `object` where the shape is catalog-defined.
    pub value_type: &'static str,
    /// Whether a document may not carry the field at all, which no comparison
    /// matches and `$exists` reports.
    pub optional: bool,
    /// Whether a present value can be NaN. NaN fails every comparison while
    /// `$exists` still reports true, so absent and unavailable differ.
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

/// Fields the Villar fitter writes onto a ZTF alert.
///
/// A fit that is skipped writes every one of these as NaN rather than leaving
/// them out, so a filter comparing on one silently matches nothing for those
/// alerts instead of erroring.
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

/// Cross-match fields, from the catalogs the survey is configured against.
///
/// Generated from each catalog's projection rather than listed, so a catalog
/// added to the config appears here without anyone editing this file.
///
/// The join that brings these in flattens them to the top level, so a filter
/// names `cross_matches.<catalog>`; a path through `aux` reads as empty and is
/// rejected when the filter is saved.
fn cross_match_fields(crossmatch: &[CatalogXmatchConfig]) -> Vec<EnrichmentField> {
    let mut out = Vec::new();
    for catalog in crossmatch {
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

/// Association products, written as sub-documents rather than struct fields.
fn association_fields(survey: &Survey, host_galaxy_enabled: bool) -> Vec<EnrichmentField> {
    let mut out = Vec::new();
    // A path advertised but never written reads as an empty night, not a missing enricher.
    if host_galaxy_enabled {
        out.push(EnrichmentField::new(
            "host_galaxy.best_host.d_dlr",
            "double",
            true,
            false,
            "Directional light radius offset to the best host galaxy.",
        ));
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
        // Each entry carries its own geometry, so a window statistic has one
        // value per point rather than the alert's.
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

/// The enrichers a deployment runs, since a field exists only if one wrote it.
#[derive(Debug, Clone, Copy, Default)]
pub struct EnabledEnrichers {
    pub host_galaxy: bool,
    /// The fit runs on a GPU context, so this follows `gpu.is_active()`.
    pub villar: bool,
}

/// Every field a filter can reference that the Avro packet does not describe.
/// Only what this deployment actually writes: a caller passes the flags that
/// decide whether an enricher runs, so the list describes the alerts a filter
/// will see rather than the alerts BOOM could produce under some config.
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

    /// The count is two plus one per parameter per band, and every one of them
    /// can be NaN, which is the part a filter author gets wrong.
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

    /// The names come from the crate that does the fitting, so a `gpu` build
    /// fails here rather than letting the advertised list drift from it.
    #[cfg(feature = "gpu")]
    #[test]
    fn test_villar_names_match_the_fitter() {
        assert_eq!(VILLAR_PARAMS, villar_pso::PARAM_NAMES);
        assert_eq!(VILLAR_BANDS, villar_pso::FILTERS);
    }

    /// A catalog in the config appears without this file being edited.
    #[test]
    fn test_cross_matches_follow_the_configured_catalogs() {
        let catalog = CatalogXmatchConfig {
            catalog: "NED".to_string(),
            radius: crate::conf::arcsec_to_radians(300.0),
            projection: mongodb::bson::doc! { "_id": 1, "z": 1, "Diam": 1 },
            ..Default::default()
        };
        let fields = cross_match_fields(&[catalog]);
        let paths: Vec<&str> = fields.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"cross_matches.NED"));
        assert!(paths.contains(&"cross_matches.NED.z"));
        assert!(paths.contains(&"cross_matches.NED.Diam"));
        // `_id` is the catalog's own key, not something to filter on.
        assert!(!paths.contains(&"cross_matches.NED._id"));
    }

    /// Advertising a field nothing writes is the ambiguity this endpoint exists
    /// to remove: a filter on it matches nothing, which reads as a quiet night.
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
        // And it is the only difference the flag makes.
        assert_eq!(on.len(), off.len() + 1);
    }

    /// The association runs on every survey, so none of them may omit the field.
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

    /// The fit is ZTF-only and needs a GPU context, so a CPU deployment writes
    /// none of these and must not offer them.
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
