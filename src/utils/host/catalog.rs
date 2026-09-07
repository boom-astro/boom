//! Turning cross-match documents into [`GalaxyCandidate`]s.
//!
//! NED-LVS gives an angular diameter with an axis ratio and position angle,
//! Legacy Survey gives a Tractor half-light radius plus ellipticity components.
//! Both end up as the same semi-axes and PA.

use mongodb::bson::{Bson, Document};

use crate::utils::spatial::get_opt_f64_from_doc as opt_f64;

use super::config::HostGalaxyConfig;
use super::dlr::compute_dlr;
use super::ellipse::Ellipse;
use super::sersic::{isophotal_semi_major, sersic_index_for_type, total_mag};
use super::types::GalaxyCandidate;

fn opt_string(doc: &Document, key: &str) -> Option<String> {
    match doc.get(key) {
        Some(Bson::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// Every NED-LVS key the reader depends on, for the projection drift test.
#[cfg(test)]
pub const NED_LVS_REQUIRED_KEYS: &[&str] = &[
    "_id",
    "ra",
    "dec",
    "objtype",
    "z",
    "z_unc",
    "Diam",
    "Diam_ba",
    "Diam_pa",
    "DistMpc",
    "DistMpc_method",
];

/// `None` when the row cannot support a DLR. About a fifth of NED-LVS carries
/// no diameter, so that is an ordinary outcome rather than an error.
///
/// Keys are the NED-LVS FITS column names verbatim, which is what the ingest
/// writes. `Diam` is the major-axis *diameter*, so the semi-major axis is
/// `Diam / 2`; `Diam_ba` is the minor-to-major ratio; `Diam_pa` is degrees east
/// of north.
pub fn galaxy_from_ned_lvs(doc: &Document, config: &HostGalaxyConfig) -> Option<GalaxyCandidate> {
    let ra = opt_f64(doc, "ra")?;
    let dec = opt_f64(doc, "dec")?;

    let objtype = opt_string(doc, "objtype");
    if let Some(t) = objtype.as_deref() {
        if config
            .ned_lvs_excluded_objtypes
            .iter()
            .any(|excluded| excluded.eq_ignore_ascii_case(t))
        {
            return None;
        }
    }

    let a_arcsec = opt_f64(doc, "Diam").filter(|d| *d > 0.0)? / 2.0;

    let axis_ratio = opt_f64(doc, "Diam_ba")
        .filter(|r| *r > 0.0 && *r <= 1.0)
        .unwrap_or(1.0);
    let axis_ratio = bounded_axis_ratio(axis_ratio, config)?;

    // 2MASS diameters carry a PA fixed at 90 deg, elongated or not.
    let diam_survey = opt_string(doc, "Diam_survey");
    let orientation_is_nominal = diam_survey
        .as_deref()
        .is_some_and(|s| s.eq_ignore_ascii_case("2MASS"))
        && axis_ratio < 1.0;

    Some(GalaxyCandidate {
        ra,
        dec,
        a_arcsec,
        b_arcsec: a_arcsec * axis_ratio,
        pa_deg: opt_f64(doc, "Diam_pa").unwrap_or(0.0),
        redshift: opt_f64(doc, "z"),
        redshift_err: opt_f64(doc, "z_unc"),
        dist_mpc: opt_f64(doc, "DistMpc"),
        dist_mpc_method: opt_string(doc, "DistMpc_method"),
        mag: opt_f64(doc, "m_Ks"),
        mag_err: opt_f64(doc, "m_Ks_unc"),
        objtype,
        objname: opt_string(doc, "_id"),
        catalog: Some(NED_LVS.to_string()),
        size_is_isophotal: true,
        diam_survey,
        orientation_is_nominal,
    })
}

fn bounded_axis_ratio(axis_ratio: f64, config: &HostGalaxyConfig) -> Option<f64> {
    if !axis_ratio.is_finite() || axis_ratio < config.min_axis_ratio {
        return None;
    }
    Some(axis_ratio.max(config.pinned_axis_ratio).min(1.0))
}

/// Tractor model for a marginally resolved source.
const REX_TYPE: &str = "REX";

/// Too small to separate from a point source, too faint to be reliably shaped,
/// or blended enough to be a fragment of the galaxy it sits inside.
fn rejected_as_marginal_rex(doc: &Document, config: &HostGalaxyConfig) -> bool {
    if opt_f64(doc, "shape_r").unwrap_or(0.0) < config.rex_min_shape_r_arcsec {
        return true;
    }
    // Absent columns mean no S/N was measured, which must not reject the row.
    if let (Some(flux), Some(ivar)) = (opt_f64(doc, "flux_r"), opt_f64(doc, "flux_ivar_r")) {
        if flux > 0.0 && ivar > 0.0 && flux * ivar.sqrt() < config.rex_min_snr {
            return true;
        }
    }
    opt_f64(doc, "fracflux_r").is_some_and(|f| f > config.rex_max_fracflux)
}

fn isophotal_semi_major_for(
    doc: &Document,
    ellipse: &Ellipse,
    config: &HostGalaxyConfig,
) -> Option<f64> {
    let objtype = opt_string(doc, "objtype")?;
    let n = sersic_index_for_type(&objtype, opt_f64(doc, "sersic"))?;
    let m_tot = total_mag(opt_f64(doc, "flux_r")?)?;
    isophotal_semi_major(ellipse.a, ellipse.axis_ratio, n, m_tot, config.isophote_mag)
}

pub fn galaxy_from_ls_dr10(doc: &Document, config: &HostGalaxyConfig) -> Option<GalaxyCandidate> {
    let ra = opt_f64(doc, "ra")?;
    let dec = opt_f64(doc, "dec")?;

    let objtype = opt_string(doc, "objtype");
    if config.exclude_star_like {
        if let Some(t) = objtype.as_deref() {
            if config
                .star_type_values
                .iter()
                .any(|excluded| excluded.eq_ignore_ascii_case(t))
            {
                return None;
            }
        }
    }

    let shape_r = opt_f64(doc, "shape_r").filter(|r| *r > 0.0)?;
    let shape_e1 = opt_f64(doc, "shape_e1").unwrap_or(0.0);
    let shape_e2 = opt_f64(doc, "shape_e2").unwrap_or(0.0);

    if objtype.as_deref() == Some(REX_TYPE) && rejected_as_marginal_rex(doc, config) {
        return None;
    }

    let mut ellipse =
        Ellipse::from_tractor(shape_r, shape_e1, shape_e2, config.min_axis_arcsec).ok()?;
    bounded_axis_ratio(ellipse.axis_ratio, config)?;

    // A row that cannot be rescaled keeps R_e, undersized against NED-LVS D25.
    let size_is_isophotal = match isophotal_semi_major_for(doc, &ellipse, config) {
        Some(a25) => {
            ellipse = ellipse.scaled_to_semi_major(a25, config.min_axis_arcsec);
            true
        }
        None => false,
    };

    Some(GalaxyCandidate {
        ra,
        dec,
        a_arcsec: ellipse.a,
        b_arcsec: ellipse.b,
        pa_deg: ellipse.pa_rad.to_degrees(),
        redshift: opt_f64(doc, "z"),
        redshift_err: opt_f64(doc, "z_unc"),
        dist_mpc: None,
        dist_mpc_method: None,
        mag: None,
        mag_err: None,
        objtype,
        objname: opt_string(doc, "_id"),
        catalog: Some(LS_DR10.to_string()),
        size_is_isophotal,
        diam_survey: None,
        orientation_is_nominal: false,
    })
}

pub const NED_LVS: &str = "NED_LVS";
pub const LS_DR10: &str = "LSDR10";

/// NED-LVS comes first, for its curated diameters and redshift-independent
/// distances. Legacy Survey shreds large galaxies into many rows, each of which
/// would split the posterior among fragments of one galaxy, so a Legacy row
/// inside an accepted NED-LVS galaxy (d_DLR <= 1) is dropped.
pub fn collect_galaxies(
    xmatches: &std::collections::HashMap<String, Vec<Document>>,
    config: &HostGalaxyConfig,
) -> Vec<GalaxyCandidate> {
    let mut galaxies: Vec<GalaxyCandidate> = xmatches
        .get(&config.ned_lvs_catalog)
        .map(|docs| {
            docs.iter()
                .filter_map(|d| galaxy_from_ned_lvs(d, config))
                .collect()
        })
        .unwrap_or_default();

    let Some(ls_docs) = xmatches.get(&config.ls_dr10_catalog) else {
        return galaxies;
    };

    let ned_ellipses: Vec<(f64, f64, Ellipse)> = galaxies
        .iter()
        .filter_map(|g| {
            Ellipse::from_candidate(g, config.min_axis_arcsec)
                .ok()
                .map(|e| (g.ra, g.dec, e))
        })
        .collect();

    for doc in ls_docs {
        let Some(candidate) = galaxy_from_ls_dr10(doc, config) else {
            continue;
        };
        let is_fragment = ned_ellipses.iter().any(|(ra, dec, ellipse)| {
            compute_dlr(candidate.ra, candidate.dec, *ra, *dec, ellipse).fractional_offset <= 1.0
        });
        if !is_fragment {
            galaxies.push(candidate);
        }
    }

    galaxies
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;
    use std::collections::HashMap;

    fn ned_lvs_doc() -> Document {
        doc! {
            "_id": "NGC 4321",
            "ra": 185.728_75,
            "dec": 15.822_3,
            "objtype": "G",
            "z": 0.005_24,
            "z_unc": 0.000_01,
            "Diam": 444.0_f64,
            "Diam_ba": 0.87_f64,
            "Diam_pa": 30.0_f64,
            "Diam_survey": "SGA",
        }
    }

    fn ls_doc(id: &str, ra: f64, dec: f64) -> Document {
        doc! {
            "_id": id,
            "ra": ra,
            "dec": dec,
            "objtype": "SER",
            "shape_r": 1.5_f64,
            "shape_e1": 0.2_f64,
            "shape_e2": 0.0_f64,
        }
    }

    #[test]
    fn test_from_ned_lvs_maps_diameter_to_semi_major() {
        let g = galaxy_from_ned_lvs(&ned_lvs_doc(), &HostGalaxyConfig::default()).unwrap();
        assert_close!(g.a_arcsec, 222.0);
        assert_close!(g.b_arcsec, 222.0 * 0.87);
        assert_close!(g.pa_deg, 30.0);
        assert_close!(g.redshift.unwrap(), 0.005_24);
        assert_eq!(g.objname.as_deref(), Some("NGC 4321"));
        assert_eq!(g.catalog.as_deref(), Some(NED_LVS));
    }

    #[test]
    fn test_from_ned_lvs_rejects_rows_without_a_diameter() {
        // The ~19% of NED-LVS with no diameter arrives as explicit nulls.
        let mut d = ned_lvs_doc();
        d.insert("Diam", Bson::Null);
        d.insert("Diam_ba", Bson::Null);
        d.insert("Diam_pa", Bson::Null);
        assert!(galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).is_none());

        let mut d = ned_lvs_doc();
        d.remove("Diam");
        assert!(galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).is_none());

        let mut d = ned_lvs_doc();
        d.insert("Diam", 0.0_f64);
        assert!(galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).is_none());
    }

    #[test]
    fn test_from_ned_lvs_missing_axis_ratio_is_circular() {
        let mut d = ned_lvs_doc();
        d.insert("Diam_ba", Bson::Null);
        d.insert("Diam_pa", Bson::Null);
        let g = galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).unwrap();
        assert_close!(g.b_arcsec, g.a_arcsec);
        assert_close!(g.pa_deg, 0.0);
    }

    #[test]
    fn test_from_ned_lvs_requires_a_position() {
        let mut d = ned_lvs_doc();
        d.insert("ra", Bson::Null);
        assert!(galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).is_none());
    }

    #[test]
    fn test_from_ned_lvs_empty_strings_are_absent() {
        // Absent string columns arrive as "" from the ingest.
        let mut d = ned_lvs_doc();
        d.insert("objtype", "");
        let g = galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).unwrap();
        assert!(g.objtype.is_none());
    }

    #[test]
    fn test_from_ned_lvs_carries_distance_and_its_method() {
        // NED-LVS fills dist_mpc for every row, only ~1% redshift-independently.
        let mut d = ned_lvs_doc();
        d.insert("DistMpc", 16.8_f64);
        d.insert("DistMpc_method", "zIndependent");
        let g = galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).unwrap();
        assert_close!(g.dist_mpc.unwrap(), 16.8);
        assert_eq!(g.dist_mpc_method.as_deref(), Some("zIndependent"));

        let mut d = ned_lvs_doc();
        d.insert("DistMpc", 3200.0_f64);
        d.insert("DistMpc_method", "Redshift");
        let g = galaxy_from_ned_lvs(&d, &HostGalaxyConfig::default()).unwrap();
        assert_eq!(g.dist_mpc_method.as_deref(), Some("Redshift"));

        let g = galaxy_from_ned_lvs(&ned_lvs_doc(), &HostGalaxyConfig::default()).unwrap();
        assert!(g.dist_mpc.is_none());
        assert!(g.dist_mpc_method.is_none());
    }

    #[test]
    fn test_from_ls_dr10_uses_tractor_shape() {
        let g =
            galaxy_from_ls_dr10(&ls_doc("ls-1", 10.0, 20.0), &HostGalaxyConfig::default()).unwrap();
        assert_close!(g.a_arcsec, 1.5);
        assert_close!(g.b_arcsec, 1.5 * (0.8 / 1.2), epsilon = 1e-9);
        assert_eq!(g.catalog.as_deref(), Some(LS_DR10));
    }

    #[test]
    fn test_from_ls_dr10_excludes_point_sources() {
        let config = HostGalaxyConfig::default();
        let mut d = ls_doc("ls-1", 10.0, 20.0);
        d.insert("objtype", "PSF");
        assert!(galaxy_from_ls_dr10(&d, &config).is_none());

        let config = HostGalaxyConfig {
            exclude_star_like: false,
            ..Default::default()
        };
        assert!(galaxy_from_ls_dr10(&d, &config).is_some());
    }

    #[test]
    fn test_from_ls_dr10_requires_a_shape() {
        let config = HostGalaxyConfig::default();
        let mut d = ls_doc("ls-1", 10.0, 20.0);
        d.insert("shape_r", Bson::Null);
        assert!(galaxy_from_ls_dr10(&d, &config).is_none());
    }

    #[test]
    fn test_collect_prefers_ned_lvs_and_drops_shredded_fragments() {
        let config = HostGalaxyConfig::default();
        // NGC 4321 spans a = 222 arcsec, so rows landing inside it are fragments.
        let inside_a = ls_doc("frag-1", 185.728_75, 15.822_3 + 20.0 / 3600.0);
        let inside_b = ls_doc("frag-2", 185.728_75 + 25.0 / 3600.0, 15.822_3);
        let outside = ls_doc("other", 185.728_75 + 600.0 / 3600.0, 15.822_3);

        let mut xmatches = HashMap::new();
        xmatches.insert(config.ned_lvs_catalog.clone(), vec![ned_lvs_doc()]);
        xmatches.insert(
            config.ls_dr10_catalog.clone(),
            vec![inside_a, inside_b, outside],
        );

        let galaxies = collect_galaxies(&xmatches, &config);

        assert_eq!(galaxies.len(), 2, "fragments should be absorbed");
        assert_eq!(galaxies[0].catalog.as_deref(), Some(NED_LVS));
        assert_eq!(galaxies[1].objname.as_deref(), Some("other"));
    }

    #[test]
    fn test_collect_falls_back_to_ls_when_ned_has_no_shape() {
        let config = HostGalaxyConfig::default();
        let mut no_diam = ned_lvs_doc();
        no_diam.insert("Diam", Bson::Null);

        let mut xmatches = HashMap::new();
        xmatches.insert(config.ned_lvs_catalog.clone(), vec![no_diam]);
        xmatches.insert(
            config.ls_dr10_catalog.clone(),
            vec![ls_doc("ls-1", 185.728_75, 15.822_3)],
        );

        let galaxies = collect_galaxies(&xmatches, &config);
        assert_eq!(galaxies.len(), 1);
        assert_eq!(galaxies[0].catalog.as_deref(), Some(LS_DR10));
    }

    #[test]
    fn test_collect_handles_missing_catalogs() {
        let config = HostGalaxyConfig::default();
        assert!(collect_galaxies(&HashMap::new(), &config).is_empty());
    }
}

#[cfg(test)]
mod projection_tests {
    use super::NED_LVS_REQUIRED_KEYS;

    // These drifted once (`diam` vs `Diam`) and nothing failed: absent reads as 0.
    #[test]
    fn test_config_projects_every_key_the_reader_needs() {
        let config = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yaml"))
            .expect("config.yaml");
        let block = config
            .split("- catalog: NED_LVS")
            .nth(1)
            .expect("a NED_LVS crossmatch entry");
        let projection = block.split("- catalog:").next().expect("entry body");

        for key in NED_LVS_REQUIRED_KEYS {
            assert!(
                projection.contains(&format!("{key}: 1")),
                "NED_LVS projection is missing `{key}`, which the reader depends on"
            );
        }
    }

    #[test]
    fn test_ned_lvs_entry_reads_the_ned_collection() {
        let config = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yaml"))
            .expect("config.yaml");
        let block = config
            .split("- catalog: NED_LVS")
            .nth(1)
            .expect("a NED_LVS crossmatch entry");
        assert!(
            block
                .split("- catalog:")
                .next()
                .unwrap()
                .contains("collection: NED"),
            "NED_LVS must read the NED collection; there is no NED_LVS collection"
        );
    }
}

#[cfg(test)]
mod legacy_shape_tests {
    use super::*;
    use mongodb::bson::doc;

    /// A REX row that passes every cut: resolved, well measured, unblended.
    fn good_rex() -> Document {
        doc! {
            "_id": "ls-rex",
            "ra": 10.0,
            "dec": 20.0,
            "objtype": "REX",
            "shape_r": 1.2_f64,
            "shape_e1": 0.0_f64,
            "shape_e2": 0.0_f64,
            "flux_r": 100.0_f64,
            "flux_ivar_r": 1.0_f64,
            "fracflux_r": 0.1_f64,
        }
    }

    fn accepted(doc: &Document) -> bool {
        galaxy_from_ls_dr10(doc, &HostGalaxyConfig::default()).is_some()
    }

    #[test]
    fn test_well_measured_rex_is_kept() {
        assert!(accepted(&good_rex()));
    }

    #[test]
    fn test_small_rex_is_rejected() {
        let mut d = good_rex();
        d.insert("shape_r", 0.2_f64);
        assert!(!accepted(&d));
    }

    #[test]
    fn test_low_snr_rex_is_rejected() {
        let mut d = good_rex();
        // snr = flux * sqrt(ivar) = 2, below the default cut of 5.
        d.insert("flux_r", 2.0_f64);
        d.insert("flux_ivar_r", 1.0_f64);
        assert!(!accepted(&d));
    }

    #[test]
    fn test_blended_rex_is_rejected() {
        let mut d = good_rex();
        d.insert("fracflux_r", 0.9_f64);
        assert!(!accepted(&d));
    }

    #[test]
    fn test_rex_without_a_signal_to_noise_measurement_is_still_judged_on_size() {
        for key in ["flux_r", "flux_ivar_r"] {
            let mut d = good_rex();
            d.remove(key);
            assert!(
                accepted(&d),
                "REX missing {key} has no S/N to judge, so size and blending decide"
            );
        }

        let mut small = good_rex();
        small.remove("flux_ivar_r");
        small.insert("shape_r", 0.05_f64);
        assert!(!accepted(&small), "an unmeasurable REX is still too small");
    }

    #[test]
    fn test_rex_cuts_do_not_apply_to_other_types() {
        for objtype in ["EXP", "DEV", "SER"] {
            let mut d = good_rex();
            d.insert("objtype", objtype);
            d.insert("shape_r", 0.2_f64);
            d.insert("flux_r", 2.0_f64);
            assert!(accepted(&d), "{objtype} should not face the REX cuts");
        }
    }

    #[test]
    fn test_legacy_shape_is_rescaled_to_the_isophote() {
        let mut d = good_rex();
        d.insert("shape_r", 2.0_f64);
        d.insert("flux_r", 1000.0_f64); // ~15 mag, comfortably above the isophote
        let g = galaxy_from_ls_dr10(&d, &HostGalaxyConfig::default()).expect("kept");
        assert!(
            g.a_arcsec > 2.0,
            "semi-major {} should exceed R_e = 2 after conversion",
            g.a_arcsec
        );
    }

    #[test]
    fn test_row_without_flux_keeps_its_half_light_radius() {
        let mut d = good_rex();
        d.insert("objtype", "EXP");
        d.remove("flux_r");
        let g = galaxy_from_ls_dr10(&d, &HostGalaxyConfig::default()).expect("kept");
        assert!((g.a_arcsec - 1.2).abs() < 1e-9, "got {}", g.a_arcsec);
    }

    #[test]
    fn test_conversion_preserves_the_axis_ratio() {
        let mut d = good_rex();
        d.insert("objtype", "EXP");
        d.insert("shape_e1", 0.3_f64);
        d.insert("flux_r", 1000.0_f64);
        let mut unconverted = d.clone();
        unconverted.remove("flux_r");

        let converted = galaxy_from_ls_dr10(&d, &HostGalaxyConfig::default()).unwrap();
        let plain = galaxy_from_ls_dr10(&unconverted, &HostGalaxyConfig::default()).unwrap();
        let q_converted = converted.b_arcsec / converted.a_arcsec;
        let q_plain = plain.b_arcsec / plain.a_arcsec;
        assert!(
            (q_converted - q_plain).abs() < 1e-9,
            "axis ratio changed: {q_converted} vs {q_plain}"
        );
        assert!(converted.a_arcsec > plain.a_arcsec);
    }
}

#[cfg(test)]
mod review_tests {
    use super::*;
    use mongodb::bson::doc;

    fn ned(objtype: &str, ba: f64, survey: &str) -> Document {
        doc! {
            "_id": "NGC 1234", "ra": 10.0, "dec": 20.0,
            "Diam": 60.0, "Diam_ba": ba, "Diam_pa": 90.0,
            "objtype": objtype, "Diam_survey": survey,
        }
    }

    #[test]
    fn test_non_host_object_types_are_excluded() {
        let config = HostGalaxyConfig::default();
        for objtype in ["QSO", "AbLS", "EmLS", "EmObj", "Q_Lens", "G_Lens"] {
            assert!(
                galaxy_from_ned_lvs(&ned(objtype, 0.5, "SDSS"), &config).is_none(),
                "{objtype} should not be a host candidate"
            );
        }
        assert!(galaxy_from_ned_lvs(&ned("G", 0.5, "SDSS"), &config).is_some());
    }

    #[test]
    fn test_axis_ratio_is_bounded_rather_than_the_minor_axis() {
        let config = HostGalaxyConfig::default();

        assert!(galaxy_from_ned_lvs(&ned("G", 0.02, "SDSS"), &config).is_none());

        let pinned = galaxy_from_ned_lvs(&ned("G", 0.07, "SDSS"), &config).expect("pinned");
        assert_close!(pinned.a_arcsec, 30.0);
        assert_close!(pinned.b_arcsec, 30.0 * config.pinned_axis_ratio);

        let kept = galaxy_from_ned_lvs(&ned("G", 0.4, "SDSS"), &config).expect("kept");
        assert_close!(kept.b_arcsec, 30.0 * 0.4);
    }

    #[test]
    fn test_a_2mass_orientation_is_flagged_as_nominal() {
        let config = HostGalaxyConfig::default();

        let two_mass = galaxy_from_ned_lvs(&ned("G", 0.4, "2MASS"), &config).expect("2mass");
        assert_eq!(two_mass.diam_survey.as_deref(), Some("2MASS"));
        assert!(two_mass.orientation_is_nominal);

        // Round: the position angle carries no information either way.
        let round = galaxy_from_ned_lvs(&ned("G", 1.0, "2MASS"), &config).expect("round");
        assert!(!round.orientation_is_nominal);

        let sdss = galaxy_from_ned_lvs(&ned("G", 0.4, "SDSS"), &config).expect("sdss");
        assert!(!sdss.orientation_is_nominal);
    }

    #[test]
    fn test_duplicate_rows_are_excluded_with_point_sources() {
        let config = HostGalaxyConfig::default();
        for objtype in ["PSF", "DUP"] {
            let d = doc! {
                "_id": "x", "ra": 10.0, "dec": 20.0, "objtype": objtype,
                "shape_r": 2.0, "shape_e1": 0.1, "shape_e2": 0.0, "flux_r": 100.0,
            };
            assert!(
                galaxy_from_ls_dr10(&d, &config).is_none(),
                "{objtype} has no galaxy extent"
            );
        }
    }

    #[test]
    fn test_a_half_light_fallback_is_reported() {
        let config = HostGalaxyConfig::default();
        let base = doc! {
            "_id": "x", "ra": 10.0, "dec": 20.0, "objtype": "EXP",
            "shape_r": 2.0, "shape_e1": 0.1, "shape_e2": 0.0, "flux_r": 100.0,
        };
        let converted = galaxy_from_ls_dr10(&base, &config).expect("converted");
        assert!(converted.size_is_isophotal);

        // No flux, so no total magnitude, so no isophote.
        let mut no_flux = base.clone();
        no_flux.remove("flux_r");
        let fallback = galaxy_from_ls_dr10(&no_flux, &config).expect("still a candidate");
        assert!(!fallback.size_is_isophotal);

        // SER rows carry no Sersic index in the current ingest.
        let mut ser = base.clone();
        ser.insert("objtype", "SER");
        let ser = galaxy_from_ls_dr10(&ser, &config).expect("ser");
        assert!(!ser.size_is_isophotal);
    }
}
