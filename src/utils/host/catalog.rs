//! Turning cross-match documents into [`GalaxyCandidate`]s.
//!
//! Two catalogs supply galaxy shapes, and they describe them differently:
//! NED-LVS gives an angular diameter with an axis ratio and position angle,
//! while Legacy Survey gives Tractor half-light radius plus ellipticity
//! components. Both end up as the same semi-axes + PA.

use mongodb::bson::{Bson, Document};

use super::config::HostGalaxyConfig;
use super::dlr::compute_dlr;
use super::ellipse::Ellipse;
use super::types::GalaxyCandidate;

/// Read a numeric field, treating missing, null and non-finite alike as absent.
///
/// The NED-LVS ingest writes explicit nulls for absent quantities rather than
/// omitting the key, so both shapes have to be handled.
fn opt_f64(doc: &Document, key: &str) -> Option<f64> {
    let value = match doc.get(key) {
        Some(Bson::Double(v)) => *v,
        Some(Bson::Int32(v)) => *v as f64,
        Some(Bson::Int64(v)) => *v as f64,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

/// Read a string field, treating missing, null and empty alike as absent.
fn opt_string(doc: &Document, key: &str) -> Option<String> {
    match doc.get(key) {
        Some(Bson::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// Convert a NED-LVS cross-match document into a [`GalaxyCandidate`].
///
/// Returns `None` when the row cannot support a DLR — no position, or no
/// usable angular diameter. About a fifth of NED-LVS carries no diameter, so
/// this is an ordinary outcome rather than an error.
///
/// Field mapping, following the NED-LVS column definitions:
/// - `diam` is the angular *major-axis diameter* (2a) in arcsec, so the
///   semi-major axis is `diam / 2`.
/// - `diam_ba` is the minor-to-major axis ratio, giving `b = a * (b/a)`.
/// - `diam_pa` is the ellipse position angle in degrees east of north.
pub fn galaxy_from_ned_lvs(doc: &Document) -> Option<GalaxyCandidate> {
    let ra = opt_f64(doc, "ra")?;
    let dec = opt_f64(doc, "dec")?;

    let diam = opt_f64(doc, "diam").filter(|d| *d > 0.0)?;
    let a_arcsec = diam / 2.0;

    // A missing axis ratio means we know the size but not the elongation;
    // treating it as circular is the neutral choice.
    let axis_ratio = opt_f64(doc, "diam_ba")
        .filter(|r| *r > 0.0 && *r <= 1.0)
        .unwrap_or(1.0);
    let b_arcsec = a_arcsec * axis_ratio;
    let pa_deg = opt_f64(doc, "diam_pa").unwrap_or(0.0);

    Some(GalaxyCandidate {
        ra,
        dec,
        a_arcsec,
        b_arcsec,
        pa_deg,
        redshift: opt_f64(doc, "z"),
        redshift_err: opt_f64(doc, "z_unc"),
        dist_mpc: opt_f64(doc, "dist_mpc"),
        dist_mpc_method: opt_string(doc, "dist_mpc_method"),
        mag: opt_f64(doc, "m_Ks"),
        mag_err: opt_f64(doc, "m_Ks_unc"),
        objtype: opt_string(doc, "objtype"),
        objname: opt_string(doc, "_id"),
        catalog: Some(NED_LVS.to_string()),
        shape_from_image: false,
    })
}

/// Convert a Legacy Survey (Tractor) cross-match document into a
/// [`GalaxyCandidate`].
///
/// Returns `None` for rows without a position or a usable `shape_r`, and — when
/// `exclude_star_like` is set — for rows whose morphological `type` marks them
/// as point sources, which have no meaningful galaxy extent.
pub fn galaxy_from_ls_dr10(doc: &Document, config: &HostGalaxyConfig) -> Option<GalaxyCandidate> {
    let ra = opt_f64(doc, "ra")?;
    let dec = opt_f64(doc, "dec")?;

    let objtype = opt_string(doc, "type");
    if config.exclude_star_like {
        if let Some(t) = objtype.as_deref() {
            if t == config.star_type_value {
                return None;
            }
        }
    }

    let shape_r = opt_f64(doc, "shape_r").filter(|r| *r > 0.0)?;
    let shape_e1 = opt_f64(doc, "shape_e1").unwrap_or(0.0);
    let shape_e2 = opt_f64(doc, "shape_e2").unwrap_or(0.0);

    let ellipse =
        Ellipse::from_tractor(shape_r, shape_e1, shape_e2, config.min_axis_arcsec).ok()?;

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
        shape_from_image: false,
    })
}

/// Catalog label recorded on candidates sourced from NED-LVS.
pub const NED_LVS: &str = "NED_LVS";
/// Catalog label recorded on candidates sourced from Legacy Survey DR10.
pub const LS_DR10: &str = "LS_DR10";

/// Build the candidate list from a survey's cross-match results.
///
/// NED-LVS is preferred: it carries curated diameters and redshift-independent
/// distances for exactly the nearby, well-resolved galaxies host association
/// cares most about. Legacy Survey then fills in the sky NED-LVS does not
/// cover — either because the galaxy is absent or because its row has no
/// diameter.
///
/// The two are not simply concatenated. Legacy Survey shreds large galaxies
/// into many catalog rows, so a bright nearby host can appear as dozens of
/// Tractor sources sitting on top of its NED-LVS entry. Each would be scored as
/// an independent candidate and split the posterior among fragments of one
/// galaxy. Any Legacy Survey row falling inside an accepted NED-LVS galaxy
/// (d_DLR <= 1, i.e. within its light radius in that direction) is therefore
/// dropped as a fragment of it.
pub fn collect_galaxies(
    xmatches: &std::collections::HashMap<String, Vec<Document>>,
    config: &HostGalaxyConfig,
) -> Vec<GalaxyCandidate> {
    let mut galaxies: Vec<GalaxyCandidate> = xmatches
        .get(&config.ned_lvs_catalog)
        .map(|docs| docs.iter().filter_map(galaxy_from_ned_lvs).collect())
        .unwrap_or_default();

    let Some(ls_docs) = xmatches.get(&config.ls_dr10_catalog) else {
        return galaxies;
    };

    // Precompute the NED-LVS ellipses once; every Legacy Survey row is tested
    // against all of them.
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

    /// A row shaped like the NED-LVS ingest writes them, nulls included.
    fn ned_lvs_doc() -> Document {
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
            "diam_survey": "SGA",
        }
    }

    fn ls_doc(id: &str, ra: f64, dec: f64) -> Document {
        doc! {
            "_id": id,
            "ra": ra,
            "dec": dec,
            "type": "SER",
            "shape_r": 1.5_f64,
            "shape_e1": 0.2_f64,
            "shape_e2": 0.0_f64,
        }
    }

    #[test]
    fn test_from_ned_lvs_maps_diameter_to_semi_major() {
        let g = galaxy_from_ned_lvs(&ned_lvs_doc()).unwrap();
        // diam is the full major-axis diameter (2a), so a = diam/2
        assert_close!(g.a_arcsec, 222.0);
        assert_close!(g.b_arcsec, 222.0 * 0.87);
        assert_close!(g.pa_deg, 30.0);
        assert_close!(g.redshift.unwrap(), 0.005_24);
        assert_eq!(g.objname.as_deref(), Some("NGC 4321"));
        assert_eq!(g.catalog.as_deref(), Some(NED_LVS));
        assert!(!g.shape_from_image);
    }

    #[test]
    fn test_from_ned_lvs_rejects_rows_without_a_diameter() {
        // The ~19% of NED-LVS with no diameter arrives as explicit nulls.
        let mut d = ned_lvs_doc();
        d.insert("diam", Bson::Null);
        d.insert("diam_ba", Bson::Null);
        d.insert("diam_pa", Bson::Null);
        assert!(galaxy_from_ned_lvs(&d).is_none());

        // Omitted entirely (pre-null-fix documents) must behave the same.
        let mut d = ned_lvs_doc();
        d.remove("diam");
        assert!(galaxy_from_ned_lvs(&d).is_none());

        // A zero diameter is not a usable shape either.
        let mut d = ned_lvs_doc();
        d.insert("diam", 0.0_f64);
        assert!(galaxy_from_ned_lvs(&d).is_none());
    }

    #[test]
    fn test_from_ned_lvs_missing_axis_ratio_is_circular() {
        let mut d = ned_lvs_doc();
        d.insert("diam_ba", Bson::Null);
        d.insert("diam_pa", Bson::Null);
        let g = galaxy_from_ned_lvs(&d).unwrap();
        assert_close!(g.b_arcsec, g.a_arcsec);
        assert_close!(g.pa_deg, 0.0);
    }

    #[test]
    fn test_from_ned_lvs_requires_a_position() {
        let mut d = ned_lvs_doc();
        d.insert("ra", Bson::Null);
        assert!(galaxy_from_ned_lvs(&d).is_none());
    }

    #[test]
    fn test_from_ned_lvs_empty_strings_are_absent() {
        // Absent string columns arrive as "" from the ingest.
        let mut d = ned_lvs_doc();
        d.insert("objtype", "");
        let g = galaxy_from_ned_lvs(&d).unwrap();
        assert!(g.objtype.is_none());
    }

    #[test]
    fn test_from_ned_lvs_carries_distance_and_its_method() {
        // The method matters: NED-LVS populates dist_mpc for every row, but
        // only ~1% is genuinely redshift-independent.
        let mut d = ned_lvs_doc();
        d.insert("dist_mpc", 16.8_f64);
        d.insert("dist_mpc_method", "zIndependent");
        let g = galaxy_from_ned_lvs(&d).unwrap();
        assert_close!(g.dist_mpc.unwrap(), 16.8);
        assert_eq!(g.dist_mpc_method.as_deref(), Some("zIndependent"));

        let mut d = ned_lvs_doc();
        d.insert("dist_mpc", 3200.0_f64);
        d.insert("dist_mpc_method", "Redshift");
        let g = galaxy_from_ned_lvs(&d).unwrap();
        assert_eq!(g.dist_mpc_method.as_deref(), Some("Redshift"));

        let g = galaxy_from_ned_lvs(&ned_lvs_doc()).unwrap();
        assert!(g.dist_mpc.is_none());
        assert!(g.dist_mpc_method.is_none());
    }

    #[test]
    fn test_from_ls_dr10_uses_tractor_shape() {
        let g =
            galaxy_from_ls_dr10(&ls_doc("ls-1", 10.0, 20.0), &HostGalaxyConfig::default()).unwrap();
        assert_close!(g.a_arcsec, 1.5);
        // e = 0.2 -> q = (1-0.2)/(1+0.2) = 2/3
        assert_close!(g.b_arcsec, 1.5 * (0.8 / 1.2), epsilon = 1e-9);
        assert_eq!(g.catalog.as_deref(), Some(LS_DR10));
    }

    #[test]
    fn test_from_ls_dr10_excludes_point_sources() {
        let config = HostGalaxyConfig::default();
        let mut d = ls_doc("ls-1", 10.0, 20.0);
        d.insert("type", "PSF");
        assert!(galaxy_from_ls_dr10(&d, &config).is_none());

        // ...unless the exclusion is turned off
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
        // NGC 4321 spans a = 222 arcsec. Legacy Survey rows landing inside it
        // are fragments of the same galaxy, not independent hosts.
        let inside_a = ls_doc("frag-1", 185.728_75, 15.822_3 + 20.0 / 3600.0);
        let inside_b = ls_doc("frag-2", 185.728_75 + 25.0 / 3600.0, 15.822_3);
        // A genuinely separate galaxy well outside it.
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
        // NED-LVS row present but with no diameter -> contributes nothing, so
        // the Legacy Survey rows must not be suppressed by it.
        let mut no_diam = ned_lvs_doc();
        no_diam.insert("diam", Bson::Null);

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
