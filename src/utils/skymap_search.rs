//! Scoring an alert position against a gravitational-wave localization.
//!
//! The credible level is not expressible as a database stage: it needs the
//! skymap's per-pixel probability density and, in three dimensions, a distance
//! for the alert. So a query narrows candidates with the localization's MOC and
//! the exact level is computed here over what comes back.

use crate::utils::spatial::get_f64_from_doc;
use crate::utils::{
    cosmology::luminosity_distance_mpc,
    moc::{CredibleVolumeIndex, LIGO3dskymap},
};
use mongodb::bson::Document;

/// Host-galaxy redshifts from an alert's `cross_matches`, best-first and
/// deduplicated by 3-arcsec proximity so one galaxy isn't counted per catalog.
///
/// Priority: 0 = DESI spec (zwarn=0), 1 = NED SPEC, 2 = DESI spec (zwarn!=0)
/// and LSDR10 spec, 3 = NED PHOT, 4 = LSDR10 photo-z.
pub fn extract_host_redshifts(cross_matches: Option<&Document>) -> Vec<f64> {
    /// `priority` ranks a row (lower = better) or returns None to skip it.
    fn extract_catalog_zs(
        cross_matches: Option<&Document>,
        catalog: &str,
        z_field: &str,
        priority: impl Fn(&Document) -> Option<u8>,
        ranked: &mut Vec<(u8, f64, f64, f64)>,
    ) {
        let Some(arr) = cross_matches.and_then(|cm| cm.get_array(catalog).ok()) else {
            return;
        };
        for v in arr {
            let Some(m) = v.as_document() else { continue };
            let Some(p) = priority(m) else { continue };
            // Document::get_f64 rejects the Int32 these catalogs sometimes store.
            let Some(z) = get_f64_from_doc(m, z_field).filter(|&z| z > 0.0) else {
                continue;
            };
            let Some(ra) = get_f64_from_doc(m, "ra") else {
                continue;
            };
            let Some(dec) = get_f64_from_doc(m, "dec") else {
                continue;
            };
            ranked.push((p, ra, dec, z));
        }
    }

    let mut ranked: Vec<(u8, f64, f64, f64)> = Vec::new(); // (priority, ra, dec, z)

    extract_catalog_zs(
        cross_matches,
        "DESI_DR1",
        "z",
        |m| {
            if m.get_str("spectype").map(|s| s == "STAR").unwrap_or(false) {
                return None;
            }
            Some(if get_f64_from_doc(m, "zwarn").unwrap_or(1.0) == 0.0 {
                0
            } else {
                2
            })
        },
        &mut ranked,
    );
    extract_catalog_zs(
        cross_matches,
        "NED",
        "z",
        |m| {
            Some(
                if m.get_str("z_tech").map(|s| s == "SPEC").unwrap_or(false) {
                    1
                } else {
                    3
                },
            )
        },
        &mut ranked,
    );
    // A Legacy row carrying both is deduplicated below, keeping the spectroscopic one.
    extract_catalog_zs(cross_matches, "LSDR10", "z_spec", |_| Some(2), &mut ranked);
    extract_catalog_zs(
        cross_matches,
        "LSDR10",
        "z_phot_median",
        |_| Some(4),
        &mut ranked,
    );

    ranked.sort_by_key(|&(p, _, _, _)| p);
    const DEDUP_ARCSEC: f64 = 3.0;
    let mut kept: Vec<(f64, f64, f64)> = Vec::new(); // (ra, dec, z)
    for (_, ra, dec, z) in ranked {
        let is_dup = kept.iter().any(|&(kra, kdec, _)| {
            let dra = (ra - kra) * dec.to_radians().cos();
            let ddec = dec - kdec;
            (dra * dra + ddec * ddec).sqrt() * 3600.0 < DEDUP_ARCSEC
        });
        if !is_dup {
            kept.push((ra, dec, z));
        }
    }
    kept.into_iter().map(|(_, _, z)| z).collect()
}

/// Credible level of an alert position, or `None` when nothing places it.
///
/// Uses the host redshift, since a position's level is only meaningful at a
/// distance; the best (smallest) over the hosts is taken, as a single galaxy
/// among several is enough to place the alert inside the volume. `None` means
/// undetermined, not outside: an alert with no host has only the 2D test, which
/// the caller has already applied through the MOC.
pub fn credible_level_at(
    skymap: &LIGO3dskymap,
    idx: &CredibleVolumeIndex,
    ra: f64,
    dec: f64,
    host_galaxy: Option<&Document>,
    cross_matches: Option<&Document>,
) -> Option<f64> {
    let best = host_redshifts(host_galaxy, cross_matches)
        .into_iter()
        .filter_map(|z| idx.searched_prob_vol_at(skymap, ra, dec, luminosity_distance_mpc(z)))
        .fold(f64::INFINITY, f64::min);
    best.is_finite().then_some(best)
}

/// The redshifts to place the alert at.
///
/// A directional-light-radius association names the galaxy the transient sits
/// in, so where there is one its redshift is the only distance worth trying.
/// Without one nothing says which neighbour is the host, and the level is taken
/// at whichever catalogued redshift places the alert deepest in the volume --
/// an upper bound on how well it fits, over every galaxy that could be the host.
pub fn host_redshifts(
    host_galaxy: Option<&Document>,
    cross_matches: Option<&Document>,
) -> Vec<f64> {
    let dlr_z = host_galaxy
        .and_then(|h| h.get_document("best_host").ok())
        .and_then(|b| get_f64_from_doc(b, "z"))
        .filter(|&z| z > 0.0);
    match dlr_z {
        Some(z) => vec![z],
        None => extract_host_redshifts(cross_matches),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::moc::parse_3d_skymap;
    use mongodb::bson::doc;

    /// A position deep in the localization, at a distance the map likes, scores
    /// a low credible level; the same position with no host scores nothing.
    #[test]
    fn test_a_host_places_the_alert_in_the_volume() {
        let skymap =
            parse_3d_skymap("./data/S240618ah_bayestar.fits").expect("the test skymap parses");
        let idx = CredibleVolumeIndex::build(&skymap, 200);

        // The highest-probability pixel that carries a usable distance fit.
        let best = skymap
            .prob
            .iter()
            .enumerate()
            .filter(|&(i, &p)| {
                p > 0.0 && skymap.distsigma[i].is_finite() && skymap.distsigma[i] > 0.0
            })
            .max_by(|a, b| a.1.partial_cmp(b.1).expect("probabilities compare"))
            .map(|(i, _)| i)
            .expect("the map has a usable pixel");
        let uniq = skymap.uniq[best];
        let (order, ipix) = (
            crate::utils::moc::uniq_to_order(uniq),
            crate::utils::moc::uniq_to_ipix(uniq),
        );
        let (lon, lat) = cdshealpix::nested::center(order, ipix);
        let (ra, dec) = (lon.to_degrees(), lat.to_degrees());

        // The redshift whose luminosity distance matches the pixel's own estimate.
        let d_mpc = skymap.distmu[best];
        assert!(d_mpc.is_finite() && d_mpc > 0.0, "the pixel has a distance");
        let z = {
            let (mut lo, mut hi) = (1e-4, 1.0);
            for _ in 0..60 {
                let mid = 0.5 * (lo + hi);
                if luminosity_distance_mpc(mid) < d_mpc {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            0.5 * (lo + hi)
        };

        let with_host = doc! { "NED": [ doc! { "ra": ra, "dec": dec, "z": z, "z_tech": "SPEC" } ] };
        let level = credible_level_at(&skymap, &idx, ra, dec, None, Some(&with_host))
            .expect("a host places the alert");
        assert!(
            (0.0..=1.0).contains(&level),
            "credible level {level} is out of range"
        );
        assert!(
            level < 0.9,
            "the peak pixel should be well inside, got {level}"
        );

        // No cross-matches at all: nothing to place it with.
        assert!(credible_level_at(&skymap, &idx, ra, dec, None, None).is_none());
    }

    /// A directional-light-radius association names the host, so its redshift is
    /// used alone: the catalogue sweep would otherwise take whichever neighbour
    /// places the alert deepest in the volume, which is a different question.
    #[test]
    fn test_the_dlr_host_overrides_the_catalogue_sweep() {
        let cross_matches = doc! {
            "NED": [ doc! { "ra": 10.0, "dec": 20.0, "z": 0.05, "z_tech": "SPEC" } ]
        };
        let host = doc! { "best_host": doc! { "ra": 10.0, "dec": 20.0, "z": 0.2 } };

        assert_eq!(
            host_redshifts(Some(&host), Some(&cross_matches)),
            vec![0.2],
            "the associated host's redshift should be the only one tried"
        );
        // And without an association every catalogued redshift is a candidate.
        assert_eq!(
            host_redshifts(None, Some(&cross_matches)),
            vec![0.05],
            "the sweep should still run when nothing is associated"
        );
    }

    /// An association that never found a galaxy, or one carrying no redshift,
    /// must not suppress the sweep -- that would lose the only distance there is.
    #[test]
    fn test_a_hostless_association_falls_back_to_the_sweep() {
        let cross_matches = doc! {
            "NED": [ doc! { "ra": 10.0, "dec": 20.0, "z": 0.05, "z_tech": "SPEC" } ]
        };
        for host in [
            doc! {},
            doc! { "best_host": doc! { "ra": 10.0, "dec": 20.0 } },
            doc! { "best_host": doc! { "ra": 10.0, "dec": 20.0, "z": 0.0 } },
        ] {
            assert_eq!(
                host_redshifts(Some(&host), Some(&cross_matches)),
                vec![0.05],
                "fell through to nothing for {host:?}"
            );
        }
    }
}
