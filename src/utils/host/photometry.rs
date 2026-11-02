use std::collections::HashMap;

use apache_avro_macros::serdavro;
use flare::spatial::great_circle_distance;
use mongodb::bson::Document;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::conf::{self, CatalogXmatchConfig};
use crate::utils::enums::Survey;
use crate::utils::spatial::{get_opt_f64_from_doc as opt_f64, xmatch};

use super::association::{HostGalaxyAssociation, StoredHostCandidate};
use super::catalog::{opt_objname, LS_DR10};
use super::config::HostGalaxyConfig;

const LS_NANOMAGGY_ZERO_POINT: f64 = 22.5;
const MAG_ERR_PER_INVERSE_SNR: f64 = 2.5 / std::f64::consts::LN_10;

/// Observed AB magnitudes, not corrected for Galactic extinction.
#[serdavro]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostPhotometry {
    pub catalog: String,
    pub source_id: Option<String>,
    /// `psf` (Pan-STARRS) misses part of an extended galaxy's light, `model` does not.
    pub mag_type: String,
    pub sep_arcsec: f64,
    pub mag_g: Option<f64>,
    pub mag_g_err: Option<f64>,
    pub mag_r: Option<f64>,
    pub mag_r_err: Option<f64>,
    pub mag_i: Option<f64>,
    pub mag_i_err: Option<f64>,
    pub mag_z: Option<f64>,
    pub mag_z_err: Option<f64>,
    pub mag_y: Option<f64>,
    pub mag_y_err: Option<f64>,
}

impl HostPhotometry {
    fn has_magnitude(&self) -> bool {
        [self.mag_g, self.mag_r, self.mag_i, self.mag_z, self.mag_y]
            .iter()
            .any(Option::is_some)
    }
}

fn legacy_mag(doc: &Document, band: &str) -> Option<f64> {
    let flux = opt_f64(doc, &format!("flux_{band}")).filter(|f| *f > 0.0)?;
    Some(LS_NANOMAGGY_ZERO_POINT - 2.5 * flux.log10())
}

fn legacy_mag_err(doc: &Document, band: &str) -> Option<f64> {
    let flux = opt_f64(doc, &format!("flux_{band}")).filter(|f| *f > 0.0)?;
    let ivar = opt_f64(doc, &format!("flux_ivar_{band}")).filter(|v| *v > 0.0)?;
    Some(MAG_ERR_PER_INVERSE_SNR / (flux * ivar.sqrt()))
}

/// Pan-STARRS stores -999 for an unmeasured band.
fn ps1_value(doc: &Document, key: &str) -> Option<f64> {
    opt_f64(doc, key).filter(|v| *v > 0.0)
}

pub fn from_legacy(doc: &Document, catalog: &str, sep_arcsec: f64) -> Option<HostPhotometry> {
    let photometry = HostPhotometry {
        catalog: catalog.to_string(),
        source_id: opt_objname(doc, "_id"),
        mag_type: "model".to_string(),
        sep_arcsec,
        mag_g: legacy_mag(doc, "g"),
        mag_g_err: legacy_mag_err(doc, "g"),
        mag_r: legacy_mag(doc, "r"),
        mag_r_err: legacy_mag_err(doc, "r"),
        mag_i: legacy_mag(doc, "i"),
        mag_i_err: legacy_mag_err(doc, "i"),
        mag_z: legacy_mag(doc, "z"),
        mag_z_err: legacy_mag_err(doc, "z"),
        mag_y: None,
        mag_y_err: None,
    };
    photometry.has_magnitude().then_some(photometry)
}

pub fn from_ps1(doc: &Document, catalog: &str, sep_arcsec: f64) -> Option<HostPhotometry> {
    let photometry = HostPhotometry {
        catalog: catalog.to_string(),
        source_id: opt_objname(doc, "_id"),
        mag_type: "psf".to_string(),
        sep_arcsec,
        mag_g: ps1_value(doc, "gMeanPSFMag"),
        mag_g_err: ps1_value(doc, "gMeanPSFMagErr"),
        mag_r: ps1_value(doc, "rMeanPSFMag"),
        mag_r_err: ps1_value(doc, "rMeanPSFMagErr"),
        mag_i: ps1_value(doc, "iMeanPSFMag"),
        mag_i_err: ps1_value(doc, "iMeanPSFMagErr"),
        mag_z: ps1_value(doc, "zMeanPSFMag"),
        mag_z_err: ps1_value(doc, "zMeanPSFMagErr"),
        mag_y: ps1_value(doc, "yMeanPSFMag"),
        mag_y_err: ps1_value(doc, "yMeanPSFMagErr"),
    };
    photometry.has_magnitude().then_some(photometry)
}

fn nearest(docs: &[Document], ra: f64, dec: f64, max_arcsec: f64) -> Option<(&Document, f64)> {
    docs.iter()
        .filter_map(|doc| {
            let sep =
                great_circle_distance(ra, dec, opt_f64(doc, "ra")?, opt_f64(doc, "dec")?) * 3600.0;
            (sep <= max_arcsec).then_some((doc, sep))
        })
        .min_by(|a, b| a.1.total_cmp(&b.1))
}

pub fn photometry_from_xmatches(
    best: &StoredHostCandidate,
    xmatches: &HashMap<String, Vec<Document>>,
    config: &HostGalaxyConfig,
) -> Option<HostPhotometry> {
    let rows = xmatches.get(&config.ls_dr10_catalog)?;
    let own_row = (best.catalog.as_deref() == Some(LS_DR10))
        .then_some(best.objname.as_deref())
        .flatten()
        .and_then(|id| {
            rows.iter()
                .find(|doc| opt_objname(doc, "_id").as_deref() == Some(id))
        });
    let (row, sep) = match own_row {
        Some(row) => (row, 0.0),
        None => nearest(rows, best.ra, best.dec, config.photometry_match_arcsec)?,
    };
    from_legacy(row, &config.ls_dr10_catalog, sep)
}

async fn photometry_from_ps1(
    best: &StoredHostCandidate,
    config: &HostGalaxyConfig,
    xmatch_configs: &[CatalogXmatchConfig],
    survey: &Survey,
    object_id: &str,
    db: &mongodb::Database,
) -> Option<HostPhotometry> {
    let mut ps1 = xmatch_configs
        .iter()
        .find(|c| c.catalog == config.ps1_catalog)?
        .clone();
    ps1.radius = conf::arcsec_to_radians(config.photometry_match_arcsec);
    ps1.use_distance = false;
    ps1.angular_size_key = None;
    ps1.max_results = None;

    let matches = match xmatch(
        best.ra,
        best.dec,
        object_id,
        survey,
        std::slice::from_ref(&ps1),
        db,
    )
    .await
    {
        Ok(matches) => matches,
        Err(e) => {
            warn!(object_id, error = %e, "host photometry lookup failed");
            return None;
        }
    };
    let rows = matches.get(&ps1.catalog)?;
    let (row, sep) = nearest(rows, best.ra, best.dec, config.photometry_match_arcsec)?;
    from_ps1(row, &ps1.catalog, sep)
}

pub async fn attach_photometry(
    association: &mut HostGalaxyAssociation,
    xmatches: &HashMap<String, Vec<Document>>,
    config: &HostGalaxyConfig,
    xmatch_configs: &[CatalogXmatchConfig],
    survey: &Survey,
    object_id: &str,
    db: &mongodb::Database,
) {
    let Some(best) = association.best_host.as_ref() else {
        return;
    };
    let photometry = match photometry_from_xmatches(best, xmatches, config) {
        Some(photometry) => Some(photometry),
        None => photometry_from_ps1(best, config, xmatch_configs, survey, object_id, db).await,
    };
    if photometry.is_none() {
        return;
    }
    if let Some(first) = association.candidates.first_mut() {
        first.photometry = photometry.clone();
    }
    if let Some(best) = association.best_host.as_mut() {
        best.photometry = photometry;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    fn best_host(catalog: &str, objname: &str, ra: f64, dec: f64) -> StoredHostCandidate {
        StoredHostCandidate {
            objname: Some(objname.to_string()),
            catalog: Some(catalog.to_string()),
            objtype: Some("G".to_string()),
            size_is_isophotal: true,
            orientation_is_nominal: false,
            ra,
            dec,
            sep_arcsec: 10.0,
            sep_kpc: None,
            dlr_arcsec: 10.0,
            d_dlr: 1.0,
            dlr_rank: 1,
            posterior: 0.7,
            z: None,
            dist_mpc: None,
            dist_mpc_method: None,
            a_arcsec: 20.0,
            b_arcsec: 10.0,
            pa_deg: 0.0,
            photometry: None,
        }
    }

    fn legacy_row(id: i64, ra: f64, dec: f64) -> Document {
        doc! {
            "_id": id, "ra": ra, "dec": dec,
            "flux_g": 100.0, "flux_r": 250.0, "flux_i": 0.0, "flux_z": 400.0,
            "flux_ivar_r": 4.0,
        }
    }

    #[test]
    fn test_legacy_fluxes_become_ab_magnitudes() {
        let p = from_legacy(&legacy_row(7, 10.0, 20.0), LS_DR10, 0.0).unwrap();
        assert_close!(p.mag_g.unwrap(), 17.5);
        assert_close!(p.mag_r.unwrap(), 22.5 - 2.5 * 250.0_f64.log10());
        assert_close!(
            p.mag_r_err.unwrap(),
            MAG_ERR_PER_INVERSE_SNR / (250.0 * 2.0)
        );
        assert!(p.mag_i.is_none(), "a zero flux is not a magnitude");
        assert!(p.mag_g_err.is_none(), "no ivar, no error");
        assert_eq!(p.source_id.as_deref(), Some("7"));
        assert_eq!(p.mag_type, "model");
    }

    #[test]
    fn test_ps1_sentinels_are_dropped() {
        let row = doc! {
            "_id": 1_i64, "ra": 10.0, "dec": 20.0,
            "gMeanPSFMag": 19.1, "gMeanPSFMagErr": 0.02,
            "rMeanPSFMag": -999.0, "rMeanPSFMagErr": -999.0,
        };
        let p = from_ps1(&row, "PS1_DR2", 0.4).unwrap();
        assert_close!(p.mag_g.unwrap(), 19.1);
        assert!(p.mag_r.is_none());
        assert_eq!(p.mag_type, "psf");

        let empty = doc! { "_id": 2_i64, "ra": 10.0, "dec": 20.0, "gMeanPSFMag": -999.0 };
        assert!(from_ps1(&empty, "PS1_DR2", 0.4).is_none());
    }

    #[test]
    fn test_ned_host_takes_the_legacy_row_on_its_position() {
        let config = HostGalaxyConfig::default();
        let mut xmatches = HashMap::new();
        xmatches.insert(
            config.ls_dr10_catalog.clone(),
            vec![
                legacy_row(1, 10.0, 20.0),
                legacy_row(2, 10.0, 20.0 + 0.5 / 3600.0),
            ],
        );
        let on_host = best_host("NED", "NGC 1", 10.0, 20.0 + 0.5 / 3600.0);
        let p = photometry_from_xmatches(&on_host, &xmatches, &config).unwrap();
        assert_eq!(p.source_id.as_deref(), Some("2"));
        assert_close!(p.sep_arcsec, 0.0, epsilon = 1e-6);

        let off_footprint = best_host("NED", "NGC 2", 50.0, -10.0);
        assert!(photometry_from_xmatches(&off_footprint, &xmatches, &config).is_none());
    }
}
