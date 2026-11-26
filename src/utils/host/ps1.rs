use std::collections::HashMap;

use mongodb::bson::{self, doc, Document};
use mongodb::Database;
use tracing::warn;

use crate::catalogs::{types::Ps1Host, PS1_HOSTS_COLLECTION};
use crate::conf::{arcsec_to_radians, CatalogXmatchConfig};
use crate::utils::{enums::Survey, spatial::xmatch};

use super::catalog::{add_spectroscopic_redshift, PHOTOMETRIC};
use super::{
    associate_host, GalaxyCandidate, HostGalaxyAssociation, HostGalaxyConfig, Ps1FallbackConfig,
    Transient,
};

fn candidate(row: Ps1Host, cuts: &Ps1FallbackConfig) -> Option<GalaxyCandidate> {
    if row.strm_class != "GALAXY"
        || !(cuts.min_galaxy_score..=1.0).contains(&row.strm_prob_galaxy)
        || !row.extendedness_sigma.is_finite()
        || row.extendedness_sigma == -999.0
        || row.extendedness_sigma < cuts.min_extendedness_sigma
        || !row.kron_radius_arcsec.is_finite()
        || row.kron_radius_arcsec <= 0.0
    {
        return None;
    }

    let redshift = row.strm_z_phot.filter(|z| z.is_finite() && *z > 0.0);
    let redshift_err =
        redshift.and_then(|_| row.strm_z_phot_err.filter(|e| e.is_finite() && *e > 0.0));
    // The stack Kron aperture is a circular proxy for the galaxy light radius.
    Some(GalaxyCandidate {
        ra: row.ra,
        dec: row.dec,
        a_arcsec: row.kron_radius_arcsec,
        b_arcsec: row.kron_radius_arcsec,
        pa_deg: 0.0,
        redshift,
        redshift_err,
        redshift_type: redshift.map(|_| PHOTOMETRIC.to_string()),
        dist_mpc: None,
        dist_mpc_method: None,
        mag: None,
        mag_err: None,
        objtype: Some(row.strm_class),
        objname: Some(row.id.to_string()),
        catalog: Some(PS1_HOSTS_COLLECTION.to_string()),
        size_is_isophotal: false,
        diam_survey: None,
        orientation_is_nominal: true,
        rex_snr_band: None,
    })
}

pub(super) async fn associate(
    ra: f64,
    dec: f64,
    xmatches: &HashMap<String, Vec<Document>>,
    config: &HostGalaxyConfig,
    survey: &Survey,
    object_id: &str,
    db: &Database,
) -> Option<HostGalaxyAssociation> {
    let cuts = config.ps1_fallback.as_ref()?;
    let search = CatalogXmatchConfig {
        catalog: PS1_HOSTS_COLLECTION.to_string(),
        radius: arcsec_to_radians(cuts.radius_arcsec),
        projection: doc! {
            "_id": 1, "ra": 1, "dec": 1, "rExtNSigma": 1, "rKronRad": 1,
            "strm_class": 1, "strm_prob_galaxy": 1, "strm_z_phot": 1, "strm_z_phot_err": 1,
        },
        ..Default::default()
    };
    let mut matches = match xmatch(ra, dec, object_id, survey, &[search], db).await {
        Ok(matches) => matches,
        Err(error) => {
            warn!(object_id, %error, "Pan-STARRS host search failed");
            return None;
        }
    };
    let galaxies = matches
        .remove(PS1_HOSTS_COLLECTION)?
        .into_iter()
        .filter_map(|row| bson::from_document::<Ps1Host>(row).ok())
        .filter_map(|row| candidate(row, cuts))
        .map(|mut galaxy| {
            if let Some(spectra) = xmatches.get(&config.desi_catalog) {
                add_spectroscopic_redshift(&mut galaxy, spectra, config);
            }
            galaxy
        })
        .collect::<Vec<_>>();
    let mut association_config = config.association_config();
    association_config.max_fractional_offset = cuts.max_dlr;
    let result = associate_host(&Transient::new(ra, dec), &galaxies, &association_config).ok()?;
    (result.n_passed_dlr > 0).then(|| HostGalaxyAssociation::from_result(&result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::host::associate_with_photometry;
    use crate::utils::spatial::Coordinates;
    use mongodb::IndexModel;

    const RA: f64 = 74.2534823;
    const DEC: f64 = 69.1721358;
    const HOST: &str = r#"{"_id":191000742646808255,"ra":74.2654807,"dec":69.17252295,
        "rKronRad":9.750800132751465,"rExtNSigma":41.554500579833984,"strm_class":"GALAXY",
        "strm_prob_galaxy":0.9994375,"strm_z_phot":0.0380897969,"strm_z_phot_err":0.0086123286}"#;

    async fn replay(
        db: &Database,
        matches: &HashMap<String, Vec<Document>>,
        config: &HostGalaxyConfig,
    ) -> HostGalaxyAssociation {
        associate_with_photometry(
            RA,
            DEC,
            matches,
            config,
            &[],
            &Survey::Ztf,
            "ZTF26aciomda",
            db,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn ps1_fallback_replays_ztf26aciomda() -> anyhow::Result<()> {
        let test_db = crate::conf::get_test_db().await;
        let db = test_db
            .client()
            .database(&format!("test_ps1_host_{}", uuid::Uuid::new_v4().simple()));
        let catalog = db.collection::<Document>(PS1_HOSTS_COLLECTION);
        let mut host: Ps1Host = serde_json::from_str(HOST)?;
        let mut host_doc = bson::to_document(&host)?;
        host_doc.insert(
            "coordinates",
            bson::to_document(&Coordinates::new(host.ra, host.dec))?,
        );

        // A closer candidate with a valid Kron radius but an insufficient STRM score.
        host.id = 1;
        host.ra = RA;
        host.dec = DEC;
        host.strm_prob_galaxy = 0.69;
        let mut contaminant = bson::to_document(&host)?;
        contaminant.insert(
            "coordinates",
            bson::to_document(&Coordinates::new(RA, DEC))?,
        );
        catalog.insert_many([host_doc, contaminant]).await?;
        catalog
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "coordinates.radec_geojson": "2dsphere" })
                    .build(),
            )
            .await?;

        let disabled = HostGalaxyConfig {
            enabled: true,
            ..Default::default()
        };
        let enabled = HostGalaxyConfig {
            ps1_fallback: Some(Ps1FallbackConfig::default()),
            ..disabled.clone()
        };
        let mut matches = HashMap::new();
        let without_fallback = replay(&db, &matches, &disabled).await;
        let fallback = replay(&db, &matches, &enabled).await;
        matches.insert(
            "NED".into(),
            vec![doc! { "_id": "control", "ra": RA, "dec": DEC, "Diam": 1.0 }],
        );
        let primary = replay(&db, &matches, &enabled).await;
        matches.get_mut("NED").unwrap()[0].insert("dec", DEC + 20.0 / 3600.0);
        let rejected_primary = replay(&db, &matches, &enabled).await;
        db.drop().await?;

        assert_eq!(without_fallback, HostGalaxyAssociation::empty());
        assert_eq!(fallback.n_candidates_searched, 1);
        let best = fallback.best_host.as_ref().unwrap();
        assert_eq!(best.objname.as_deref(), Some("191000742646808255"));
        assert_eq!(best.catalog.as_deref(), Some(PS1_HOSTS_COLLECTION));
        assert_eq!(best.z_type.as_deref(), Some(PHOTOMETRIC));
        assert_eq!(best.z, Some(0.0380897969));
        assert_eq!(best.z_err, Some(0.0086123286));
        assert!((best.d_dlr - 1.5815).abs() < 0.001);
        assert_eq!(primary.best_host.unwrap().catalog.as_deref(), Some("NED"));
        assert_eq!(rejected_primary, fallback);
        Ok(())
    }
}
