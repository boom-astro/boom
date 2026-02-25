use crate::alert::DecamCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, PerBandProperties, PhotometryMag,
};
use lightcurve_fitting::{
    build_mag_bands, fit_nonparametric, fit_thermal, LightcurveFittingResult,
};
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub fn create_decam_alert_pipeline() -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "DECAM_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates": fetch_timeseries_op(
                    "aux.prv_candidates",
                    "candidate.jd",
                    365,
                    None
                ),
                "fp_hists": fetch_timeseries_op(
                    "aux.fp_hists",
                    "candidate.jd",
                    365,
                    Some(vec![doc! {
                        "$gte": [
                            "$$x.snr",
                            3.0
                        ]
                    }]),
                )
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.magpsf": 1,
                "prv_candidates.sigmapsf": 1,
                "prv_candidates.band": 1,
                "fp_hists.jd": 1,
                "fp_hists.magpsf": 1,
                "fp_hists.sigmapsf": 1,
                "fp_hists.band": 1,
            }
        },
    ]
}

/// DECAM alert structure used to deserialize alerts
/// from the database, used by the enrichment worker
/// to compute features and ML scores
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DecamAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: DecamCandidate,
    pub prv_candidates: Vec<PhotometryMag>,
    pub fp_hists: Vec<PhotometryMag>,
}

/// DECAM alert properties computed during enrichment
/// and inserted back into the alert document
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DecamAlertProperties {
    pub stationary: bool,
    pub photstats: PerBandProperties,
}

pub struct DecamEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for DecamEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("DECAM_alerts");

        let input_queue = "DECAM_alerts_enrichment_queue".to_string();
        let output_queue = "DECAM_alerts_filter_queue".to_string();

        Ok(DecamEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_pipeline: create_decam_alert_pipeline(),
        })
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<DecamAlertForEnrichment> =
            fetch_alerts(&candids, &self.alert_pipeline, &self.alert_collection).await?;

        if alerts.len() != candids.len() {
            warn!(
                "only {} alerts fetched from {} candids",
                alerts.len(),
                candids.len()
            );
        }

        if alerts.is_empty() {
            return Ok(vec![]);
        }

        let now = flare::Time::now().to_jd();

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            let (properties, lightcurve) = self.get_alert_properties(&alert).await?;

            // Lightcurve fitting (GP nonparametric + parametric + thermal)
            let lc = lightcurve.clone();
            let fitting_result = match tokio::time::timeout(
                std::time::Duration::from_secs(60),
                tokio::task::spawn_blocking(move || {
                    let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
                    let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
                    let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
                    let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();
                    let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
                    let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);
                    let thermal = fit_thermal(&mag_bands, Some(&trained_gps));
                    LightcurveFittingResult {
                        nonparametric,
                        parametric: vec![],
                        thermal,
                    }
                }),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    warn!("Lightcurve fitting panicked for candid {}: {}", candid, e);
                    LightcurveFittingResult {
                        nonparametric: vec![],
                        parametric: vec![],
                        thermal: None,
                    }
                }
                Err(_) => {
                    warn!("Lightcurve fitting timed out (60s) for candid {}", candid);
                    LightcurveFittingResult {
                        nonparametric: vec![],
                        parametric: vec![],
                        thermal: None,
                    }
                }
            };

            let update_alert_document = doc! {
                "$set": {
                    "properties": mongify(&properties),
                    "lightcurve_fitting": mongify(&fitting_result),
                    "updated_at": now,
                }
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": candid})
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{}", candid));
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        Ok(processed_alerts)
    }
}

impl DecamEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &DecamAlertForEnrichment,
    ) -> Result<(DecamAlertProperties, Vec<PhotometryMag>), EnrichmentWorkerError> {
        let prv_candidates = alert.prv_candidates.clone();
        let fp_hists = alert.fp_hists.clone();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        Ok((
            DecamAlertProperties {
                stationary,
                photstats,
            },
            lightcurve,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::lightcurves::{Band, PhotometryMag};
    use lightcurve_fitting::{
        build_mag_bands, fit_nonparametric, fit_thermal, LightcurveFittingResult,
    };

    /// Helper: build a synthetic DECam-like lightcurve with g, r, i, z bands.
    fn make_decam_synthetic_lightcurve() -> Vec<PhotometryMag> {
        let mut lc = Vec::new();
        let jd_start = 2460000.0;
        let band_configs: Vec<(Band, usize, f64, f64, f32)> = vec![
            (Band::G, 15, 21.0, 1.0, 0.07),
            (Band::R, 15, 21.3, 0.8, 0.08),
            (Band::I, 10, 21.5, 0.6, 0.10),
            (Band::Z, 8, 21.8, 0.5, 0.12),
        ];
        for (band, n_pts, base_mag, amplitude, err) in band_configs {
            for i in 0..n_pts {
                let t = jd_start + i as f64 * 2.0;
                let phase = (i as f64 - (n_pts as f64 / 3.0)) / 4.0;
                let mag = base_mag - amplitude * (-phase * phase).exp();
                lc.push(PhotometryMag {
                    time: t,
                    mag: mag as f32,
                    mag_err: err,
                    band: band.clone(),
                });
            }
        }
        lc
    }

    #[test]
    fn test_decam_fitting_multiband() {
        let lc = make_decam_synthetic_lightcurve();
        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
        assert_eq!(mag_bands.len(), 4);

        let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);
        let thermal = fit_thermal(&mag_bands, Some(&trained_gps));

        // g, r, i have enough points (>= 5); z has 8 so should also fit
        assert!(nonparametric.len() >= 3);

        // Full result should serialize
        let result = LightcurveFittingResult {
            nonparametric,
            parametric: vec![],
            thermal,
        };
        let json = serde_json::to_string(&result)
            .expect("LightcurveFittingResult should serialize to JSON");
        assert!(json.contains("nonparametric"));
        assert!(json.contains("thermal"));
    }

    #[test]
    fn test_decam_fitting_empty() {
        let mag_bands = build_mag_bands(&[], &[], &[], &[]);
        let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);
        let thermal = fit_thermal(&mag_bands, Some(&trained_gps));

        assert!(nonparametric.is_empty());
        assert!(thermal.is_none());
    }

    #[test]
    fn test_decam_fitting_few_points() {
        // Only 3 points in one band — below threshold, should produce no results
        let lc = vec![
            PhotometryMag {
                time: 2460000.0,
                mag: 21.0,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2460002.0,
                mag: 20.5,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2460004.0,
                mag: 21.0,
                mag_err: 0.1,
                band: Band::G,
            },
        ];
        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
        let (nonparametric, _) = fit_nonparametric(&mag_bands);

        // 3 points is below the 5-point minimum for GP fitting
        assert!(nonparametric.is_empty());
    }
}
