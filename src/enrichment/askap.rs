use crate::alert::AskapCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::enums::Survey;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub fn create_askap_alert_pipeline() -> Vec<Document> {
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
                "from": "ASKAP_alerts_aux",
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
                    3650,
                    None
                ),
                "fp_hists": fetch_timeseries_op(
                    "aux.fp_hists",
                    "candidate.jd",
                    3650,
                    None
                )
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.flux_peak": 1,
                "prv_candidates.flux_peak_err": 1,
                "fp_hists.jd": 1,
                "fp_hists.flux_peak": 1,
                "fp_hists.flux_peak_err": 1,
            }
        },
    ]
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct AskapFluxPoint {
    pub jd: f64,
    pub flux_peak: f32,
    pub flux_peak_err: f32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AskapAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: AskapCandidate,
    #[serde(default)]
    pub prv_candidates: Vec<AskapFluxPoint>,
    #[serde(default)]
    pub fp_hists: Vec<AskapFluxPoint>,
}

/// VAST-style variability metrics over the peak-flux lightcurve.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AskapAlertProperties {
    pub n_measurements: usize,
    pub flux_peak_max: Option<f64>,
    pub flux_peak_min: Option<f64>,
    /// Weighted reduced chi-square about the weighted mean (VAST eta).
    pub eta_peak: Option<f64>,
    /// Modulation index: std / mean (VAST V).
    pub v_peak: Option<f64>,
}

fn variability_metrics(points: &[AskapFluxPoint]) -> AskapAlertProperties {
    let n = points.len();
    let fluxes: Vec<f64> = points.iter().map(|p| p.flux_peak as f64).collect();
    let flux_peak_max = fluxes
        .iter()
        .cloned()
        .fold(None, |m: Option<f64>, f| Some(m.map_or(f, |m| m.max(f))));
    let flux_peak_min = fluxes
        .iter()
        .cloned()
        .fold(None, |m: Option<f64>, f| Some(m.map_or(f, |m| m.min(f))));

    if n < 2 {
        return AskapAlertProperties {
            n_measurements: n,
            flux_peak_max,
            flux_peak_min,
            eta_peak: None,
            v_peak: None,
        };
    }

    let mean = fluxes.iter().sum::<f64>() / n as f64;
    let var = fluxes.iter().map(|f| (f - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);
    let v_peak = if mean != 0.0 {
        Some(var.sqrt() / mean)
    } else {
        None
    };

    // eta: (1/(N-1)) * sum(w_i * (f_i - wmean)^2), w_i = 1/sigma_i^2
    let weights: Vec<f64> = points
        .iter()
        .map(|p| {
            let s = p.flux_peak_err as f64;
            if s > 0.0 {
                1.0 / (s * s)
            } else {
                0.0
            }
        })
        .collect();
    let wsum: f64 = weights.iter().sum();
    let eta_peak = if wsum > 0.0 {
        let wmean = fluxes.iter().zip(&weights).map(|(f, w)| f * w).sum::<f64>() / wsum;
        Some(
            fluxes
                .iter()
                .zip(&weights)
                .map(|(f, w)| w * (f - wmean).powi(2))
                .sum::<f64>()
                / (n as f64 - 1.0),
        )
    } else {
        None
    };

    AskapAlertProperties {
        n_measurements: n,
        flux_peak_max,
        flux_peak_min,
        eta_peak,
        v_peak,
    }
}

pub struct AskapEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for AskapEnrichmentWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        _shared_models: Option<std::sync::Arc<crate::enrichment::models::SharedModels>>,
    ) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("ASKAP_alerts");

        let input_queue = "ASKAP_alerts_enrichment_queue".to_string();
        let output_queue = "ASKAP_alerts_filter_queue".to_string();

        Ok(AskapEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_pipeline: create_askap_alert_pipeline(),
        })
    }

    fn survey() -> Survey {
        Survey::Askap
    }

    fn disable_babamul(&mut self) {}

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
        let alerts: Vec<AskapAlertForEnrichment> =
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

        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            let lightcurve = [alert.prv_candidates.clone(), alert.fp_hists.clone()].concat();
            let properties = variability_metrics(&lightcurve);

            let update_alert_document = doc! {
                "$set": {
                    "properties": mongify(&properties),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(jd: f64, flux: f32, err: f32) -> AskapFluxPoint {
        AskapFluxPoint {
            jd,
            flux_peak: flux,
            flux_peak_err: err,
        }
    }

    #[test]
    fn test_variability_metrics_single_point() {
        let props = variability_metrics(&[pt(1.0, 10.0, 0.5)]);
        assert_eq!(props.n_measurements, 1);
        assert_eq!(props.flux_peak_max, Some(10.0));
        assert!(props.eta_peak.is_none());
        assert!(props.v_peak.is_none());
    }

    #[test]
    fn test_variability_metrics_constant_source() {
        let props = variability_metrics(&[pt(1.0, 10.0, 0.5), pt(2.0, 10.0, 0.5)]);
        assert_eq!(props.n_measurements, 2);
        assert_eq!(props.eta_peak, Some(0.0));
        assert_eq!(props.v_peak, Some(0.0));
    }

    #[test]
    fn test_variability_metrics_variable_source() {
        let props = variability_metrics(&[pt(1.0, 10.0, 1.0), pt(2.0, 20.0, 1.0)]);
        // wmean = 15, eta = (1*(10-15)^2 + 1*(20-15)^2) / 1 = 50
        assert!((props.eta_peak.unwrap() - 50.0).abs() < 1e-9);
        // mean 15, std = sqrt(50) -> v = sqrt(50)/15
        assert!((props.v_peak.unwrap() - (50.0_f64).sqrt() / 15.0).abs() < 1e-9);
    }
}
