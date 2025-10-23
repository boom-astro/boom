use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, get_array_element};
use crate::utils::lightcurves::{analyze_photometry, parse_photometry};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub fn create_lsst_alert_pipeline() -> Vec<Document> {
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
                "from": "LSST_alerts_aux",
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
                ),
                "aliases": get_array_element("aux.aliases"),
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
                "fp_hists.snr": 1,
            }
        },
    ]
}

pub struct Babamul {
    kafka_producer: rdkafka::producer::FutureProducer,
}

impl Babamul {
    fn new(config: config::Config) -> Self {
        // Read Kafka producer config from kafka: producer in the config
        // TODO: Do this properly
        let kafka_producer_host = config
            .get_string("kafka.producer")
            .unwrap_or_else(|_| "broker:29092".to_string());

        // Create Kafka producer
        let kafka_producer: rdkafka::producer::FutureProducer =
            rdkafka::config::ClientConfig::new()
                // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
                // .set("debug", "broker,topic,msg")
                .set("bootstrap.servers", &kafka_producer_host)
                .set("message.timeout.ms", "5000")
                // it's best to increase batch.size if the cluster
                // is running on another machine. Locally, lower means less
                // latency, since we are not limited by network speed anyways
                .set("batch.size", "16384")
                .set("linger.ms", "5")
                .set("acks", "1")
                .set("max.in.flight.requests.per.connection", "5")
                .set("retries", "3")
                .create()
                .unwrap();
        Babamul { kafka_producer }
    }

    fn process_alert(
        &self,
        alert: Document,
        properties: Document,
    ) -> Result<(), EnrichmentWorkerError> {
        // Merge properties into the alert document
        let mut merged = alert.clone();
        merged.insert("properties", Bson::Document(properties.clone()));

        // Determine if this alert is worth sending to Babamul
        let min_reliability = 0.5;
        let pixel_flags = vec![1, 2, 3];
        let sso = false;

        if (merged.get_f64("candidate.reliability").unwrap_or(0.0) < min_reliability)
            || (merged
                .get_document("candidate")
                .unwrap()
                .get_i32("pixel_flags")
                .map(|pf| pixel_flags.contains(&pf))
                .unwrap_or(false))
            || (merged
                .get_document("properties")
                .unwrap()
                .get_bool("rock")
                .unwrap_or(false)
                == sso)
        {
            // Skip this alert, it doesn't meet the criteria
            return Ok(());
        }

        // Determine which topic this alert should go to
        // Is it a star, galaxy, or none, and does it have a ZTF crossmatch?
        // TODO: Send to topic
        let _producer = &self.kafka_producer;

        Ok(())
    }
}

pub struct LsstEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_pipeline: Vec<Document>,
    babamul: Option<Babamul>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for LsstEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config_file: config::Config = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let client = db.client().clone();
        let alert_collection = db.collection("LSST_alerts");

        let input_queue = "LSST_alerts_enrichment_queue".to_string();
        let output_queue = "LSST_alerts_filter_queue".to_string();

        // Detect if Babamul is enabled from the config
        let babamul_enabled = crate::conf::babamul_enabled(&config_file);
        let babamul: Option<Babamul> = if babamul_enabled {
            Some(Babamul::new(config_file))
        } else {
            None
        };

        Ok(LsstEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_pipeline: create_lsst_alert_pipeline(),
            babamul,
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
        // TODO: Can we define a struct for what comes out here?
        let alerts =
            fetch_alerts(&candids, &self.alert_pipeline, &self.alert_collection, None).await?;

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

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for i in 0..alerts.len() {
            let candid = alerts[i].get_i64("_id")?;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let properties = self.get_alert_properties(&alerts[i]).await?;

            let find_document = doc! {
                "_id": candid
            };

            let update_alert_document = doc! {
                "$set": {
                    "properties": properties.clone(),
                }
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(find_document)
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{}", candid));

            // If Babamul is enabled, process this alert for its streams
            if self.babamul.is_some() {
                self.babamul
                    .as_ref()
                    .unwrap()
                    .process_alert(alerts[i].clone(), properties.clone())?;
            }
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        Ok(processed_alerts)
    }
}

impl LsstEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &Document,
    ) -> Result<Document, EnrichmentWorkerError> {
        // Compute numerical and boolean features from lightcurve and candidate analysis
        let candidate = alert.get_document("candidate")?;

        let jd = candidate.get_f64("jd")?;

        let is_rock = candidate.get_bool("is_sso").unwrap_or(false);

        let prv_candidates = alert.get_array("prv_candidates")?;
        let fp_hists = alert.get_array("fp_hists")?;
        let mut lightcurve =
            parse_photometry(prv_candidates, "jd", "magpsf", "sigmapsf", "band", jd);
        lightcurve.extend(parse_photometry(
            fp_hists, "jd", "magpsf", "sigmapsf", "band", jd,
        ));

        let (photstats, _, stationary) = analyze_photometry(lightcurve);

        let properties = doc! {
            // properties
            "rock": is_rock,
            "stationary": stationary,
            "photstats": photstats,
        };
        Ok(properties)
    }
}
