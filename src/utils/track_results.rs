use crate::conf::AppConfig;
use crate::filter::{
    build_loaded_filters, build_ztf_alerts, create_producer, load_alert_schema, run_filter,
    send_alert_to_kafka, watchlist_projections, ztf_results_pipeline, FilterResults,
    FilterWorkerError, LoadedFilter,
};
use crate::utils::bson_number;
use crate::utils::cutouts::CutoutStorage;
use crate::utils::enums::Survey;
use crate::utils::tracks::{list_tracks, StoredTrack, TrackQuery, COUNTERS_COLLECTION};
use apache_avro::Schema;
use mongodb::bson::{doc, Document};
use mongodb::Collection;
use rdkafka::producer::FutureProducer;
use std::collections::HashMap;
use tracing::{info, instrument, warn};

const CURSOR_ID: &str = "linker_results_cursor";
const RESULTS_TOPIC: &str = "ZTF_alerts_results";
const PAGE_SIZE: usize = 500;
const FILTER_BATCH: usize = 1000;

struct Passed {
    jd: f64,
    candid: i64,
    annotations: String,
}

pub struct TrackPublisher {
    producer: FutureProducer,
    schema: Schema,
    cutouts: CutoutStorage,
    pipeline: Vec<Document>,
    watchlists: HashMap<String, Document>,
}

impl TrackPublisher {
    pub async fn new(config: &AppConfig) -> Result<Self, FilterWorkerError> {
        Ok(TrackPublisher {
            producer: create_producer(&config.kafka.producer).await?,
            schema: load_alert_schema()?,
            cutouts: config.build_cutout_storage(&Survey::Ztf).await?,
            pipeline: ztf_results_pipeline(),
            watchlists: watchlist_projections(config, &Survey::Ztf),
        })
    }

    #[instrument(skip_all, err)]
    pub async fn publish(&self, db: &mongodb::Database) -> Result<usize, FilterWorkerError> {
        let counters = db.collection::<Document>(COUNTERS_COLLECTION);
        let now = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
        let filters: Vec<LoadedFilter> = build_loaded_filters(
            &None,
            &Survey::Ztf,
            &db.collection("filters"),
            &self.watchlists,
        )
        .await?
        .into_iter()
        .filter(|filter| filter.reads_track)
        .collect();
        let cursor = read_cursor(&counters).await?;
        let (Some(mut query), false) = (cursor, filters.is_empty()) else {
            save_cursor(&counters, now, None).await?;
            info!(
                filters = filters.len(),
                "no track filter or cursor yet, running from now on"
            );
            return Ok(0);
        };
        let alerts = db.collection::<Document>("ZTF_alerts");
        let mut published = 0;
        loop {
            let page = list_tracks(db, &query, PAGE_SIZE).await?;
            published += self.publish_page(&alerts, &filters, &page.tracks).await?;
            let full = page.tracks.len() == PAGE_SIZE;
            let Some(next) = page.next else {
                break;
            };
            save_cursor(&counters, next.updated_since, Some(next.after_id.as_str())).await?;
            query.updated_since = next.updated_since;
            query.after_id = Some(next.after_id);
            if !full {
                break;
            }
        }
        Ok(published)
    }

    async fn publish_page(
        &self,
        alerts: &Collection<Document>,
        filters: &[LoadedFilter],
        tracks: &[StoredTrack],
    ) -> Result<usize, FilterWorkerError> {
        let track_of: HashMap<i64, &str> = tracks
            .iter()
            .flat_map(|track| {
                track
                    .member_keys()
                    .into_iter()
                    .filter(|(survey, _)| *survey == Survey::Ztf)
                    .map(move |(_, id)| (id, track.id.as_str()))
            })
            .collect();
        let members = programids_and_epochs(alerts, track_of.keys().copied().collect()).await?;

        let mut latest: HashMap<(&str, &str), Passed> = HashMap::new();
        for filter in filters {
            let permitted = filter
                .permissions
                .get(&Survey::Ztf)
                .cloned()
                .unwrap_or_default();
            let candids: Vec<i64> = members
                .iter()
                .filter(|(_, (programid, _))| permitted.contains(programid))
                .map(|(&candid, _)| candid)
                .collect();
            for batch in candids.chunks(FILTER_BATCH) {
                for doc in run_filter(batch, &filter.id, filter.pipeline.clone(), alerts).await? {
                    let candid = doc.get_i64("_id")?;
                    let (Some(&track_id), Some(&(_, jd))) =
                        (track_of.get(&candid), members.get(&candid))
                    else {
                        continue;
                    };
                    let key = (filter.id.as_str(), track_id);
                    if latest.get(&key).is_some_and(|kept| kept.jd >= jd) {
                        continue;
                    }
                    let mut annotations =
                        doc.get_document("annotations").cloned().unwrap_or_default();
                    annotations.insert("track_id", track_id);
                    latest.insert(
                        key,
                        Passed {
                            jd,
                            candid,
                            annotations: serde_json::to_string(&annotations)?,
                        },
                    );
                }
            }
        }

        let names: HashMap<&str, &str> = filters
            .iter()
            .map(|filter| (filter.id.as_str(), filter.name.as_str()))
            .collect();
        let passed_at = chrono::Utc::now().timestamp_millis() as f64;
        let mut results: HashMap<i64, Vec<FilterResults>> = HashMap::new();
        for ((filter_id, _), passed) in latest {
            results
                .entry(passed.candid)
                .or_default()
                .push(FilterResults {
                    filter_id: filter_id.to_string(),
                    filter_name: names[filter_id].to_string(),
                    passed_at,
                    annotations: passed.annotations,
                });
        }

        let mut published = 0;
        for (candid, filter_results) in results {
            let built = match build_ztf_alerts(
                &HashMap::from([(candid, filter_results)]),
                &self.pipeline,
                alerts,
                &self.cutouts,
            )
            .await
            {
                Ok(built) => built,
                Err(error) => {
                    warn!(%error, candid, "could not build the alert of a track");
                    continue;
                }
            };
            for alert in &built {
                let delivery =
                    send_alert_to_kafka(alert, &self.schema, &self.producer, RESULTS_TOPIC).await?;
                let failed = match delivery.await {
                    Ok(Ok(_)) => None,
                    Ok(Err((error, _))) => Some(error.to_string()),
                    Err(error) => Some(error.to_string()),
                };
                if let Some(error) = failed {
                    return Err(FilterWorkerError::Kafka(format!(
                        "could not deliver alert {candid}: {error}"
                    )));
                }
                published += 1;
            }
        }
        Ok(published)
    }
}

async fn programids_and_epochs(
    alerts: &Collection<Document>,
    candids: Vec<i64>,
) -> Result<HashMap<i64, (i32, f64)>, mongodb::error::Error> {
    let mut out = HashMap::new();
    if candids.is_empty() {
        return Ok(out);
    }
    let mut cursor = alerts
        .find(doc! { "_id": { "$in": candids } })
        .projection(doc! { "candidate.programid": 1, "candidate.jd": 1 })
        .await?;
    while cursor.advance().await? {
        let alert = cursor.deserialize_current()?;
        let Ok(candidate) = alert.get_document("candidate") else {
            continue;
        };
        if let (Ok(candid), Ok(programid), Ok(jd)) = (
            alert.get_i64("_id"),
            candidate.get_i32("programid"),
            candidate.get_f64("jd"),
        ) {
            out.insert(candid, (programid, jd));
        }
    }
    Ok(out)
}

async fn read_cursor(
    counters: &Collection<Document>,
) -> Result<Option<TrackQuery>, mongodb::error::Error> {
    let cursor = counters.find_one(doc! { "_id": CURSOR_ID }).await?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let Some(updated_since) = cursor.get("updated_since").and_then(bson_number) else {
        return Ok(None);
    };
    Ok(Some(TrackQuery {
        updated_since,
        after_id: cursor.get_str("after_id").ok().map(str::to_string),
        ..TrackQuery::default()
    }))
}

async fn save_cursor(
    counters: &Collection<Document>,
    updated_since: f64,
    after_id: Option<&str>,
) -> Result<(), mongodb::error::Error> {
    counters
        .replace_one(
            doc! { "_id": CURSOR_ID },
            doc! { "_id": CURSOR_ID, "updated_since": updated_since, "after_id": after_id },
        )
        .upsert(true)
        .await?;
    Ok(())
}
