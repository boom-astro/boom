use crate::conf::AppConfig;
use crate::filter::{
    build_ztf_alerts, create_producer, load_alert_schema, send_alert_to_kafka,
    ztf_results_pipeline, Filter, FilterError, FilterResults, FilterWorkerError,
};
use crate::utils::bson_number;
use crate::utils::cutouts::CutoutStorage;
use crate::utils::enums::Survey;
use crate::utils::linking::night_of;
use crate::utils::tracks::{list_tracks, StoredTrack, TrackQuery, COUNTERS_COLLECTION};
use apache_avro::Schema;
use mongodb::bson::{doc, Document};
use mongodb::Collection;
use rdkafka::producer::FutureProducer;
use std::collections::{HashMap, HashSet};
use tracing::{info, instrument, warn};

const CURSOR_ID: &str = "linker_results_cursor";
const RESULTS_TOPIC: &str = "ZTF_alerts_results";
const PAGE_SIZE: usize = 500;
const MIN_ZTF_NIGHTS: usize = 2;

pub struct TrackPublisher {
    filter_id: String,
    producer: FutureProducer,
    schema: Schema,
    cutouts: CutoutStorage,
    pipeline: Vec<Document>,
}

impl TrackPublisher {
    pub async fn new(config: &AppConfig) -> Result<Option<Self>, FilterWorkerError> {
        let filter_id = config.linker.results_filter_id.clone();
        if filter_id.is_empty() {
            return Ok(None);
        }
        Ok(Some(TrackPublisher {
            filter_id,
            producer: create_producer(&config.kafka.producer).await?,
            schema: load_alert_schema()?,
            cutouts: config.build_cutout_storage(&Survey::Ztf).await?,
            pipeline: ztf_results_pipeline(),
        }))
    }

    #[instrument(skip_all, err)]
    pub async fn publish(&self, db: &mongodb::Database) -> Result<usize, FilterWorkerError> {
        let counters = db.collection::<Document>(COUNTERS_COLLECTION);
        let Some(mut query) = read_cursor(&counters).await? else {
            let now = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
            save_cursor(&counters, now, None).await?;
            info!("no results cursor yet, publishing tracks updated from now on");
            return Ok(0);
        };
        let filter = db
            .collection::<Filter>("filters")
            .find_one(doc! { "_id": &self.filter_id })
            .await?
            .ok_or(FilterError::FilterNotFound)?;
        let programids = filter
            .permissions
            .get(&Survey::Ztf)
            .cloned()
            .unwrap_or_default();
        let alerts = db.collection::<Document>("ZTF_alerts");
        let mut published = 0;
        loop {
            let page = list_tracks(db, &query, PAGE_SIZE).await?;
            for track in page.tracks.iter().filter(|t| t.near_planet.is_none()) {
                let Some(candid) = latest_ztf_alert(&alerts, track, &programids).await? else {
                    continue;
                };
                let result = FilterResults {
                    filter_id: filter.id.clone(),
                    filter_name: filter.name.clone(),
                    passed_at: chrono::Utc::now().timestamp_millis() as f64,
                    annotations: annotations(track),
                };
                let built = match build_ztf_alerts(
                    &HashMap::from([(candid, vec![result])]),
                    &self.pipeline,
                    &alerts,
                    &self.cutouts,
                )
                .await
                {
                    Ok(built) => built,
                    Err(error) => {
                        warn!(%error, track = %track.id, candid, "could not build the track's alert");
                        continue;
                    }
                };
                for alert in &built {
                    let delivery =
                        send_alert_to_kafka(alert, &self.schema, &self.producer, RESULTS_TOPIC)
                            .await?;
                    let failed = match delivery.await {
                        Ok(Ok(_)) => None,
                        Ok(Err((error, _))) => Some(error.to_string()),
                        Err(error) => Some(error.to_string()),
                    };
                    if let Some(error) = failed {
                        return Err(FilterWorkerError::Kafka(format!(
                            "could not deliver track {}: {}",
                            track.id, error
                        )));
                    }
                    published += 1;
                }
            }
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
        unknown_only: true,
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

async fn latest_ztf_alert(
    alerts: &Collection<Document>,
    track: &StoredTrack,
    programids: &[i32],
) -> Result<Option<i64>, mongodb::error::Error> {
    if track.epochs.len() != track.members.len() {
        return Ok(None);
    }
    let ztf: Vec<(i64, f64)> = track
        .member_keys()
        .into_iter()
        .zip(&track.epochs)
        .filter(|((survey, _), _)| *survey == Survey::Ztf)
        .map(|((_, id), &jd)| (id, jd))
        .collect();
    let nights: HashSet<i64> = ztf.iter().map(|&(_, jd)| night_of(jd)).collect();
    if nights.len() < MIN_ZTF_NIGHTS {
        return Ok(None);
    }
    let ids: Vec<i64> = ztf.into_iter().map(|(id, _)| id).collect();
    let latest = alerts
        .find_one(doc! {
            "_id": { "$in": ids },
            "candidate.programid": { "$in": programids.to_vec() },
        })
        .sort(doc! { "candidate.jd": -1 })
        .projection(doc! { "_id": 1 })
        .await?;
    Ok(latest.and_then(|alert| alert.get_i64("_id").ok()))
}

fn annotations(track: &StoredTrack) -> String {
    let mut annotations = serde_json::json!({
        "track_id": track.id,
        "n_detections": track.n_detections,
        "n_nights": track.n_nights,
        "arc_days": track.arc_days,
    });
    if let Some(rms_ratio) = track.rms_ratio {
        annotations["rms_ratio"] = rms_ratio.into();
    }
    if let Some(neo_score) = track.neo_score {
        annotations["neo_score"] = neo_score.into();
    }
    if let Some(kind) = &track.kind {
        annotations["kind"] = kind.clone().into();
    }
    if !track.flags.is_empty() {
        annotations["flags"] = track.flags.clone().into();
    }
    annotations.to_string()
}
