use crate::conf::AppConfig;
use crate::filter::{
    build_decam_alerts, build_loaded_filters, build_lsst_alerts, build_winter_alerts,
    build_ztf_alerts, create_decam_filter_alert_pipeline, create_lsst_filter_alert_pipeline,
    create_producer, create_winter_filter_alert_pipeline, load_alert_schema, run_filter,
    send_alert_to_kafka, watchlist_projections, ztf_results_pipeline, Alert, FilterResults,
    FilterWorkerError, LoadedFilter, SURVEYS_REQUIRING_PERMISSIONS,
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
const PAGE_SIZE: usize = 500;
const FILTER_BATCH: usize = 1000;

struct Passed {
    jd: f64,
    candid: i64,
    annotations: String,
}

struct SurveyResults {
    survey: Survey,
    cutouts: CutoutStorage,
    pipeline: Vec<Document>,
    watchlists: HashMap<String, Document>,
}

impl SurveyResults {
    async fn build(
        &self,
        results: &HashMap<i64, Vec<FilterResults>>,
        alerts: &Collection<Document>,
    ) -> Result<Vec<Alert>, FilterWorkerError> {
        let (pipeline, cutouts) = (&self.pipeline, &self.cutouts);
        match self.survey {
            Survey::Lsst => build_lsst_alerts(results, pipeline, alerts, cutouts).await,
            Survey::Decam => build_decam_alerts(results, pipeline, alerts, cutouts).await,
            Survey::Winter => build_winter_alerts(results, pipeline, alerts, cutouts).await,
            _ => build_ztf_alerts(results, pipeline, alerts, cutouts).await,
        }
    }
}

pub struct TrackPublisher {
    producer: FutureProducer,
    schema: Schema,
    surveys: Vec<SurveyResults>,
}

impl TrackPublisher {
    pub async fn new(config: &AppConfig) -> Result<Self, FilterWorkerError> {
        let mut surveys = Vec::new();
        for (survey, pipeline) in [
            (Survey::Ztf, ztf_results_pipeline()),
            (Survey::Lsst, create_lsst_filter_alert_pipeline()),
            (Survey::Decam, create_decam_filter_alert_pipeline()),
            (Survey::Winter, create_winter_filter_alert_pipeline()),
        ] {
            surveys.push(SurveyResults {
                cutouts: config.build_cutout_storage(&survey).await?,
                watchlists: watchlist_projections(config, &survey),
                survey,
                pipeline,
            });
        }
        Ok(TrackPublisher {
            producer: create_producer(&config.kafka.producer).await?,
            schema: load_alert_schema()?,
            surveys,
        })
    }

    async fn track_filters(
        &self,
        db: &mongodb::Database,
        filter_ids: Option<&[String]>,
    ) -> Result<Vec<(&SurveyResults, Vec<LoadedFilter>)>, FilterWorkerError> {
        let mut by_survey = Vec::new();
        for survey in &self.surveys {
            let filters: Vec<LoadedFilter> = build_loaded_filters(
                &None,
                &survey.survey,
                &db.collection("filters"),
                &survey.watchlists,
            )
            .await?
            .into_iter()
            .filter(|filter| filter.reads_track)
            .filter(|filter| filter_ids.is_none_or(|ids| ids.contains(&filter.id)))
            .collect();
            if !filters.is_empty() {
                by_survey.push((survey, filters));
            }
        }
        Ok(by_survey)
    }

    #[instrument(skip_all, err)]
    pub async fn publish(
        &self,
        db: &mongodb::Database,
        min_last_jd: f64,
    ) -> Result<usize, FilterWorkerError> {
        let counters = db.collection::<Document>(COUNTERS_COLLECTION);
        let now = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
        let filters = self.track_filters(db, None).await?;
        let cursor = read_cursor(&counters).await?;
        let (Some(mut query), false) = (cursor, filters.is_empty()) else {
            save_cursor(&counters, now, None).await?;
            info!(
                surveys = filters.len(),
                "no track filter or cursor yet, running from now on"
            );
            return Ok(0);
        };
        query.min_last_jd = Some(min_last_jd);
        let mut published = 0;
        loop {
            let page = list_tracks(db, &query, PAGE_SIZE).await?;
            for (survey, filters) in &filters {
                published += self.publish_page(db, survey, filters, &page.tracks).await?;
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

    #[instrument(skip_all, err)]
    pub async fn publish_tracks(
        &self,
        db: &mongodb::Database,
        tracks: &[StoredTrack],
        filter_ids: Option<&[String]>,
    ) -> Result<usize, FilterWorkerError> {
        let filters = self.track_filters(db, filter_ids).await?;
        let mut published = 0;
        for page in tracks.chunks(PAGE_SIZE) {
            for (survey, filters) in &filters {
                published += self.publish_page(db, survey, filters, page).await?;
            }
        }
        Ok(published)
    }

    async fn publish_page(
        &self,
        db: &mongodb::Database,
        survey: &SurveyResults,
        filters: &[LoadedFilter],
        tracks: &[StoredTrack],
    ) -> Result<usize, FilterWorkerError> {
        let alerts = db.collection::<Document>(&format!("{}_alerts", survey.survey));
        let target = &survey.survey;
        let track_of: HashMap<i64, &str> = tracks
            .iter()
            .flat_map(|track| {
                track
                    .member_keys()
                    .into_iter()
                    .filter(move |(member_survey, _)| member_survey == target)
                    .map(move |(_, id)| (id, track.id.as_str()))
            })
            .collect();
        if track_of.is_empty() {
            return Ok(0);
        }
        let members =
            programids_and_epochs(&alerts, &survey.survey, track_of.keys().copied().collect())
                .await?;

        let mut latest: HashMap<(&str, &str), Passed> = HashMap::new();
        for filter in filters {
            let permitted = filter
                .permissions
                .get(&survey.survey)
                .cloned()
                .unwrap_or_default();
            let checked = SURVEYS_REQUIRING_PERMISSIONS.contains(&survey.survey);
            let candids: Vec<i64> = members
                .iter()
                .filter(|(_, (programid, _))| !checked || permitted.contains(programid))
                .map(|(&candid, _)| candid)
                .collect();
            for batch in candids.chunks(FILTER_BATCH) {
                for doc in run_filter(batch, &filter.id, filter.pipeline.clone(), &alerts).await? {
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

        let topic = format!("{}_alerts_results", survey.survey);
        let mut published = 0;
        for (candid, filter_results) in results {
            let built = match survey
                .build(&HashMap::from([(candid, filter_results)]), &alerts)
                .await
            {
                Ok(built) => built,
                Err(error) => {
                    warn!(%error, candid, survey = %survey.survey, "could not build the alert of a track");
                    continue;
                }
            };
            for alert in &built {
                let delivery =
                    send_alert_to_kafka(alert, &self.schema, &self.producer, &topic).await?;
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
    survey: &Survey,
    candids: Vec<i64>,
) -> Result<HashMap<i64, (i32, f64)>, mongodb::error::Error> {
    let mut out = HashMap::new();
    if candids.is_empty() {
        return Ok(out);
    }
    let checked = SURVEYS_REQUIRING_PERMISSIONS.contains(survey);
    let mut cursor = alerts
        .find(doc! { "_id": { "$in": candids } })
        .projection(doc! { "candidate.programid": 1, "candidate.jd": 1 })
        .await?;
    while cursor.advance().await? {
        let alert = cursor.deserialize_current()?;
        let Ok(candidate) = alert.get_document("candidate") else {
            continue;
        };
        let programid = if checked {
            candidate.get_i32("programid").ok()
        } else {
            Some(1)
        };
        if let (Ok(candid), Some(programid), Ok(jd)) =
            (alert.get_i64("_id"), programid, candidate.get_f64("jd"))
        {
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
