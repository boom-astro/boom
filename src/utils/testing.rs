use crate::{
    alert::{
        AlertWorker, DecamAlertWorker, LightcurveJdOnly, LsstAlertWorker, SchemaRegistry,
        ZtfAlertWorker, LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL, LSST_SCHEMA_REGISTRY_URL,
    },
    conf,
    filter::{Filter, FilterVersion},
    utils::{db::initialize_survey_indexes, enums::Survey},
};
use apache_avro::{
    from_avro_datum,
    types::{Record, Value},
    Reader, Schema, Writer,
};
use async_trait::async_trait;
use mongodb::bson::doc;
use rand::Rng;
use redis::AsyncCommands;
use std::fs;
use std::io::Read;
// Utility for unit tests

pub const TEST_CONFIG_FILE: &str = "tests/config.test.yaml";

pub async fn ztf_alert_worker() -> ZtfAlertWorker {
    // initialize the ZTF indexes
    initialize_survey_indexes(&Survey::Ztf, &conf::get_test_db().await)
        .await
        .unwrap();
    ZtfAlertWorker::new(TEST_CONFIG_FILE).await.unwrap()
}

pub async fn lsst_alert_worker() -> LsstAlertWorker {
    // initialize the ZTF indexes
    initialize_survey_indexes(&Survey::Lsst, &conf::get_test_db().await)
        .await
        .unwrap();
    LsstAlertWorker::new(TEST_CONFIG_FILE).await.unwrap()
}

pub async fn decam_alert_worker() -> DecamAlertWorker {
    // initialize the ZTF indexes
    initialize_survey_indexes(&Survey::Decam, &conf::get_test_db().await)
        .await
        .unwrap();
    DecamAlertWorker::new(TEST_CONFIG_FILE).await.unwrap()
}

// drops alert collections from the database
pub async fn drop_alert_collections(
    alert_collection_name: &str,
    alert_cutout_collection_name: &str,
    alert_aux_collection_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = conf::load_config(Some(TEST_CONFIG_FILE)).unwrap();
    let db = config.build_db().await?;
    db.collection::<mongodb::bson::Document>(alert_collection_name)
        .drop()
        .await?;
    db.collection::<mongodb::bson::Document>(alert_cutout_collection_name)
        .drop()
        .await?;
    db.collection::<mongodb::bson::Document>(alert_aux_collection_name)
        .drop()
        .await?;
    Ok(())
}

pub async fn drop_alert_from_collections(
    candid: i64,
    stream_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = conf::load_config(Some(TEST_CONFIG_FILE)).unwrap();
    let db = config.build_db().await?;
    let alert_collection_name = format!("{}_alerts", stream_name);
    let alert_cutout_collection_name = format!("{}_alerts_cutouts", stream_name);
    let alert_aux_collection_name = format!("{}_alerts_aux", stream_name);

    let filter = doc! {"_id": candid};
    let alert = db
        .collection::<mongodb::bson::Document>(&alert_collection_name)
        .find_one(filter.clone())
        .await?;

    if let Some(alert) = alert {
        // delete the alert from the alerts collection
        db.collection::<mongodb::bson::Document>(&alert_collection_name)
            .delete_one(filter.clone())
            .await?;

        // delete the alert from the cutouts collection
        db.collection::<mongodb::bson::Document>(&alert_cutout_collection_name)
            .delete_one(filter.clone())
            .await?;

        // delete the object from the aux collection
        let object_id = alert.get_str("objectId")?;
        db.collection::<mongodb::bson::Document>(&alert_aux_collection_name)
            .delete_one(doc! {"_id": object_id})
            .await?;
    }

    Ok(())
}

const ZTF_TEST_PIPELINE: &str = "[{\"$match\": {\"candidate.drb\": {\"$gt\": 0.5}, \"candidate.ndethist\": {\"$gt\": 1.0}, \"candidate.magpsf\": {\"$lte\": 18.5}}}, {\"$project\": {\"annotations.mag_now\": {\"$round\": [\"$candidate.magpsf\", 2]}}}]";
const ZTF_TEST_PIPELINE_PRV_CANDIDATES: &str = "[{\"$match\": {\"prv_candidates.0\": {\"$exists\": true}, \"candidate.drb\": {\"$gt\": 0.5}, \"candidate.ndethist\": {\"$gt\": 1.0}, \"candidate.magpsf\": {\"$lte\": 18.5}}}, {\"$project\": {\"objectId\": 1, \"annotations.mag_now\": {\"$round\": [\"$candidate.magpsf\", 2]}}}]";
const LSST_TEST_PIPELINE: &str = "[{\"$match\": {\"candidate.reliability\": {\"$gt\": 0.1}, \"candidate.snr\": {\"$gt\": 5.0}, \"candidate.magpsf\": {\"$lte\": 25.0}}}, {\"$project\": {\"objectId\": 1, \"annotations.mag_now\": {\"$round\": [\"$candidate.magpsf\", 2]}}}]";

pub async fn remove_test_filter(
    filter_id: &str,
    survey: &Survey,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = conf::load_config(Some(TEST_CONFIG_FILE)).unwrap();
    let db = config.build_db().await?;
    let _ = db
        .collection::<mongodb::bson::Document>("filters")
        .delete_many(doc! {"_id": filter_id, "catalog": &format!("{}_alerts", survey)})
        .await;

    Ok(())
}

// we want to replace the 3 insert_test_..._filter functions with a single function that
// takes the survey as argument
pub async fn insert_custom_test_filter(
    survey: &Survey,
    pipeline_str: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let filter_id = uuid::Uuid::new_v4().to_string();
    let filter_name = format!("test_filter_{}", &filter_id[..8]);

    let now = flare::Time::now().to_jd();
    let mut permissions = std::collections::HashMap::new();
    permissions.insert(survey.clone(), vec![1]);
    let filter_obj = Filter {
        id: filter_id.clone(),
        name: filter_name,
        description: Some("Test filter".to_string()),
        survey: survey.clone(),
        user_id: "test_user".to_string(),
        permissions,
        active: true,
        active_fid: "v2e0fs".to_string(),
        fv: vec![FilterVersion {
            fid: "v2e0fs".to_string(),
            pipeline: pipeline_str.to_string(),
            changelog: None,
            created_at: now,
        }],
        created_at: now,
        updated_at: now,
    };

    let config = conf::load_config(Some(TEST_CONFIG_FILE)).unwrap();
    let db = config.build_db().await?;
    let _ = db
        .collection::<Filter>("filters")
        .insert_one(filter_obj)
        .await;

    Ok(filter_id)
}

// we want to replace the 3 insert_test_..._filter functions with a single function that
// takes the survey as argument
pub async fn insert_test_filter(
    survey: &Survey,
    use_prv_candidates: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let pipeline = match (survey, use_prv_candidates) {
        (Survey::Ztf, true) => ZTF_TEST_PIPELINE_PRV_CANDIDATES,
        (Survey::Ztf, false) => ZTF_TEST_PIPELINE,
        (Survey::Lsst, _) => LSST_TEST_PIPELINE,
        _ => {
            return Err(Box::from(format!(
                "Unsupported survey for test filter: {}",
                survey
            )));
        }
    };

    insert_custom_test_filter(survey, pipeline).await
}

pub async fn empty_processed_alerts_queue(
    input_queue_name: &str,
    output_queue_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = conf::load_config(Some(TEST_CONFIG_FILE)).unwrap();
    let mut con = config.build_redis().await?;
    con.del::<&str, usize>(input_queue_name).await.unwrap();
    con.del::<&str, usize>("{}_temp").await.unwrap();
    con.del::<&str, usize>(output_queue_name).await.unwrap();

    Ok(())
}

pub fn get_jds(points: &[LightcurveJdOnly]) -> Vec<f64> {
    points.iter().map(|point| point.jd).collect()
}

pub fn assert_strictly_increasing_unique(points: &[LightcurveJdOnly]) {
    assert!(points.iter().all(|point| point.jd.is_finite()));
    assert!(points.windows(2).all(|window| window[0].jd < window[1].jd));
}

pub struct AuxBranchSnapshot {
    pub series: Vec<Vec<LightcurveJdOnly>>,
    pub version: Option<i32>,
}

#[async_trait]
pub trait AuxUpdateBranchTestAdapter {
    type Worker;
    type ExistingAux;
    type SurveyMatches;
    type Updates;

    async fn load_existing(&self, worker: &Self::Worker, object_id: &str) -> Self::ExistingAux;

    fn snapshot(&self, existing_aux: &Self::ExistingAux) -> AuxBranchSnapshot;

    fn survey_matches(&self) -> Self::SurveyMatches;

    fn empty_updates(&self) -> Self::Updates;

    fn updates_at_jds(&mut self, jds: &[f64]) -> Self::Updates;

    async fn inject_corrupted_existing(&self, worker: &Self::Worker, object_id: &str);

    fn expected_repaired_jds(&self) -> Vec<Vec<f64>>;

    async fn inject_non_finite_existing(&self, worker: &Self::Worker, object_id: &str);

    fn expected_non_finite_repaired_jds(&self) -> Vec<Vec<f64>>;

    async fn apply_update(
        &self,
        worker: &mut Self::Worker,
        object_id: &str,
        updates: Self::Updates,
        survey_matches: &Self::SurveyMatches,
        existing_aux: &Self::ExistingAux,
    );
}

pub async fn assert_update_aux_branches_and_fallback<A>(
    worker: &mut A::Worker,
    object_id: &str,
    adapter: &mut A,
) where
    A: AuxUpdateBranchTestAdapter,
{
    fn lengths(snapshot: &AuxBranchSnapshot) -> Vec<usize> {
        snapshot.series.iter().map(Vec::len).collect()
    }

    fn shifted_last(snapshot: &AuxBranchSnapshot, delta: f64) -> Vec<f64> {
        snapshot
            .series
            .iter()
            .map(|series| series.last().expect("series should not be empty").jd + delta)
            .collect()
    }

    fn shifted_first(snapshot: &AuxBranchSnapshot, delta: f64) -> Vec<f64> {
        snapshot
            .series
            .iter()
            .map(|series| series.first().expect("series should not be empty").jd + delta)
            .collect()
    }

    let survey_matches = adapter.survey_matches();

    // Branch: empty updates should repair corrupted, unsorted, duplicated existing lightcurves.
    adapter.inject_corrupted_existing(worker, object_id).await;
    let corrupted_existing = adapter.load_existing(worker, object_id).await;
    let corrupted_snapshot = adapter.snapshot(&corrupted_existing);

    adapter
        .apply_update(
            worker,
            object_id,
            adapter.empty_updates(),
            &survey_matches,
            &corrupted_existing,
        )
        .await;

    let repaired_existing = adapter.load_existing(worker, object_id).await;
    let repaired_snapshot = adapter.snapshot(&repaired_existing);
    for (series, expected_jds) in repaired_snapshot
        .series
        .iter()
        .zip(adapter.expected_repaired_jds().iter())
    {
        assert_strictly_increasing_unique(series);
        assert_eq!(get_jds(series), *expected_jds);
    }
    assert_eq!(
        repaired_snapshot.version,
        Some(corrupted_snapshot.version.unwrap_or(0) + 1),
        "repair update should bump version"
    );

    // Branch: non-finite existing values should also be repaired by fallback.
    adapter.inject_non_finite_existing(worker, object_id).await;
    let non_finite_existing = adapter.load_existing(worker, object_id).await;
    let non_finite_snapshot = adapter.snapshot(&non_finite_existing);

    adapter
        .apply_update(
            worker,
            object_id,
            adapter.empty_updates(),
            &survey_matches,
            &non_finite_existing,
        )
        .await;

    let after_non_finite_existing = adapter.load_existing(worker, object_id).await;
    let after_non_finite = adapter.snapshot(&after_non_finite_existing);
    for (series, expected_jds) in after_non_finite
        .series
        .iter()
        .zip(adapter.expected_non_finite_repaired_jds().iter())
    {
        assert_strictly_increasing_unique(series);
        assert_eq!(get_jds(series), *expected_jds);
    }
    assert_eq!(
        after_non_finite.version,
        Some(non_finite_snapshot.version.unwrap_or(0) + 1),
        "non-finite repair update should bump version"
    );

    // Branch: empty push updates => update uses $set only.
    let existing_before = after_non_finite_existing;
    let before = adapter.snapshot(&existing_before);
    let len_before = lengths(&before);

    adapter
        .apply_update(
            worker,
            object_id,
            adapter.empty_updates(),
            &survey_matches,
            &existing_before,
        )
        .await;

    let after_empty_existing = adapter.load_existing(worker, object_id).await;
    let after_empty = adapter.snapshot(&after_empty_existing);
    assert_eq!(lengths(&after_empty), len_before);
    assert_eq!(
        after_empty.version,
        Some(before.version.unwrap_or(0) + 1),
        "empty update should still bump version"
    );

    // Branch: append-only updates without sort.
    let append_jds = shifted_last(&after_empty, 100.0);
    let append_updates = adapter.updates_at_jds(&append_jds);
    adapter
        .apply_update(
            worker,
            object_id,
            append_updates,
            &survey_matches,
            &after_empty_existing,
        )
        .await;

    let after_append_existing = adapter.load_existing(worker, object_id).await;
    let after_append = adapter.snapshot(&after_append_existing);
    let expected_append_lengths: Vec<usize> = len_before.iter().map(|len| len + 1).collect();
    assert_eq!(lengths(&after_append), expected_append_lengths);

    // Branch: overlap requires full update with sort.
    let sort_jds = shifted_first(&after_append, -50.0);
    let sort_updates = adapter.updates_at_jds(&sort_jds);
    adapter
        .apply_update(
            worker,
            object_id,
            sort_updates,
            &survey_matches,
            &after_append_existing,
        )
        .await;

    let after_sort_existing = adapter.load_existing(worker, object_id).await;
    let after_sort = adapter.snapshot(&after_sort_existing);
    let expected_sorted_lengths: Vec<usize> = len_before.iter().map(|len| len + 2).collect();
    assert_eq!(lengths(&after_sort), expected_sorted_lengths);
    for series in &after_sort.series {
        assert_strictly_increasing_unique(series);
    }

    // Branch: optimistic-lock miss triggers fallback update.
    let stale_aux = adapter.load_existing(worker, object_id).await;
    let stale_snapshot = adapter.snapshot(&stale_aux);
    let fresh_aux = adapter.load_existing(worker, object_id).await;

    let concurrent_jds = shifted_last(&after_sort, 10.0);
    let concurrent_updates = adapter.updates_at_jds(&concurrent_jds);
    adapter
        .apply_update(
            worker,
            object_id,
            concurrent_updates,
            &survey_matches,
            &fresh_aux,
        )
        .await;

    let fallback_jds: Vec<f64> = concurrent_jds.iter().map(|jd| jd + 1.0).collect();
    let fallback_updates = adapter.updates_at_jds(&fallback_jds);
    adapter
        .apply_update(
            worker,
            object_id,
            fallback_updates,
            &survey_matches,
            &stale_aux,
        )
        .await;

    let after_fallback = adapter.snapshot(&adapter.load_existing(worker, object_id).await);
    for (series, jd) in after_fallback.series.iter().zip(fallback_jds.iter()) {
        assert!(
            series.iter().any(|point| (point.jd - jd).abs() < 1e-9),
            "fallback update should insert jd={jd}"
        );
    }
    assert_eq!(
        after_fallback.version,
        Some(stale_snapshot.version.unwrap_or(0) + 2)
    );
}

pub fn randomize_object_id(survey: &Survey) -> String {
    let mut rng = rand::rng();
    match survey {
        Survey::Ztf | Survey::Decam => {
            let mut object_id = survey.to_string();
            for _ in 0..2 {
                object_id.push(rng.random_range('0'..='9'));
            }
            for _ in 0..7 {
                object_id.push(rng.random_range('a'..='z'));
            }
            object_id
        }
        Survey::Lsst => format!("{}", rand::rng().random_range(0..i64::MAX)),
    }
}

#[derive(Clone, Debug)]
pub struct AlertRandomizer {
    survey: Survey,
    payload: Option<Vec<u8>>,
    schema: Option<Schema>,
    schema_registry: Option<SchemaRegistry>,
    candid: Option<i64>,
    object_id: Option<String>, // Use String for all, convert as needed
    ra: Option<f64>,
    dec: Option<f64>,
}

impl AlertRandomizer {
    pub fn new(survey: Survey) -> Self {
        let schema_registry = match survey {
            Survey::Lsst => Some(SchemaRegistry::new(
                Survey::Lsst,
                LSST_SCHEMA_REGISTRY_URL,
                Some(LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL.to_string()),
            )),
            _ => None,
        };
        Self {
            survey,
            payload: None,
            schema: None,
            schema_registry,
            candid: None,
            object_id: None,
            ra: None,
            dec: None,
        }
    }

    pub fn new_randomized(survey: Survey) -> Self {
        let (object_id, payload, schema, schema_registry) = match survey {
            Survey::Ztf | Survey::Decam => {
                let payload = match survey {
                    Survey::Ztf => {
                        fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap()
                    }
                    Survey::Decam => fs::read("tests/data/alerts/decam/alert.avro").unwrap(),
                    _ => unreachable!(),
                };
                let reader = Reader::new(&payload[..]).unwrap();
                let schema = reader.writer_schema().clone();
                (
                    Some(randomize_object_id(&survey)),
                    Some(payload),
                    Some(schema),
                    None,
                )
            }
            Survey::Lsst => {
                let payload = fs::read("tests/data/alerts/lsst/7912941781254298.avro").unwrap();
                (
                    Some(randomize_object_id(&survey)),
                    Some(payload),
                    None,
                    Some(SchemaRegistry::new(
                        Survey::Lsst,
                        LSST_SCHEMA_REGISTRY_URL,
                        Some(LSST_SCHEMA_REGISTRY_GITHUB_FALLBACK_URL.to_string()),
                    )),
                )
            }
        };
        let candid = Some(rand::rng().random_range(0..i64::MAX));
        let ra = Some(rand::rng().random_range(0.0..360.0));
        let dec = Some(rand::rng().random_range(-90.0..90.0));
        Self {
            survey,
            payload,
            schema,
            schema_registry,
            candid,
            object_id,
            ra,
            dec,
        }
    }

    pub fn path(mut self, path: &str) -> Self {
        let payload = fs::read(path).unwrap();
        match self.survey {
            Survey::Lsst => self.payload = Some(payload),
            _ => {
                let reader = Reader::new(&payload[..]).unwrap();
                let schema = reader.writer_schema().clone();
                self.payload = Some(payload);
                self.schema = Some(schema);
            }
        }
        self
    }

    pub fn objectid(mut self, object_id: impl Into<String>) -> Self {
        self.object_id = Some(object_id.into());
        self
    }
    pub fn candid(mut self, candid: i64) -> Self {
        self.candid = Some(candid);
        self
    }
    pub fn ra(mut self, ra: f64) -> Self {
        self.ra = Some(ra);
        self
    }
    pub fn dec(mut self, dec: f64) -> Self {
        self.dec = Some(dec);
        self
    }
    pub fn rand_object_id(mut self) -> Self {
        self.object_id = Some(randomize_object_id(&self.survey));
        self
    }
    pub fn rand_candid(mut self) -> Self {
        self.candid = Some(rand::rng().random_range(0..i64::MAX));
        self
    }
    pub fn rand_ra(mut self) -> Self {
        self.ra = Some(rand::rng().random_range(0.0..360.0));
        self
    }
    pub fn rand_dec(mut self) -> Self {
        self.dec = Some(rand::rng().random_range(-90.0..90.0));
        self
    }

    fn update_candidate_fields(
        candidate_record: &mut Vec<(String, Value)>,
        ra: &mut Option<f64>,
        dec: &mut Option<f64>,
        candid: &mut Option<i64>,
    ) {
        for (key, value) in candidate_record.iter_mut() {
            match key.as_str() {
                "ra" => {
                    if let Some(r) = ra {
                        *value = Value::Double(*r);
                    } else {
                        *ra = Some(Self::value_to_f64(value));
                    }
                }
                "dec" => {
                    if let Some(d) = dec {
                        *value = Value::Double(*d);
                    } else {
                        *dec = Some(Self::value_to_f64(value));
                    }
                }
                "candid" => {
                    if let Some(c) = candid {
                        *value = Value::Long(*c);
                    } else {
                        *candid = Some(Self::value_to_i64(value));
                    }
                }
                _ => {}
            }
        }
    }

    // For LSST, similar logic for diaSource
    fn update_diasource_fields(
        candidate_record: &mut Vec<(String, Value)>,
        candid: &mut Option<i64>,
        object_id: &mut Option<String>,
        ra: &mut Option<f64>,
        dec: &mut Option<f64>,
    ) {
        for (key, value) in candidate_record.iter_mut() {
            match key.as_str() {
                "diaSourceId" => {
                    if let Some(id) = candid {
                        *value = Value::Long(*id);
                    } else {
                        *candid = Some(Self::value_to_i64(value));
                    }
                }
                "diaObjectId" => {
                    if let Some(ref id) = object_id {
                        let id_i64 = id.parse::<i64>().unwrap();
                        *value = Value::Union(1_u32, Box::new(Value::Long(id_i64)));
                    } else {
                        *object_id = Some(Self::value_to_i64(value).to_string());
                    }
                }
                "ra" => {
                    if let Some(r) = ra {
                        *value = Value::Double(*r);
                    } else {
                        *ra = Some(Self::value_to_f64(value));
                    }
                }
                "dec" => {
                    if let Some(d) = dec {
                        *value = Value::Double(*d);
                    } else {
                        *dec = Some(Self::value_to_f64(value));
                    }
                }
                _ => {}
            }
        }
    }

    pub async fn get(self) -> (i64, String, f64, f64, Vec<u8>) {
        match self.survey {
            Survey::Ztf | Survey::Decam => {
                // Use the same logic for ZTF/Decam, just different objectId prefix
                let mut candid = self.candid;
                let mut object_id = self.object_id;
                let mut ra = self.ra;
                let mut dec = self.dec;
                let (payload, schema) = match (self.payload, self.schema) {
                    (Some(payload), Some(schema)) => (payload, schema),
                    _ => {
                        let payload = match self.survey {
                            Survey::Ztf => {
                                fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap()
                            }
                            Survey::Decam => {
                                fs::read("tests/data/alerts/decam/alert.avro").unwrap()
                            }
                            _ => panic!("Unsupported survey for test payload"),
                        };
                        let reader = Reader::new(&payload[..]).unwrap();
                        let schema = reader.writer_schema().clone();
                        (payload, schema)
                    }
                };
                let reader = Reader::new(&payload[..]).unwrap();
                let value = reader.into_iter().next().unwrap().unwrap();
                let mut record = match value {
                    Value::Record(record) => record,
                    _ => panic!("Not a record"),
                };

                for i in 0..record.len() {
                    let (key, value) = &mut record[i];
                    match key.as_str() {
                        "objectId" => {
                            if let Some(ref id) = object_id {
                                *value = Value::String(id.clone());
                            } else {
                                object_id = Some(Self::value_to_string(value));
                            }
                        }
                        "candid" => {
                            if let Some(id) = candid {
                                *value = Value::Long(id);
                            } else {
                                candid = Some(Self::value_to_i64(value));
                            }
                        }
                        "candidate" => {
                            if let Value::Record(candidate_record) = value {
                                Self::update_candidate_fields(
                                    candidate_record,
                                    &mut ra,
                                    &mut dec,
                                    &mut candid,
                                );
                            }
                        }
                        _ => {}
                    }
                }
                let mut writer = Writer::new(&schema, Vec::new());
                let mut new_record = Record::new(writer.schema()).unwrap();
                for (key, value) in record {
                    new_record.put(&key, value);
                }
                writer.append(new_record).unwrap();
                let new_payload = writer.into_inner().unwrap();
                (
                    candid.unwrap(),
                    object_id.unwrap(),
                    ra.unwrap(),
                    dec.unwrap(),
                    new_payload,
                )
            }
            Survey::Lsst => {
                // LSST-specific logic
                let mut candid = self.candid;
                let mut object_id = self.object_id;
                let mut ra = self.ra;
                let mut dec = self.dec;
                let payload = self.payload.unwrap_or_else(|| {
                    fs::read("tests/data/alerts/lsst/7912941781254298.avro").unwrap()
                });
                let header = payload[0..5].to_vec();
                let magic = header[0];
                if magic != 0_u8 {
                    panic!("Not a valid avro file");
                }
                let schema_id = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
                let mut schema_registry = self.schema_registry.expect("Missing schema registry");
                let schema = schema_registry
                    .get_schema("alert-packet", schema_id)
                    .await
                    .unwrap();
                let value = from_avro_datum(&schema, &mut &payload[5..], None).unwrap();
                let mut record = match value {
                    Value::Record(record) => record,
                    _ => panic!("Not a record"),
                };

                for i in 0..record.len() {
                    let (key, value) = &mut record[i];
                    match key.as_str() {
                        "diaSourceId" => {
                            if let Some(id) = candid {
                                *value = Value::Long(id);
                            } else {
                                candid = Some(Self::value_to_i64(value));
                            }
                        }
                        "diaSource" => {
                            if let Value::Record(candidate_record) = value {
                                Self::update_diasource_fields(
                                    candidate_record,
                                    &mut candid,
                                    &mut object_id,
                                    &mut ra,
                                    &mut dec,
                                );
                            }
                        }
                        _ => {}
                    }
                }

                let mut writer = Writer::new(&schema, Vec::new());
                let mut new_record = Record::new(&schema).unwrap();
                for (key, value) in record {
                    new_record.put(&key, value);
                }
                writer.append(new_record).unwrap();
                let new_payload = writer.into_inner().unwrap();

                // Find the start idx of the data
                let mut cursor = std::io::Cursor::new(&new_payload);
                let mut buf = [0; 4];
                cursor.read_exact(&mut buf).unwrap();
                if buf != [b'O', b'b', b'j', 1u8] {
                    panic!("Not a valid avro file");
                }
                let meta_schema = Schema::map(Schema::Bytes);
                from_avro_datum(&meta_schema, &mut cursor, None).unwrap();
                let mut buf = [0; 16];
                cursor.read_exact(&mut buf).unwrap();
                let mut buf: [u8; 4] = [0; 4];
                cursor.read_exact(&mut buf).unwrap();
                let start_idx = cursor.position();

                // conform with the schema registry-like format
                let new_payload = [&header, &new_payload[start_idx as usize..]].concat();

                (
                    candid.unwrap(),
                    object_id.unwrap(),
                    ra.unwrap(),
                    dec.unwrap(),
                    new_payload,
                )
            }
        }
    }

    // Helper conversion functions (same as before)
    fn value_to_string(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            _ => panic!("Not a string"),
        }
    }
    fn value_to_i64(value: &Value) -> i64 {
        match value {
            Value::Long(l) => *l,
            Value::Union(_, box_value) => match box_value.as_ref() {
                Value::Long(l) => *l,
                _ => panic!("Not a long"),
            },
            _ => panic!("Not a long"),
        }
    }
    fn value_to_f64(value: &Value) -> f64 {
        match value {
            Value::Double(d) => *d,
            _ => panic!("Not a double"),
        }
    }
}
