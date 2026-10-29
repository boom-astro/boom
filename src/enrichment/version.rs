//! Enrichment provenance and staleness.
//!
//! Each alert's `enrichment_set` names a row in [`SETS_COLLECTION`] recording the models
//! (by file SHA-256) and the derivation versions in [`DERIVATIONS`] that produced it.
//! See `docs/alert-processing.md`.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

const DRIFT_QUERY_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

const MAX_TIME_MS_EXPIRED: i32 = 50;

const NAMESPACE_NOT_FOUND: i32 = 26;

fn is_namespace_not_found(error: &mongodb::error::Error) -> bool {
    matches!(error.kind.as_ref(),
        mongodb::error::ErrorKind::Command(e) if e.code == NAMESPACE_NOT_FOUND)
}

fn is_max_time_expired(error: &mongodb::error::Error) -> bool {
    matches!(error.kind.as_ref(),
        mongodb::error::ErrorKind::Command(e) if e.code == MAX_TIME_MS_EXPIRED)
}

/// Interned enrichment sets, one row per distinct combination ever run.
pub const SETS_COLLECTION: &str = "enrichment_sets";

const COUNTERS_COLLECTION: &str = "enrichment_set_counters";

/// Global, not per survey, because set `_id`s are unique across surveys.
const COUNTER_ID: &str = "enrichment_set";

/// Set published by each survey's workers; the API reads it, since resolving would intern rows.
pub const CURRENT_COLLECTION: &str = "enrichment_current";

/// One ONNX model the enrichment pipeline loads.
pub struct ModelFile {
    /// Key under `classifications`, e.g. `acai_h`.
    pub field: &'static str,
    pub version: &'static str,
    pub path: &'static str,
}

/// Every model loaded in models/mod.rs must be listed here, or the fingerprint misses it.
pub const ZTF_MODELS: &[ModelFile] = &[
    ModelFile {
        field: "cider",
        version: "plus_embedding",
        path: "data/models/cider_fusion_plus_embedding.onnx",
    },
    ModelFile {
        field: "acai_h",
        version: "d1_dnn_20201130",
        path: "data/models/acai_h.d1_dnn_20201130.onnx",
    },
    ModelFile {
        field: "acai_n",
        version: "d1_dnn_20201130",
        path: "data/models/acai_n.d1_dnn_20201130.onnx",
    },
    ModelFile {
        field: "acai_v",
        version: "d1_dnn_20201130",
        path: "data/models/acai_v.d1_dnn_20201130.onnx",
    },
    ModelFile {
        field: "acai_o",
        version: "d1_dnn_20201130",
        path: "data/models/acai_o.d1_dnn_20201130.onnx",
    },
    ModelFile {
        field: "acai_b",
        version: "d1_dnn_20201130",
        path: "data/models/acai_b.d1_dnn_20201130.onnx",
    },
    ModelFile {
        field: "btsbot",
        version: "v2.0.0",
        path: "data/models/btsbot-v2.0.0.onnx",
    },
];

/// Bump a version whenever a change alters what enrichment writes.
pub const DERIVATIONS: &[(&str, u32)] = &[
    // photstats and multisurvey_photstats.
    ("photstats", 1),
    ("sso", 1),
    // rock, star, near_brightstar and stationary.
    ("crossmatch_flags", 1),
    ("detection_history", 1),
    // hosted. Also bump when the DLR scoring parameters change: hosts are rewritten in place.
    ("host_association", 1),
    // applecider_fusion, applecider_outputs and fusion_embedding.
    ("applecider_postprocess", 1),
];

/// Only GPU builds compute `villar_fit`; bump it when the PSO fit or its `PsoConfig` changes.
#[cfg(feature = "gpu")]
pub const GPU_DERIVATIONS: &[(&str, u32)] = &[("villar_fit", 1)];
#[cfg(not(feature = "gpu"))]
pub const GPU_DERIVATIONS: &[(&str, u32)] = &[];

/// A model as recorded on a set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelIdentity {
    pub name: String,
    pub sha256: String,
}

/// What produced an alert's enrichment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnrichmentSet {
    /// The integer stamped onto each alert.
    #[serde(rename = "_id")]
    pub id: i64,
    pub survey: String,
    pub models: BTreeMap<String, ModelIdentity>,
    pub derivations: BTreeMap<String, u32>,
    pub fingerprint: String,
    pub first_seen: f64,
    /// Set when an operator decides alerts at this set need no reprocessing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted: Option<Acceptance>,
}

/// A decision that a non-current set is good enough.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Acceptance {
    /// Who decided, realm-qualified as elsewhere.
    pub actor: String,
    pub reason: String,
    pub accepted_at: f64,
    /// The current set at decision time; the acceptance lapses once that moves on.
    pub accepted_against: i64,
}

#[derive(thiserror::Error, Debug)]
pub enum VersionError {
    #[error("failed to read model {path}: {source}")]
    ModelUnreadable {
        path: String,
        source: std::io::Error,
    },
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error("failed to allocate an enrichment set id")]
    IdAllocationFailed,
    #[error("failed to intern the enrichment set: {0}")]
    InternFailed(String),
    #[error("no enrichment set {set_id} for survey {survey}")]
    UnknownSet { set_id: i64, survey: String },
    #[error("enrichment set {0} is the current one, so there is nothing to accept")]
    AcceptCurrent(i64),
}

impl VersionError {
    /// Caller errors, which the acceptance routes map to a 4xx.
    pub fn is_bad_request(&self) -> bool {
        matches!(
            self,
            VersionError::UnknownSet { .. } | VersionError::AcceptCurrent(_)
        )
    }
}

fn hash_file(path: &str) -> Result<String, VersionError> {
    let bytes = std::fs::read(Path::new(path)).map_err(|source| VersionError::ModelUnreadable {
        path: path.to_string(),
        source,
    })?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn current_models(
    models: &[ModelFile],
) -> Result<BTreeMap<String, ModelIdentity>, VersionError> {
    models
        .iter()
        .map(|model| {
            Ok((
                model.field.to_string(),
                ModelIdentity {
                    name: model.version.to_string(),
                    sha256: hash_file(model.path)?,
                },
            ))
        })
        .collect()
}

/// `gpu_active` is the runtime config, not the build: a GPU image can run with the GPU off.
pub fn current_derivations(gpu_active: bool) -> BTreeMap<String, u32> {
    let gpu: &[(&str, u32)] = if gpu_active { GPU_DERIVATIONS } else { &[] };
    DERIVATIONS
        .iter()
        .chain(gpu)
        .map(|(name, version)| (name.to_string(), *version))
        .collect()
}

pub fn fingerprint(
    survey: &str,
    models: &BTreeMap<String, ModelIdentity>,
    derivations: &BTreeMap<String, u32>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(survey.as_bytes());
    for (field, identity) in models {
        hasher.update(field.as_bytes());
        hasher.update(identity.sha256.as_bytes());
    }
    for (name, version) in derivations {
        hasher.update(name.as_bytes());
        hasher.update(version.to_string().as_bytes());
    }
    hex(&hasher.finalize())
}

/// Only ZTF stamps today; selecting `stale` on another survey would reprocess it forever.
pub fn stamps_enrichment_set(survey: &str) -> bool {
    survey.eq_ignore_ascii_case("ztf")
}

/// The set id for what this process is about to run, interning it if new.
pub async fn resolve_current_set(
    db: &mongodb::Database,
    survey: &str,
    models: &[ModelFile],
    gpu_active: bool,
) -> Result<EnrichmentSet, VersionError> {
    let models = current_models(models)?;
    let derivations = current_derivations(gpu_active);
    let fingerprint = fingerprint(survey, &models, &derivations);
    let set = intern(db, survey, models, derivations, fingerprint).await?;
    publish_current_set(db, survey, set.id).await?;
    Ok(set)
}

async fn intern(
    db: &mongodb::Database,
    survey: &str,
    models: BTreeMap<String, ModelIdentity>,
    derivations: BTreeMap<String, u32>,
    fingerprint: String,
) -> Result<EnrichmentSet, VersionError> {
    use mongodb::bson::doc;

    let sets = db.collection::<EnrichmentSet>(SETS_COLLECTION);
    if let Some(existing) = sets.find_one(doc! { "fingerprint": &fingerprint }).await? {
        return Ok(existing);
    }

    // The counter can lag behind existing ids (e.g. a restored backup); each retry advances it.
    const ATTEMPTS: usize = 5;
    let mut last_error = None;

    for _ in 0..ATTEMPTS {
        let counter = db
            .collection::<mongodb::bson::Document>(COUNTERS_COLLECTION)
            .find_one_and_update(
                doc! { "_id": COUNTER_ID },
                doc! { "$inc": { "next_id": 1i64 } },
            )
            .upsert(true)
            .return_document(mongodb::options::ReturnDocument::After)
            .await?;
        let id = counter
            .and_then(|d| d.get_i64("next_id").ok())
            .ok_or(VersionError::IdAllocationFailed)?;

        let set = EnrichmentSet {
            id,
            survey: survey.to_string(),
            models: models.clone(),
            derivations: derivations.clone(),
            fingerprint: fingerprint.clone(),
            first_seen: chrono::Utc::now().timestamp() as f64,
            accepted: None,
        };

        match sets.insert_one(&set).await {
            Ok(_) => return Ok(set),
            Err(e) => {
                // A concurrent worker interned the same set.
                if let Some(existing) = sets.find_one(doc! { "fingerprint": &fingerprint }).await? {
                    return Ok(existing);
                }
                last_error = Some(e.to_string());
            }
        }
    }

    Err(VersionError::InternFailed(
        last_error.unwrap_or_else(|| "exhausted id attempts".to_string()),
    ))
}

/// Which components differ between two sets.
pub fn diff(from: &EnrichmentSet, to: &EnrichmentSet) -> Vec<String> {
    let mut changed = Vec::new();
    for (field, identity) in &to.models {
        if from.models.get(field).map(|m| &m.sha256) != Some(&identity.sha256) {
            changed.push(field.clone());
        }
    }
    for field in from.models.keys() {
        if !to.models.contains_key(field) {
            changed.push(field.clone());
        }
    }
    for (name, version) in &to.derivations {
        if from.derivations.get(name) != Some(version) {
            changed.push(name.clone());
        }
    }
    for name in from.derivations.keys() {
        if !to.derivations.contains_key(name) {
            changed.push(name.clone());
        }
    }
    changed.sort();
    changed.dedup();
    changed
}

/// What the admin page shows about enrichment drift.
#[derive(Debug, Clone, Serialize)]
pub struct DriftStatus {
    pub survey: String,
    /// `None` until a worker has published; the other fields are then empty.
    pub current_set: Option<i64>,
    /// Sets seen on alerts that are neither current nor accepted.
    pub stale_sets: Vec<StaleSet>,
    pub accepted_sets: Vec<i64>,
    /// Whether some alerts predate stamping and so cannot be shown to be current.
    pub has_unstamped: bool,
    /// Drift query timed out (index still building): an empty `stale_sets` proves nothing.
    pub drift_unknown: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StaleSet {
    pub id: i64,
    /// Models or derivations that differ from the current set.
    pub changed: Vec<String>,
}

/// Upserted so a worker rolled back to an older release moves the current set backwards.
pub async fn publish_current_set(
    db: &mongodb::Database,
    survey: &str,
    set_id: i64,
) -> Result<(), VersionError> {
    use mongodb::bson::doc;
    db.collection::<mongodb::bson::Document>(CURRENT_COLLECTION)
        .update_one(
            doc! { "_id": survey },
            doc! { "$set": { "set_id": set_id, "updated_at": chrono::Utc::now().timestamp() } },
        )
        .upsert(true)
        .await?;
    Ok(())
}

/// `None` means no enrichment worker has started since stamping was deployed.
pub async fn current_set_id(
    db: &mongodb::Database,
    survey: &str,
) -> Result<Option<i64>, VersionError> {
    use mongodb::bson::doc;
    Ok(db
        .collection::<mongodb::bson::Document>(CURRENT_COLLECTION)
        .find_one(doc! { "_id": survey })
        .await?
        .and_then(|d| d.get_i64("set_id").ok()))
}

/// The current set plus those accepted against it; acceptances lapse when it moves on.
pub async fn acceptable_set_ids(
    db: &mongodb::Database,
    survey: &str,
    current: i64,
) -> Result<Vec<i64>, VersionError> {
    use futures::TryStreamExt;
    use mongodb::bson::doc;

    let accepted: Vec<EnrichmentSet> = db
        .collection::<EnrichmentSet>(SETS_COLLECTION)
        .find(doc! { "survey": survey, "accepted.accepted_against": current })
        .await?
        .try_collect()
        .await?;

    let mut ids: Vec<i64> = accepted.into_iter().map(|s| s.id).collect();
    ids.push(current);
    ids.sort_unstable();
    Ok(ids)
}

/// The query for alerts that need re-enriching.
pub fn stale_filter(acceptable: &[i64]) -> mongodb::bson::Document {
    use mongodb::bson::doc;
    // `$nin` also matches alerts with no `enrichment_set`, i.e. enriched before stamping.
    doc! { "enrichment_set": { "$nin": acceptable } }
}

/// Whether the distinct in [`drift_status`] can be trusted: it needs the
/// `enrichment_set` index to see alerts missing the field. A survey that has
/// never ingested has no collection to list, and no alert that could be stale,
/// so it reads normally rather than reporting the absence as doubt.
async fn can_read_drift(
    alerts: &mongodb::Collection<mongodb::bson::Document>,
) -> Result<bool, VersionError> {
    use futures::TryStreamExt;
    let indexes: Vec<_> = match alerts.list_indexes().await {
        Ok(cursor) => cursor.try_collect().await?,
        Err(e) if is_namespace_not_found(&e) => return Ok(true),
        Err(e) => return Err(e.into()),
    };
    Ok(indexes.iter().any(|index| {
        index
            .keys
            .keys()
            .next()
            .is_some_and(|k| k == "enrichment_set")
    }))
}

/// Uses distinct set ids, not per-set counts: counting scans the archive and the page polls.
pub async fn drift_status(
    db: &mongodb::Database,
    survey: &str,
) -> Result<DriftStatus, VersionError> {
    use mongodb::bson::doc;

    let Some(current_id) = current_set_id(db, survey).await? else {
        return Ok(DriftStatus {
            survey: survey.to_string(),
            current_set: None,
            stale_sets: Vec::new(),
            accepted_sets: Vec::new(),
            has_unstamped: false,
            drift_unknown: false,
        });
    };
    let current = db
        .collection::<EnrichmentSet>(SETS_COLLECTION)
        .find_one(doc! { "_id": current_id })
        .await?;
    let acceptable = acceptable_set_ids(db, survey, current_id).await?;

    let alerts =
        db.collection::<mongodb::bson::Document>(&format!("{}_alerts", survey.to_uppercase()));

    // Without the index the distinct is both expensive and wrong: a collection
    // scan emits nothing for an alert missing the field, so unstamped alerts
    // are invisible and an archive with no stamps at all reads "up to date".
    // The index's null is what makes them visible, so until it exists the only
    // honest answer is that drift is not known.
    if !can_read_drift(&alerts).await? {
        return Ok(DriftStatus {
            survey: survey.to_string(),
            current_set: Some(current_id),
            stale_sets: Vec::new(),
            accepted_sets: acceptable
                .into_iter()
                .filter(|id| *id != current_id)
                .collect(),
            has_unstamped: false,
            drift_unknown: true,
        });
    }

    let seen = match alerts
        .distinct("enrichment_set", doc! {})
        .max_time(DRIFT_QUERY_BUDGET)
        .await
    {
        Ok(seen) => seen,
        Err(e) if is_max_time_expired(&e) => {
            return Ok(DriftStatus {
                survey: survey.to_string(),
                current_set: Some(current_id),
                stale_sets: Vec::new(),
                accepted_sets: acceptable
                    .into_iter()
                    .filter(|id| *id != current_id)
                    .collect(),
                has_unstamped: false,
                drift_unknown: true,
            });
        }
        Err(e) => return Err(e.into()),
    };

    let mut stale_sets = Vec::new();
    let mut has_unstamped = false;
    for value in seen {
        match value.as_i64() {
            Some(id) if !acceptable.contains(&id) => {
                let old = db
                    .collection::<EnrichmentSet>(SETS_COLLECTION)
                    .find_one(doc! { "_id": id })
                    .await?;
                let changed = match (&old, &current) {
                    (Some(old), Some(current)) => diff(old, current),
                    // A missing row cannot be diffed, but its alerts are still stale.
                    _ => Vec::new(),
                };
                stale_sets.push(StaleSet { id, changed });
            }
            Some(_) => {}
            None => has_unstamped = true,
        }
    }
    stale_sets.sort_by_key(|s| s.id);

    Ok(DriftStatus {
        survey: survey.to_string(),
        current_set: Some(current_id),
        stale_sets,
        accepted_sets: acceptable
            .into_iter()
            .filter(|id| *id != current_id)
            .collect(),
        has_unstamped,
        drift_unknown: false,
    })
}

/// Record that a non-current set is acceptable, so it stops being reported as drift.
pub async fn accept_set(
    db: &mongodb::Database,
    survey: &str,
    set_id: i64,
    current: i64,
    actor: &str,
    reason: &str,
) -> Result<(), VersionError> {
    use mongodb::bson::doc;

    if set_id == current {
        return Err(VersionError::AcceptCurrent(set_id));
    }
    let acceptance = Acceptance {
        actor: actor.to_string(),
        reason: reason.to_string(),
        accepted_at: chrono::Utc::now().timestamp() as f64,
        accepted_against: current,
    };
    // Filtered on survey: `acceptable_set_ids` would never see another survey's acceptance.
    let outcome = db
        .collection::<EnrichmentSet>(SETS_COLLECTION)
        .update_one(
            doc! { "_id": set_id, "survey": survey },
            doc! { "$set": { "accepted": mongodb::bson::to_bson(&acceptance)
            .map_err(|e| VersionError::InternFailed(e.to_string()))? } },
        )
        .await?;
    if outcome.matched_count == 0 {
        return Err(VersionError::UnknownSet {
            set_id,
            survey: survey.to_string(),
        });
    }
    Ok(())
}

/// Withdraw an acceptance, so the set is reported as drift again.
pub async fn unaccept_set(
    db: &mongodb::Database,
    survey: &str,
    set_id: i64,
) -> Result<(), VersionError> {
    use mongodb::bson::doc;
    let outcome = db
        .collection::<EnrichmentSet>(SETS_COLLECTION)
        .update_one(
            doc! { "_id": set_id, "survey": survey },
            doc! { "$unset": { "accepted": "" } },
        )
        .await?;
    if outcome.matched_count == 0 {
        return Err(VersionError::UnknownSet {
            set_id,
            survey: survey.to_string(),
        });
    }
    Ok(())
}

pub async fn initialize_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    use mongodb::bson::doc;
    db.collection::<EnrichmentSet>(SETS_COLLECTION)
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "fingerprint": 1 })
                .options(
                    mongodb::options::IndexOptions::builder()
                        .unique(true)
                        .build(),
                )
                .build(),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(name: &str, sha: &str) -> ModelIdentity {
        ModelIdentity {
            name: name.to_string(),
            sha256: sha.to_string(),
        }
    }

    fn set(id: i64, models: &[(&str, &str)], derivations: &[(&str, u32)]) -> EnrichmentSet {
        let models: BTreeMap<String, ModelIdentity> = models
            .iter()
            .map(|(f, sha)| (f.to_string(), model("v", sha)))
            .collect();
        let derivations: BTreeMap<String, u32> = derivations
            .iter()
            .map(|(n, v)| (n.to_string(), *v))
            .collect();
        EnrichmentSet {
            id,
            survey: "ztf".into(),
            fingerprint: fingerprint("ztf", &models, &derivations),
            models,
            derivations,
            first_seen: 0.0,
            accepted: None,
        }
    }

    #[test]
    fn every_declared_model_file_exists_and_is_readable() {
        for m in ZTF_MODELS {
            assert!(
                std::path::Path::new(m.path).exists(),
                "{} is declared but missing at {}",
                m.field,
                m.path
            );
        }
    }

    #[test]
    fn the_loader_and_the_stamp_read_the_same_declaration() {
        for m in ZTF_MODELS {
            let loaded = crate::enrichment::models::model_path_for_test(m.field);
            assert_eq!(loaded, m.path, "{} loads a different file", m.field);
        }
    }

    #[test]
    fn hashing_the_declared_models_produces_one_entry_each() {
        let models = current_models(ZTF_MODELS).expect("models are readable");
        assert_eq!(models.len(), ZTF_MODELS.len());
        for (field, identity) in &models {
            assert_eq!(identity.sha256.len(), 64, "{field}");
            assert!(identity.sha256.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn two_models_do_not_share_a_hash() {
        let models = current_models(ZTF_MODELS).expect("models are readable");
        let mut hashes: Vec<&String> = models.values().map(|m| &m.sha256).collect();
        let count = hashes.len();
        hashes.sort();
        hashes.dedup();
        assert_eq!(hashes.len(), count, "two declared models are the same file");
    }

    #[test]
    fn the_fingerprint_is_stable_and_order_independent() {
        let a = set(1, &[("acai_h", "aa"), ("btsbot", "bb")], &[("sso", 1)]);
        let b = set(2, &[("btsbot", "bb"), ("acai_h", "aa")], &[("sso", 1)]);
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn changing_a_model_changes_the_fingerprint() {
        let before = set(1, &[("btsbot", "aa")], &[]);
        let after = set(2, &[("btsbot", "bb")], &[]);
        assert_ne!(before.fingerprint, after.fingerprint);
    }

    #[test]
    fn changing_a_derivation_version_changes_the_fingerprint() {
        let before = set(1, &[], &[("photstats", 1)]);
        let after = set(2, &[], &[("photstats", 2)]);
        assert_ne!(before.fingerprint, after.fingerprint);
    }

    #[test]
    fn a_diff_names_only_what_moved() {
        let before = set(1, &[("acai_h", "aa"), ("btsbot", "bb")], &[("sso", 1)]);
        let after = set(2, &[("acai_h", "aa"), ("btsbot", "cc")], &[("sso", 1)]);
        assert_eq!(diff(&before, &after), vec!["btsbot"]);
    }

    #[test]
    fn a_diff_reports_a_changed_derivation_too() {
        let before = set(1, &[("acai_h", "aa")], &[("sso", 1), ("photstats", 1)]);
        let after = set(2, &[("acai_h", "aa")], &[("sso", 2), ("photstats", 1)]);
        assert_eq!(diff(&before, &after), vec!["sso"]);
    }

    #[test]
    fn a_model_that_stopped_running_counts_as_changed() {
        let before = set(1, &[("acai_h", "aa"), ("retired", "bb")], &[]);
        let after = set(2, &[("acai_h", "aa")], &[]);
        assert_eq!(diff(&before, &after), vec!["retired"]);
    }

    #[test]
    fn a_derivation_that_stopped_being_declared_counts_as_changed() {
        let before = set(1, &[], &[("sso", 1), ("retired", 1)]);
        let after = set(2, &[], &[("sso", 1)]);
        assert_eq!(diff(&before, &after), vec!["retired"]);
    }

    #[test]
    fn an_unchanged_set_diffs_to_nothing() {
        let a = set(1, &[("acai_h", "aa")], &[("sso", 1)]);
        let b = set(2, &[("acai_h", "aa")], &[("sso", 1)]);
        assert!(diff(&a, &b).is_empty());
    }

    /// A collection scan emits no value for an alert missing the field, so
    /// inferring "are any unstamped" from the distinct reports an all-clear on
    /// an archive where nothing is stamped at all. The index's null is what
    /// makes them visible.
    #[tokio::test]
    async fn drift_is_unknown_until_the_set_index_exists() {
        use mongodb::bson::doc;
        let db = crate::conf::get_test_db().await;
        let survey = "drift_idx_test";
        let alerts = db.collection::<mongodb::bson::Document>("DRIFT_IDX_TEST_alerts");
        alerts.drop().await.ok();

        let set = resolve_current_set(&db, survey, ZTF_MODELS, false)
            .await
            .expect("resolves");
        publish_current_set(&db, survey, set.id)
            .await
            .expect("publishes");
        alerts
            .insert_many(vec![
                doc! { "_id": 1, "enrichment_set": set.id },
                doc! { "_id": 2 },
            ])
            .await
            .expect("inserts");

        let status = drift_status(&db, survey).await.expect("reads");
        assert!(
            status.drift_unknown,
            "without the index an unstamped alert is invisible, so drift cannot be known"
        );

        alerts
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(doc! { "enrichment_set": 1 })
                    .build(),
            )
            .await
            .expect("creates the index");

        let status = drift_status(&db, survey).await.expect("reads");
        assert!(
            !status.drift_unknown,
            "the index is there, so drift is known"
        );
        assert!(status.has_unstamped, "the unstamped alert must be reported");
        alerts.drop().await.ok();
    }

    #[tokio::test]
    async fn a_set_is_interned_once_and_reused() {
        let db = crate::conf::get_test_db().await;
        let first = resolve_current_set(&db, "ztf_test", ZTF_MODELS, false)
            .await
            .expect("resolves");
        let second = resolve_current_set(&db, "ztf_test", ZTF_MODELS, false)
            .await
            .expect("resolves");
        assert_eq!(first.id, second.id, "the same models produced two ids");
        assert_eq!(first.fingerprint, second.fingerprint);
        assert_eq!(first.models.len(), ZTF_MODELS.len());
        assert_eq!(first.derivations.len(), DERIVATIONS.len());
    }

    #[tokio::test]
    async fn a_changed_model_interns_a_new_set() {
        let db = crate::conf::get_test_db().await;
        let survey = format!("ztf_change_{}", uuid::Uuid::new_v4().simple());

        let before = resolve_current_set(&db, &survey, ZTF_MODELS, false)
            .await
            .expect("resolves");

        let swapped: Vec<ModelFile> = ZTF_MODELS
            .iter()
            .map(|m| ModelFile {
                field: m.field,
                version: m.version,
                path: if m.field == "acai_h" {
                    "data/models/acai_n.d1_dnn_20201130.onnx"
                } else {
                    m.path
                },
            })
            .collect();
        let after = resolve_current_set(&db, &survey, &swapped, false)
            .await
            .expect("resolves");

        assert_ne!(before.id, after.id, "a changed model must intern a new set");
        assert_eq!(
            diff(&before, &after),
            vec!["acai_h"],
            "the diff must name only what moved, so a rerun can be selective"
        );

        let _ = db
            .collection::<EnrichmentSet>(SETS_COLLECTION)
            .delete_many(mongodb::bson::doc! { "survey": &survey })
            .await;
    }

    #[tokio::test]
    async fn two_surveys_do_not_collide_on_a_set_id() {
        let db = crate::conf::get_test_db().await;
        let run = uuid::Uuid::new_v4().simple().to_string();
        let a = format!("surveyA_{run}");
        let b = format!("surveyB_{run}");

        let first = resolve_current_set(&db, &a, ZTF_MODELS, false)
            .await
            .expect("first survey resolves");
        let second = resolve_current_set(&db, &b, ZTF_MODELS, false)
            .await
            .expect("second survey resolves");

        assert_ne!(
            first.id, second.id,
            "two surveys were given the same set id"
        );
        assert_ne!(first.fingerprint, second.fingerprint);

        for survey in [&a, &b] {
            let _ = db
                .collection::<EnrichmentSet>(SETS_COLLECTION)
                .delete_many(mongodb::bson::doc! { "survey": survey })
                .await;
        }
    }

    #[test]
    fn the_stale_filter_catches_unstamped_alerts() {
        let filter = stale_filter(&[7]);
        assert_eq!(
            filter,
            mongodb::bson::doc! { "enrichment_set": { "$nin": [7i64] } }
        );
    }

    #[tokio::test]
    async fn resolving_publishes_what_the_worker_is_running() {
        let db = crate::conf::get_test_db().await;
        let survey = format!("publish_{}", uuid::Uuid::new_v4().simple());

        assert_eq!(
            current_set_id(&db, &survey).await.unwrap(),
            None,
            "nothing is published before a worker starts"
        );

        let set = resolve_current_set(&db, &survey, ZTF_MODELS, false)
            .await
            .expect("resolves");
        assert_eq!(current_set_id(&db, &survey).await.unwrap(), Some(set.id));
    }

    #[tokio::test]
    async fn a_rollback_moves_the_current_set_backwards() {
        let db = crate::conf::get_test_db().await;
        let survey = format!("rollback_{}", uuid::Uuid::new_v4().simple());

        let first = resolve_current_set(&db, &survey, ZTF_MODELS, false)
            .await
            .expect("resolves");
        publish_current_set(&db, &survey, first.id + 1000)
            .await
            .expect("publishes");
        assert_eq!(
            current_set_id(&db, &survey).await.unwrap(),
            Some(first.id + 1000)
        );
        publish_current_set(&db, &survey, first.id)
            .await
            .expect("publishes");
        assert_eq!(current_set_id(&db, &survey).await.unwrap(), Some(first.id));
    }

    #[tokio::test]
    async fn drift_status_reads_rather_than_resolving() {
        let db = crate::conf::get_test_db().await;
        let survey = format!("driftread_{}", uuid::Uuid::new_v4().simple());

        let before = drift_status(&db, &survey).await.expect("reads");
        assert_eq!(
            before.current_set, None,
            "no worker has published, so there is nothing to compare against"
        );
        assert!(before.stale_sets.is_empty());
        assert_eq!(
            db.collection::<EnrichmentSet>(SETS_COLLECTION)
                .count_documents(mongodb::bson::doc! { "survey": &survey })
                .await
                .unwrap(),
            0,
            "reading the status interned a set"
        );

        let set = resolve_current_set(&db, &survey, ZTF_MODELS, false)
            .await
            .expect("resolves");
        let after = drift_status(&db, &survey).await.expect("reads");
        assert_eq!(after.current_set, Some(set.id));
    }

    #[tokio::test]
    async fn an_acceptance_must_name_a_set_of_this_survey() {
        let db = crate::conf::get_test_db().await;
        let mine = format!("mine_{}", uuid::Uuid::new_v4().simple());
        let theirs = format!("theirs_{}", uuid::Uuid::new_v4().simple());

        let current = resolve_current_set(&db, &mine, ZTF_MODELS, false)
            .await
            .expect("resolves");
        let other = resolve_current_set(&db, &theirs, ZTF_MODELS, false)
            .await
            .expect("resolves");

        for (set_id, label) in [
            (current.id + 9_999, "an id that exists nowhere"),
            (other.id, "another survey's set"),
        ] {
            let outcome =
                accept_set(&db, &mine, set_id, current.id, "babamul:someone", "why").await;
            assert!(
                matches!(outcome, Err(VersionError::UnknownSet { .. })),
                "{label} was accepted: {outcome:?}"
            );
        }

        let outcome =
            accept_set(&db, &mine, current.id, current.id, "babamul:someone", "why").await;
        assert!(
            matches!(outcome, Err(VersionError::AcceptCurrent(_))),
            "accepting the current set was allowed: {outcome:?}"
        );

        assert!(matches!(
            unaccept_set(&db, &mine, other.id).await,
            Err(VersionError::UnknownSet { .. })
        ));
    }

    #[tokio::test]
    async fn accepting_a_set_changes_no_alert() {
        let db = crate::conf::get_test_db().await;
        let survey = format!("accept_{}", uuid::Uuid::new_v4().simple());

        let current = resolve_current_set(&db, &survey, ZTF_MODELS, false)
            .await
            .expect("resolves");
        let older = intern(
            &db,
            &survey,
            current.models.clone(),
            [("photstats".to_string(), 99u32)].into_iter().collect(),
            format!("older-{survey}"),
        )
        .await
        .expect("interned");

        assert_eq!(
            acceptable_set_ids(&db, &survey, current.id).await.unwrap(),
            vec![current.id],
            "nothing is acceptable but the current set until someone says so"
        );

        accept_set(
            &db,
            &survey,
            older.id,
            current.id,
            "babamul:someone",
            "no output change",
        )
        .await
        .expect("accepted");

        let mut acceptable = acceptable_set_ids(&db, &survey, current.id).await.unwrap();
        acceptable.sort_unstable();
        let mut expected = vec![current.id, older.id];
        expected.sort_unstable();
        assert_eq!(
            acceptable, expected,
            "the accepted set joins the current one"
        );

        let stored = db
            .collection::<EnrichmentSet>(SETS_COLLECTION)
            .find_one(mongodb::bson::doc! { "_id": older.id })
            .await
            .unwrap()
            .unwrap();
        let acceptance = stored.accepted.expect("recorded");
        assert_eq!(acceptance.reason, "no output change");
        assert_eq!(acceptance.accepted_against, current.id);

        unaccept_set(&db, &survey, older.id)
            .await
            .expect("withdrawn");
        assert_eq!(
            acceptable_set_ids(&db, &survey, current.id).await.unwrap(),
            vec![current.id]
        );

        let _ = db
            .collection::<EnrichmentSet>(SETS_COLLECTION)
            .delete_many(mongodb::bson::doc! { "survey": &survey })
            .await;
    }

    #[tokio::test]
    async fn an_acceptance_does_not_survive_the_current_set_moving_on() {
        let db = crate::conf::get_test_db().await;
        let survey = format!("scoped_{}", uuid::Uuid::new_v4().simple());

        let current = resolve_current_set(&db, &survey, ZTF_MODELS, false)
            .await
            .expect("resolves");
        let older = intern(
            &db,
            &survey,
            current.models.clone(),
            [("photstats".to_string(), 98u32)].into_iter().collect(),
            format!("older2-{survey}"),
        )
        .await
        .expect("interned");

        accept_set(
            &db,
            &survey,
            older.id,
            current.id,
            "babamul:someone",
            "fine for now",
        )
        .await
        .expect("accepted");

        let moved_on = current.id + 1000;
        assert_eq!(
            acceptable_set_ids(&db, &survey, moved_on).await.unwrap(),
            vec![moved_on],
            "the acceptance was scoped to the set it was made against"
        );

        let _ = db
            .collection::<EnrichmentSet>(SETS_COLLECTION)
            .delete_many(mongodb::bson::doc! { "survey": &survey })
            .await;
    }

    #[test]
    fn declared_derivations_are_unique_and_named() {
        let mut names: Vec<&str> = DERIVATIONS
            .iter()
            .chain(GPU_DERIVATIONS)
            .map(|(n, _)| *n)
            .collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "a derivation is declared twice");
        assert!(!names.is_empty(), "no derivation versions are declared");
    }

    #[test]
    fn the_gpu_derivations_are_recorded_only_when_the_gpu_will_run() {
        let off = current_derivations(false);
        let on = current_derivations(true);
        for (name, _) in GPU_DERIVATIONS {
            assert!(!off.contains_key(*name), "{name} claimed with the GPU off");
            assert!(on.contains_key(*name), "{name} absent with the GPU on");
        }
        for (name, version) in DERIVATIONS {
            assert_eq!(off.get(*name), Some(version));
            assert_eq!(on.get(*name), Some(version));
        }
        #[cfg(feature = "gpu")]
        assert_ne!(
            fingerprint("ztf", &BTreeMap::new(), &off),
            fingerprint("ztf", &BTreeMap::new(), &on)
        );
    }
}
