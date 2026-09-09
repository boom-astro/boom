//! What produced an alert's enrichment, and how to tell when it is stale.
//!
//! Enrichment is applied once, as an alert is ingested. When it changes -- a new
//! classifier, a corrected formula -- every previously enriched alert holds
//! values the current code would not produce. Without a record of *what*
//! produced them there is no query for "alerts needing re-enrichment", and the
//! only remedy is re-running everything.
//!
//! The scars of that are already visible on `ZtfAlertProperties`: fields like
//! `sso` are `Option` with a comment telling consumers not to read `None` as
//! "not an asteroid", because absence means "enriched before this existed"
//! rather than "evaluated and found negative". That is this problem, worked
//! around one field at a time.
//!
//! So each alert carries one integer, `enrichment_set`, naming a row in
//! [`SETS_COLLECTION`] that records exactly what ran:
//!
//! ```jsonc
//! { "_id": 7, "survey": "ztf",
//!   "models": { "acai_h": { "name": "d1_dnn_20201130", "sha256": "ab12…" }, … },
//!   "derivations": { "photstats": 1, "sso": 1, "crossmatch_flags": 1 },
//!   "fingerprint": "…", "first_seen": 1765000000 }
//! ```
//!
//! One integer per alert rather than a version per field: at ~10^9 alerts the
//! difference is tens of gigabytes of pure bookkeeping. `{ enrichment_set: {
//! $ne: <current> } }` is then an indexable, exact query for what is stale.
//!
//! **Interning rather than a counter is what keeps reprocessing selective.**
//! Diffing two sets says *which* components moved, so a run can re-score one
//! model instead of six, or recompute `properties.sso` and leave the
//! classifications alone.
//!
//! ## Models are hashed; derivations are declared
//!
//! A model's identity is the SHA-256 of its file. A filename is a claim -- the
//! weights behind `acai_h.d1_dnn_20201130.onnx` can be replaced without anyone
//! renaming it -- and a content hash cannot lie. The declared name rides along
//! as a human label.
//!
//! Derivation logic cannot be hashed the same way. "Did this change alter the
//! output?" is a semantic question: a refactor does not, a corrected formula
//! does, and hashing the source would flag every comment edit until nobody
//! trusted the signal. So those versions are **declared by hand** in
//! [`DERIVATIONS`], bumped when a change alters what enrichment writes. See
//! `docs/alert-processing.md`.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

/// Interned enrichment sets, one row per distinct combination ever run.
pub const SETS_COLLECTION: &str = "enrichment_sets";

/// Allocates the integer ids for [`SETS_COLLECTION`].
const COUNTERS_COLLECTION: &str = "enrichment_set_counters";

/// The single sequence every set id comes from.
///
/// Deliberately not per-survey: `_id` is unique across the whole collection, so
/// a per-survey counter would have ZTF's set 1 and LSST's set 1 collide the
/// first time a second survey enriched anything. Ids are globally unique and a
/// set carries its survey as a field.
const COUNTER_ID: &str = "enrichment_set";

/// One ONNX model the enrichment pipeline loads.
///
/// Declared once here rather than at each load site: the CPU and GPU paths both
/// need the path, and a stamp is worthless if the two can disagree about which
/// file is in use.
pub struct ModelFile {
    /// Key under `classifications`, e.g. `acai_h`.
    pub field: &'static str,
    /// The published version, for humans reading a set back.
    pub version: &'static str,
    pub path: &'static str,
}

/// Every model the ZTF enrichment worker runs.
pub const ZTF_MODELS: &[ModelFile] = &[
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

/// Versions of the enrichment logic that is code rather than a model.
///
/// **Bump one when a change alters what enrichment writes.** A refactor that
/// produces identical output should not; a corrected formula, a changed
/// threshold, or a new field must. Getting this wrong in the cautious direction
/// costs a reprocessing run; getting it wrong the other way leaves values that
/// look current and are not.
///
/// Kept coarse deliberately -- a handful of components, not one per field -- so
/// that the judgement call is made a few times a year rather than weekly.
pub const DERIVATIONS: &[(&str, u32)] = &[
    // photstats and multisurvey_photstats: per-band photometric statistics.
    ("photstats", 1),
    // sso: solar system association, geometry, and the activity metrics derived
    // from it.
    ("sso", 1),
    // rock / star / near_brightstar / stationary, derived from crossmatches.
    ("crossmatch_flags", 1),
    // detection_history: the per-object detection summary.
    ("detection_history", 1),
];

/// A model as recorded on a set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelIdentity {
    /// The declared version, as a label.
    pub name: String,
    /// SHA-256 of the file, which is the actual identity.
    pub sha256: String,
}

/// What produced an alert's enrichment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnrichmentSet {
    /// The integer stamped onto each alert.
    #[serde(rename = "_id")]
    pub id: i64,
    pub survey: String,
    /// Keyed by classification field, so a diff says which model moved.
    pub models: BTreeMap<String, ModelIdentity>,
    pub derivations: BTreeMap<String, u32>,
    /// Digest of everything above; the key this set is interned on.
    pub fingerprint: String,
    pub first_seen: f64,
    /// Set when an operator has decided alerts at this set need not be
    /// reprocessed, even though it is not the current one.
    ///
    /// **This does not rewrite any alert.** Each alert keeps saying exactly
    /// which set produced it; acceptance is recorded here, once, against the
    /// set. Stamping alerts with a set that did not produce them would make the
    /// provenance record lie, which is the one thing it exists not to do -- and
    /// it would cost a write per alert across the archive to do it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted: Option<Acceptance>,
}

/// A decision that a non-current set is good enough.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Acceptance {
    /// Who decided, realm-qualified as elsewhere.
    pub actor: String,
    /// Why. Required, because "this is fine" is only useful to the next person
    /// if it says on what grounds.
    pub reason: String,
    pub accepted_at: f64,
    /// The set that was current when the decision was made. If the current set
    /// moves on again, that is a new decision to make rather than one already
    /// covered.
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
}

/// Hash a model file.
fn hash_file(path: &str) -> Result<String, VersionError> {
    let bytes = std::fs::read(Path::new(path)).map_err(|source| VersionError::ModelUnreadable {
        path: path.to_string(),
        source,
    })?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex(&hasher.finalize()))
}

/// Lowercase hex, since sha2's output type does not implement `LowerHex`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Read the declared models and hash each one.
///
/// Done once per process at worker startup, not per alert: it is a few
/// megabytes of I/O and the answer cannot change while the process runs.
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

pub fn current_derivations() -> BTreeMap<String, u32> {
    DERIVATIONS
        .iter()
        .map(|(name, version)| (name.to_string(), *version))
        .collect()
}

/// Digest of a model and derivation combination.
///
/// Over a `BTreeMap`, so the ordering is the key ordering and the digest does
/// not change when a declaration is reordered.
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

/// The set id for what this process is about to run, interning it if new.
///
/// Resolved once at startup and reused for every alert. A new row appears only
/// when a model file or a declared derivation version changes, which is a
/// deploy-time event.
pub async fn resolve_current_set(
    db: &mongodb::Database,
    survey: &str,
    models: &[ModelFile],
) -> Result<EnrichmentSet, VersionError> {
    let models = current_models(models)?;
    let derivations = current_derivations();
    let fingerprint = fingerprint(survey, &models, &derivations);
    intern(db, survey, models, derivations, fingerprint).await
}

/// Find the set with this fingerprint, or create it.
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

    // Retried because the counter and the collection can fall out of step --
    // a restored backup, a removed row, or a change to how ids were allocated
    // can leave the next id already taken. The counter advances on every
    // attempt, so a retry moves past the collision rather than repeating it.
    // Without this the worker refuses to start until someone intervenes.
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
                // Another worker interned the same set between the read and the
                // write: its row is as good as ours, and the id it allocated is
                // the one alerts must carry.
                if let Some(existing) = sets.find_one(doc! { "fingerprint": &fingerprint }).await? {
                    return Ok(existing);
                }
                // Otherwise the id itself collided; take the next one.
                last_error = Some(e.to_string());
            }
        }
    }

    Err(VersionError::InternFailed(
        last_error.unwrap_or_else(|| "exhausted id attempts".to_string()),
    ))
}

/// Which components differ between two sets.
///
/// This is why sets record components rather than one opaque number: a
/// reprocessing run can re-score only what moved.
pub fn diff(from: &EnrichmentSet, to: &EnrichmentSet) -> Vec<String> {
    let mut changed = Vec::new();
    for (field, identity) in &to.models {
        if from.models.get(field).map(|m| &m.sha256) != Some(&identity.sha256) {
            changed.push(field.clone());
        }
    }
    // A model that is no longer run is a change too: its scores are now stale
    // in the strongest sense.
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
    changed.sort();
    changed.dedup();
    changed
}

/// What the admin page shows about enrichment drift.
#[derive(Debug, Clone, Serialize)]
pub struct DriftStatus {
    pub survey: String,
    /// The set this release would produce.
    pub current_set: i64,
    /// Sets seen on alerts that are neither current nor accepted, with what
    /// each one differs by. Empty means nothing needs reprocessing.
    pub stale_sets: Vec<StaleSet>,
    /// Sets an operator has explicitly accepted.
    pub accepted_sets: Vec<i64>,
    /// Whether any alert carries no set at all -- enriched before stamping
    /// existed, so it cannot be shown to be current.
    pub has_unstamped: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StaleSet {
    pub id: i64,
    /// Which models or derivations differ from the current set. This is what a
    /// reprocessing run would have to redo.
    pub changed: Vec<String>,
}

/// The set ids that do not need reprocessing: the current one, plus any an
/// operator has accepted against it.
///
/// Acceptance is scoped to the set it was made against. If the current set has
/// moved on since, the old decision no longer applies -- someone said "set 6 is
/// as good as set 7", not "set 6 is good forever".
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
    // `$nin` also matches documents with no `enrichment_set` at all, which is
    // what we want: those were enriched before stamping and cannot be shown to
    // be current.
    doc! { "enrichment_set": { "$nin": acceptable } }
}

/// Which sets alerts are actually sitting at, and how they differ.
///
/// Reads the *distinct* set ids on the collection rather than counting alerts:
/// a count per set is an index scan over the whole archive, and the admin page
/// polls. Distinct over an indexed field with a handful of values is cheap; the
/// magnitude of the drift is a question for the reprocessing run, not for a
/// status poll.
pub async fn drift_status(
    db: &mongodb::Database,
    survey: &str,
    models: &[ModelFile],
) -> Result<DriftStatus, VersionError> {
    use mongodb::bson::doc;

    let current = resolve_current_set(db, survey, models).await?;
    let acceptable = acceptable_set_ids(db, survey, current.id).await?;

    let alerts =
        db.collection::<mongodb::bson::Document>(&format!("{}_alerts", survey.to_uppercase()));
    let seen = alerts.distinct("enrichment_set", doc! {}).await?;

    let mut stale_sets = Vec::new();
    let mut has_unstamped = false;
    for value in seen {
        match value.as_i64() {
            Some(id) if !acceptable.contains(&id) => {
                // What a reprocessing run would have to redo. A set that is no
                // longer in the registry cannot be diffed, and is reported with
                // no detail rather than omitted.
                let changed = match db
                    .collection::<EnrichmentSet>(SETS_COLLECTION)
                    .find_one(doc! { "_id": id })
                    .await?
                {
                    Some(old) => diff(&old, &current),
                    None => Vec::new(),
                };
                stale_sets.push(StaleSet { id, changed });
            }
            Some(_) => {}
            // A null or absent value: enriched before stamping existed.
            None => has_unstamped = true,
        }
    }
    stale_sets.sort_by_key(|s| s.id);

    Ok(DriftStatus {
        survey: survey.to_string(),
        current_set: current.id,
        stale_sets,
        accepted_sets: acceptable
            .into_iter()
            .filter(|id| *id != current.id)
            .collect(),
        has_unstamped,
    })
}

/// Record that a non-current set is acceptable, so it stops being reported as
/// drift.
///
/// Writes one document. It does **not** touch a single alert: the alternative --
/// stamping alerts with a set that did not produce them -- would cost a write
/// per alert and, far worse, would make each of those alerts claim provenance
/// it does not have.
pub async fn accept_set(
    db: &mongodb::Database,
    set_id: i64,
    current: i64,
    actor: &str,
    reason: &str,
) -> Result<(), VersionError> {
    use mongodb::bson::doc;

    if set_id == current {
        // Nothing to accept, and recording it would imply the current set was
        // in some way doubtful.
        return Ok(());
    }
    let acceptance = Acceptance {
        actor: actor.to_string(),
        reason: reason.to_string(),
        accepted_at: chrono::Utc::now().timestamp() as f64,
        accepted_against: current,
    };
    db.collection::<EnrichmentSet>(SETS_COLLECTION)
        .update_one(
            doc! { "_id": set_id },
            doc! { "$set": { "accepted": mongodb::bson::to_bson(&acceptance)
            .map_err(|e| VersionError::InternFailed(e.to_string()))? } },
        )
        .await?;
    Ok(())
}

/// Withdraw an acceptance, so the set is reported as drift again.
pub async fn unaccept_set(db: &mongodb::Database, set_id: i64) -> Result<(), VersionError> {
    use mongodb::bson::doc;
    db.collection::<EnrichmentSet>(SETS_COLLECTION)
        .update_one(
            doc! { "_id": set_id },
            doc! { "$unset": { "accepted": "" } },
        )
        .await?;
    Ok(())
}

/// Index for the drift query.
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
        // A stamp naming a model nobody can hash is worse than none: the worker
        // would fail at startup rather than at the point of confusion.
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
        // If the models loaded and the models hashed could differ, an alert
        // could claim a model it was not scored by -- the exact failure the
        // stamp exists to prevent.
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
            // A sha256 rendered as hex.
            assert_eq!(identity.sha256.len(), 64, "{field}");
            assert!(identity.sha256.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn two_models_do_not_share_a_hash() {
        // A copy-paste in the declaration would otherwise go unnoticed, and two
        // fields claiming one file would make a diff meaningless.
        let models = current_models(ZTF_MODELS).expect("models are readable");
        let mut hashes: Vec<&String> = models.values().map(|m| &m.sha256).collect();
        let count = hashes.len();
        hashes.sort();
        hashes.dedup();
        assert_eq!(hashes.len(), count, "two declared models are the same file");
    }

    #[test]
    fn the_fingerprint_is_stable_and_order_independent() {
        // Reordering a declaration must not invalidate the archive.
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
        // The half a content hash cannot cover: code that produces different
        // output from the same models.
        let before = set(1, &[], &[("photstats", 1)]);
        let after = set(2, &[], &[("photstats", 2)]);
        assert_ne!(before.fingerprint, after.fingerprint);
    }

    #[test]
    fn a_diff_names_only_what_moved() {
        // The reason sets record components: re-score one model, not six.
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
        // Its scores are stale in the strongest sense: nothing produces them
        // any more, so they can never be refreshed in place.
        let before = set(1, &[("acai_h", "aa"), ("retired", "bb")], &[]);
        let after = set(2, &[("acai_h", "aa")], &[]);
        assert_eq!(diff(&before, &after), vec!["retired"]);
    }

    #[test]
    fn an_unchanged_set_diffs_to_nothing() {
        let a = set(1, &[("acai_h", "aa")], &[("sso", 1)]);
        let b = set(2, &[("acai_h", "aa")], &[("sso", 1)]);
        assert!(diff(&a, &b).is_empty());
    }

    #[tokio::test]
    async fn a_set_is_interned_once_and_reused() {
        // The property the whole scheme rests on: two workers starting against
        // the same models must stamp the same id, or the drift query splits the
        // archive along a line that means nothing.
        let db = crate::conf::get_test_db().await;
        let first = resolve_current_set(&db, "ztf_test", ZTF_MODELS)
            .await
            .expect("resolves");
        let second = resolve_current_set(&db, "ztf_test", ZTF_MODELS)
            .await
            .expect("resolves");
        assert_eq!(first.id, second.id, "the same models produced two ids");
        assert_eq!(first.fingerprint, second.fingerprint);
        // Hashes are of the real files on disk.
        assert_eq!(first.models.len(), ZTF_MODELS.len());
        assert_eq!(first.derivations.len(), DERIVATIONS.len());
    }

    #[tokio::test]
    async fn a_changed_model_interns_a_new_set() {
        // What a deploy with new weights looks like: a new row, a new id, and a
        // diff naming exactly the model that moved.
        let db = crate::conf::get_test_db().await;
        let survey = format!("ztf_change_{}", uuid::Uuid::new_v4().simple());

        let before = resolve_current_set(&db, &survey, ZTF_MODELS)
            .await
            .expect("resolves");

        // Stand in for retrained weights by declaring a different file for one
        // field; every other model is unchanged.
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
        let after = resolve_current_set(&db, &survey, &swapped)
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
        // Regression: the id sequence was once per-survey while `_id` is unique
        // across the collection, so ZTF's set 1 and LSST's set 1 collided the
        // first time a second survey enriched anything.
        let db = crate::conf::get_test_db().await;
        let run = uuid::Uuid::new_v4().simple().to_string();
        let a = format!("surveyA_{run}");
        let b = format!("surveyB_{run}");

        let first = resolve_current_set(&db, &a, ZTF_MODELS)
            .await
            .expect("first survey resolves");
        let second = resolve_current_set(&db, &b, ZTF_MODELS)
            .await
            .expect("second survey resolves");

        assert_ne!(
            first.id, second.id,
            "two surveys were given the same set id"
        );
        // The same models under a different survey are a different set, because
        // the survey is part of what a set identifies.
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
        // `$nin` matches a document with no such field, which is what makes
        // pre-stamping alerts show up as stale. They cannot be shown to be
        // current, so treating them as fine would be a guess.
        let filter = stale_filter(&[7]);
        assert_eq!(
            filter,
            mongodb::bson::doc! { "enrichment_set": { "$nin": [7i64] } }
        );
    }

    #[tokio::test]
    async fn accepting_a_set_changes_no_alert() {
        // The point of accepting rather than restamping: an alert stamped with
        // a set that did not produce it would claim provenance it does not
        // have, which is the one thing the stamp exists not to do.
        let db = crate::conf::get_test_db().await;
        let survey = format!("accept_{}", uuid::Uuid::new_v4().simple());

        let current = resolve_current_set(&db, &survey, ZTF_MODELS)
            .await
            .expect("resolves");
        // A second, different set to stand in for an older enrichment.
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

        // The reason is recorded, so the next person can see the grounds.
        let stored = db
            .collection::<EnrichmentSet>(SETS_COLLECTION)
            .find_one(mongodb::bson::doc! { "_id": older.id })
            .await
            .unwrap()
            .unwrap();
        let acceptance = stored.accepted.expect("recorded");
        assert_eq!(acceptance.reason, "no output change");
        assert_eq!(acceptance.accepted_against, current.id);

        // Withdrawing puts it back into drift.
        unaccept_set(&db, older.id).await.expect("withdrawn");
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
        // Someone said "set 6 is as good as set 7", not "set 6 is good
        // forever". When the current set becomes 8, that is a new decision.
        let db = crate::conf::get_test_db().await;
        let survey = format!("scoped_{}", uuid::Uuid::new_v4().simple());

        let current = resolve_current_set(&db, &survey, ZTF_MODELS)
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

        accept_set(&db, older.id, current.id, "babamul:someone", "fine for now")
            .await
            .expect("accepted");

        // A later current set: the old acceptance no longer applies.
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
        let mut names: Vec<&str> = DERIVATIONS.iter().map(|(n, _)| *n).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "a derivation is declared twice");
        assert!(!names.is_empty(), "no derivation versions are declared");
    }
}
