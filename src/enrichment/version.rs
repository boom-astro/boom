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

    // Allocated from a counter rather than a document count, which would reuse
    // an id if a set were ever removed.
    let counter = db
        .collection::<mongodb::bson::Document>(COUNTERS_COLLECTION)
        .find_one_and_update(doc! { "_id": survey }, doc! { "$inc": { "next_id": 1i64 } })
        .upsert(true)
        .return_document(mongodb::options::ReturnDocument::After)
        .await?;
    let id = counter
        .and_then(|d| d.get_i64("next_id").ok())
        .ok_or(VersionError::IdAllocationFailed)?;

    let set = EnrichmentSet {
        id,
        survey: survey.to_string(),
        models,
        derivations,
        fingerprint: fingerprint.clone(),
        first_seen: chrono::Utc::now().timestamp() as f64,
    };

    match sets.insert_one(&set).await {
        Ok(_) => Ok(set),
        // Another worker interned the same set between the read and the write.
        // Its row is as good as ours, and the id it allocated is the one alerts
        // must carry.
        Err(_) => sets
            .find_one(doc! { "fingerprint": &fingerprint })
            .await?
            .ok_or(VersionError::IdAllocationFailed),
    }
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
