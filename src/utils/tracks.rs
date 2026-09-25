//! Persistent identity for linked moving-object tracks.
//!
//! A linking run names its tracks by position in its own output, which is
//! meaningless across runs: one more night of data renames everything. A
//! consumer that keys on the id -- SkyPortal mints an object per track, and an
//! MPC submission quotes it as the `trkSub` -- needs an id that survives the
//! track gaining detections.
//!
//! Identity here is by shared membership rather than by hashing the contents,
//! because the contents are what changes. A run's track that shares detections
//! with a stored one *is* that track, extended.

use apache_avro::AvroSchema;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};

/// Detections two tracks must share to be judged the same object.
///
/// One is too weak: an unrelated track that happens to absorb a single shared
/// detection would take over the stored track's identity.
pub const SHARED_FOR_IDENTITY: usize = 2;

/// How well a bound orbit reproduces a track.
///
/// Propagation is Keplerian and bound-only, so a hyperbolic object cannot fit
/// however clean its astrometry. `Poor` and `None` are where a distant or
/// unbound object shows up, so the distinction has to survive persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundFit {
    /// Converged within the residual a real bound orbit should leave.
    Good,
    /// Converged, but no bound orbit reproduces the arc this well.
    Poor,
    /// No bound orbit exists for the fitted state.
    None,
    /// Too few points to constrain six parameters.
    Ungated,
}

impl BoundFit {
    pub fn as_str(&self) -> &'static str {
        match self {
            BoundFit::Good => "good",
            BoundFit::Poor => "poor",
            BoundFit::None => "none",
            BoundFit::Ungated => "ungated",
        }
    }
}

/// A track as stored, keyed by an id that outlives any one linking run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTrack {
    #[serde(rename = "_id")]
    pub id: String,
    /// Candids, ascending. The identity of the track.
    pub members: Vec<i64>,
    /// Epoch of each member, parallel to `members`. Held so a later run that
    /// sees only part of the track can still count its nights and span.
    #[serde(default)]
    pub epochs: Vec<f64>,
    /// `good`, `poor`, `none` or `ungated`; absent on tracks from the tracklet
    /// path, which gates on its own residual.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_fit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_fit_residual_arcsec: Option<f64>,
    pub n_detections: i32,
    pub n_nights: i32,
    pub arc_days: f64,
    pub first_jd: f64,
    pub last_jd: f64,
    /// MPC designation once the track is matched to a known object, which
    /// distinguishes a recovery from a discovery candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub designation: Option<String>,
}

/// What a filter sees on each member alert.
///
/// Every member carries this, not just the newest, so a filter matches on any
/// epoch. The candids are deliberately not here: carrying every member on every
/// member is quadratic on the stream, and a consumer that wants the other
/// epochs looks the track up by id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema, utoipa::ToSchema)]
pub struct AlertTrack {
    pub id: String,
    pub n_detections: i32,
    pub n_nights: i32,
    pub arc_days: f64,
    pub first_jd: f64,
    pub last_jd: f64,
    /// `good`, `poor`, `none` or `ungated`. A coherent track with no good bound
    /// solution is what a distant or unbound object looks like, so this is
    /// filterable rather than buried in the run's log.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_fit: Option<String>,
    /// Set once the track is matched to a known object, which separates a
    /// recovery from a discovery candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub designation: Option<String>,
}

impl From<&StoredTrack> for AlertTrack {
    fn from(t: &StoredTrack) -> Self {
        AlertTrack {
            id: t.id.clone(),
            n_detections: t.n_detections,
            n_nights: t.n_nights,
            arc_days: t.arc_days,
            first_jd: t.first_jd,
            last_jd: t.last_jd,
            bound_fit: t.bound_fit.clone(),
            designation: t.designation.clone(),
        }
    }
}

/// How an incoming track relates to what is already stored.
#[derive(Debug, Clone, PartialEq)]
pub enum Identity {
    /// Shares enough detections with exactly one stored track.
    Extends(String),
    /// Bridges several stored tracks, which one object's detections split into
    /// before enough nights existed to join them.
    Merges(Vec<String>),
    /// Nothing stored shares enough detections.
    New,
}

/// Which stored track, if any, an incoming set of detections belongs to.
pub fn identify(members: &[i64], stored: &[StoredTrack]) -> Identity {
    let incoming: std::collections::HashSet<i64> = members.iter().copied().collect();
    let mut matched: Vec<String> = stored
        .iter()
        .filter(|s| {
            s.members.iter().filter(|m| incoming.contains(m)).count() >= SHARED_FOR_IDENTITY
        })
        .map(|s| s.id.clone())
        .collect();
    matched.sort();
    match matched.len() {
        0 => Identity::New,
        1 => Identity::Extends(matched.remove(0)),
        _ => Identity::Merges(matched),
    }
}

/// The id a merge keeps and the ids it absorbs.
///
/// The lowest survives, being the earliest minted, so a consumer that already
/// recorded the track under it still resolves. The rest are returned rather
/// than dropped: a consumer holding an object per superseded id needs to be
/// told which ones to fold in.
pub fn resolve_merge(ids: &[String]) -> Option<(String, Vec<String>)> {
    let survivor = ids.iter().min()?.clone();
    let superseded = ids.iter().filter(|i| **i != survivor).cloned().collect();
    Some((survivor, superseded))
}

/// Format a minted id. Sequential and short enough to quote as an MPC `trkSub`,
/// which is limited to 8 characters.
pub fn format_id(n: u64) -> String {
    format!("BT{n:06}")
}

/// Summarise a track's detections, given their epochs.
///
/// `nights` counts distinct local nights, so a pair split either side of
/// midnight UTC is still one night.
pub fn summarise(members: &[i64], jds: &[f64]) -> (i32, i32, f64, f64, f64) {
    let nights: std::collections::HashSet<i64> = jds
        .iter()
        .map(|&jd| crate::utils::linking::night_of(jd))
        .collect();
    let first = jds.iter().cloned().fold(f64::MAX, f64::min);
    let last = jds.iter().cloned().fold(f64::MIN, f64::max);
    (
        members.len() as i32,
        nights.len() as i32,
        last - first,
        first,
        last,
    )
}

/// The `$set` that stamps a track onto one of its member alerts.
pub fn alert_update(track: &AlertTrack) -> Document {
    doc! { "$set": { "track": mongodb::bson::to_bson(track).unwrap_or(mongodb::bson::Bson::Null) } }
}

pub const TRACKS_COLLECTION: &str = "ZTF_tracks";
const COUNTERS_COLLECTION: &str = "boom_counters";

/// Next sequence number, allocated atomically so concurrent runs cannot mint
/// the same id.
async fn next_sequence(db: &mongodb::Database) -> Result<u64, mongodb::error::Error> {
    let doc = db
        .collection::<Document>(COUNTERS_COLLECTION)
        .find_one_and_update(doc! { "_id": "tracks" }, doc! { "$inc": { "seq": 1i64 } })
        .upsert(true)
        .return_document(mongodb::options::ReturnDocument::After)
        .await?;
    Ok(doc.and_then(|d| d.get_i64("seq").ok()).unwrap_or(1) as u64)
}

/// Stored tracks sharing any detection with `members`, which is the candidate
/// set `identify` then applies its threshold to.
async fn overlapping(
    db: &mongodb::Database,
    members: &[i64],
) -> Result<Vec<StoredTrack>, mongodb::error::Error> {
    let mut cursor = db
        .collection::<StoredTrack>(TRACKS_COLLECTION)
        .find(doc! { "members": { "$in": members } })
        .await?;
    let mut out = Vec::new();
    while cursor.advance().await? {
        if let Ok(t) = cursor.deserialize_current() {
            out.push(t);
        }
    }
    Ok(out)
}

/// What an upsert would do, resolved against what is stored but not yet written.
///
/// Separate from the write so a dry run can report the outcome without minting
/// an id, which increments a counter shared with every other run.
#[derive(Debug, Clone, PartialEq)]
pub struct UpsertPlan {
    /// `None` for a track nothing stored matches; commit mints it.
    pub id: Option<String>,
    pub members: Vec<i64>,
    pub epochs: Vec<f64>,
    pub n_detections: i32,
    pub n_nights: i32,
    pub arc_days: f64,
    pub first_jd: f64,
    pub last_jd: f64,
    pub bound_fit: Option<String>,
    pub bound_fit_residual_arcsec: Option<f64>,
    pub designation: Option<String>,
    /// Ids a merge would absorb and then delete.
    pub superseded: Vec<String>,
    /// Detections dropped because another track already owns them.
    pub contested: Vec<i64>,
}

impl UpsertPlan {
    /// One line naming what the write would do, for a dry run's log.
    pub fn describe(&self) -> String {
        let what = match (&self.id, self.superseded.is_empty()) {
            (None, _) => "new".to_string(),
            (Some(id), true) => format!("extends {id}"),
            (Some(id), false) => {
                format!("merges into {id}, absorbing {}", self.superseded.join(", "))
            }
        };
        let contested = if self.contested.is_empty() {
            String::new()
        } else {
            format!(
                ", {} detection(s) left with another track",
                self.contested.len()
            )
        };
        format!(
            "{what}: {} detections over {} nights, {:.2} d arc{contested}",
            self.n_detections, self.n_nights, self.arc_days
        )
    }
}

/// What a committed upsert did.
#[derive(Debug, Clone, PartialEq)]
pub struct Upserted {
    pub track: StoredTrack,
    /// Ids absorbed by a merge, now gone from the collection.
    pub superseded: Vec<String>,
}

/// Resolve a run's track against what is stored, without writing anything.
///
/// `members` and `jds` are parallel and need not be sorted. A merge takes the
/// union of every absorbed track's detections, so no epoch is lost when two
/// partial tracks turn out to be one object.
pub async fn plan_upsert(
    db: &mongodb::Database,
    members: &[i64],
    jds: &[f64],
    designation: Option<String>,
    bound_fit: Option<(BoundFit, Option<f64>)>,
) -> Result<UpsertPlan, mongodb::error::Error> {
    let existing = overlapping(db, members).await?;
    let identity = identify(members, &existing);
    let (id, superseded): (Option<String>, Vec<String>) = match &identity {
        Identity::New => (None, Vec::new()),
        Identity::Extends(id) => (Some(id.clone()), Vec::new()),
        Identity::Merges(ids) => {
            let (survivor, gone) = resolve_merge(ids).expect("a merge has ids");
            (Some(survivor), gone)
        }
    };
    // Tracks this run's detections touch but does not take over. A detection
    // belongs to one track, so it stays with the one that already holds it and
    // is dropped here rather than stamped over.
    let absorbed: Vec<&String> = id.iter().chain(superseded.iter()).collect();
    let mut owned_elsewhere: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for t in existing.iter().filter(|t| !absorbed.contains(&&t.id)) {
        owned_elsewhere.extend(t.members.iter().copied());
    }

    // Epochs carried per member so an extension keeps the nights and the span
    // of detections this run did not see.
    let mut by_candid: std::collections::BTreeMap<i64, f64> = members
        .iter()
        .zip(jds.iter())
        .filter(|(m, _)| !owned_elsewhere.contains(m))
        .map(|(m, j)| (*m, *j))
        .collect();
    for t in existing.iter().filter(|t| absorbed.contains(&&t.id)) {
        for (m, j) in t.members.iter().zip(t.epochs.iter()) {
            by_candid.entry(*m).or_insert(*j);
        }
    }
    let mut contested: Vec<i64> = members
        .iter()
        .copied()
        .filter(|m| owned_elsewhere.contains(m))
        .collect();
    contested.sort_unstable();

    let all: Vec<i64> = by_candid.keys().copied().collect();
    let epochs: Vec<f64> = by_candid.values().copied().collect();
    let (n_detections, n_nights, arc_days, first_jd, last_jd) = summarise(&all, &epochs);
    Ok(UpsertPlan {
        id,
        members: all,
        epochs,
        n_detections,
        n_nights,
        arc_days,
        first_jd,
        last_jd,
        bound_fit: bound_fit.map(|(f, _)| f.as_str().to_string()),
        bound_fit_residual_arcsec: bound_fit.and_then(|(_, r)| r),
        designation,
        superseded,
        contested,
    })
}

/// Write a plan, minting an id if it needs one.
pub async fn commit_upsert(
    db: &mongodb::Database,
    plan: UpsertPlan,
) -> Result<Upserted, mongodb::error::Error> {
    let id = match plan.id {
        Some(id) => id,
        None => format_id(next_sequence(db).await?),
    };
    let stored = StoredTrack {
        id: id.clone(),
        members: plan.members,
        epochs: plan.epochs,
        bound_fit: plan.bound_fit,
        bound_fit_residual_arcsec: plan.bound_fit_residual_arcsec,
        n_detections: plan.n_detections,
        n_nights: plan.n_nights,
        arc_days: plan.arc_days,
        first_jd: plan.first_jd,
        last_jd: plan.last_jd,
        designation: plan.designation,
    };
    let collection = db.collection::<StoredTrack>(TRACKS_COLLECTION);
    collection
        .replace_one(doc! { "_id": &id }, &stored)
        .upsert(true)
        .await?;
    if !plan.superseded.is_empty() {
        collection
            .delete_many(doc! { "_id": { "$in": &plan.superseded } })
            .await?;
    }
    Ok(Upserted {
        track: stored,
        superseded: plan.superseded,
    })
}

/// Stamp the track onto every one of its member alerts, so a filter matches on
/// any epoch rather than only the one that closed the track.
pub async fn stamp_members(
    db: &mongodb::Database,
    stored: &StoredTrack,
) -> Result<u64, mongodb::error::Error> {
    let update = alert_update(&AlertTrack::from(stored));
    let result = db
        .collection::<Document>("ZTF_alerts")
        .update_many(doc! { "_id": { "$in": &stored.members } }, update)
        .await?;
    Ok(result.modified_count)
}

/// One track by id, which is how a consumer reaches the epochs the alert block
/// deliberately does not carry.
pub async fn track_by_id(
    db: &mongodb::Database,
    id: &str,
) -> Result<Option<StoredTrack>, mongodb::error::Error> {
    db.collection::<StoredTrack>(TRACKS_COLLECTION)
        .find_one(doc! { "_id": id })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(id: &str, members: &[i64]) -> StoredTrack {
        StoredTrack {
            id: id.to_string(),
            members: members.to_vec(),
            // One per night, so a helper track's nights match its detections.
            epochs: (0..members.len()).map(|k| 2460000.0 + k as f64).collect(),
            bound_fit: None,
            bound_fit_residual_arcsec: None,
            n_detections: members.len() as i32,
            n_nights: 2,
            arc_days: 1.0,
            first_jd: 2460000.0,
            last_jd: 2460001.0,
            designation: None,
        }
    }

    /// The property the whole module exists for: a track that gains detections
    /// keeps the id a consumer already recorded.
    #[test]
    fn test_a_growing_track_keeps_its_id() {
        let db = vec![stored("BT000001", &[10, 11, 12])];
        // The next night adds two detections and re-links the same object.
        let grown = [10, 11, 12, 13, 14];
        assert_eq!(identify(&grown, &db), Identity::Extends("BT000001".into()));
    }

    /// Losing detections must not rename it either: a tighter cut can drop an
    /// epoch the previous run kept.
    #[test]
    fn test_a_shrinking_track_keeps_its_id() {
        let db = vec![stored("BT000001", &[10, 11, 12, 13])];
        assert_eq!(
            identify(&[11, 12], &db),
            Identity::Extends("BT000001".into())
        );
    }

    /// A single shared detection is a coincidence, not an identity -- otherwise
    /// an unrelated track that absorbs one epoch inherits the stored id.
    #[test]
    fn test_one_shared_detection_is_not_the_same_track() {
        let db = vec![stored("BT000001", &[10, 11, 12])];
        assert_eq!(identify(&[12, 90, 91], &db), Identity::New);
    }

    #[test]
    fn test_an_unrelated_track_is_new() {
        let db = vec![stored("BT000001", &[10, 11, 12])];
        assert_eq!(identify(&[90, 91, 92], &db), Identity::New);
    }

    /// One object's detections can be stored as two tracks before enough nights
    /// exist to join them; the run that joins them reports both.
    #[test]
    fn test_bridging_two_stored_tracks_merges_them() {
        let db = vec![stored("BT000002", &[10, 11]), stored("BT000005", &[20, 21])];
        let joined = [10, 11, 20, 21];
        match identify(&joined, &db) {
            Identity::Merges(ids) => assert_eq!(ids, vec!["BT000002", "BT000005"]),
            other => panic!("expected a merge, got {other:?}"),
        }
        // The merge must also name what it absorbed, or a consumer holding an
        // object per id never learns to fold them.
        let (survivor, superseded) =
            resolve_merge(&["BT000005".into(), "BT000002".into()]).expect("a merge resolves");
        assert_eq!(survivor, "BT000002");
        assert_eq!(superseded, vec!["BT000005"]);
    }

    /// Two epochs a few hours apart either side of midnight UTC are one night.
    #[test]
    fn test_nights_are_counted_locally() {
        let (n_det, nights, arc, ..) = summarise(&[1, 2, 3], &[2460000.9, 2460001.05, 2460002.9]);
        assert_eq!(n_det, 3);
        assert_eq!(nights, 2, "the first two epochs are one night");
        assert!((arc - 2.0).abs() < 1e-9, "arc {arc}");
    }

    /// The id is quoted as an MPC trkSub, which is capped at 8 characters.
    #[test]
    fn test_minted_ids_fit_a_trksub() {
        assert_eq!(format_id(1), "BT000001");
        assert!(format_id(999_999).len() <= 8);
    }

    /// A run that re-links only part of a track must not shrink its night count:
    /// the stored epochs are what make the extension additive.
    #[test]
    fn test_an_extension_keeps_the_nights_it_already_had() {
        // Stored: three detections on three nights. This run sees only the last
        // two, plus one new night.
        let prev = stored("BT000001", &[10, 11, 12]);
        assert_eq!(prev.epochs.len(), 3);
        let mut merged: std::collections::BTreeMap<i64, f64> =
            [(11, prev.epochs[1]), (12, prev.epochs[2]), (13, 2460009.0)]
                .into_iter()
                .collect();
        for (m, j) in prev.members.iter().zip(prev.epochs.iter()) {
            merged.entry(*m).or_insert(*j);
        }
        let all: Vec<i64> = merged.keys().copied().collect();
        let epochs: Vec<f64> = merged.values().copied().collect();
        let (n_det, nights, ..) = summarise(&all, &epochs);
        assert_eq!(n_det, 4, "the stored detection this run missed was dropped");
        assert_eq!(nights, 4, "a night the run did not see was dropped");
    }

    /// The vocabulary is persisted, so a distant or unbound track stays
    /// distinguishable from a clean one after a restart.
    #[test]
    fn test_bound_fit_survives_as_a_filterable_string() {
        assert_eq!(BoundFit::Good.as_str(), "good");
        assert_eq!(BoundFit::Poor.as_str(), "poor");
        assert_eq!(BoundFit::None.as_str(), "none");
        assert_eq!(BoundFit::Ungated.as_str(), "ungated");
        let mut t = stored("BT000001", &[10, 11]);
        t.bound_fit = Some(BoundFit::Poor.as_str().to_string());
        assert_eq!(AlertTrack::from(&t).bound_fit.as_deref(), Some("poor"));
    }

    /// Epochs are quoted as MPC astrometry, where 1e-5 d is 0.86 s -- enough to
    /// put a visible timing error into a submission.
    #[test]
    fn test_epochs_keep_full_precision() {
        let mut t = stored("BT000001", &[10, 11]);
        t.first_jd = 2461293.8666435;
        let carried = AlertTrack::from(&t);
        assert_eq!(carried.first_jd, 2461293.8666435);
    }
}
