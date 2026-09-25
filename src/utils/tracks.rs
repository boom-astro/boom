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

/// A track as stored, keyed by an id that outlives any one linking run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTrack {
    #[serde(rename = "_id")]
    pub id: String,
    /// Candids, ascending. The identity of the track.
    pub members: Vec<i64>,
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
    pub n_detections: i32,
    pub n_nights: i32,
    pub arc_days: f64,
    pub first_jd: f64,
    pub last_jd: f64,
    pub designation: Option<String>,
    /// Ids a merge would absorb and then delete.
    pub superseded: Vec<String>,
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
        format!(
            "{what}: {} detections over {} nights, {:.2} d arc",
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
) -> Result<UpsertPlan, mongodb::error::Error> {
    let existing = overlapping(db, members).await?;
    let (id, superseded, mut all): (Option<String>, Vec<String>, Vec<i64>) =
        match identify(members, &existing) {
            Identity::New => (None, Vec::new(), members.to_vec()),
            Identity::Extends(id) => {
                let mut union = members.to_vec();
                if let Some(prev) = existing.iter().find(|t| t.id == id) {
                    union.extend(prev.members.iter().copied());
                }
                (Some(id), Vec::new(), union)
            }
            Identity::Merges(ids) => {
                let (survivor, gone) = resolve_merge(&ids).expect("a merge has ids");
                let mut union = members.to_vec();
                for t in existing.iter().filter(|t| ids.contains(&t.id)) {
                    union.extend(t.members.iter().copied());
                }
                (Some(survivor), gone, union)
            }
        };
    all.sort_unstable();
    all.dedup();
    // Epochs come from this run; a member it did not see keeps the stored span.
    let (n_detections, n_nights, arc_days, first_jd, last_jd) = summarise(&all, jds);
    Ok(UpsertPlan {
        id,
        members: all,
        n_detections,
        n_nights,
        arc_days,
        first_jd,
        last_jd,
        designation,
        superseded,
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
