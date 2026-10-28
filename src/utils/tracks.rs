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

use crate::utils::enums::Survey;
use crate::utils::heliolinc::MU;
use crate::utils::linking::{night_at, Detection};
use crate::utils::sso_geometry::{dot, norm, Site, DECAM, RUBIN, WINTER, ZTF};
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Detections two tracks must share to be judged the same object.
///
/// One is too weak: an unrelated track that happens to absorb a single shared
/// detection would take over the stored track's identity.
pub const SHARED_FOR_IDENTITY: usize = 2;

/// How well a bound orbit reproduces a track.
///
/// Fits stay bound unless `max_vinf_km_s` is set, so a hyperbolic object cannot
/// fit however clean its astrometry. `Poor` and `None` are where a distant or
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

    pub fn parse(s: &str) -> Option<BoundFit> {
        match s {
            "good" => Some(BoundFit::Good),
            "poor" => Some(BoundFit::Poor),
            "none" => Some(BoundFit::None),
            "ungated" => Some(BoundFit::Ungated),
            _ => None,
        }
    }

    /// Lower is better: a confident bound orbit first, then the ones worth a
    /// look, then those too short to judge.
    pub fn rank(&self) -> u8 {
        match self {
            BoundFit::Good => 0,
            BoundFit::Poor => 1,
            BoundFit::None => 2,
            BoundFit::Ungated => 3,
        }
    }
}

/// Residuals closer than this are the same fit, refitted.
const SAME_RESIDUAL_ARCSEC: f64 = 0.01;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(into = "OrbitWithElements")]
pub struct StoredOrbit {
    /// UTC, like the detections.
    pub epoch_jd: f64,
    pub pos: [f64; 3],
    pub vel: [f64; 3],
}

#[derive(Serialize)]
struct OrbitWithElements {
    epoch_jd: f64,
    pos: [f64; 3],
    vel: [f64; 3],
    e: f64,
    q: f64,
}

impl From<StoredOrbit> for OrbitWithElements {
    fn from(orbit: StoredOrbit) -> Self {
        let (e, q) = conic(&orbit);
        OrbitWithElements {
            epoch_jd: orbit.epoch_jd,
            pos: orbit.pos,
            vel: orbit.vel,
            e,
            q,
        }
    }
}

/// A track as stored, keyed by an id that outlives any one linking run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTrack {
    #[serde(rename = "_id")]
    pub id: String,
    /// Alert ids, ordered with `member_surveys`. The identity of the track.
    pub members: Vec<i64>,
    /// Empty on tracks written before it, whose member ids encode their survey.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub member_surveys: Vec<Survey>,
    /// Epoch of each member, parallel to `members`. Held so a later run that
    /// sees only part of the track can still count its nights and span.
    #[serde(default)]
    pub epochs: Vec<f64>,
    /// `good`, `poor`, `none` or `ungated`. A track from the tracklet path is
    /// `good`, with the residual its orbit fit left, or `ungated`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_fit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_fit_residual_arcsec: Option<f64>,
    /// How many detections the verdict was fit on. Absent on tracks written
    /// before it was recorded, where it is taken to be all of them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_fit_detections: Option<i32>,
    /// Root mean square of the members' site astrometric errors, arcseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_rms_arcsec: Option<f64>,
    /// `bound_fit_residual_arcsec` over `expected_rms_arcsec`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rms_ratio: Option<f64>,
    /// Reasons to look again before believing the track, such as `to_confirm`.
    /// Nothing is dropped for them. Absent on tracks written before it.
    #[serde(default)]
    pub flags: Vec<String>,
    pub n_detections: i32,
    pub n_nights: i32,
    pub arc_days: f64,
    pub first_jd: f64,
    pub last_jd: f64,
    /// MPC designation once the track is matched to a known object, which
    /// distinguishes a recovery from a discovery candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub designation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The giant planet every detection sat next to: likely one of its satellites.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub near_planet: Option<String>,
    /// When the track was last written, Unix seconds. A client polling for new
    /// and extended tracks asks for the ones updated since its last look.
    /// Zero on tracks written before the field existed.
    #[serde(default)]
    pub updated_at: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orbit: Option<StoredOrbit>,
}

impl StoredTrack {
    pub fn member_keys(&self) -> Vec<Member> {
        if self.member_surveys.is_empty() {
            self.members.iter().map(|&m| legacy_member(m)).collect()
        } else {
            self.member_surveys
                .iter()
                .cloned()
                .zip(self.members.iter().copied())
                .collect()
        }
    }

    pub fn explicit(self) -> Self {
        let (member_surveys, members): (Vec<Survey>, Vec<i64>) =
            self.member_keys().into_iter().unzip();
        StoredTrack {
            members,
            member_surveys,
            ..self
        }
    }
}

/// Stamped on every member alert. The candids are deliberately not here:
/// carrying every member on every member is quadratic, and a client that wants
/// the other epochs looks the track up by id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub near_planet: Option<String>,
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
            kind: t.kind.clone(),
            near_planet: t.near_planet.clone(),
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
pub fn identify(members: &[Member], stored: &[StoredTrack]) -> Identity {
    let incoming: HashSet<&Member> = members.iter().collect();
    let mut matched: Vec<String> = stored
        .iter()
        .filter(|s| {
            s.member_keys()
                .iter()
                .filter(|m| incoming.contains(m))
                .count()
                >= SHARED_FOR_IDENTITY
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
    // MPC-style number packing: ids stay 8 characters and sort as strings in minting order.
    const BASE62: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    if n < 6_200_000 {
        let lead = BASE62[(n / 100_000) as usize] as char;
        return format!("BT{lead}{:05}", n % 100_000);
    }
    let mut rest = n - 6_200_000;
    let mut tail = [b'0'; 5];
    for digit in tail.iter_mut().rev() {
        *digit = BASE62[(rest % 62) as usize];
        rest /= 62;
    }
    format!("BT~{}", String::from_utf8_lossy(&tail))
}

const WINTER_IDS: i64 = 51_000_000_000_000_000;
const DECAM_IDS: i64 = 52_000_000_000_000_000;
const LSST_IDS: i64 = 53_000_000_000_000_000;
const ZTF_IDS: i64 = 400_000_000_000_000_000;

/// A survey and the `_id` of an alert in its collection.
pub type Member = (Survey, i64);

/// Unique within a run: DECam ids span the positive i64, so they are negated.
pub fn detection_id(survey: &Survey, alert_id: i64) -> i64 {
    match survey {
        Survey::Decam => alert_id.wrapping_neg(),
        _ => alert_id,
    }
}

pub fn site_survey(site: Option<&Site>) -> Option<Survey> {
    match site.map(|s| s.mpc_code) {
        None | Some("I41") => Some(Survey::Ztf),
        Some("X05") => Some(Survey::Lsst),
        Some("W84") => Some(Survey::Decam),
        Some("675") => Some(Survey::Winter),
        _ => None,
    }
}

pub fn member_of(d: &Detection) -> Option<Member> {
    let survey = site_survey(d.site.as_ref())?;
    let id = detection_id(&survey, d.id);
    Some((survey, id))
}

fn legacy_member_id(survey: &Survey, alert_id: i64) -> i64 {
    match survey {
        Survey::Ztf | Survey::Lsst => alert_id,
        Survey::Decam => DECAM_IDS.wrapping_add(alert_id),
        Survey::Winter => WINTER_IDS.wrapping_add(alert_id),
    }
}

fn legacy_member(id: i64) -> Member {
    match id {
        WINTER_IDS..DECAM_IDS => (Survey::Winter, id - WINTER_IDS),
        DECAM_IDS..LSST_IDS => (Survey::Decam, id - DECAM_IDS),
        LSST_IDS..ZTF_IDS => (Survey::Lsst, id),
        _ => (Survey::Ztf, id),
    }
}

/// `None` for ZTF, the default site of every fit.
pub fn survey_site(survey: &Survey) -> Option<Site> {
    match survey {
        Survey::Ztf => None,
        Survey::Lsst => Some(RUBIN),
        Survey::Decam => Some(DECAM),
        Survey::Winter => Some(WINTER),
    }
}

pub fn expected_rms_arcsec(members: &[Member]) -> Option<f64> {
    if members.is_empty() {
        return None;
    }
    let sum: f64 = members
        .iter()
        .map(|(survey, _)| {
            survey_site(survey)
                .unwrap_or(ZTF)
                .astrometric_sigma_arcsec
                .powi(2)
        })
        .sum();
    Some((sum / members.len() as f64).sqrt())
}

/// Flag for a track whose detections cannot pin an orbit down.
pub const TO_CONFIRM: &str = "to_confirm";

/// Whether the detections pin an orbit down: a pair on each of two nights gives
/// the motion twice, and four nights give enough positions on their own. Three
/// single detections are six measurements for six parameters, which any orbit
/// fits with no residual.
pub fn constrained(members: &[Member], jds: &[f64]) -> bool {
    let mut per_night: HashMap<i64, usize> = HashMap::new();
    for ((survey, _), &jd) in members.iter().zip(jds) {
        *per_night
            .entry(night_at(jd, survey_site(survey).as_ref()))
            .or_default() += 1;
    }
    per_night.len() >= 4 || per_night.values().filter(|&&n| n >= 2).count() >= 2
}

pub fn track_flags(members: &[Member], jds: &[f64]) -> Vec<String> {
    let mut flags = Vec::new();
    if !constrained(members, jds) {
        flags.push(TO_CONFIRM.to_string());
    }
    flags
}

pub fn alerts_by_survey(members: &[Member]) -> HashMap<Survey, Vec<i64>> {
    let mut out: HashMap<Survey, Vec<i64>> = HashMap::new();
    for (survey, id) in members {
        out.entry(survey.clone()).or_default().push(*id);
    }
    out
}

/// Summarise a track's detections, given their epochs.
///
/// `nights` counts distinct local nights, so a pair split either side of
/// midnight UTC is still one night.
pub fn summarise(members: &[Member], jds: &[f64]) -> (i32, i32, f64, f64, f64) {
    let nights: HashSet<i64> = members
        .iter()
        .zip(jds)
        .map(|((survey, _), &jd)| night_at(jd, survey_site(survey).as_ref()))
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
/// Small documents of shared state: id sequences, the tracks lock, and the
/// last night the linker finished.
pub const COUNTERS_COLLECTION: &str = "boom_counters";
pub const ALIASES_COLLECTION: &str = "ZTF_tracks_aliases";

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

const LOCK_ID: &str = "tracks_lock";
/// How long a lock outlives its holder's last renewal. Short, because a
/// holder that dies keeps every other writer out until it lapses; a pass
/// renews it as it goes, so the lease bounds the gap, not the run.
pub const LOCK_LEASE: std::time::Duration = std::time::Duration::from_secs(15 * 60);
/// How often a pass renews the lock it holds.
pub const LOCK_RENEW_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// The tracks lock, held. Only its holder can renew or release it, so a run
/// whose lease lapsed cannot release the lock another run has since taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracksLock {
    owner: String,
}

/// The lease's end, computed by the server from its own clock, as an update
/// pipeline stage. Every run reads and writes the lease against the same clock,
/// so a run on a host whose clock runs ahead cannot judge a live lock expired.
fn extend_lease() -> Vec<Document> {
    vec![doc! { "$set": { "expires_at": { "$add": ["$$NOW", LOCK_LEASE.as_millis() as i64] } } }]
}

/// Take the tracks lock, or `None` when another run holds it.
///
/// One atomic update of the lock document, created if absent: it hands the
/// lease to this run only when no live lease is recorded, judged by the
/// server's clock, and the run holds the lock if it comes back as the owner.
/// A document from before leases were recorded counts as lapsed.
pub async fn acquire_lock(
    db: &mongodb::Database,
) -> Result<Option<TracksLock>, mongodb::error::Error> {
    let lock = TracksLock {
        owner: uuid::Uuid::new_v4().to_string(),
    };
    let update = vec![
        // A missing lease compares as less than any time, so it is free too.
        doc! { "$set": { "free": { "$lt": ["$expires_at", "$$NOW"] } } },
        doc! { "$set": {
            "owner": { "$cond": ["$free", &lock.owner, "$owner"] },
            "expires_at": {
                "$cond": ["$free", { "$add": ["$$NOW", LOCK_LEASE.as_millis() as i64] }, "$expires_at"]
            },
        } },
        doc! { "$unset": "free" },
    ];
    let after = match db
        .collection::<Document>(COUNTERS_COLLECTION)
        .find_one_and_update(doc! { "_id": LOCK_ID }, update)
        .upsert(true)
        .return_document(mongodb::options::ReturnDocument::After)
        .await
    {
        Ok(after) => after,
        // Two runs creating the document at once: the other one won.
        Err(error) if is_duplicate_key(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let owner = after.as_ref().and_then(|d| d.get_str("owner").ok());
    Ok((owner == Some(lock.owner.as_str())).then_some(lock))
}

fn is_duplicate_key(error: &mongodb::error::Error) -> bool {
    match &*error.kind {
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(e)) => {
            e.code == 11000
        }
        mongodb::error::ErrorKind::Command(e) => e.code == 11000,
        _ => false,
    }
}

/// Extend the lease. False when the lock is no longer this holder's: it
/// lapsed and another run took it, so the caller must stop writing.
pub async fn renew_lock(
    db: &mongodb::Database,
    lock: &TracksLock,
) -> Result<bool, mongodb::error::Error> {
    let result = db
        .collection::<Document>(COUNTERS_COLLECTION)
        .update_one(
            doc! { "_id": LOCK_ID, "owner": &lock.owner },
            extend_lease(),
        )
        .await?;
    Ok(result.matched_count == 1)
}

/// Release the lock if this holder still has it, and never anyone else's.
pub async fn release_lock(
    db: &mongodb::Database,
    lock: &TracksLock,
) -> Result<(), mongodb::error::Error> {
    db.collection::<Document>(COUNTERS_COLLECTION)
        .delete_one(doc! { "_id": LOCK_ID, "owner": &lock.owner })
        .await?;
    Ok(())
}

/// Whether some run holds a live lock, so a caller can wait rather than do
/// work it could not store.
pub async fn lock_is_held(db: &mongodb::Database) -> Result<bool, mongodb::error::Error> {
    let held = db
        .collection::<Document>(COUNTERS_COLLECTION)
        .find_one(doc! { "_id": LOCK_ID, "$expr": { "$gte": ["$expires_at", "$$NOW"] } })
        .await?;
    Ok(held.is_some())
}

/// Stored tracks sharing any detection with `members`, which is the candidate
/// set `identify` then applies its threshold to.
async fn overlapping(
    db: &mongodb::Database,
    members: &[Member],
) -> Result<Vec<StoredTrack>, mongodb::error::Error> {
    let ids: Vec<i64> = members
        .iter()
        .flat_map(|(survey, id)| [*id, legacy_member_id(survey, *id)])
        .collect();
    let mut cursor = db
        .collection::<StoredTrack>(TRACKS_COLLECTION)
        .find(doc! { "members": { "$in": ids } })
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
    pub members: Vec<Member>,
    pub epochs: Vec<f64>,
    pub n_detections: i32,
    pub n_nights: i32,
    pub arc_days: f64,
    pub first_jd: f64,
    pub last_jd: f64,
    pub bound_fit: Option<String>,
    pub bound_fit_residual_arcsec: Option<f64>,
    pub bound_fit_detections: Option<i32>,
    pub orbit: Option<StoredOrbit>,
    pub designation: Option<String>,
    pub kind: Option<String>,
    pub near_planet: Option<String>,
    /// Ids a merge would absorb and then delete.
    pub superseded: Vec<String>,
    /// Detections dropped because another track already owns them.
    pub contested: Vec<Member>,
    /// Detections dropped because the orbit does not reproduce them, left for
    /// other tracks.
    pub dropped: Vec<Member>,
    /// When the stored track was last written, if this plan would write it
    /// back exactly as it is. A nightly run re-finds every track still in its
    /// window, and rewriting those would make each look new to a client
    /// polling `updated_at`.
    pub unchanged_since: Option<f64>,
}

impl UpsertPlan {
    /// Whether what is left still meets the thresholds the run was searched at.
    ///
    /// Dropping detections another track owns can leave too little behind, and a
    /// remnant stored as a track is worse than no track at all.
    pub fn meets(&self, min_detections: usize, min_nights: usize) -> bool {
        self.n_detections as usize >= min_detections && self.n_nights as usize >= min_nights
    }

    pub fn with_kind(mut self, kind: &str) -> Self {
        if self.kind.as_deref() != Some(kind) {
            self.kind = Some(kind.to_string());
            self.unchanged_since = None;
        }
        self
    }

    /// One line naming what the write would do, for a dry run's log.
    pub fn describe(&self) -> String {
        let what = match (&self.id, self.superseded.is_empty()) {
            (Some(id), _) if self.unchanged_since.is_some() => format!("unchanged {id}"),
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
        let dropped = if self.dropped.is_empty() {
            String::new()
        } else {
            format!(", {} detection(s) off its orbit", self.dropped.len())
        };
        format!(
            "{what}: {} detections over {} nights, {:.2} d arc{contested}{dropped}",
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

/// How many detections a stored verdict was fit on. A track written before
/// that was recorded is taken to have been fit on all of them, which is what
/// both the linking and THOR passes did.
fn fit_detections(t: &StoredTrack) -> i32 {
    t.bound_fit_detections.unwrap_or(t.n_detections)
}

/// Resolve a run's track against what is stored, without writing anything.
///
/// `members` and `jds` are parallel and need not be sorted. A merge takes the
/// union of every absorbed track's detections, so no epoch is lost when two
/// partial tracks turn out to be one object.
pub async fn plan_upsert(
    db: &mongodb::Database,
    members: &[Member],
    jds: &[f64],
    designation: Option<String>,
    near_planet: Option<String>,
    bound_fit: Option<(BoundFit, Option<f64>)>,
    orbit: Option<StoredOrbit>,
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
    let carried = |field: fn(&StoredTrack) -> Option<String>| {
        absorbed
            .iter()
            .find_map(|a| existing.iter().find(|t| &&t.id == a).and_then(field))
    };
    let designation = designation.or_else(|| carried(|t| t.designation.clone()));
    let near_planet = near_planet.or_else(|| carried(|t| t.near_planet.clone()));
    let kind = carried(|t| t.kind.clone());
    let mut owned_elsewhere: HashSet<Member> = HashSet::new();
    for t in existing.iter().filter(|t| !absorbed.contains(&&t.id)) {
        owned_elsewhere.extend(t.member_keys());
    }

    // Epochs carried per member so an extension keeps the nights and the span
    // of detections this run did not see.
    let mut by_candid: std::collections::BTreeMap<Member, f64> = members
        .iter()
        .zip(jds.iter())
        .filter(|(m, _)| !owned_elsewhere.contains(*m))
        .map(|(m, j)| (m.clone(), *j))
        .collect();
    for t in existing.iter().filter(|t| absorbed.contains(&&t.id)) {
        for (m, j) in t.member_keys().into_iter().zip(t.epochs.iter()) {
            by_candid.entry(m).or_insert(*j);
        }
    }
    let mut contested: Vec<Member> = members
        .iter()
        .filter(|m| owned_elsewhere.contains(*m))
        .cloned()
        .collect();
    contested.sort_unstable();

    // The verdict fit on more of the track stands, whichever way it went: a
    // THOR cluster over the last few nights of an object linked across two
    // weeks does not overrule the two-week fit, and a longer arc no bound orbit
    // fits replaces a good fit on fewer nights. A fit on as much of it is the
    // newer one and replaces it. Without this the linking and THOR passes
    // would overwrite each other's verdict every night.
    //
    // Counted over this run's own detections, not ones another track keeps.
    let incoming_fit =
        bound_fit.map(|(fit, r)| (fit, r, (members.len() - contested.len()) as i32, orbit));
    let stored_fit = existing
        .iter()
        .filter(|t| absorbed.contains(&&t.id))
        .filter_map(|t| {
            let fit = BoundFit::parse(t.bound_fit.as_deref()?)?;
            Some((fit, t.bound_fit_residual_arcsec, fit_detections(t), t.orbit))
        })
        .max_by_key(|&(fit, _, fit_on, _)| (fit_on, std::cmp::Reverse(fit.rank())));
    let bound_fit = match (incoming_fit, stored_fit) {
        (Some(new), Some(old)) if old.2 > new.2 => Some(old),
        (None, old) => old,
        (new, _) => new,
    };

    let all: Vec<Member> = by_candid.keys().cloned().collect();
    let epochs: Vec<f64> = by_candid.values().copied().collect();
    let (n_detections, n_nights, arc_days, first_jd, last_jd) = summarise(&all, &epochs);
    let bound_fit_residual_arcsec = bound_fit.and_then(|(_, r, _, _)| r);
    let bound_fit_detections = bound_fit.map(|(_, _, n, _)| n);
    let orbit = bound_fit.and_then(|(_, _, _, o)| o);
    let bound_fit = bound_fit.map(|(f, _, _, _)| f.as_str().to_string());
    let unchanged_since = match (&id, superseded.is_empty()) {
        (Some(id), true) => existing
            .iter()
            .find(|t| &t.id == id)
            .filter(|t| {
                t.member_keys() == all
                    && t.epochs == epochs
                    && t.designation == designation
                    && t.near_planet == near_planet
                    && t.bound_fit == bound_fit
                    && (orbit.is_none() || t.orbit.is_some())
                    && bound_fit_detections.is_none_or(|n| n == fit_detections(t))
                    && match (t.bound_fit_residual_arcsec, bound_fit_residual_arcsec) {
                        (Some(a), Some(b)) => (a - b).abs() < SAME_RESIDUAL_ARCSEC,
                        (a, b) => a.is_none() && b.is_none(),
                    }
            })
            .map(|t| t.updated_at),
        _ => None,
    };
    Ok(UpsertPlan {
        id,
        members: all,
        epochs,
        n_detections,
        n_nights,
        arc_days,
        first_jd,
        last_jd,
        bound_fit,
        bound_fit_residual_arcsec,
        bound_fit_detections,
        orbit,
        designation,
        kind,
        near_planet,
        superseded,
        contested,
        dropped: Vec::new(),
        unchanged_since,
    })
}

/// Write a plan, minting an id if it needs one. A plan that would write the
/// stored track back unchanged writes nothing, so its `updated_at` stands.
pub async fn commit_upsert(
    db: &mongodb::Database,
    plan: UpsertPlan,
) -> Result<Upserted, mongodb::error::Error> {
    let id = match plan.id {
        Some(id) => id,
        None => format_id(next_sequence(db).await?),
    };
    let expected_rms_arcsec = expected_rms_arcsec(&plan.members);
    let rms_ratio = plan
        .bound_fit_residual_arcsec
        .zip(expected_rms_arcsec)
        .map(|(rms, expected)| rms / expected);
    let flags = track_flags(&plan.members, &plan.epochs);
    let (member_surveys, members): (Vec<Survey>, Vec<i64>) = plan.members.into_iter().unzip();
    let stored = StoredTrack {
        id: id.clone(),
        members,
        member_surveys,
        epochs: plan.epochs,
        bound_fit: plan.bound_fit,
        bound_fit_residual_arcsec: plan.bound_fit_residual_arcsec,
        bound_fit_detections: plan.bound_fit_detections,
        expected_rms_arcsec,
        rms_ratio,
        flags,
        n_detections: plan.n_detections,
        n_nights: plan.n_nights,
        arc_days: plan.arc_days,
        first_jd: plan.first_jd,
        last_jd: plan.last_jd,
        designation: plan.designation,
        kind: plan.kind,
        near_planet: plan.near_planet,
        orbit: plan.orbit,
        updated_at: plan
            .unchanged_since
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as f64 / 1000.0),
    };
    if plan.unchanged_since.is_some() {
        return Ok(Upserted {
            track: stored,
            superseded: Vec::new(),
        });
    }
    let collection = db.collection::<StoredTrack>(TRACKS_COLLECTION);
    collection
        .replace_one(doc! { "_id": &id }, &stored)
        .upsert(true)
        .await?;
    // Only a stamp this track or one it absorbed left, never another track's.
    let owners: Vec<String> = std::iter::once(id.clone())
        .chain(plan.superseded.iter().cloned())
        .collect();
    for (survey, ids) in alerts_by_survey(&plan.dropped) {
        db.collection::<Document>(&format!("{survey}_alerts"))
            .update_many(
                doc! { "_id": { "$in": ids }, "track.id": { "$in": &owners } },
                doc! { "$unset": { "track": "" } },
            )
            .await?;
    }
    if !plan.superseded.is_empty() {
        collection
            .delete_many(doc! { "_id": { "$in": &plan.superseded } })
            .await?;
        let aliases = db.collection::<Document>(ALIASES_COLLECTION);
        aliases
            .update_many(
                doc! { "superseded_by": { "$in": &plan.superseded } },
                doc! { "$set": { "superseded_by": &id } },
            )
            .await?;
        for old in &plan.superseded {
            aliases
                .replace_one(
                    doc! { "_id": old },
                    doc! { "_id": old, "superseded_by": &id },
                )
                .upsert(true)
                .await?;
        }
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
    stamp(db, stored, false).await
}

/// Stamp the track onto the members that do not carry it yet.
///
/// For a track re-found unchanged, whose members were stamped when it was
/// written: this repairs a stamp that failed then, and otherwise matches and
/// writes nothing.
pub async fn stamp_missing(
    db: &mongodb::Database,
    stored: &StoredTrack,
) -> Result<u64, mongodb::error::Error> {
    stamp(db, stored, true).await
}

async fn stamp(
    db: &mongodb::Database,
    stored: &StoredTrack,
    missing_only: bool,
) -> Result<u64, mongodb::error::Error> {
    let update = alert_update(&AlertTrack::from(stored));
    let mut modified = 0;
    for (survey, ids) in alerts_by_survey(&stored.member_keys()) {
        let mut filter = doc! { "_id": { "$in": ids } };
        if missing_only {
            filter.insert("track.id", doc! { "$ne": &stored.id });
        }
        modified += db
            .collection::<Document>(&format!("{survey}_alerts"))
            .update_many(filter, update.clone())
            .await?
            .modified_count;
    }
    Ok(modified)
}

/// One track by id, which is how a consumer reaches the epochs the alert block
/// deliberately does not carry.
pub async fn track_by_id(
    db: &mongodb::Database,
    id: &str,
) -> Result<Option<StoredTrack>, mongodb::error::Error> {
    let tracks = db.collection::<StoredTrack>(TRACKS_COLLECTION);
    if let Some(track) = tracks.find_one(doc! { "_id": id }).await? {
        return Ok(Some(track));
    }
    let alias = db
        .collection::<Document>(ALIASES_COLLECTION)
        .find_one(doc! { "_id": id })
        .await?;
    match alias.as_ref().and_then(|a| a.get_str("superseded_by").ok()) {
        Some(survivor) => tracks.find_one(doc! { "_id": survivor }).await,
        None => Ok(None),
    }
}

pub async fn public_track_by_id(
    db: &mongodb::Database,
    id: &str,
) -> Result<Option<StoredTrack>, mongodb::error::Error> {
    let Some(track) = track_by_id(db, id).await? else {
        return Ok(None);
    };
    let ztf: Vec<i64> = track
        .member_keys()
        .into_iter()
        .filter(|(survey, _)| *survey == Survey::Ztf)
        .map(|(_, id)| id)
        .collect();
    let mut cursor = db
        .collection::<Document>("ZTF_alerts")
        .find(doc! { "_id": { "$in": ztf }, "candidate.programid": 1 })
        .projection(doc! { "_id": 1 })
        .await?;
    let mut public = HashSet::new();
    while cursor.advance().await? {
        if let Ok(id) = cursor.current().get_i64("_id") {
            public.insert((Survey::Ztf, id));
        }
    }
    Ok(restrict_to(&track, &public))
}

pub fn restrict_to(track: &StoredTrack, keep: &HashSet<Member>) -> Option<StoredTrack> {
    let (kept, epochs): (Vec<Member>, Vec<f64>) = track
        .member_keys()
        .into_iter()
        .zip(track.epochs.iter().copied())
        .filter(|(m, _)| keep.contains(m))
        .unzip();
    if kept.is_empty() {
        return None;
    }
    let (n_detections, n_nights, arc_days, first_jd, last_jd) = summarise(&kept, &epochs);
    let orbit = track.orbit.filter(|_| kept.len() == track.members.len());
    let flags = track_flags(&kept, &epochs);
    let (member_surveys, members): (Vec<Survey>, Vec<i64>) = kept.into_iter().unzip();
    Some(StoredTrack {
        orbit,
        flags,
        members,
        member_surveys,
        epochs,
        n_detections,
        n_nights,
        arc_days,
        first_jd,
        last_jd,
        ..track.clone()
    })
}

pub fn conic(orbit: &StoredOrbit) -> (f64, f64) {
    let r = norm(&orbit.pos);
    let v2 = dot(&orbit.vel, &orbit.vel);
    let rv = dot(&orbit.pos, &orbit.vel);
    let h2 = r * r * v2 - rv * rv;
    let e = (1.0 + (v2 - 2.0 * MU / r) * h2 / (MU * MU)).max(0.0).sqrt();
    (e, h2 / (MU * (1.0 + e)))
}

#[derive(Debug, Default)]
pub struct TrackQuery {
    pub updated_since: f64,
    pub after_id: Option<String>,
    pub unknown_only: bool,
    pub min_e: Option<f64>,
    pub max_q: Option<f64>,
    pub min_nights: Option<i32>,
    pub max_rms_ratio: Option<f64>,
}

impl TrackQuery {
    fn window(&self) -> Document {
        let mut filter = doc! { "updated_at": { "$gte": self.updated_since } };
        if let Some(after_id) = &self.after_id {
            filter.insert(
                "$nor",
                vec![doc! { "updated_at": self.updated_since, "_id": { "$lte": after_id } }],
            );
        }
        filter
    }

    pub fn filter(&self) -> Document {
        let mut filter = self.window();
        if self.unknown_only {
            filter.insert("designation", mongodb::bson::Bson::Null);
        }
        if let Some(min_nights) = self.min_nights {
            filter.insert("n_nights", doc! { "$gte": min_nights });
        }
        if let Some(min_e) = self.min_e {
            filter.insert("orbit.e", doc! { "$gte": min_e });
        }
        if let Some(max_q) = self.max_q {
            filter.insert("orbit.q", doc! { "$lte": max_q });
        }
        if let Some(max_rms_ratio) = self.max_rms_ratio {
            filter.insert("rms_ratio", doc! { "$lte": max_rms_ratio });
        }
        filter
    }
}

#[derive(Debug, Serialize)]
pub struct TrackCursor {
    pub updated_since: f64,
    pub after_id: String,
}

#[derive(Debug, Serialize)]
pub struct TrackPage {
    pub tracks: Vec<StoredTrack>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<TrackCursor>,
}

pub async fn list_tracks(
    db: &mongodb::Database,
    query: &TrackQuery,
    limit: usize,
) -> Result<TrackPage, mongodb::error::Error> {
    let tracks = db.collection::<StoredTrack>(TRACKS_COLLECTION);
    // Read before the page, so a short page moves the cursor past what the cuts rejected.
    let newest = tracks
        .clone_with_type::<Document>()
        .find_one(query.window())
        .sort(doc! { "updated_at": -1, "_id": -1 })
        .projection(doc! { "updated_at": 1 })
        .await?
        .and_then(|d| {
            Some(TrackCursor {
                updated_since: d.get_f64("updated_at").ok()?,
                after_id: d.get_str("_id").ok()?.to_string(),
            })
        });
    let mut cursor = tracks
        .find(query.filter())
        .sort(doc! { "updated_at": 1, "_id": 1 })
        .limit(limit as i64)
        .await?;
    let mut page = TrackPage {
        tracks: Vec::new(),
        next: newest,
    };
    let mut scanned = 0;
    while cursor.advance().await? {
        scanned += 1;
        if let Ok(track) = cursor.deserialize_current() {
            page.tracks.push(track.explicit());
        }
    }
    if scanned == limit {
        page.next = page.tracks.last().map(|track| TrackCursor {
            updated_since: track.updated_at,
            after_id: track.id.clone(),
        });
    }
    Ok(page)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn ztf(ids: &[i64]) -> Vec<Member> {
        ids.iter().map(|&id| (Survey::Ztf, id)).collect()
    }

    fn stored(id: &str, members: &[i64]) -> StoredTrack {
        StoredTrack {
            id: id.to_string(),
            members: members.to_vec(),
            member_surveys: Vec::new(),
            // One per night, so a helper track's nights match its detections.
            epochs: (0..members.len()).map(|k| 2460000.0 + k as f64).collect(),
            bound_fit: None,
            bound_fit_residual_arcsec: None,
            bound_fit_detections: None,
            expected_rms_arcsec: None,
            rms_ratio: None,
            flags: Vec::new(),
            n_detections: members.len() as i32,
            n_nights: 2,
            arc_days: 1.0,
            first_jd: 2460000.0,
            last_jd: 2460001.0,
            designation: None,
            kind: None,
            near_planet: None,
            updated_at: 0.0,
            orbit: None,
        }
    }

    #[test]
    fn test_single_detections_on_three_nights_are_to_confirm() {
        let nights = |per: &[usize]| -> (Vec<Member>, Vec<f64>) {
            let jds: Vec<f64> = per
                .iter()
                .enumerate()
                .flat_map(|(n, &k)| (0..k).map(move |i| 2460000.0 + n as f64 + i as f64 * 0.01))
                .collect();
            (ztf(&(1..=jds.len() as i64).collect::<Vec<_>>()), jds)
        };
        let (m, j) = nights(&[1, 1, 1]);
        assert_eq!(track_flags(&m, &j), vec![TO_CONFIRM.to_string()]);
        let (m, j) = nights(&[2, 1, 1]);
        assert!(!constrained(&m, &j));
        let (m, j) = nights(&[2, 2]);
        assert!(constrained(&m, &j));
        let (m, j) = nights(&[1, 1, 1, 1]);
        assert!(track_flags(&m, &j).is_empty());
    }

    /// The property the whole module exists for: a track that gains detections
    /// keeps the id a consumer already recorded.
    #[test]
    fn test_a_growing_track_keeps_its_id() {
        let db = vec![stored("BT000001", &[10, 11, 12])];
        // The next night adds two detections and re-links the same object.
        let grown = ztf(&[10, 11, 12, 13, 14]);
        assert_eq!(identify(&grown, &db), Identity::Extends("BT000001".into()));
    }

    /// Losing detections must not rename it either: a tighter cut can drop an
    /// epoch the previous run kept.
    #[test]
    fn test_a_shrinking_track_keeps_its_id() {
        let db = vec![stored("BT000001", &[10, 11, 12, 13])];
        assert_eq!(
            identify(&ztf(&[11, 12]), &db),
            Identity::Extends("BT000001".into())
        );
    }

    /// A single shared detection is a coincidence, not an identity -- otherwise
    /// an unrelated track that absorbs one epoch inherits the stored id.
    #[test]
    fn test_one_shared_detection_is_not_the_same_track() {
        let db = vec![stored("BT000001", &[10, 11, 12])];
        assert_eq!(identify(&ztf(&[12, 90, 91]), &db), Identity::New);
    }

    #[test]
    fn test_an_unrelated_track_is_new() {
        let db = vec![stored("BT000001", &[10, 11, 12])];
        assert_eq!(identify(&ztf(&[90, 91, 92]), &db), Identity::New);
    }

    /// One object's detections can be stored as two tracks before enough nights
    /// exist to join them; the run that joins them reports both.
    #[test]
    fn test_bridging_two_stored_tracks_merges_them() {
        let db = vec![stored("BT000002", &[10, 11]), stored("BT000005", &[20, 21])];
        let joined = ztf(&[10, 11, 20, 21]);
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
        let (n_det, nights, arc, ..) =
            summarise(&ztf(&[1, 2, 3]), &[2460000.9, 2460001.05, 2460002.9]);
        assert_eq!(n_det, 3);
        assert_eq!(nights, 2, "the first two epochs are one night");
        assert!((arc - 2.0).abs() < 1e-9, "arc {arc}");
    }

    /// The id is quoted as an MPC trkSub, which is capped at 8 characters.
    #[test]
    fn test_minted_ids_fit_a_trksub() {
        assert_eq!(format_id(1), "BT000001");
        assert_eq!(format_id(999_999), "BT999999");
        assert_eq!(format_id(1_000_000), "BTA00000");
        assert_eq!(format_id(6_200_000), "BT~00000");
        let ids: Vec<String> = [1, 999_999, 1_000_000, 6_199_999, 6_200_000, 900_000_000]
            .into_iter()
            .map(format_id)
            .collect();
        assert!(ids.iter().all(|id| id.len() == 8), "{ids:?}");
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    }

    /// A track whittled down by contested detections must not be stored as a
    /// remnant that the run's own thresholds would never have produced.
    #[test]
    fn test_a_plan_below_the_run_thresholds_is_not_viable() {
        let plan = UpsertPlan {
            id: None,
            members: ztf(&[10]),
            epochs: vec![2460000.0],
            n_detections: 1,
            n_nights: 1,
            arc_days: 0.0,
            first_jd: 2460000.0,
            last_jd: 2460000.0,
            bound_fit: None,
            bound_fit_residual_arcsec: None,
            bound_fit_detections: None,
            orbit: None,
            designation: None,
            kind: None,
            near_planet: None,
            superseded: Vec::new(),
            contested: ztf(&[11, 12]),
            dropped: Vec::new(),
            unchanged_since: None,
        };
        assert!(
            !plan.meets(2, 2),
            "one detection on one night is not a track"
        );
        assert!(!plan.meets(2, 1), "still short on detections");
        assert!(
            plan.meets(1, 1),
            "meets the thresholds it was measured against"
        );
        assert!(
            plan.describe().contains("left with another track"),
            "a dry run should say why it shrank: {}",
            plan.describe()
        );
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
        let (n_det, nights, ..) = summarise(&ztf(&all), &epochs);
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

    #[test]
    fn test_restricting_a_track_recounts_what_is_left() {
        let t = stored("BT000001", &[10, 11, 12, 13]);
        let kept = restrict_to(&t, &ztf(&[11, 13]).into_iter().collect()).expect("two remain");
        assert_eq!(kept.members, vec![11, 13]);
        assert_eq!(kept.epochs, vec![2460001.0, 2460003.0]);
        assert_eq!(kept.n_detections, 2);
        assert_eq!(kept.n_nights, 2);
        assert_eq!(kept.first_jd, 2460001.0);
        assert_eq!(kept.last_jd, 2460003.0);
        assert!(restrict_to(&t, &ztf(&[99]).into_iter().collect()).is_none());
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

    fn orbit_at(e: f64, q: f64, nu_deg: f64) -> StoredOrbit {
        let p = q * (1.0 + e);
        let nu = nu_deg.to_radians();
        let r = p / (1.0 + e * nu.cos());
        let speed = (MU / p).sqrt();
        StoredOrbit {
            epoch_jd: 2460000.0,
            pos: [r * nu.cos(), r * nu.sin(), 0.0],
            vel: [-speed * nu.sin(), speed * (e + nu.cos()), 0.0],
        }
    }

    #[test]
    fn test_an_orbit_is_written_with_its_eccentricity_and_perihelion() {
        for (e, q, nu) in [
            (0.0, 2.5, 0.0),
            (0.1, 2.2, 130.0),
            (0.6, 0.9, -40.0),
            (1.0, 1.0, 50.0),
            (3.36, 2.0, 70.0),
        ] {
            let orbit = orbit_at(e, q, nu);
            let written = mongodb::bson::to_document(&orbit).unwrap();
            let (got_e, got_q) = (written.get_f64("e").unwrap(), written.get_f64("q").unwrap());
            assert!((got_e - e).abs() < 1e-6, "e {e}: {got_e}");
            assert!((got_q - q).abs() < 1e-9, "q {q}: {got_q}");
            assert_eq!(
                mongodb::bson::from_document::<StoredOrbit>(written).unwrap(),
                orbit
            );
        }
    }

    #[test]
    fn test_a_track_query_filters_on_updated_at_first() {
        let query = TrackQuery {
            updated_since: 1.7e9,
            after_id: Some("BT000009".to_string()),
            unknown_only: true,
            min_e: Some(1.0),
            max_q: Some(1.3),
            min_nights: Some(3),
            max_rms_ratio: Some(4.0),
        };
        let filter = query.filter();
        assert_eq!(filter.keys().next().map(String::as_str), Some("updated_at"));
        assert_eq!(
            filter.get_document("updated_at").unwrap(),
            &doc! { "$gte": 1.7e9 }
        );
        assert_eq!(
            filter.get_array("$nor").unwrap()[0],
            doc! { "updated_at": 1.7e9, "_id": { "$lte": "BT000009" } }.into()
        );
        assert_eq!(filter.get("designation"), Some(&mongodb::bson::Bson::Null));
        assert_eq!(
            filter.get_document("n_nights").unwrap(),
            &doc! { "$gte": 3 }
        );
        assert_eq!(
            filter.get_document("orbit.e").unwrap(),
            &doc! { "$gte": 1.0 }
        );
        assert_eq!(
            filter.get_document("orbit.q").unwrap(),
            &doc! { "$lte": 1.3 }
        );
        assert_eq!(
            filter.get_document("rms_ratio").unwrap(),
            &doc! { "$lte": 4.0 }
        );
        assert_eq!(
            TrackQuery::default().filter(),
            doc! { "updated_at": { "$gte": 0.0 } }
        );
    }

    /// Candids no other test uses, so tests can share the database.
    fn fresh_members(n: i64) -> Vec<i64> {
        let base = ZTF_IDS + (uuid::Uuid::new_v4().as_u128() as i64).abs() / 64;
        (base..base + n).collect()
    }

    async fn store(
        db: &mongodb::Database,
        members: &[i64],
        fit: (BoundFit, Option<f64>),
    ) -> Upserted {
        // Each detection's epoch follows from its id, as in a real run where a
        // detection has one epoch however many tracks find it.
        let jds: Vec<f64> = members
            .iter()
            .map(|m| 2460000.0 + m.rem_euclid(1000) as f64)
            .collect();
        let plan = plan_upsert(db, &ztf(members), &jds, None, None, Some(fit), None)
            .await
            .unwrap();
        commit_upsert(db, plan).await.unwrap()
    }

    /// A database of its own, for tests that take the tracks lock: it is
    /// shared by everything writing to one database, so tests running in
    /// parallel would otherwise find each other holding it.
    ///
    /// Named after the test rather than at random, and emptied before use, so
    /// a run that fails before dropping it leaves one database to reuse, not a
    /// new one every time.
    pub(crate) async fn private_db(test: &str) -> mongodb::Database {
        let db = crate::conf::get_test_db()
            .await
            .client()
            .database(&format!("boom_test_lock_{test}"));
        db.drop().await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_only_the_holder_releases_or_renews_the_lock() {
        let db = private_db("owner").await;
        let held = acquire_lock(&db).await.unwrap().expect("the lock is free");
        assert!(acquire_lock(&db).await.unwrap().is_none(), "already held");
        assert!(lock_is_held(&db).await.unwrap());

        let stale = TracksLock {
            owner: "a run whose lease lapsed".to_string(),
        };
        release_lock(&db, &stale).await.unwrap();
        assert!(
            lock_is_held(&db).await.unwrap(),
            "a former holder must not release the current holder's lock"
        );
        assert!(!renew_lock(&db, &stale).await.unwrap());
        assert!(renew_lock(&db, &held).await.unwrap());

        release_lock(&db, &held).await.unwrap();
        assert!(!lock_is_held(&db).await.unwrap());
        assert!(acquire_lock(&db).await.unwrap().is_some());
        db.drop().await.unwrap();
    }

    /// A longer arc no bound orbit fits is the anomaly the verdict exists to
    /// keep, so it replaces a better verdict fit on less of the track.
    #[tokio::test]
    async fn test_a_verdict_from_more_of_the_track_stands() {
        let db = crate::conf::get_test_db().await;
        let members = fresh_members(5);
        let short = store(&db, &members[..3], (BoundFit::Good, Some(1.2))).await;
        assert_eq!(short.track.bound_fit_detections, Some(3));
        let longer = store(&db, &members, (BoundFit::None, None)).await;
        assert_eq!(longer.track.id, short.track.id);
        assert_eq!(longer.track.bound_fit.as_deref(), Some("none"));
        assert_eq!(longer.track.bound_fit_residual_arcsec, None);
        assert_eq!(longer.track.bound_fit_detections, Some(5));
    }

    /// A THOR pair over the last nights of a linked object must not wipe the
    /// orbit the linking pass fit across the whole arc.
    #[tokio::test]
    async fn test_a_worse_verdict_does_not_overwrite_a_stored_one() {
        let db = crate::conf::get_test_db().await;
        let members = fresh_members(6);
        let linked = store(&db, &members, (BoundFit::Good, Some(0.8))).await;
        let pair = store(&db, &members[4..], (BoundFit::Ungated, None)).await;
        assert_eq!(pair.track.id, linked.track.id, "the pair extends the track");
        assert_eq!(pair.track.bound_fit.as_deref(), Some("good"));
        assert_eq!(pair.track.bound_fit_residual_arcsec, Some(0.8));

        let refit = store(&db, &members, (BoundFit::Good, Some(0.5))).await;
        assert_eq!(
            refit.track.bound_fit_residual_arcsec,
            Some(0.5),
            "an equally good verdict is the newer fit and replaces it"
        );
    }

    /// Re-finding a track exactly as stored must not make it look new to a
    /// client polling `updated_at`.
    #[tokio::test]
    async fn test_an_unchanged_track_keeps_its_updated_at() {
        let db = crate::conf::get_test_db().await;
        let members = fresh_members(4);
        let first = store(&db, &members, (BoundFit::Good, Some(0.8))).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let again = store(&db, &members, (BoundFit::Good, Some(0.8001))).await;
        assert_eq!(again.track.updated_at, first.track.updated_at);
        let stored = track_by_id(&db, &first.track.id).await.unwrap().unwrap();
        assert_eq!(stored.updated_at, first.track.updated_at);

        let mut longer = members.clone();
        longer.extend(fresh_members(1));
        let extended = store(&db, &longer, (BoundFit::Good, Some(0.8))).await;
        assert_eq!(extended.track.id, first.track.id);
        assert!(
            extended.track.updated_at > first.track.updated_at,
            "an extension is a change"
        );
    }

    /// The linking pass fits a whole two weeks and THOR the last few nights,
    /// both every night. Neither may overwrite the other's verdict in turn, or
    /// every track both find would look updated every night.
    #[tokio::test]
    async fn test_the_linking_and_thor_verdicts_do_not_take_turns() {
        let db = crate::conf::get_test_db().await;
        let members = fresh_members(14);
        let linked = store(&db, &members, (BoundFit::Good, Some(0.6))).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let thor = store(&db, &members[9..], (BoundFit::Good, Some(1.1))).await;
        assert_eq!(thor.track.id, linked.track.id);
        assert_eq!(thor.track.bound_fit_residual_arcsec, Some(0.6));
        assert_eq!(thor.track.bound_fit_detections, Some(14));
        assert_eq!(thor.track.updated_at, linked.track.updated_at);

        let relinked = store(&db, &members, (BoundFit::Good, Some(0.6))).await;
        assert_eq!(relinked.track.updated_at, linked.track.updated_at);
    }

    /// A track stored before verdicts recorded their detection count was fit
    /// on all of them; re-finding it unchanged must not rewrite it.
    #[tokio::test]
    async fn test_a_track_from_before_fit_counts_is_not_rewritten() {
        let db = crate::conf::get_test_db().await;
        let members = fresh_members(4);
        let first = store(&db, &members, (BoundFit::Good, Some(0.8))).await;
        db.collection::<Document>(TRACKS_COLLECTION)
            .update_one(
                doc! { "_id": &first.track.id },
                doc! { "$unset": { "bound_fit_detections": "" } },
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let again = store(&db, &members, (BoundFit::Good, Some(0.8))).await;
        assert_eq!(again.track.updated_at, first.track.updated_at);
        let stored = track_by_id(&db, &first.track.id).await.unwrap().unwrap();
        assert_eq!(stored.updated_at, first.track.updated_at);
    }

    /// Detections another track keeps are dropped from this one, so they are
    /// no part of what its verdict rests on.
    #[tokio::test]
    async fn test_contested_detections_do_not_count_toward_a_verdict() {
        let db = crate::conf::get_test_db().await;
        let other = fresh_members(3);
        store(&db, &other, (BoundFit::Good, Some(0.5))).await;
        let own = fresh_members(4);
        let mut found = own.clone();
        found.push(other[0]);
        let track = store(&db, &found, (BoundFit::Good, Some(0.9))).await;
        assert_eq!(track.track.members, own);
        assert_eq!(track.track.bound_fit_detections, Some(4));
    }

    /// A lease that ran out, or a lock document from before leases were
    /// recorded, is taken over; a live one is not.
    #[tokio::test]
    async fn test_a_lapsed_lock_is_taken_over() {
        let db = private_db("lapsed").await;
        let counters = db.collection::<Document>(COUNTERS_COLLECTION);
        let past = mongodb::bson::DateTime::from_millis(0);
        counters
            .insert_one(doc! { "_id": LOCK_ID, "owner": "gone", "expires_at": past })
            .await
            .unwrap();
        assert!(!lock_is_held(&db).await.unwrap());
        let taken = acquire_lock(&db)
            .await
            .unwrap()
            .expect("a lapsed lease is free");
        assert!(acquire_lock(&db).await.unwrap().is_none());
        release_lock(&db, &taken).await.unwrap();

        counters.insert_one(doc! { "_id": LOCK_ID }).await.unwrap();
        assert!(
            acquire_lock(&db).await.unwrap().is_some(),
            "no lease recorded"
        );
        db.drop().await.unwrap();
    }

    /// Repairing an unchanged track's stamps touches only the alerts missing it.
    #[tokio::test]
    async fn test_stamp_missing_writes_only_unstamped_members() {
        let db = crate::conf::get_test_db().await;
        let alerts = db.collection::<Document>("ZTF_alerts");
        let members = fresh_members(4);
        for &id in &members {
            alerts.insert_one(doc! { "_id": id }).await.unwrap();
        }
        let stored = stored(&format!("BT{}", members[0]), &members);
        alerts
            .update_many(
                doc! { "_id": { "$in": &members[..3] } },
                alert_update(&AlertTrack::from(&stored)),
            )
            .await
            .unwrap();
        assert_eq!(stamp_missing(&db, &stored).await.unwrap(), 1);
        assert_eq!(stamp_missing(&db, &stored).await.unwrap(), 0);
        alerts
            .delete_many(doc! { "_id": { "$in": &members } })
            .await
            .unwrap();
    }

    #[test]
    fn test_a_track_written_before_member_surveys_decodes_its_ids() {
        let old = stored(
            "BT000001",
            &[51_000_000_017_600_000, 52_000_000_000_461_147, 10],
        );
        assert_eq!(
            old.explicit().member_keys(),
            vec![
                (Survey::Winter, 17_600_000),
                (Survey::Decam, 461_147),
                (Survey::Ztf, 10)
            ]
        );
    }

    /// DECam ids are hashed over the whole positive i64, so no id range names the survey.
    #[tokio::test]
    async fn test_a_hashed_decam_member_is_stamped_in_its_own_collection() {
        let db = crate::conf::get_test_db().await;
        let decam_id =
            8_979_027_391_976_047_686 - (uuid::Uuid::new_v4().as_u128() % 1_000_000) as i64;
        let ztf_ids = fresh_members(2);
        let members = vec![
            (Survey::Ztf, ztf_ids[0]),
            (Survey::Ztf, ztf_ids[1]),
            (Survey::Decam, decam_id),
        ];
        let decam = db.collection::<Document>("DECAM_alerts");
        decam.insert_one(doc! { "_id": decam_id }).await.unwrap();
        let jds = [2460000.8, 2460001.8, 2460002.8];
        let plan = plan_upsert(&db, &members, &jds, None, None, None, None)
            .await
            .unwrap();
        let id = commit_upsert(&db, plan).await.unwrap().track.id;
        let stored = track_by_id(&db, &id).await.unwrap().unwrap();
        assert_eq!(stored.member_keys(), members);
        assert_eq!(stamp_members(&db, &stored).await.unwrap(), 1);
        let stamped = decam
            .find_one(doc! { "_id": decam_id, "track.id": &id })
            .await
            .unwrap();
        assert!(stamped.is_some());
        decam.delete_one(doc! { "_id": decam_id }).await.unwrap();
        db.collection::<Document>(TRACKS_COLLECTION)
            .delete_one(doc! { "_id": &id })
            .await
            .unwrap();
    }

    /// However many runs ask at once, exactly one gets the lock.
    #[tokio::test]
    async fn test_one_of_many_concurrent_runs_takes_the_lock() {
        let db = private_db("race").await;
        let tries = (0..8).map(|_| {
            let db = db.clone();
            tokio::spawn(async move { acquire_lock(&db).await.unwrap() })
        });
        let mut held = 0;
        for t in tries {
            held += t.await.unwrap().is_some() as usize;
        }
        assert_eq!(held, 1);
        db.drop().await.unwrap();
    }
}
