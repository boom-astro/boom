//! The `link_tracks` task: find intra-night tracklets and link them into
//! moving-object tracks.
//!
//! Two things in one, because they share every threshold. A production run
//! persists what it finds: each track lands in `<survey>_tracks` under a
//! durable id short enough to quote as an MPC `trkSub`, and each member alert
//! is stamped with a `track` block. A tuning run sets `dry_run` and
//! writes nothing, reporting what it would have done in the run's result so
//! the numbers can be read off the admin page rather than a terminal.
//!
//! Tuning is done here rather than on a terminal so the thresholds that
//! produced a stored track are the run's recorded parameters.
//!
//! `input` names a **staged** dump under the shared data path rather than an
//! arbitrary file, because the worker cannot see the operator's disk -- the
//! same arrangement staged catalogs use. `out_tracks` names a file written
//! into the export area, which the admin page lists and streams.
//!
//! The two file flags move accordingly. `input` names a **staged** dump under
//! the shared data path rather than an arbitrary file, because the worker
//! cannot see the operator's disk -- the same arrangement staged catalogs use.
//! `out_tracks` names a file written into the export area, which the admin page
//! already lists and streams.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::heliolinc::{default_hypotheses, link_tracklets, LinkConfig, Track};
use crate::utils::linking::{
    circular_mean_deg, find_tracklets, night_of, Detection, Tracklet, TrackletConfig,
};
use crate::utils::orbit_fit::{fit_within, Observation};
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{error, info};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "link_tracks";

/// What a track has to clear to be stored, and whether to store it at all.
#[derive(Debug, Clone, Copy)]
struct PersistGates {
    dry_run: bool,
    min_detections: usize,
    min_nights: usize,
}

/// Where a staged input dump and the written tracks live, under the shared
/// data path the worker mounts.
const DATA_PATH_ENV: &str = "BOOM_CATALOG_DATA_PATH";

/// What a client may ask for.
///
/// Field names match the `find_tracklets` flags they came from, so a recipe
/// someone had in their shell history transfers directly.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LinkTracksParams {
    /// Start of the night, JD. Defaults to the most recent night with data.
    #[serde(default)]
    pub jd_start: Option<f64>,
    /// Length of the window, days.
    #[serde(default = "d_span")]
    pub span: f64,
    /// Link the known solar system detections and score against `ssnamenr`.
    /// This is how the thresholds get calibrated.
    #[serde(default)]
    pub known: bool,
    /// Minimum drb for a detection to be considered.
    #[serde(default = "d_drb")]
    pub drb: f64,
    /// Restrict the search to a cone, degrees. All three are needed together;
    /// the region is tested by HEALPix range.
    #[serde(default)]
    pub ra: Option<f64>,
    #[serde(default)]
    pub dec: Option<f64>,
    #[serde(default)]
    pub radius: Option<f64>,
    /// Detections per tracklet. Two is the useful floor: ZTF's nominal cadence
    /// is two visits to a field per night.
    #[serde(default = "d_min_detections")]
    pub min_detections: usize,
    /// Reject a pair whose magnitudes disagree by more than this many combined
    /// sigma. 0 disables the test.
    #[serde(default = "d_max_mag_sigma")]
    pub max_mag_sigma: f64,
    /// Fastest apparent motion a tracklet may have, degrees per day.
    #[serde(default = "d_max_rate")]
    pub max_rate: f64,
    /// Shortest on-sky arc a pair may span, arcseconds.
    #[serde(default = "d_min_arc")]
    pub min_arc: f64,
    /// Shortest time between two detections of a pair, days.
    #[serde(default = "d_min_pair_dt")]
    pub min_pair_dt: f64,
    /// Longest a tracklet may span, days. Separate from `span`, which is how
    /// much data to read: widening this widens the pair search radius.
    #[serde(default = "d_max_tracklet_span")]
    pub max_tracklet_span: f64,
    /// Distinct nights a track must appear on.
    #[serde(default = "d_min_nights")]
    pub min_nights: usize,
    /// Largest sky residual a fitted orbit may leave, arcseconds.
    #[serde(default = "d_max_residual")]
    pub max_residual: f64,
    /// Write the tracks as JSON lines to this name in the export area, where
    /// the admin page lists and streams them. A name, not a path.
    #[serde(default)]
    pub out_tracks: Option<String>,
    /// Store the tracks and stamp each onto its member alerts, so a filter can
    /// match on them. Off by default: a tuning run should not write.
    #[serde(default)]
    pub persist: bool,
    /// Report what `persist` would write without writing it. Each track is
    /// resolved against the stored ones only, so two tracks of one object in
    /// the same run show as two new tracks where a real run merges them.
    #[serde(default)]
    pub dry_run: bool,
    /// Recover objects tracklet-lessly, in the manner of THOR, instead of
    /// linking tracklets. Reaches objects detected only once a night.
    #[serde(default)]
    pub thor: bool,
    /// Heliocentric distances to place trial orbits at, au.
    #[serde(default = "d_thor_distances")]
    pub thor_distances: Vec<f64>,
    /// Distinct nights a THOR cluster must appear on. Two drops purity to 80%.
    #[serde(default = "d_thor_min_nights")]
    pub thor_min_nights: usize,
    /// THOR cluster cell size, arcseconds. Library default when unset.
    #[serde(default)]
    pub cluster_radius: Option<f64>,
    /// Largest scatter a THOR cluster may have about its refitted drift,
    /// arcseconds. Library default when unset.
    #[serde(default)]
    pub max_cluster_rms: Option<f64>,
    /// Largest residual rate THOR searches, degrees/day. Library default when
    /// unset.
    #[serde(default)]
    pub max_residual_rate: Option<f64>,
    /// Rate grid steps per axis. Widening the range without raising this
    /// coarsens the grid. Library default when unset.
    #[serde(default)]
    pub rate_steps: Option<usize>,
    /// Keep a THOR cluster whose best bound orbit leaves up to this residual,
    /// arcseconds, reported as a poor fit.
    #[serde(default = "d_max_unbound_residual")]
    pub max_unbound_residual: f64,
    /// Identify the detections against stored tracks instead of linking.
    #[serde(default)]
    pub identify: bool,
    /// Identification radius, arcseconds.
    #[serde(default = "d_identify_radius")]
    pub identify_radius: f64,
    /// Match each track to the MPC catalogue and record the designation of the
    /// object it is, if any. A track with a designation is a recovery of a
    /// known object rather than a discovery candidate. Reads MPC_orbits, so a
    /// run needs the database even when the detections came from a dump.
    #[serde(default)]
    pub match_known: bool,
    /// Share of a track's detections that must match one catalogued object for
    /// the track to be that object.
    #[serde(default = "d_known_fraction")]
    pub known_fraction: f64,
    /// Distinct nights those matching detections must span.
    #[serde(default = "d_known_min_nights")]
    pub known_min_nights: usize,
    /// How far any matching detection's separation may stray from their
    /// median, arcseconds. A catalogued object sits at a steady offset over a
    /// few nights; an unrelated neighbor drifts.
    #[serde(default = "d_known_max_scatter")]
    pub known_max_scatter: f64,
    /// How much further the separation may stray per day from the middle of
    /// the arc, arcseconds: a catalogued orbit's error drifts slowly over a
    /// long track.
    #[serde(default = "d_known_max_drift")]
    pub known_max_drift: f64,
    /// How many tracklets or tracks to log individually.
    #[serde(default = "d_show")]
    pub show: usize,
    /// Read detections from a staged JSONL dump instead of the database. A
    /// name under `<data path>/link_tracks/`, not an arbitrary path: the worker
    /// cannot read the operator's disk.
    #[serde(default)]
    pub input: Option<String>,
    /// Find tracklets per night, then link them across nights.
    #[serde(default)]
    pub link: bool,
    /// Position tolerance when clustering hypotheses, au.
    #[serde(default = "d_position_tol")]
    pub position_tol: f64,
    /// Velocity tolerance when clustering hypotheses, au/day.
    #[serde(default = "d_velocity_tol")]
    pub velocity_tol: f64,
}

fn d_span() -> f64 {
    0.5
}
fn d_drb() -> f64 {
    0.8
}
fn d_min_detections() -> usize {
    2
}
fn d_max_mag_sigma() -> f64 {
    5.0
}
fn d_max_rate() -> f64 {
    1.0
}
fn d_min_arc() -> f64 {
    10.0
}
fn d_min_pair_dt() -> f64 {
    0.1 / 24.0
}
fn d_max_tracklet_span() -> f64 {
    3.0 / 24.0
}
fn d_min_nights() -> usize {
    2
}
fn d_max_residual() -> f64 {
    2.0
}
fn d_thor_distances() -> Vec<f64> {
    vec![1.8, 2.2, 2.6, 3.0, 3.4]
}
fn d_thor_min_nights() -> usize {
    3
}
fn d_max_unbound_residual() -> f64 {
    10.0
}
fn d_known_fraction() -> f64 {
    2.0 / 3.0
}

fn d_known_min_nights() -> usize {
    2
}

fn d_known_max_scatter() -> f64 {
    10.0
}

fn d_known_max_drift() -> f64 {
    2.0
}

fn d_identify_radius() -> f64 {
    120.0
}
fn d_show() -> usize {
    20
}
fn d_position_tol() -> f64 {
    0.002
}
fn d_velocity_tol() -> f64 {
    0.0004
}

/// Guards against a submission that would read the whole archive into memory
/// or write a file nobody asked for.
const MAX_SPAN_DAYS: f64 = 30.0;

impl LinkTracksParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if !(self.span > 0.0 && self.span <= MAX_SPAN_DAYS) {
            return Err(format!("span must be between 0 and {MAX_SPAN_DAYS} days"));
        }
        if self.min_detections < 2 {
            return Err("min_detections must be at least 2: a tracklet is a pair".to_string());
        }
        // All three or none: a cone with one side missing silently searches the
        // whole sky, which is the slow answer rather than the wrong one, but it
        // is not what was asked for.
        let cone = [self.ra, self.dec, self.radius];
        if cone.iter().any(Option::is_some) && !cone.iter().all(Option::is_some) {
            return Err("ra, dec and radius are needed together".to_string());
        }
        if self.persist && self.dry_run {
            return Err("persist and dry_run are opposites; pick one".to_string());
        }
        // The binary never needed these checked, because clap only ever handed
        // it the declared defaults. A task takes whatever JSON a client sends,
        // and both ends of this range fail quietly: above 1 no track can ever
        // match, at or below 0 a single chance match names the track.
        if self.match_known {
            if !(self.known_fraction > 0.0 && self.known_fraction <= 1.0) {
                return Err("known_fraction must be above 0 and at most 1".to_string());
            }
            if self.known_min_nights < 1 {
                return Err("known_min_nights must be at least 1".to_string());
            }
            if self.known_max_scatter < 0.0 || self.known_max_drift < 0.0 {
                return Err("known_max_scatter and known_max_drift cannot be negative".to_string());
            }
        }
        for name in [&self.input, &self.out_tracks].into_iter().flatten() {
            if name.contains('/') || name.contains("..") {
                return Err(format!(
                    "{name:?} must be a file name, not a path: the worker reads and writes \
                     only under its own data directory"
                ));
            }
        }
        Ok(())
    }
}

/// Tracklets found independently in each night the detections span.
fn tracklets_per_night(detections: &[Detection], cfg: &TrackletConfig) -> Vec<Tracklet> {
    let mut by_night: HashMap<i64, Vec<Detection>> = HashMap::new();
    for d in detections {
        by_night.entry(night_of(d.jd)).or_default().push(*d);
    }
    let mut nights: Vec<_> = by_night.into_iter().collect();
    nights.sort_by_key(|(n, _)| *n);
    // Nights share nothing, and collecting in order keeps the result independent
    // of which finishes first.
    nights
        .par_iter()
        .flat_map(|(night, dets)| {
            let found = find_tracklets(dets, cfg);
            info!(
                "night {}: {} detections -> {} tracklets",
                night,
                dets.len(),
                found.len()
            );
            found
        })
        .collect()
}

/// How well tracks reproduce the labels: pure, mixed, and objects recovered.
fn score_tracks(
    tracks: &[crate::utils::heliolinc::Track],
    tracklets: &[Tracklet],
    labels: &HashMap<i64, String>,
) -> (usize, usize, usize) {
    let name_of = |t: &Tracklet| -> Option<&str> {
        t.ids
            .iter()
            .find_map(|id| labels.get(id).map(|s| s.as_str()))
    };
    let mut pure = 0;
    let mut mixed = 0;
    let mut recovered: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for track in tracks {
        let names: std::collections::HashSet<&str> = track
            .members
            .iter()
            .filter_map(|&m| name_of(&tracklets[m]))
            .collect();
        if names.len() == 1 {
            pure += 1;
            recovered.extend(names);
        } else if names.len() > 1 {
            mixed += 1;
        }
    }
    (pure, mixed, recovered.len())
}

/// One line of a dump: the fields `load` would have projected.
#[derive(serde::Deserialize)]
struct DumpRow {
    /// Decimal string: a candid exceeds what a JSON number holds exactly.
    id: String,
    jd: f64,
    ra: f64,
    dec: f64,
    #[serde(default)]
    ssnamenr: Option<String>,
    #[serde(default)]
    magpsf: Option<f64>,
    #[serde(default)]
    sigmapsf: Option<f64>,
    #[serde(default)]
    fid: Option<i32>,
}

/// ZTF filter id as the single letter ADES wants.
fn ztf_band(fid: Option<i32>) -> Option<char> {
    match fid {
        Some(1) => Some('g'),
        Some(2) => Some('r'),
        Some(3) => Some('i'),
        _ => None,
    }
}

/// Detections, and the `ssnamenr` label for those that carry one.
///
/// The labels are what a `known` run scores against, so they travel with the
/// detections rather than being looked up again later.
type Labelled = (Vec<Detection>, HashMap<i64, String>);

/// Detections from a JSONL dump, with labels where the rows carry them.
fn load_file(path: &str) -> Result<Labelled, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    let mut detections = Vec::new();
    let mut labels = HashMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let row: DumpRow = serde_json::from_str(line)?;
        let id: i64 = row.id.parse()?;
        if let Some(name) = row.ssnamenr {
            labels.insert(id, name);
        }
        detections.push(Detection {
            id,
            jd: row.jd,
            ra: row.ra,
            dec: row.dec,
            mag: row.magpsf,
            mag_err: row.sigmapsf,
            band: ztf_band(row.fid),
        });
    }
    Ok((detections, labels))
}

/// Detections for the window, with the `ssnamenr` label when there is one.
async fn load(
    db: &mongodb::Database,
    jd_start: f64,
    span: f64,
    drb: f64,
    known: bool,
    region: Option<(f64, f64, f64)>,
) -> Result<(Vec<Detection>, HashMap<i64, String>), Box<dyn std::error::Error>> {
    // Absent on an unassociated detection, so $exists rather than a null type.
    let association = if known {
        doc! { "$type": "string" }
    } else {
        doc! { "$exists": false }
    };
    let mut filter = doc! {
        "candidate.jd": { "$gte": jd_start, "$lt": jd_start + span },
        "candidate.ssnamenr": association,
        "candidate.drb": { "$gt": drb },
        "candidate.isdiffpos": true,
    };
    // A mover lands on fresh sky each exposure, so nothing persistent sits there.
    if !known {
        filter.insert("properties.stationary", false);
        filter.insert("properties.rock", false);
    }
    // By HEALPix range rather than 2dsphere: that index carries no time, so it
    // scans the whole baseline in the region before the date is applied.
    if let Some((ra, dec, radius)) = region {
        let moc = crate::utils::moc::moc_from_cone(ra, dec, radius)?;
        let region_filter = crate::utils::moc::moc_hpx_filter(&moc)?;
        for (k, v) in region_filter {
            filter.insert(k, v);
        }
        info!(ra, dec, radius, "restricting to a cone");
    }

    let projection = doc! {
        "_id": 1,
        "candidate.jd": 1,
        "candidate.ra": 1,
        "candidate.dec": 1,
        "candidate.ssnamenr": 1,
        "candidate.magpsf": 1,
        "candidate.sigmapsf": 1,
        "candidate.fid": 1,
    };

    let mut cursor = db
        .collection::<Document>("ZTF_alerts")
        .find(filter)
        .projection(projection)
        .await?;

    let mut detections = Vec::new();
    let mut labels = HashMap::new();
    while let Some(doc) = cursor.next().await {
        let doc = doc?;
        let Ok(candidate) = doc.get_document("candidate") else {
            continue;
        };
        let (Ok(id), Ok(jd), Ok(ra), Ok(dec)) = (
            doc.get_i64("_id"),
            candidate.get_f64("jd"),
            candidate.get_f64("ra"),
            candidate.get_f64("dec"),
        ) else {
            continue;
        };
        if let Ok(name) = candidate.get_str("ssnamenr") {
            labels.insert(id, name.to_string());
        }
        detections.push(Detection {
            id,
            jd,
            ra,
            dec,
            mag: candidate.get_f64("magpsf").ok(),
            mag_err: candidate.get_f64("sigmapsf").ok(),
            band: ztf_band(candidate.get_i32("fid").ok()),
        });
    }
    Ok((detections, labels))
}

/// The most recent JD with alerts, floored to the start of that night.
async fn latest_night(db: &mongodb::Database) -> Result<f64, Box<dyn std::error::Error>> {
    let doc = db
        .collection::<Document>("ZTF_alerts")
        .find_one(doc! {})
        .sort(doc! { "candidate.jd": -1 })
        .projection(doc! { "candidate.jd": 1 })
        .await?
        .ok_or("no alerts")?;
    let jd = doc.get_document("candidate")?.get_f64("jd")?;
    // Nights run across a JD boundary, so step back to the preceding noon.
    Ok(night_of(jd) as f64 + 0.5)
}

/// How well the tracklets reproduce the `ssnamenr` labels.
fn score(tracklets: &[Tracklet], labels: &HashMap<i64, String>) -> (usize, usize, usize) {
    let mut pure = 0;
    let mut mixed = 0;
    let mut unlabelled = 0;
    for t in tracklets {
        let names: std::collections::HashSet<&str> = t
            .ids
            .iter()
            .filter_map(|id| labels.get(id).map(|s| s.as_str()))
            .collect();
        match names.len() {
            0 => unlabelled += 1,
            1 => pure += 1,
            _ => mixed += 1,
        }
    }
    (pure, mixed, unlabelled)
}

/// Write each track as a JSON line: its orbit, and every detection under it.
///
/// Enough for a consumer to rebuild the track without reading the database --
/// the epochs carry their own positions, which is what a reviewer needs.
///
/// `known` is parallel to `tracks`, or empty when tracks were not matched.
#[allow(clippy::too_many_arguments)]
fn dump_tracks(
    path: &str,
    tracks: &[Track],
    tracklets: &[Tracklet],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    known: &[Option<String>],
) -> Result<usize, Box<dyn std::error::Error>> {
    use std::io::Write;
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);

    for (i, track) in tracks.iter().enumerate() {
        // Tracklets of one track can share a detection, so an epoch is reported
        // once rather than once per tracklet that contains it.
        let mut epochs: Vec<serde_json::Value> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for &m in &track.members {
            for id in &tracklets[m].ids {
                if !seen.insert(*id) {
                    continue;
                }
                let Some(d) = by_id.get(id) else { continue };
                epochs.push(serde_json::json!({
                    "candid": d.id.to_string(),
                    "jd": d.jd,
                    "ra": d.ra,
                    "dec": d.dec,
                    "mag": d.mag,
                    "band": d.band.map(|b| b.to_string()),
                    "ssnamenr": labels.get(&d.id),
                }));
            }
        }
        epochs.sort_by(|a, b| {
            a["jd"]
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&b["jd"].as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let line = serde_json::json!({
            "track_id": format!("boom_trk_{i:06}"),
            "n_tracklets": track.members.len(),
            "nights": track.nights,
            "residual_arcsec": track.residual_arcsec,
            "rms_au": track.rms_au,
            "hypothesis_r_au": track.hypothesis.r_au,
            "hypothesis_rdot_au_per_day": track.hypothesis.rdot_au_per_day,
            "known_designation": known.get(i).cloned().flatten(),
            "epochs": epochs,
        });
        writeln!(file, "{line}")?;
    }
    file.flush()?;
    Ok(tracks.len())
}

use crate::utils::tracks::BoundFit;

/// A bound-orbit verdict with the residual that produced it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Verdict(BoundFit, Option<f64>);

impl Verdict {
    fn residual(&self) -> Option<f64> {
        self.1
    }

    /// Sort key: a confident bound orbit first, then the ones worth a look.
    fn rank(&self) -> (u8, f64) {
        let order = match self.0 {
            BoundFit::Good => 0,
            BoundFit::Poor => 1,
            BoundFit::None => 2,
            BoundFit::Ungated => 3,
        };
        (order, self.1.unwrap_or(0.0))
    }

    fn label(&self) -> String {
        match (self.0, self.1) {
            (BoundFit::Good, Some(r)) => format!("{r:.2}\""),
            (BoundFit::Poor, Some(r)) => format!("{r:.2}\" poor"),
            (BoundFit::None, _) => "no bound orbit".to_string(),
            _ => "ungated".to_string(),
        }
    }
}

/// Store each THOR cluster the same way a linked track is stored.
///
/// The bound-fit verdict goes with it: a cluster no bound orbit reproduces is
/// the interesting one, and persisting it as though it were clean would lose
/// exactly what makes it worth looking at.
#[allow(clippy::too_many_arguments)]
async fn persist_clusters(
    db: &mongodb::Database,
    clusters: &[(crate::utils::thor::Cluster, Verdict)],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    known: &[Option<String>],
    dry_run: bool,
    min_detections: usize,
    min_nights: usize,
) {
    use crate::utils::tracks::{
        acquire_lock, commit_upsert, plan_upsert, release_lock, stamp_members,
    };
    if !dry_run {
        match acquire_lock(db).await {
            Ok(true) => {}
            Ok(false) => {
                error!("another run is persisting tracks, not writing");
                return;
            }
            Err(e) => {
                error!("could not take the tracks lock: {}", e);
                return;
            }
        }
    }
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    let (mut stored, mut stamped) = (0usize, 0u64);
    for (i, (cluster, verdict)) in clusters.iter().enumerate() {
        let members: Vec<i64> = cluster.ids.clone();
        let jds: Vec<f64> = members
            .iter()
            .filter_map(|id| by_id.get(id))
            .map(|d| d.jd)
            .collect();
        if jds.len() != members.len() {
            error!("a cluster references detections not in this run, skipping");
            continue;
        }
        // The survey's own label comes first, then the catalogue match.
        let designation = members
            .iter()
            .find_map(|id| labels.get(id))
            .map(|l| crate::utils::mpcorb::normalize_ztf_ssnamenr(l).unwrap_or_else(|| l.clone()))
            .or_else(|| known.get(i).cloned().flatten());
        let fit = Some((verdict.0, verdict.residual()));
        let plan = match plan_upsert(db, &members, &jds, designation, fit).await {
            Ok(plan) => plan,
            Err(e) => {
                error!("could not resolve a cluster: {}", e);
                continue;
            }
        };
        if !plan.meets(min_detections, min_nights) {
            info!(
                "dropping a track: {} detections over {} nights after {} contested detection(s) were left with another track",
                plan.n_detections,
                plan.n_nights,
                plan.contested.len()
            );
            continue;
        }
        if dry_run {
            info!("would store {}", plan.describe());
            stored += 1;
            stamped += plan.members.len() as u64;
            continue;
        }
        match commit_upsert(db, plan).await {
            Ok(up) => {
                stored += 1;
                match stamp_members(db, &up.track).await {
                    Ok(n) => stamped += n,
                    Err(e) => error!("could not stamp {}: {}", up.track.id, e),
                }
            }
            Err(e) => error!("could not store a cluster: {}", e),
        }
    }
    if !dry_run {
        if let Err(e) = release_lock(db).await {
            error!("could not release the tracks lock: {}", e);
        }
    }
    let what = if dry_run { "would store" } else { "stored" };
    info!(
        "{} {} thor clusters, {} alerts stamped",
        what, stored, stamped
    );
}

/// Store each track under a durable id and stamp it onto its member alerts.
///
/// One track at a time rather than in bulk: identity is decided against what is
/// already stored, so two tracks of the same object in one run must see each
/// other's writes.
///
/// `known` is parallel to `tracks`, or empty when tracks were not matched to
/// the catalogue.
async fn persist_tracks(
    db: &mongodb::Database,
    tracks: &[Track],
    tracklets: &[Tracklet],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    known: &[Option<String>],
    gates: PersistGates,
) {
    let PersistGates {
        dry_run,
        min_detections,
        min_nights,
    } = gates;
    use crate::utils::tracks::{
        acquire_lock, commit_upsert, plan_upsert, release_lock, stamp_members,
    };
    if !dry_run {
        match acquire_lock(db).await {
            Ok(true) => {}
            Ok(false) => {
                error!("another run is persisting tracks, not writing");
                return;
            }
            Err(e) => {
                error!("could not take the tracks lock: {}", e);
                return;
            }
        }
    }
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    let (mut stored, mut stamped, mut merged) = (0usize, 0u64, 0usize);
    for (i, track) in tracks.iter().enumerate() {
        let mut members: Vec<i64> = track
            .members
            .iter()
            .flat_map(|&m| tracklets[m].ids.iter().copied())
            .collect();
        members.sort_unstable();
        members.dedup();
        let jds: Vec<f64> = members
            .iter()
            .filter_map(|id| by_id.get(id))
            .map(|d| d.jd)
            .collect();
        if jds.len() != members.len() {
            error!("a track references detections not in this run, skipping");
            continue;
        }
        // A track of a known object records the designation, which is what tells
        // a consumer this is a recovery rather than a discovery candidate. The
        // survey's own label comes first, then the catalogue match.
        let designation = members
            .iter()
            .find_map(|id| labels.get(id))
            .map(|l| crate::utils::mpcorb::normalize_ztf_ssnamenr(l).unwrap_or_else(|| l.clone()))
            .or_else(|| known.get(i).cloned().flatten());
        // None means too few points to constrain an orbit; anything that
        // survived with a residual already passed the gate.
        let fit = Some(match track.residual_arcsec {
            Some(r) => (BoundFit::Good, Some(r)),
            None => (BoundFit::Ungated, None),
        });
        let plan = match plan_upsert(db, &members, &jds, designation, fit).await {
            Ok(plan) => plan,
            Err(e) => {
                error!("could not resolve a track: {}", e);
                continue;
            }
        };
        if !plan.meets(min_detections, min_nights) {
            info!(
                "dropping a track: {} detections over {} nights after {} contested detection(s) were left with another track",
                plan.n_detections,
                plan.n_nights,
                plan.contested.len()
            );
            continue;
        }
        if dry_run {
            info!("would store {}", plan.describe());
            stored += 1;
            merged += plan.superseded.len();
            stamped += plan.members.len() as u64;
            continue;
        }
        let superseded = plan.superseded.clone();
        match commit_upsert(db, plan).await {
            Ok(up) => {
                stored += 1;
                merged += superseded.len();
                if !superseded.is_empty() {
                    info!("track {} absorbed {}", up.track.id, superseded.join(", "));
                }
                match stamp_members(db, &up.track).await {
                    Ok(n) => stamped += n,
                    Err(e) => error!("could not stamp {}: {}", up.track.id, e),
                }
            }
            Err(e) => error!("could not store a track: {}", e),
        }
    }
    if !dry_run {
        if let Err(e) = release_lock(db).await {
            error!("could not release the tracks lock: {}", e);
        }
    }
    if dry_run {
        info!(
            "dry run: would store {} tracks, stamp {} alerts, absorb {} superseded ids",
            stored, stamped, merged
        );
    } else {
        info!(
            "stored {} tracks, stamped {} alerts, absorbed {} superseded ids",
            stored, stamped, merged
        );
    }
}

/// Write each THOR cluster as a JSON line, mirroring `dump_tracks`.
///
/// `orbit_residual_arcsec` is null on a pair, which carries too few points to
/// fit an orbit and so passes the gate unchecked rather than vouched for.
/// `known` is parallel to `clusters`, or empty when they were not matched.
fn dump_clusters(
    path: &str,
    clusters: &[(crate::utils::thor::Cluster, Verdict)],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    known: &[Option<String>],
) -> Result<usize, Box<dyn std::error::Error>> {
    use std::io::Write;
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);

    for (i, (cluster, residual)) in clusters.iter().enumerate() {
        let mut epochs: Vec<serde_json::Value> = Vec::new();
        for id in &cluster.ids {
            let Some(d) = by_id.get(id) else { continue };
            epochs.push(serde_json::json!({
                "candid": d.id.to_string(),
                "jd": d.jd,
                "ra": d.ra,
                "dec": d.dec,
                "mag": d.mag,
                "band": d.band.map(|b| b.to_string()),
                "ssnamenr": labels.get(&d.id),
            }));
        }
        epochs.sort_by(|a, b| {
            a["jd"]
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&b["jd"].as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let line = serde_json::json!({
            "track_id": format!("boom_thor_{i:06}"),
            "n_detections": cluster.ids.len(),
            "nights": cluster.nights,
            "orbit_residual_arcsec": residual.residual(),
            // A coherent cluster with no good bound solution is what a distant
            // or unbound object looks like, so the reason is carried, not lost.
            "bound_fit": residual.0.as_str(),
            "cluster_rms_arcsec": cluster.rms_arcsec,
            "rate_x_deg_per_day": cluster.rate_x_deg_per_day,
            "rate_y_deg_per_day": cluster.rate_y_deg_per_day,
            "known_designation": known.get(i).cloned().flatten(),
            "epochs": epochs,
        });
        writeln!(file, "{line}")?;
    }
    file.flush()?;
    Ok(clusters.len())
}

/// Recover objects without tracklets, sweeping trial orbits over sky patches.
///
/// A trial orbit only governs the detections near where it sits -- beyond a
/// couple of degrees the co-moving frame no longer applies -- so the sky is
/// divided into patches and each is searched with its own orbits. One orbit at
/// the centre of a whole night's coverage governs almost nothing.
async fn run_thor(
    params: &LinkTracksParams,
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    db: Option<&mongodb::Database>,
) {
    use crate::utils::heliolinc::{sky_track, test_orbits};
    use crate::utils::thor;
    use rayon::prelude::*;

    let mut cfg = thor::Config {
        min_detections: params.min_detections.max(2),
        min_nights: params.thor_min_nights,
        ..thor::Config::default()
    };
    if let Some(v) = params.cluster_radius {
        cfg.cluster_radius_arcsec = v;
    }
    if let Some(v) = params.max_cluster_rms {
        cfg.max_rms_arcsec = v;
    }
    if let Some(v) = params.max_residual_rate {
        cfg.max_residual_rate_deg_per_day = v;
    }
    if let Some(v) = params.rate_steps {
        cfg.rate_steps = v;
    }

    let jds: Vec<f64> = detections.iter().map(|d| d.jd).collect();
    let (lo, hi) = jds
        .iter()
        .fold((f64::MAX, f64::MIN), |(a, b), &j| (a.min(j), b.max(j)));
    let epoch = 0.5 * (lo + hi);
    let steps = (((hi - lo) / 0.5).ceil() as usize).max(2);
    let sample: Vec<f64> = (0..=steps)
        .map(|k| lo + (hi - lo) * k as f64 / steps as f64)
        .collect();

    // Four grids, each shifted half a patch in RA, Dec or both. A single grid
    // cuts objects on its boundaries in half, leaving each part below
    // `min_detections`; with the shifts, any object spanning less than half a
    // patch lies wholly inside one patch of at least one grid. The copies this
    // makes are dropped by the deduplication after the orbit-fit gate.
    let patch_deg = cfg.max_offset_deg;
    let mut patches: HashMap<(u8, i64, i64), Vec<Detection>> = HashMap::new();
    for d in detections {
        for (grid, (ox, oy)) in [(0.0, 0.0), (0.5, 0.0), (0.0, 0.5), (0.5, 0.5)]
            .into_iter()
            .enumerate()
        {
            let dy = (d.dec / patch_deg + oy).floor() as i64;
            // One RA cut per band, off the band centre rather than each
            // detection's own dec, or the same RA lands in different patches at
            // either edge of the band. Equal-area, so bands narrow to the poles.
            let band_dec = ((dy as f64 - oy + 0.5) * patch_deg).clamp(-89.9, 89.9);
            let scale = band_dec.to_radians().cos().max(0.05);
            let bins = ((360.0 * scale / patch_deg).round() as i64).max(1);
            // rem_euclid closes the band into a ring, so RA 0/360 is not a seam.
            let dx = ((d.ra * bins as f64 / 360.0 + ox).floor() as i64).rem_euclid(bins);
            patches.entry((grid as u8, dx, dy)).or_default().push(*d);
        }
    }
    let patches: Vec<Vec<Detection>> = patches
        .into_values()
        .filter(|v| v.len() >= cfg.min_detections)
        .collect();
    info!(
        "{} sky patches of {:.1} deg over 4 offset grids, {} trial distances each",
        patches.len(),
        patch_deg,
        params.thor_distances.len()
    );

    let started = std::time::Instant::now();
    let clusters: Vec<(thor::Cluster, crate::utils::heliolinc::State)> = patches
        .par_iter()
        .flat_map(|patch| {
            // On the circle: a patch straddling RA 0 would otherwise centre on
            // 180 and put every trial orbit on the far side of the sky.
            let Some(ra0) = circular_mean_deg(patch.iter().map(|d| d.ra)) else {
                return Vec::new();
            };
            // Declination does not wrap, so its mean is the ordinary one.
            let dec0 = patch.iter().map(|d| d.dec).sum::<f64>() / patch.len() as f64;
            let mut found = Vec::new();
            for (state, _r) in test_orbits(ra0, dec0, epoch, &params.thor_distances) {
                let Some((ra, dec)) = sky_track(&state, epoch, &sample) else {
                    continue;
                };
                let track = thor::TestOrbitTrack {
                    jd: sample.clone(),
                    ra,
                    dec,
                };
                // The trial orbit seeds the fit: it is near the truth by
                // construction, which is why the cluster formed around it.
                found.extend(
                    thor::recover(patch, &track, &cfg)
                        .into_iter()
                        .map(|c| (c, state)),
                );
            }
            found
        })
        .collect();

    info!(
        "{} clusters from {} detections over {} patches in {:.1}s",
        clusters.len(),
        detections.len(),
        patches.len(),
        started.elapsed().as_secs_f64()
    );

    // Gate on how well one orbit reproduces the cluster's own positions, as the
    // tracklet path does. Two points cannot constrain six parameters, so those
    // pass through ungated and are reported separately rather than counted as
    // though the astrometry had vouched for them.
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    let gate_start = std::time::Instant::now();
    let mut scored: Vec<(thor::Cluster, Verdict)> = clusters
        .into_par_iter()
        .filter_map(|(c, seed)| {
            let obs: Vec<Observation> = c
                .ids
                .iter()
                .filter_map(|id| by_id.get(id))
                .map(|d| Observation {
                    jd: d.jd,
                    ra: d.ra,
                    dec: d.dec,
                })
                .collect();
            if obs.len() < 3 {
                return Some((c, Verdict(BoundFit::Ungated, None)));
            }
            // Screened against the looser gate, since a poor fit is still kept,
            // and converged if it passes it, so the residual it is ranked and
            // persisted on is the orbit's rather than where the fit stopped.
            match fit_within(
                &obs,
                &seed,
                epoch,
                &crate::utils::sso_geometry::ZTF,
                params.max_unbound_residual,
            ) {
                None => Some((c, Verdict(BoundFit::None, None))),
                Some(fit) if fit.rms_arcsec <= params.max_residual => {
                    Some((c, Verdict(BoundFit::Good, Some(fit.rms_arcsec))))
                }
                Some(fit) if fit.rms_arcsec <= params.max_unbound_residual => {
                    Some((c, Verdict(BoundFit::Poor, Some(fit.rms_arcsec))))
                }
                Some(_) => None,
            }
        })
        .collect();

    // Best-fitting first, so an overlapping cluster keeps the detections the
    // astrometry supports. Ungated pairs rank last.
    scored.sort_by(|a, b| {
        let (ka, kb) = (a.1.rank(), b.1.rank());
        ka.0.cmp(&kb.0)
            .then(ka.1.partial_cmp(&kb.1).unwrap_or(std::cmp::Ordering::Equal))
            .then(b.0.ids.len().cmp(&a.0.ids.len()))
    });
    let mut claimed: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut kept: Vec<(thor::Cluster, Verdict)> = Vec::new();
    for (c, r) in scored {
        // Sharing this many detections with something already kept makes the two
        // one track downstream, where the later one would extend the earlier and
        // overwrite its verdict. Best-ranked first, so the one dropped is worse.
        let shared = c.ids.iter().filter(|id| claimed.contains(id)).count();
        if shared >= crate::utils::tracks::SHARED_FOR_IDENTITY {
            continue;
        }
        claimed.extend(c.ids.iter().copied());
        kept.push((c, r));
    }
    info!(
        "{} clusters survive the orbit fit and deduplication in {:.1}s",
        kept.len(),
        gate_start.elapsed().as_secs_f64()
    );

    let known = if params.match_known {
        let groups: Vec<Vec<i64>> = kept.iter().map(|(c, _)| c.ids.clone()).collect();
        match db {
            Some(db) => match_known(db, &groups, detections, params).await,
            None => {
                error!("match_known needs a database, which was not built");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    if let Some(path) = &params.out_tracks {
        match dump_clusters(path, &kept, detections, labels, &known) {
            Ok(n) => info!("wrote {} clusters to {}", n, path),
            Err(e) => error!("could not write clusters: {}", e),
        }
    }
    if params.persist || params.dry_run {
        match db {
            Some(db) => {
                persist_clusters(
                    db,
                    &kept,
                    detections,
                    labels,
                    &known,
                    params.dry_run,
                    cfg.min_detections,
                    cfg.min_nights,
                )
                .await
            }
            None => error!("--persist needs a database, which was not built"),
        }
    }

    for (c, resid) in kept.iter().take(params.show) {
        let name = c
            .ids
            .iter()
            .find_map(|id| labels.get(id))
            .map(|s| s.as_str())
            .unwrap_or("-");
        info!(
            "cluster n={} nights={} rms={:.2}\" orbit={} label={}",
            c.ids.len(),
            c.nights,
            c.rms_arcsec,
            resid.label(),
            name
        );
    }

    if labels.is_empty() {
        return;
    }
    let (mut pure, mut mixed) = (0usize, 0usize);
    let (mut poor_pure, mut poor_mixed) = (0usize, 0usize);
    let (mut pair_pure, mut pair_mixed) = (0usize, 0usize);
    let mut recovered = std::collections::HashSet::new();
    for (c, resid) in &kept {
        let names: std::collections::HashSet<&String> =
            c.ids.iter().filter_map(|id| labels.get(id)).collect();
        let good = resid.0 == BoundFit::Good;
        let gated = resid.residual().is_some();
        match names.len() {
            0 => {}
            1 => {
                recovered.insert((*names.iter().next().unwrap()).clone());
                if good {
                    pure += 1;
                } else if gated {
                    poor_pure += 1;
                } else {
                    pair_pure += 1;
                }
            }
            _ => {
                if good {
                    mixed += 1;
                } else if gated {
                    poor_mixed += 1;
                } else {
                    pair_mixed += 1;
                }
            }
        }
    }
    // What each object's own cadence was, so "one detection per night" describes
    // the object rather than the cluster THOR happened to build from it.
    let mut per_object_night: HashMap<(&String, i64), usize> = HashMap::new();
    for d in detections {
        if let Some(name) = labels.get(&d.id) {
            *per_object_night.entry((name, night_of(d.jd))).or_default() += 1;
        }
    }
    let mut busiest: HashMap<&String, usize> = HashMap::new();
    let mut nights_of: HashMap<&String, usize> = HashMap::new();
    for ((name, _), c) in &per_object_night {
        let e = busiest.entry(name).or_insert(0);
        *e = (*e).max(*c);
        *nights_of.entry(name).or_default() += 1;
    }
    let thor_only: std::collections::HashSet<String> = busiest
        .iter()
        .filter(|(n, &m)| m == 1 && nights_of.get(*n).copied().unwrap_or(0) >= 2)
        .map(|(n, _)| (*n).clone())
        .collect();
    let recovered_thor_only = recovered.iter().filter(|n| thor_only.contains(*n)).count();

    let total: std::collections::HashSet<&String> = labels.values().collect();
    let pct = |a: usize, b: usize| {
        if a + b == 0 {
            0.0
        } else {
            100.0 * a as f64 / (a + b) as f64
        }
    };
    info!(
        "thor gated (3+ detections): {} pure, {} mixed = {:.1}% purity",
        pure,
        mixed,
        pct(pure, mixed)
    );
    info!(
        "thor poor bound fit:        {} pure, {} mixed = {:.1}% purity",
        poor_pure,
        poor_mixed,
        pct(poor_pure, poor_mixed)
    );
    info!(
        "thor ungated (pairs):       {} pure, {} mixed = {:.1}% purity",
        pair_pure,
        pair_mixed,
        pct(pair_pure, pair_mixed)
    );
    info!(
        "thor: {} distinct objects of {}",
        recovered.len(),
        total.len()
    );
    info!(
        "of those, {} never had more than one detection in a night, of {} such objects present -- the population tracklet linking cannot reach",
        recovered_thor_only,
        thor_only.len()
    );
}

/// The catalogued object each group of detections is, if any, in the order of
/// `groups`.
///
/// Every detection in a group is matched against the whole of MPC_orbits, and
/// the group takes a designation only when most of its detections agree on one
/// across nights: a single detection near some catalogued orbit is too often
/// chance. Empty when the catalogue cannot be read, so the run goes on without
/// designations instead of failing.
async fn match_known(
    db: &mongodb::Database,
    groups: &[Vec<i64>],
    detections: &[Detection],
    params: &LinkTracksParams,
) -> Vec<Option<String>> {
    use crate::utils::identify::{identify, track_designation, IdentifyConfig, KnownRule, Match};

    let started = std::time::Instant::now();
    let orbits = match crate::utils::mpcorb::load_catalogue(db).await {
        Ok(orbits) => orbits,
        Err(error) => {
            error!(%error, "could not read MPC_orbits, so tracks are not matched");
            return Vec::new();
        }
    };
    let wanted: std::collections::HashSet<i64> = groups.iter().flatten().copied().collect();
    let mut members: Vec<Detection> = detections
        .iter()
        .filter(|d| wanted.contains(&d.id))
        .copied()
        .collect();
    members.sort_by_key(|d| d.id);
    let cfg = IdentifyConfig {
        match_radius_arcsec: params.identify_radius,
        ..IdentifyConfig::default()
    };
    let matches = identify(&members, &orbits, &cfg);
    let by_detection: HashMap<i64, &Match> = matches.iter().map(|m| (m.detection_id, m)).collect();
    let rule = KnownRule {
        min_fraction: params.known_fraction,
        min_nights: params.known_min_nights,
        max_scatter_arcsec: params.known_max_scatter,
        max_drift_arcsec_per_day: params.known_max_drift,
    };
    let known: Vec<Option<String>> = groups
        .iter()
        .map(|group| {
            let found: Vec<&Match> = group
                .iter()
                .filter_map(|id| by_detection.get(id).copied())
                .collect();
            track_designation(&found, group.len(), &rule)
        })
        .collect();
    info!(
        "{} of {} tracks are catalogued objects ({} orbits, {:.1}s)",
        known.iter().filter(|k| k.is_some()).count(),
        known.len(),
        orbits.len(),
        started.elapsed().as_secs_f64()
    );
    known
}

/// Attribute detections to catalogued objects, and score against `ssnamenr`.
///
/// Every detection here already carries IPAC's identification, so the
/// catalogue's answer can be checked directly: agreement measures whether
/// propagating MPCORB to the detection epoch lands where the object was.
async fn run_identify(
    db: &mongodb::Database,
    params: &LinkTracksParams,
    detections: &[Detection],
    labels: &HashMap<i64, String>,
) {
    use crate::utils::identify::{identify, IdentifyConfig};
    use crate::utils::mpcorb::{load_catalogue, normalize_ztf_ssnamenr};

    let started = std::time::Instant::now();
    // A read failure part-way through would leave a truncated catalogue, which
    // would silently score as a lower recall rather than as a failure.
    let orbits = match load_catalogue(db).await {
        Ok(orbits) => orbits,
        Err(error) => {
            error!(%error, "reading MPC_orbits failed");
            return;
        }
    };
    let mut epochs: Vec<f64> = orbits.iter().map(|o| o.elements.epoch_jd).collect();
    let mid_jd = detections.iter().map(|d| d.jd).sum::<f64>() / detections.len() as f64;
    epochs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median_epoch = epochs.get(epochs.len() / 2).copied().unwrap_or(0.0);
    info!(
        "{} catalogue orbits in {:.1}s; median epoch JD {:.1}, {:.0} days from these detections",
        orbits.len(),
        started.elapsed().as_secs_f64(),
        median_epoch,
        (mid_jd - median_epoch).abs()
    );

    let cfg = IdentifyConfig {
        match_radius_arcsec: params.identify_radius,
        ..IdentifyConfig::default()
    };
    let started = std::time::Instant::now();
    let matches = identify(detections, &orbits, &cfg);
    info!(
        "{} of {} detections attributed in {:.1}s",
        matches.len(),
        detections.len(),
        started.elapsed().as_secs_f64()
    );

    if labels.is_empty() {
        return;
    }
    let (mut agree, mut disagree, mut unlabelled) = (0usize, 0usize, 0usize);
    let mut seps: Vec<f64> = Vec::new();
    for m in &matches {
        match labels
            .get(&m.detection_id)
            .and_then(|s| normalize_ztf_ssnamenr(s))
        {
            None => unlabelled += 1,
            Some(truth) => {
                if truth == m.designation {
                    agree += 1;
                    seps.push(m.separation_arcsec);
                } else {
                    disagree += 1;
                }
            }
        }
    }
    let labelled: usize = detections
        .iter()
        .filter(|d| {
            labels
                .get(&d.id)
                .and_then(|s| normalize_ztf_ssnamenr(s))
                .is_some()
        })
        .count();
    seps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    info!(
        "identify: {} agree with ssnamenr, {} disagree, {} matched an unnamed detection",
        agree, disagree, unlabelled
    );
    info!(
        "recall {:.1}% of {} normalisable detections; median separation of an agreeing match {:.2} arcsec",
        100.0 * agree as f64 / labelled.max(1) as f64,
        labelled,
        seps.get(seps.len() / 2).copied().unwrap_or(f64::NAN)
    );

    // What each candidate radius would have bought, so the default is chosen
    // from the curve rather than from whichever number happened to work.
    let mut wrong: Vec<f64> = matches
        .iter()
        .filter(|m| {
            labels
                .get(&m.detection_id)
                .and_then(|s| normalize_ztf_ssnamenr(s))
                .is_some_and(|t| t != m.designation)
        })
        .map(|m| m.separation_arcsec)
        .collect();
    wrong.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    info!("radius  recall   false");
    for r in [5.0, 10.0, 20.0, 30.0, 40.0, 60.0, 90.0, 120.0, 240.0, 600.0] {
        let right = seps.partition_point(|&s| s <= r);
        let bad = wrong.partition_point(|&s| s <= r);
        info!(
            "{:6.0}  {:5.1}%  {:5.2}%",
            r,
            100.0 * right as f64 / labelled.max(1) as f64,
            100.0 * bad as f64 / (right + bad).max(1) as f64
        );
    }
}

fn failed(e: impl std::fmt::Display) -> super::TaskError {
    super::TaskError::Failed(e.to_string())
}

/// Where a staged dump is read from and a track dump is written to.
///
/// Under the shared data path, so both are visible to the worker rather than
/// to whoever submitted the run. `export/` is the directory the admin page
/// already lists and streams, so a written dump is downloadable without any
/// new plumbing.
fn staged_dir() -> PathBuf {
    PathBuf::from(std::env::var(DATA_PATH_ENV).unwrap_or_else(|_| "data/catalogs".into()))
        .join("link_tracks")
}

fn export_dir() -> PathBuf {
    PathBuf::from(std::env::var(DATA_PATH_ENV).unwrap_or_else(|_| "data/catalogs".into()))
        .join("export")
        .join("link_tracks")
}

pub async fn run(
    ctx: &TaskContext,
    params: LinkTracksParams,
) -> Result<serde_json::Value, super::TaskError> {
    params
        .validate_params()
        .map_err(super::TaskError::InvalidParams)?;
    let db = ctx.db().clone();

    let (detections, labels) = match &params.input {
        Some(name) => {
            let path = staged_dir().join(name);
            ctx.info(format!("reading staged detections from {}", path.display()));
            load_file(&path.to_string_lossy()).map_err(failed)?
        }
        None => {
            let jd_start = match params.jd_start {
                Some(jd) => jd,
                None => latest_night(&db).await.map_err(failed)?,
            };
            let region = match (params.ra, params.dec, params.radius) {
                (Some(ra), Some(dec), Some(radius)) => Some((ra, dec, radius)),
                _ => None,
            };
            ctx.info(format!(
                "loading detections from jd {jd_start} over {} day(s){}",
                params.span,
                if params.known {
                    ", known objects only"
                } else {
                    ""
                }
            ));
            load(&db, jd_start, params.span, params.drb, params.known, region)
                .await
                .map_err(failed)?
        }
    };
    ctx.info(format!(
        "{} detection(s), {} carrying an ssnamenr label",
        detections.len(),
        labels.len()
    ));
    if detections.is_empty() {
        // Not an error: an empty night is a fact about the night.
        return Ok(serde_json::json!({ "detections": 0, "tracklets": 0, "tracks": 0 }));
    }

    let cfg = TrackletConfig {
        min_detections: params.min_detections,
        max_span_days: params.max_tracklet_span,
        max_rate_deg_per_day: params.max_rate,
        min_arc_arcsec: params.min_arc,
        min_pair_dt_days: params.min_pair_dt,
        max_mag_sigma: (params.max_mag_sigma > 0.0).then_some(params.max_mag_sigma),
        ..TrackletConfig::default()
    };

    // The two alternative modes report through the log as the binary did; their
    // numbers are not yet summarised into the result.
    if params.thor {
        run_thor(&params, &detections, &labels, Some(&db)).await;
        return Ok(serde_json::json!({ "mode": "thor", "detections": detections.len() }));
    }
    if params.identify {
        run_identify(&db, &params, &detections, &labels).await;
        return Ok(serde_json::json!({ "mode": "identify", "detections": detections.len() }));
    }

    if ctx.is_canceled() {
        return Err(super::TaskError::Canceled);
    }

    let tracklets = if params.link {
        tracklets_per_night(&detections, &cfg)
    } else {
        find_tracklets(&detections, &cfg)
    };
    ctx.info(format!(
        "{} tracklet(s) from {} detection(s)",
        tracklets.len(),
        detections.len()
    ));

    // Everything worth reading off the admin page goes here rather than only
    // into the log: tuning means comparing these numbers between runs, and a
    // run's parameters are already stored beside them.
    let mut result = serde_json::json!({
        "detections": detections.len(),
        "labelled": labels.len(),
        "tracklets": tracklets.len(),
    });
    if params.known {
        let (pure, mixed, unlabelled) = score(&tracklets, &labels);
        let distinct: std::collections::HashSet<&String> = labels.values().collect();
        let linked: std::collections::HashSet<&str> = tracklets
            .iter()
            .flat_map(|t| t.ids.iter())
            .filter_map(|id| labels.get(id).map(|s| s.as_str()))
            .collect();
        ctx.info(format!(
            "against ssnamenr: {pure} pure, {mixed} mixed, {unlabelled} unlabelled; \
             {} of {} objects appear in at least one tracklet",
            linked.len(),
            distinct.len()
        ));
        result["tracklet_scoring"] = serde_json::json!({
            "pure": pure,
            "mixed": mixed,
            "unlabelled": unlabelled,
            "objects_present": distinct.len(),
            "objects_in_a_tracklet": linked.len(),
        });
    }

    if !params.link {
        for t in tracklets.iter().take(params.show) {
            info!(
                "n={} rate={:.4} deg/d rms={:.2}\" ra={:.5} dec={:.5}",
                t.ids.len(),
                t.rate_deg_per_day(),
                t.rms_arcsec,
                t.ra_ref,
                t.dec_ref
            );
        }
        return Ok(result);
    }

    if ctx.is_canceled() {
        return Err(super::TaskError::Canceled);
    }

    let jds: Vec<f64> = tracklets.iter().map(|t| t.jd_ref).collect();
    let reference_jd = (jds.iter().cloned().fold(f64::MAX, f64::min)
        + jds.iter().cloned().fold(f64::MIN, f64::max))
        / 2.0;
    let link_cfg = LinkConfig {
        hypotheses: default_hypotheses(),
        reference_jd,
        position_tol_au: params.position_tol,
        velocity_tol_au_per_day: params.velocity_tol,
        min_nights: params.min_nights,
        max_residual_arcsec: params.max_residual,
        site: crate::utils::sso_geometry::ZTF,
    };
    let tracks = link_tracklets(&tracklets, &detections, &link_cfg);
    ctx.info(format!(
        "{} track(s) from {} tracklet(s) over {} hypotheses",
        tracks.len(),
        tracklets.len(),
        link_cfg.hypotheses.len()
    ));
    result["tracks"] = serde_json::json!(tracks.len());
    if !labels.is_empty() {
        let (pure, mixed, recovered) = score_tracks(&tracks, &tracklets, &labels);
        ctx.info(format!(
            "tracks: {pure} pure, {mixed} mixed, {recovered} distinct objects recovered"
        ));
        result["track_scoring"] =
            serde_json::json!({ "pure": pure, "mixed": mixed, "recovered": recovered });
    }

    // Matched once here and shared by the dump and the store, so a track
    // cannot be written to the two with different designations.
    let known = if params.match_known {
        let groups: Vec<Vec<i64>> = tracks
            .iter()
            .map(|track| {
                let mut ids: Vec<i64> = track
                    .members
                    .iter()
                    .flat_map(|&m| tracklets[m].ids.iter().copied())
                    .collect();
                ids.sort_unstable();
                ids.dedup();
                ids
            })
            .collect();
        match_known(&db, &groups, &detections, &params).await
    } else {
        Vec::new()
    };

    if let Some(name) = &params.out_tracks {
        let dir = export_dir();
        std::fs::create_dir_all(&dir).map_err(failed)?;
        let path = dir.join(name);
        match dump_tracks(
            &path.to_string_lossy(),
            &tracks,
            &tracklets,
            &detections,
            &labels,
            &known,
        ) {
            Ok(n) => {
                ctx.info(format!("wrote {n} track(s) to {}", path.display()));
                result["written_to"] = serde_json::json!(path.display().to_string());
            }
            // Not fatal: the numbers above are the point of a tuning run, and
            // losing the dump should not throw them away.
            Err(e) => ctx.warn(format!("could not write the track dump: {e}")),
        }
    }

    if params.persist || params.dry_run {
        persist_tracks(
            &db,
            &tracks,
            &tracklets,
            &detections,
            &labels,
            &known,
            PersistGates {
                dry_run: params.dry_run,
                min_detections: params.min_detections,
                min_nights: params.min_nights,
            },
        )
        .await;
        result["persisted"] = serde_json::json!(!params.dry_run);
    }

    if params.persist {
        let survey = "ztf";
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: format!("{}_tracks", survey.to_uppercase()),
                catalog: None,
                survey: Some(survey.to_string()),
            },
            Operation::Backfill,
            doc! {
                "tracks": tracks.len() as i64,
                "detections": detections.len() as i64,
                "span": params.span,
                "min_nights": params.min_nights as i64,
                "max_residual": params.max_residual,
                "thor": params.thor,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(mongodb::bson::Bson::Null),
            },
        )
        .await;
    }

    for track in tracks.iter().take(params.show) {
        info!(
            "track n={} nights={} r={:.2} au rdot={:+.5}",
            track.members.len(),
            track.nights,
            track.hypothesis.r_au,
            track.hypothesis.rdot_au_per_day
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> LinkTracksParams {
        serde_json::from_value(serde_json::json!({})).expect("every field has a default")
    }

    #[test]
    fn the_defaults_match_the_binary_this_replaced() {
        // A recipe from somebody's shell history has to mean the same thing
        // submitted here, or a tuning run is not comparable with the ones that
        // calibrated the current thresholds.
        let p = params();
        assert_eq!(p.span, 0.5);
        assert_eq!(p.drb, 0.8);
        assert_eq!(p.min_detections, 2);
        assert_eq!(p.min_nights, 2);
        assert_eq!(p.max_residual, 2.0);
        assert_eq!(p.thor_distances, vec![1.8, 2.2, 2.6, 3.0, 3.4]);
        assert_eq!(p.thor_min_nights, 3);
        // The catalogue-matching gates, which arrived in find_tracklets after
        // this task was split out of it.
        assert!(!p.match_known);
        assert_eq!(p.known_fraction, 2.0 / 3.0);
        assert_eq!(p.known_min_nights, 2);
        assert_eq!(p.known_max_scatter, 10.0);
        assert_eq!(p.known_max_drift, 2.0);
    }

    #[test]
    fn a_catalogue_match_that_could_never_hold_is_rejected() {
        // Only checked when the matching is on, so a stored recipe that leaves
        // these at nonsense while never asking for the match still runs.
        let mut p = params();
        p.known_fraction = 1.5;
        assert!(p.validate_params().is_ok());
        p.match_known = true;
        assert!(
            p.validate_params().is_err(),
            "a fraction above 1 can never be met, so no track would be matched"
        );
        p.known_fraction = 0.0;
        assert!(
            p.validate_params().is_err(),
            "a fraction of 0 would let one chance match name the track"
        );
        p.known_fraction = 2.0 / 3.0;
        assert!(p.validate_params().is_ok());
    }

    #[test]
    fn nothing_is_written_unless_asked() {
        // The default is a tuning run. Persisting is the exception, because a
        // run that merges stored tracks cannot be undone by running it again.
        let p = params();
        assert!(!p.persist);
        assert!(!p.dry_run);
    }

    #[test]
    fn persist_and_dry_run_are_not_both_allowed() {
        let mut p = params();
        p.persist = true;
        p.dry_run = true;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn a_half_specified_cone_is_rejected() {
        let mut p = params();
        p.ra = Some(10.0);
        assert!(
            p.validate_params().is_err(),
            "a cone missing its radius would quietly search the whole sky"
        );
        p.dec = Some(0.0);
        p.radius = Some(1.0);
        assert!(p.validate_params().is_ok());
    }

    #[test]
    fn file_names_cannot_escape_the_data_directory() {
        for name in ["../../etc/passwd", "sub/dir.jsonl"] {
            let mut p = params();
            p.out_tracks = Some(name.to_string());
            assert!(p.validate_params().is_err(), "{name} should be rejected");
        }
        let mut p = params();
        p.out_tracks = Some("tuning-run-1.jsonl".to_string());
        assert!(p.validate_params().is_ok());
    }

    #[test]
    fn an_unbounded_span_is_rejected() {
        let mut p = params();
        p.span = 365.0;
        assert!(p.validate_params().is_err());
        p.span = 0.0;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn two_runs_never_persist_at_once() {
        // Merging is decided across the whole collection, so unlike the other
        // tasks this one is keyed on nothing: any two runs conflict.
        let key = crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({}));
        assert_eq!(key, Some(mongodb::bson::doc! {}));
    }
}
