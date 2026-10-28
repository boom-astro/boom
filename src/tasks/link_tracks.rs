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
use crate::utils::discovery::{
    designations, latest_night, load_window, persist_clusters, persist_tracks, reference_epoch,
    thor_clusters, track_detections, tracklets_per_night, ztf_band, PersistReport, Stop,
    ThorSearch, Verdict,
};
use crate::utils::heliolinc::{default_hypotheses, link_tracklets, Conics, LinkConfig, Track};
use crate::utils::identify::{IdentifyConfig, KnownRule};
use crate::utils::linking::{find_tracklets, night_of, Detection, Tracklet, TrackletConfig};
use crate::utils::tracks::BoundFit;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "link_tracks";

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
    /// Keep a THOR cluster whose best orbit leaves up to this residual,
    /// arcseconds, reported as a poor fit. Where a cluster no orbit fits within
    /// `max_residual` lands.
    // Aliased because this is a submitted parameter: runs recorded under the
    // old name still deserialize.
    #[serde(default = "d_max_poor_residual", alias = "max_unbound_residual")]
    pub max_poor_residual: f64,
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
fn d_max_poor_residual() -> f64 {
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
            // The dump is this survey's own rows, which is what `None` means:
            // `persist_tracks` reads it as the default site.
            site: None,
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

/// Recover objects without tracklets, in the manner of THOR.
async fn run_thor(
    ctx: &TaskContext,
    params: &LinkTracksParams,
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    db: &mongodb::Database,
    stop: &Stop,
) -> serde_json::Value {
    use crate::utils::thor;

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
    let search = ThorSearch {
        config: cfg,
        distances_au: params.thor_distances.clone(),
        max_residual_arcsec: params.max_residual,
        max_poor_residual_arcsec: params.max_poor_residual,
        site: crate::utils::sso_geometry::ZTF,
        // Bound only, as this task has always searched. Sweeping hyperbolae
        // costs about thirty times as much, so it belongs behind a parameter
        // of its own rather than arriving silently with a merge.
        conics: Conics::Bound,
    };
    let cfg = &search.config;
    let kept: Vec<(thor::Cluster, Verdict)> = thor_clusters(detections, &search);
    ctx.info(format!(
        "{} cluster(s) survive the orbit fit and deduplication",
        kept.len()
    ));
    let mut result = serde_json::json!({
        "mode": "thor",
        "detections": detections.len(),
        "labelled": labels.len(),
        "clusters": kept.len(),
    });

    let known = if params.match_known {
        let groups: Vec<Vec<i64>> = kept.iter().map(|(c, _)| c.ids.clone()).collect();
        match_known(ctx, db, &groups, detections, params).await
    } else {
        Vec::new()
    };
    result["catalogued"] = serde_json::json!(known.iter().filter(|k| k.is_some()).count());

    if let Some(name) = &params.out_tracks {
        if let Some(path) = write_dump(ctx, name, |path| {
            dump_clusters(path, &kept, detections, labels, &known)
        }) {
            result["written_to"] = serde_json::json!(path);
        }
    }
    if params.persist || params.dry_run {
        let report = persist_clusters(
            db,
            &kept,
            detections,
            labels,
            &known,
            &[],
            params.dry_run,
            cfg.min_detections,
            cfg.min_nights,
            stop,
        )
        .await;
        result["persisted"] = persisted_json(ctx, &report, params.dry_run);
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
        return result;
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
    // The same numbers in the result as in the log: tuning means comparing
    // these between runs, and the run's parameters are stored beside them.
    result["cluster_scoring"] = serde_json::json!({
        "gated": { "pure": pure, "mixed": mixed },
        "poor_bound_fit": { "pure": poor_pure, "mixed": poor_mixed },
        "ungated_pairs": { "pure": pair_pure, "mixed": pair_mixed },
        "objects_present": total.len(),
        "objects_recovered": recovered.len(),
        "single_detection_objects_present": thor_only.len(),
        "single_detection_objects_recovered": recovered_thor_only,
    });
    result
}

/// The cataloged object each group of detections is, if any, in the order of
/// `groups`.
///
/// Empty when the catalog cannot be read, so the run goes on without
/// designations instead of failing -- a track with no designation is reported
/// as a discovery candidate, which is the conservative answer.
async fn match_known(
    ctx: &TaskContext,
    db: &mongodb::Database,
    groups: &[Vec<i64>],
    detections: &[Detection],
    params: &LinkTracksParams,
) -> Vec<Option<String>> {
    let orbits = match crate::utils::mpcorb::load_catalogue(db).await {
        Ok(orbits) => orbits,
        Err(error) => {
            ctx.warn(format!(
                "could not read MPC_orbits, so tracks are not matched: {error}"
            ));
            return Vec::new();
        }
    };
    let known = designations(
        groups,
        detections,
        &orbits,
        &IdentifyConfig {
            match_radius_arcsec: params.identify_radius,
            ..IdentifyConfig::default()
        },
        &KnownRule {
            min_fraction: params.known_fraction,
            min_nights: params.known_min_nights,
            max_scatter_arcsec: params.known_max_scatter,
            max_drift_arcsec_per_day: params.known_max_drift,
        },
    );
    ctx.info(format!(
        "{} of {} tracks are cataloged objects, against {} orbits",
        known.iter().filter(|k| k.is_some()).count(),
        known.len(),
        orbits.len()
    ));
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

/// Write a dump into the export area, returning where it landed.
///
/// A failed write is reported and nothing else: the numbers are the point of a
/// tuning run, and losing the dump should not throw them away.
fn write_dump(
    ctx: &TaskContext,
    name: &str,
    dump: impl FnOnce(&str) -> Result<usize, Box<dyn std::error::Error>>,
) -> Option<String> {
    let dir = export_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        ctx.warn(format!("could not create the export directory: {e}"));
        return None;
    }
    let path = dir.join(name);
    let display = path.display().to_string();
    match dump(&path.to_string_lossy()) {
        Ok(n) => {
            ctx.info(format!("wrote {n} row(s) to {display}"));
            Some(display)
        }
        Err(e) => {
            ctx.warn(format!("could not write the dump: {e}"));
            None
        }
    }
}

/// What a persisting pass wrote, for the run's result.
fn persisted_json(ctx: &TaskContext, report: &PersistReport, dry_run: bool) -> serde_json::Value {
    if !report.ran {
        ctx.warn("another run is persisting tracks, so nothing was written".to_string());
    } else if report.interrupted {
        ctx.warn("the persisting pass stopped early; rerun to finish the night".to_string());
    }
    ctx.info(format!(
        "{} {} track(s) ({} unchanged), stamped {} alert(s), absorbed {} superseded id(s)",
        if dry_run { "would store" } else { "stored" },
        report.stored,
        report.unchanged,
        report.stamped,
        report.absorbed
    ));
    serde_json::json!({
        "dry_run": dry_run,
        "ran": report.ran,
        "complete": report.complete(),
        "stored": report.stored,
        "unchanged": report.unchanged,
        "stamped": report.stamped,
        "absorbed": report.absorbed,
        "failed": report.failed,
    })
}

/// Aborts its handle when it goes out of scope, so the cancel watcher below
/// does not outlive the run it is watching.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Turn the run's cancellation into a [`Stop`] the discovery passes honor.
///
/// A persisting pass holds the tracks lock and checks `stop` between tracks, so
/// a cancelled run ends after a whole track rather than mid-write and always
/// releases the lock. Without this a cancel would be noticed only once the pass
/// had finished every track it had.
fn watch_for_cancel(ctx: &TaskContext, stop: Arc<Stop>) -> tokio::task::JoinHandle<()> {
    let ctx = ctx.clone();
    tokio::spawn(async move {
        while !stop.requested() {
            if ctx.is_canceled() {
                stop.request();
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    })
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
            load_window(&db, jd_start, params.span, params.drb, params.known, region)
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

    // A persisting pass takes the tracks lock and checks this between tracks,
    // so a cancel lands between whole tracks rather than inside one.
    let stop = Arc::new(Stop::new());
    let watcher = watch_for_cancel(ctx, stop.clone());
    let _guard = AbortOnDrop(watcher);

    if params.thor {
        let result = run_thor(ctx, &params, &detections, &labels, &db, &stop).await;
        return if ctx.is_canceled() {
            Err(super::TaskError::Canceled)
        } else {
            Ok(result)
        };
    }
    // Identification reports through the log; it writes nothing, so there is no
    // persisting pass to summarize.
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

    let reference_jd = reference_epoch(&tracklets);
    let link_cfg = LinkConfig {
        hypotheses: default_hypotheses(),
        reference_jd,
        position_tol_au: params.position_tol,
        velocity_tol_au_per_day: params.velocity_tol,
        min_nights: params.min_nights,
        max_residual_arcsec: params.max_residual,
        site: crate::utils::sso_geometry::ZTF,
        conics: Conics::Bound,
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
            .map(|track| track_detections(track, &tracklets))
            .collect();
        match_known(ctx, &db, &groups, &detections, &params).await
    } else {
        Vec::new()
    };
    result["catalogued"] = serde_json::json!(known.iter().filter(|k| k.is_some()).count());

    if let Some(name) = &params.out_tracks {
        if let Some(path) = write_dump(ctx, name, |path| {
            dump_tracks(path, &tracks, &tracklets, &detections, &labels, &known)
        }) {
            result["written_to"] = serde_json::json!(path);
        }
    }

    if params.persist || params.dry_run {
        let report = persist_tracks(
            &db,
            &tracks,
            &tracklets,
            reference_jd,
            &detections,
            &labels,
            &known,
            &[],
            params.dry_run,
            params.min_detections,
            params.min_nights,
            &stop,
        )
        .await;
        result["persisted"] = persisted_json(ctx, &report, params.dry_run);
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
