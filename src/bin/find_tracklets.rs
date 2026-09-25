//! Run intra-night tracklet finding over one night of ZTF alerts.
//!
//! Two modes. `--known` links the detections IPAC already matched to a solar
//! system object and scores the result against those labels, which is how the
//! thresholds get calibrated. The default links the unassociated detections,
//! where anything new would be.

use boom::conf::{load_dotenv, AppConfig};
use boom::utils::heliolinc::{default_hypotheses, link_tracklets, LinkConfig, Track};
use boom::utils::linking::{
    circular_mean_deg, find_tracklets, night_of, Detection, Tracklet, TrackletConfig,
};
use boom::utils::orbit_fit::{fit_orbit, Observation};
use clap::Parser;
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use rayon::prelude::*;
use std::collections::HashMap;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(about = "Find intra-night tracklets in one night of ZTF alerts")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Start of the night, JD. Defaults to the most recent night with data.
    #[arg(long)]
    jd_start: Option<f64>,

    /// Length of the window, days.
    #[arg(long, default_value_t = 0.5)]
    span: f64,

    /// Link the known solar system detections and score against `ssnamenr`.
    #[arg(long, default_value_t = false)]
    known: bool,

    /// Minimum drb for a detection to be considered.
    #[arg(long, default_value_t = 0.8)]
    drb: f64,

    /// Restrict the search to a cone, degrees. All three are needed together;
    /// the region is tested by HEALPix range, which the
    /// `{coordinates.hpx, candidate.jd}` index serves.
    #[arg(long, requires_all = ["dec", "radius"])]
    ra: Option<f64>,
    #[arg(long, requires_all = ["ra", "radius"])]
    dec: Option<f64>,
    #[arg(long, requires_all = ["ra", "dec"])]
    radius: Option<f64>,

    /// Detections per tracklet. Two is the useful floor: ZTF's nominal cadence
    /// is two visits to a field per night.
    #[arg(long, default_value_t = 2)]
    min_detections: usize,

    /// Reject a pair whose magnitudes disagree by more than this many combined
    /// sigma. 0 disables the test.
    #[arg(long, default_value_t = 5.0)]
    max_mag_sigma: f64,

    /// Fastest apparent motion a tracklet may have, degrees per day.
    #[arg(long, default_value_t = 1.0)]
    max_rate: f64,

    /// Shortest on-sky arc a pair may span, arcseconds.
    #[arg(long, default_value_t = 10.0)]
    min_arc: f64,

    /// Shortest time between two detections of a pair, days.
    #[arg(long, default_value_t = 0.1 / 24.0)]
    min_pair_dt: f64,

    /// Longest a tracklet may span, days. Separate from `--span`, which is how
    /// much data to read: widening it widens the pair search radius. Pass
    /// 0.0625 to match heliolinx's 1.5 hour default when comparing against it.
    #[arg(long, default_value_t = 3.0 / 24.0)]
    max_tracklet_span: f64,

    /// Distinct nights a track must appear on.
    #[arg(long, default_value_t = 2)]
    min_nights: usize,

    /// Largest sky residual a fitted orbit may leave, arcseconds.
    #[arg(long, default_value_t = 2.0)]
    max_residual: f64,

    /// Write the linked tracks here as JSON, one object per line.
    #[arg(long, value_name = "FILE")]
    out_tracks: Option<String>,

    /// Store the tracks and stamp each onto its member alerts, so a filter can
    /// match on them. Needs the database even when reading a dump.
    #[arg(long, default_value_t = false)]
    persist: bool,

    /// Report what `--persist` would write without writing it. Resolves each
    /// track against the stored ones, so the identity it reports is the one a
    /// real run would use.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Recover objects tracklet-lessly, in the manner of THOR, instead of
    /// linking tracklets. Reaches objects detected only once a night.
    #[arg(long, default_value_t = false)]
    thor: bool,

    /// Heliocentric distances to place trial orbits at, au.
    #[arg(long, value_delimiter = ',', default_value = "1.8,2.2,2.6,3.0,3.4")]
    thor_distances: Vec<f64>,

    /// Distinct nights a THOR cluster must appear on. Two drops purity to 80%.
    #[arg(long, default_value_t = 3)]
    thor_min_nights: usize,

    /// THOR cluster cell size, arcseconds. Defaults to the library value.
    #[arg(long)]
    cluster_radius: Option<f64>,

    /// Largest scatter a THOR cluster may have about its refitted drift,
    /// arcseconds. Defaults to the library value.
    #[arg(long)]
    max_cluster_rms: Option<f64>,

    /// Largest residual rate THOR searches, degrees/day. Bounds how far a trial
    /// orbit may be from the truth, so a distant or unbound object needs more
    /// than the bound-orbit default. Defaults to the library value.
    #[arg(long)]
    max_residual_rate: Option<f64>,

    /// Rate grid steps per axis. Widening the range without raising this
    /// coarsens the grid. Defaults to the library value.
    #[arg(long)]
    rate_steps: Option<usize>,

    /// Keep a THOR cluster whose best bound orbit leaves up to this residual,
    /// arcseconds, reported as a poor fit. Only a bound orbit can be fitted, so
    /// a distant or unbound object lands here rather than under --max-residual.
    #[arg(long, default_value_t = 10.0)]
    max_unbound_residual: f64,

    /// Attribute detections to catalogued objects and score against `ssnamenr`.
    #[arg(long, default_value_t = false)]
    identify: bool,

    /// Radius a refined prediction must fall inside to count, arcseconds.
    #[arg(long, default_value_t = 120.0)]
    identify_radius: f64,

    /// Report at most this many tracklets.
    #[arg(long, default_value_t = 20)]
    show: usize,

    /// Read detections from a JSONL dump instead of the database.
    #[arg(long, value_name = "FILE")]
    input: Option<String>,

    /// Find tracklets per night, then link them across nights.
    #[arg(long, default_value_t = false)]
    link: bool,

    /// Position agreement required to cluster propagated states, au.
    #[arg(long, default_value_t = 0.002)]
    position_tol: f64,

    /// Velocity agreement required to cluster propagated states, au/day.
    #[arg(long, default_value_t = 0.0004)]
    velocity_tol: f64,
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
    tracks: &[boom::utils::heliolinc::Track],
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

/// Detections from a JSONL dump, with labels where the rows carry them.
fn load_file(
    path: &str,
) -> Result<(Vec<Detection>, HashMap<i64, String>), Box<dyn std::error::Error>> {
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
        let moc = boom::utils::moc::moc_from_cone(ra, dec, radius)?;
        let region_filter = boom::utils::moc::moc_hpx_filter(&moc)?;
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
fn dump_tracks(
    path: &str,
    tracks: &[Track],
    tracklets: &[Tracklet],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
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
            "epochs": epochs,
        });
        writeln!(file, "{line}")?;
    }
    file.flush()?;
    Ok(tracks.len())
}

/// How well a bound orbit reproduces a cluster.
///
/// Propagation is Keplerian and bound-only -- `state_to_elements` refuses a
/// non-negative energy -- so a hyperbolic object cannot fit however clean its
/// astrometry. `Poor` and `Unbound` are where such an object shows up, which is
/// why neither is discarded.
#[derive(Debug, Clone, Copy, PartialEq)]
enum BoundFit {
    /// Two points cannot constrain six parameters.
    Ungated,
    Good(f64),
    Poor(f64),
    /// No bound orbit exists for the fitted state.
    Unbound,
}

impl BoundFit {
    fn residual(&self) -> Option<f64> {
        match self {
            BoundFit::Good(r) | BoundFit::Poor(r) => Some(*r),
            _ => None,
        }
    }

    /// Sort key: a confident bound orbit first, then the ones worth a look.
    fn rank(&self) -> (u8, f64) {
        match self {
            BoundFit::Good(r) => (0, *r),
            BoundFit::Poor(r) => (1, *r),
            BoundFit::Unbound => (2, 0.0),
            BoundFit::Ungated => (3, 0.0),
        }
    }

    fn label(&self) -> String {
        match self {
            BoundFit::Good(r) => format!("{r:.2}\""),
            BoundFit::Poor(r) => format!("{r:.2}\" poor"),
            BoundFit::Unbound => "no bound orbit".to_string(),
            BoundFit::Ungated => "ungated".to_string(),
        }
    }
}

/// Store each track under a durable id and stamp it onto its member alerts.
///
/// One track at a time rather than in bulk: identity is decided against what is
/// already stored, so two tracks of the same object in one run must see each
/// other's writes.
async fn persist_tracks(
    db: &mongodb::Database,
    tracks: &[Track],
    tracklets: &[Tracklet],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    dry_run: bool,
) {
    use boom::utils::tracks::{commit_upsert, plan_upsert, stamp_members};
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    let (mut stored, mut stamped, mut merged) = (0usize, 0u64, 0usize);
    for track in tracks {
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
        // A track of a known object records the designation, which is what tells
        // a consumer this is a recovery rather than a discovery candidate.
        let designation = members.iter().find_map(|id| labels.get(id)).cloned();
        let plan = match plan_upsert(db, &members, &jds, designation).await {
            Ok(plan) => plan,
            Err(e) => {
                error!("could not resolve a track: {}", e);
                continue;
            }
        };
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
fn dump_clusters(
    path: &str,
    clusters: &[(boom::utils::thor::Cluster, BoundFit)],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
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
            "bound_fit": match residual {
                BoundFit::Good(_) => "good",
                BoundFit::Poor(_) => "poor",
                BoundFit::Unbound => "none",
                BoundFit::Ungated => "ungated",
            },
            "cluster_rms_arcsec": cluster.rms_arcsec,
            "rate_x_deg_per_day": cluster.rate_x_deg_per_day,
            "rate_y_deg_per_day": cluster.rate_y_deg_per_day,
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
fn run_thor(args: &Cli, detections: &[Detection], labels: &HashMap<i64, String>) {
    use boom::utils::heliolinc::{sky_track, test_orbits};
    use boom::utils::thor;
    use rayon::prelude::*;

    let mut cfg = thor::Config {
        min_detections: args.min_detections.max(2),
        min_nights: args.thor_min_nights,
        ..thor::Config::default()
    };
    if let Some(v) = args.cluster_radius {
        cfg.cluster_radius_arcsec = v;
    }
    if let Some(v) = args.max_cluster_rms {
        cfg.max_rms_arcsec = v;
    }
    if let Some(v) = args.max_residual_rate {
        cfg.max_residual_rate_deg_per_day = v;
    }
    if let Some(v) = args.rate_steps {
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
        args.thor_distances.len()
    );

    let started = std::time::Instant::now();
    let clusters: Vec<(thor::Cluster, boom::utils::heliolinc::State)> = patches
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
            for (state, _r) in test_orbits(ra0, dec0, epoch, &args.thor_distances) {
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
    let mut scored: Vec<(thor::Cluster, BoundFit)> = clusters
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
                return Some((c, BoundFit::Ungated));
            }
            match fit_orbit(&obs, &seed, epoch, 20, &boom::utils::sso_geometry::ZTF) {
                None => Some((c, BoundFit::Unbound)),
                Some(fit) if fit.rms_arcsec <= args.max_residual => {
                    Some((c, BoundFit::Good(fit.rms_arcsec)))
                }
                Some(fit) if fit.rms_arcsec <= args.max_unbound_residual => {
                    Some((c, BoundFit::Poor(fit.rms_arcsec)))
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
    let mut kept: Vec<(thor::Cluster, BoundFit)> = Vec::new();
    for (c, r) in scored {
        if c.ids.iter().all(|id| claimed.contains(id)) {
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

    if let Some(path) = &args.out_tracks {
        match dump_clusters(path, &kept, detections, labels) {
            Ok(n) => info!("wrote {} clusters to {}", n, path),
            Err(e) => error!("could not write clusters: {}", e),
        }
    }

    for (c, resid) in kept.iter().take(args.show) {
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
    let (mut pair_pure, mut pair_mixed) = (0usize, 0usize);
    let mut recovered = std::collections::HashSet::new();
    for (c, resid) in &kept {
        let names: std::collections::HashSet<&String> =
            c.ids.iter().filter_map(|id| labels.get(id)).collect();
        let gated = resid.residual().is_some();
        match names.len() {
            0 => {}
            1 => {
                recovered.insert((*names.iter().next().unwrap()).clone());
                if gated {
                    pure += 1;
                } else {
                    pair_pure += 1;
                }
            }
            _ => {
                if gated {
                    mixed += 1;
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

/// Attribute detections to catalogued objects, and score against `ssnamenr`.
///
/// Every detection here already carries IPAC's identification, so the
/// catalogue's answer can be checked directly: agreement measures whether
/// propagating MPCORB to the detection epoch lands where the object was.
async fn run_identify(args: &Cli, detections: &[Detection], labels: &HashMap<i64, String>) {
    use boom::utils::identify::{identify, IdentifyConfig, OrbitEntry};
    use boom::utils::mpcorb::{elements_from_document, normalize_ztf_ssnamenr, ORBITS_COLLECTION};
    use futures::TryStreamExt;

    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| "config.yaml".to_string());
    let config = AppConfig::from_path(&config_path).expect("failed to load config");
    let db = config.build_db().await.expect("failed to connect to mongo");

    let started = std::time::Instant::now();
    let mut cursor = db
        .collection::<Document>(ORBITS_COLLECTION)
        .find(doc! {})
        .await
        .expect("failed to read MPC_orbits");
    let mut orbits: Vec<OrbitEntry> = Vec::new();
    let mut epochs: Vec<f64> = Vec::new();
    // A read failure part-way through leaves a truncated catalogue, which would
    // silently score as a lower recall rather than as a failure.
    loop {
        let d = match cursor.try_next().await {
            Ok(Some(d)) => d,
            Ok(None) => break,
            Err(error) => {
                error!(%error, "reading MPC_orbits failed after {} orbits", orbits.len());
                return;
            }
        };
        let Ok(designation) = d.get_str("_id") else {
            continue;
        };
        let Some(elements) = elements_from_document(&d) else {
            continue;
        };
        epochs.push(elements.epoch_jd);
        orbits.push(OrbitEntry {
            designation: designation.to_string(),
            elements,
        });
    }
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
        match_radius_arcsec: args.identify_radius,
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

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    load_dotenv();

    let args = Cli::parse();
    // Persisting needs the database even when the detections came from a dump.
    let db = if args.input.is_none() || args.persist || args.dry_run {
        let config_path = args
            .config
            .clone()
            .unwrap_or_else(|| "config.yaml".to_string());
        let config = AppConfig::from_path(&config_path).expect("failed to load config");
        Some(config.build_db().await.expect("failed to connect to mongo"))
    } else {
        None
    };
    let (detections, labels) = match &args.input {
        Some(path) => load_file(path).expect("failed to read dump"),
        None => {
            let db = db.as_ref().expect("built when there is no dump");
            let jd_start = match args.jd_start {
                Some(jd) => jd,
                None => latest_night(db).await.expect("failed to find a night"),
            };
            info!(
                jd_start,
                span = args.span,
                known = args.known,
                "loading detections"
            );
            let region = match (args.ra, args.dec, args.radius) {
                (Some(ra), Some(dec), Some(radius)) => Some((ra, dec, radius)),
                _ => None,
            };
            load(db, jd_start, args.span, args.drb, args.known, region)
                .await
                .expect("failed to load detections")
        }
    };
    info!(
        "{} detections, {} carrying an ssnamenr label",
        detections.len(),
        labels.len()
    );
    if detections.is_empty() {
        return;
    }

    let cfg = TrackletConfig {
        min_detections: args.min_detections,
        max_span_days: args.max_tracklet_span,
        max_rate_deg_per_day: args.max_rate,
        min_arc_arcsec: args.min_arc,
        min_pair_dt_days: args.min_pair_dt,
        max_mag_sigma: (args.max_mag_sigma > 0.0).then_some(args.max_mag_sigma),
        ..TrackletConfig::default()
    };
    if args.thor {
        run_thor(&args, &detections, &labels);
        return;
    }

    if args.identify {
        run_identify(&args, &detections, &labels).await;
        return;
    }

    let started = std::time::Instant::now();
    let tracklets = if args.link {
        tracklets_per_night(&detections, &cfg)
    } else {
        find_tracklets(&detections, &cfg)
    };
    info!(
        "{} tracklets from {} detections in {:.1}s",
        tracklets.len(),
        detections.len(),
        started.elapsed().as_secs_f64()
    );

    if args.known {
        let (pure, mixed, unlabelled) = score(&tracklets, &labels);
        let distinct: std::collections::HashSet<&String> = labels.values().collect();
        info!(
            "against ssnamenr: {} pure, {} mixed, {} unlabelled; {} distinct objects present",
            pure,
            mixed,
            unlabelled,
            distinct.len()
        );
        let linked: std::collections::HashSet<&str> = tracklets
            .iter()
            .flat_map(|t| t.ids.iter())
            .filter_map(|id| labels.get(id).map(|s| s.as_str()))
            .collect();
        info!(
            "{} of {} objects appear in at least one tracklet",
            linked.len(),
            distinct.len()
        );
    }

    if args.link {
        let jds: Vec<f64> = tracklets.iter().map(|t| t.jd_ref).collect();
        let reference_jd = (jds.iter().cloned().fold(f64::MAX, f64::min)
            + jds.iter().cloned().fold(f64::MIN, f64::max))
            / 2.0;
        let link_cfg = LinkConfig {
            hypotheses: default_hypotheses(),
            reference_jd,
            position_tol_au: args.position_tol,
            velocity_tol_au_per_day: args.velocity_tol,
            min_nights: args.min_nights,
            max_residual_arcsec: args.max_residual,
            site: boom::utils::sso_geometry::ZTF,
        };
        let started = std::time::Instant::now();
        let tracks = link_tracklets(&tracklets, &detections, &link_cfg);
        info!(
            "{} tracks from {} tracklets over {} hypotheses in {:.1}s",
            tracks.len(),
            tracklets.len(),
            link_cfg.hypotheses.len(),
            started.elapsed().as_secs_f64()
        );
        if !labels.is_empty() {
            let (pure, mixed, recovered) = score_tracks(&tracks, &tracklets, &labels);
            info!(
                "tracks: {} pure, {} mixed, {} distinct objects recovered",
                pure, mixed, recovered
            );
        }
        if let Some(path) = &args.out_tracks {
            match dump_tracks(path, &tracks, &tracklets, &detections, &labels) {
                Ok(n) => info!("wrote {} tracks to {}", n, path),
                Err(e) => error!("could not write tracks: {}", e),
            }
        }
        if args.persist || args.dry_run {
            let db = db.as_ref().expect("built when persisting");
            persist_tracks(db, &tracks, &tracklets, &detections, &labels, args.dry_run).await;
        }

        for track in tracks.iter().take(args.show) {
            let name = track
                .members
                .iter()
                .find_map(|&m| tracklets[m].ids.iter().find_map(|id| labels.get(id)))
                .map(|s| s.as_str())
                .unwrap_or("-");
            info!(
                "track n={} nights={} r={:.2} au rdot={:+.5} label={}",
                track.members.len(),
                track.nights,
                track.hypothesis.r_au,
                track.hypothesis.rdot_au_per_day,
                name
            );
        }
        return;
    }

    for t in tracklets.iter().take(args.show) {
        let name = t
            .ids
            .iter()
            .find_map(|id| labels.get(id))
            .map(|s| s.as_str())
            .unwrap_or("-");
        info!(
            "n={} rate={:.4} deg/d pa_ra={:.4} pa_dec={:.4} rms={:.2}\" ra={:.5} dec={:.5} label={}",
            t.ids.len(),
            t.rate_deg_per_day(),
            t.ra_rate_deg_per_day,
            t.dec_rate_deg_per_day,
            t.rms_arcsec,
            t.ra_ref,
            t.dec_ref,
            name
        );
    }
}
