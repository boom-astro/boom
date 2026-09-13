//! Run intra-night tracklet finding over one night of ZTF alerts.
//!
//! Two modes. `--known` links the detections IPAC already matched to a solar
//! system object and scores the result against those labels, which is how the
//! thresholds get calibrated. The default links the unassociated detections,
//! where anything new would be.

use boom::conf::{load_dotenv, AppConfig};
use boom::utils::heliolinc::{link_tracklets, main_belt_hypotheses, LinkConfig};
use boom::utils::linking::{find_tracklets, Detection, Tracklet, TrackletConfig};
use clap::Parser;
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, Level};
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

    /// Detections per tracklet.
    #[arg(long, default_value_t = 3)]
    min_detections: usize,

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
        by_night
            .entry((d.jd - 0.5).floor() as i64)
            .or_default()
            .push(*d);
    }
    let mut nights: Vec<_> = by_night.into_iter().collect();
    nights.sort_by_key(|(n, _)| *n);
    let mut out = Vec::new();
    for (night, dets) in nights {
        let found = find_tracklets(&dets, cfg);
        info!(
            "night {}: {} detections -> {} tracklets",
            night,
            dets.len(),
            found.len()
        );
        out.extend(found);
    }
    out
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

    let projection = doc! {
        "_id": 1,
        "candidate.jd": 1,
        "candidate.ra": 1,
        "candidate.dec": 1,
        "candidate.ssnamenr": 1,
        "candidate.magpsf": 1,
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
    Ok((jd - 0.5).floor() + 0.5)
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

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    load_dotenv();

    let args = Cli::parse();
    let (detections, labels) = match &args.input {
        Some(path) => load_file(path).expect("failed to read dump"),
        None => {
            let config_path = args
                .config
                .clone()
                .unwrap_or_else(|| "config.yaml".to_string());
            let config = AppConfig::from_path(&config_path).expect("failed to load config");
            let db = config.build_db().await.expect("failed to connect to mongo");
            let jd_start = match args.jd_start {
                Some(jd) => jd,
                None => latest_night(&db).await.expect("failed to find a night"),
            };
            info!(
                jd_start,
                span = args.span,
                known = args.known,
                "loading detections"
            );
            load(&db, jd_start, args.span, args.drb, args.known)
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
        max_span_days: args.span,
        ..TrackletConfig::default()
    };
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
            hypotheses: main_belt_hypotheses(),
            reference_jd,
            position_tol_au: args.position_tol,
            velocity_tol_au_per_day: args.velocity_tol,
            min_nights: 2,
        };
        let started = std::time::Instant::now();
        let tracks = link_tracklets(&tracklets, &link_cfg);
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
