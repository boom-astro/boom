//! The moving-object discovery pipeline, shared by `find_tracklets`, which runs
//! it once by hand, and `linker`, which runs it every night unattended.
//!
//! A run loads a window of ZTF detections, finds tracklets and links them
//! across nights or recovers objects tracklet-lessly with THOR, matches what it
//! finds to the MPC catalogue, and stores the result as tracks.

use crate::utils::heliolinc::{sky_track, test_orbits, State, Track};
use crate::utils::identify::{
    identify, track_designation, IdentifyConfig, KnownRule, Match, OrbitEntry,
};
use crate::utils::linking::{
    circular_mean_deg, find_tracklets, night_of, Detection, Tracklet, TrackletConfig,
};
use crate::utils::orbit_fit::{fit_within, Observation};
use crate::utils::sso_geometry::Site;
use crate::utils::thor;
use crate::utils::tracks::{
    acquire_lock, commit_upsert, plan_upsert, release_lock, stamp_members, BoundFit,
    SHARED_FOR_IDENTITY,
};
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use tracing::{error, info};

const ALERTS_COLLECTION: &str = "ZTF_alerts";

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("could not read alerts")]
    Database(#[from] mongodb::error::Error),
    #[error("an alert document is missing a field")]
    Document(#[from] mongodb::bson::document::ValueAccessError),
    #[error("could not build the search region: {0}")]
    Region(String),
    #[error("there are no alerts to search")]
    NoAlerts,
}

/// ZTF filter id as the single letter ADES wants.
pub fn ztf_band(fid: Option<i32>) -> Option<char> {
    match fid {
        Some(1) => Some('g'),
        Some(2) => Some('r'),
        Some(3) => Some('i'),
        _ => None,
    }
}

/// Detections for the window, with the `ssnamenr` label when there is one.
///
/// `known` selects the detections IPAC already matched to a solar system
/// object, which is how a search is scored; otherwise the unassociated ones,
/// where anything new would be. `region` restricts to a cone of (RA, Dec,
/// radius), degrees.
pub async fn load_window(
    db: &mongodb::Database,
    jd_start: f64,
    span: f64,
    drb: f64,
    known: bool,
    region: Option<(f64, f64, f64)>,
) -> Result<(Vec<Detection>, HashMap<i64, String>), DiscoveryError> {
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
        let moc =
            crate::utils::moc::moc_from_cone(ra, dec, radius).map_err(DiscoveryError::Region)?;
        let region_filter =
            crate::utils::moc::moc_hpx_filter(&moc).map_err(DiscoveryError::Region)?;
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
        .collection::<Document>(ALERTS_COLLECTION)
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
pub async fn latest_night(db: &mongodb::Database) -> Result<f64, DiscoveryError> {
    let doc = db
        .collection::<Document>(ALERTS_COLLECTION)
        .find_one(doc! {})
        .sort(doc! { "candidate.jd": -1 })
        .projection(doc! { "candidate.jd": 1 })
        .await?
        .ok_or(DiscoveryError::NoAlerts)?;
    let jd = doc.get_document("candidate")?.get_f64("jd")?;
    // Nights run across a JD boundary, so step back to the preceding noon.
    Ok(night_of(jd) as f64 + 0.5)
}

/// Tracklets found independently in each night the detections span.
pub fn tracklets_per_night(detections: &[Detection], cfg: &TrackletConfig) -> Vec<Tracklet> {
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

/// The middle of the span `tracklets` cover, where their states are compared.
pub fn reference_epoch(tracklets: &[Tracklet]) -> f64 {
    let (lo, hi) = tracklets.iter().fold((f64::MAX, f64::MIN), |(lo, hi), t| {
        (lo.min(t.jd_ref), hi.max(t.jd_ref))
    });
    (lo + hi) / 2.0
}

/// A bound-orbit verdict with the residual that produced it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Verdict(pub BoundFit, pub Option<f64>);

impl Verdict {
    pub fn residual(&self) -> Option<f64> {
        self.1
    }

    /// Sort key: a confident bound orbit first, then the ones worth a look.
    pub fn rank(&self) -> (u8, f64) {
        let order = match self.0 {
            BoundFit::Good => 0,
            BoundFit::Poor => 1,
            BoundFit::None => 2,
            BoundFit::Ungated => 3,
        };
        (order, self.1.unwrap_or(0.0))
    }

    pub fn label(&self) -> String {
        match (self.0, self.1) {
            (BoundFit::Good, Some(r)) => format!("{r:.2}\""),
            (BoundFit::Poor, Some(r)) => format!("{r:.2}\" poor"),
            (BoundFit::None, _) => "no bound orbit".to_string(),
            _ => "ungated".to_string(),
        }
    }
}

/// How a THOR search sweeps trial orbits and grades what it finds.
#[derive(Debug, Clone)]
pub struct ThorSearch {
    pub config: thor::Config,
    /// Heliocentric distances to place trial orbits at, au.
    pub distances_au: Vec<f64>,
    /// Largest sky residual a good bound orbit may leave, arcseconds.
    pub max_residual_arcsec: f64,
    /// Largest residual a cluster may leave and still be kept as a poor fit.
    pub max_unbound_residual_arcsec: f64,
    /// Where the astrometry was taken from.
    pub site: Site,
}

impl Default for ThorSearch {
    /// The search `find_tracklets --thor` runs with its default flags.
    fn default() -> Self {
        ThorSearch {
            config: thor::Config {
                min_detections: 2,
                // Two nights drop purity from 100% to 80%.
                min_nights: 3,
                ..thor::Config::default()
            },
            distances_au: vec![1.8, 2.2, 2.6, 3.0, 3.4],
            max_residual_arcsec: 2.0,
            max_unbound_residual_arcsec: 10.0,
            site: crate::utils::sso_geometry::ZTF,
        }
    }
}

/// Recover objects without tracklets, sweeping trial orbits over sky patches,
/// and keep the clusters one orbit reproduces.
///
/// A trial orbit only governs the detections near where it sits -- beyond a
/// couple of degrees the co-moving frame no longer applies -- so the sky is
/// divided into patches and each is searched with its own orbits. One orbit at
/// the centre of a whole night's coverage governs almost nothing.
pub fn thor_clusters(
    detections: &[Detection],
    search: &ThorSearch,
) -> Vec<(thor::Cluster, Verdict)> {
    let cfg = &search.config;
    if detections.is_empty() {
        return Vec::new();
    }
    let (lo, hi) = detections
        .iter()
        .fold((f64::MAX, f64::MIN), |(a, b), d| (a.min(d.jd), b.max(d.jd)));
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
        search.distances_au.len()
    );

    let started = std::time::Instant::now();
    let clusters: Vec<(thor::Cluster, State)> = patches
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
            for (state, _r) in test_orbits(ra0, dec0, epoch, &search.distances_au) {
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
                    thor::recover(patch, &track, cfg)
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
                &search.site,
                search.max_unbound_residual_arcsec,
            ) {
                None => Some((c, Verdict(BoundFit::None, None))),
                Some(fit) if fit.rms_arcsec <= search.max_residual_arcsec => {
                    Some((c, Verdict(BoundFit::Good, Some(fit.rms_arcsec))))
                }
                Some(fit) if fit.rms_arcsec <= search.max_unbound_residual_arcsec => {
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
    let mut claimed: HashSet<i64> = HashSet::new();
    let mut kept: Vec<(thor::Cluster, Verdict)> = Vec::new();
    for (c, r) in scored {
        // Sharing this many detections with something already kept makes the two
        // one track downstream, where the later one would extend the earlier and
        // overwrite its verdict. Best-ranked first, so the one dropped is worse.
        let shared = c.ids.iter().filter(|id| claimed.contains(id)).count();
        if shared >= SHARED_FOR_IDENTITY {
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
    kept
}

/// The catalogued object each group of detections is, if any, in the order of
/// `groups`.
///
/// Every detection in a group is matched against the catalogue, and the group
/// takes a designation only under `rule`: a single detection near some
/// catalogued orbit is too often chance.
pub fn designations(
    groups: &[Vec<i64>],
    detections: &[Detection],
    orbits: &[OrbitEntry],
    identify_cfg: &IdentifyConfig,
    rule: &KnownRule,
) -> Vec<Option<String>> {
    let wanted: HashSet<i64> = groups.iter().flatten().copied().collect();
    let mut members: Vec<Detection> = detections
        .iter()
        .filter(|d| wanted.contains(&d.id))
        .copied()
        .collect();
    members.sort_by_key(|d| d.id);
    let matches = identify(&members, orbits, identify_cfg);
    let by_detection: HashMap<i64, &Match> = matches.iter().map(|m| (m.detection_id, m)).collect();
    groups
        .iter()
        .map(|group| {
            let found: Vec<&Match> = group
                .iter()
                .filter_map(|id| by_detection.get(id).copied())
                .collect();
            track_designation(&found, group.len(), rule)
        })
        .collect()
}

/// The detections a linked track is made of, ascending.
pub fn track_detections(track: &Track, tracklets: &[Tracklet]) -> Vec<i64> {
    let mut ids: Vec<i64> = track
        .members
        .iter()
        .flat_map(|&m| tracklets[m].ids.iter().copied())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// What a persisting pass wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct PersistReport {
    /// Whether the pass held the tracks lock, or was a dry run. A pass that
    /// found another one writing stores nothing.
    pub ran: bool,
    pub stored: usize,
    pub stamped: u64,
    /// Stored ids absorbed by merges.
    pub absorbed: usize,
}

/// Store each track under a durable id and stamp it onto its member alerts.
///
/// One track at a time rather than in bulk: identity is decided against what is
/// already stored, so two tracks of the same object in one run must see each
/// other's writes. `known` is parallel to `tracks`, or empty when tracks were
/// not matched to the catalogue.
#[allow(clippy::too_many_arguments)]
pub async fn persist_tracks(
    db: &mongodb::Database,
    tracks: &[Track],
    tracklets: &[Tracklet],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    known: &[Option<String>],
    dry_run: bool,
    min_detections: usize,
    min_nights: usize,
) -> PersistReport {
    let mut report = PersistReport::default();
    if !dry_run {
        match acquire_lock(db).await {
            Ok(true) => {}
            Ok(false) => {
                error!("another run is persisting tracks, not writing");
                return report;
            }
            Err(e) => {
                error!("could not take the tracks lock: {}", e);
                return report;
            }
        }
    }
    report.ran = true;
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    for (i, track) in tracks.iter().enumerate() {
        let members = track_detections(track, tracklets);
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
            report.stored += 1;
            report.absorbed += plan.superseded.len();
            report.stamped += plan.members.len() as u64;
            continue;
        }
        let superseded = plan.superseded.clone();
        match commit_upsert(db, plan).await {
            Ok(up) => {
                report.stored += 1;
                report.absorbed += superseded.len();
                if !superseded.is_empty() {
                    info!("track {} absorbed {}", up.track.id, superseded.join(", "));
                }
                match stamp_members(db, &up.track).await {
                    Ok(n) => report.stamped += n,
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
            report.stored, report.stamped, report.absorbed
        );
    } else {
        info!(
            "stored {} tracks, stamped {} alerts, absorbed {} superseded ids",
            report.stored, report.stamped, report.absorbed
        );
    }
    report
}

/// Store each THOR cluster the same way a linked track is stored.
///
/// The bound-fit verdict goes with it: a cluster no bound orbit reproduces is
/// the interesting one, and persisting it as though it were clean would lose
/// exactly what makes it worth looking at. `known` is parallel to `clusters`,
/// or empty when clusters were not matched to the catalogue.
#[allow(clippy::too_many_arguments)]
pub async fn persist_clusters(
    db: &mongodb::Database,
    clusters: &[(thor::Cluster, Verdict)],
    detections: &[Detection],
    labels: &HashMap<i64, String>,
    known: &[Option<String>],
    dry_run: bool,
    min_detections: usize,
    min_nights: usize,
) -> PersistReport {
    let mut report = PersistReport::default();
    if !dry_run {
        match acquire_lock(db).await {
            Ok(true) => {}
            Ok(false) => {
                error!("another run is persisting tracks, not writing");
                return report;
            }
            Err(e) => {
                error!("could not take the tracks lock: {}", e);
                return report;
            }
        }
    }
    report.ran = true;
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
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
            report.stored += 1;
            report.stamped += plan.members.len() as u64;
            continue;
        }
        match commit_upsert(db, plan).await {
            Ok(up) => {
                report.stored += 1;
                match stamp_members(db, &up.track).await {
                    Ok(n) => report.stamped += n,
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
        what, report.stored, report.stamped
    );
    report
}

/// How much of what a search could have found it did find, against labels.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Recall {
    /// Labelled objects with tracklets on at least two nights: what linking
    /// can reach.
    pub linkable: usize,
    /// Of those, the ones a track drawn from that object alone recovered.
    pub recovered: usize,
    /// Tracks whose detections belong to one labelled object.
    pub pure_tracks: usize,
    /// Tracks drawing on more than one labelled object.
    pub mixed_tracks: usize,
}

impl Recall {
    /// Recovered over linkable, as a fraction; zero when nothing was linkable.
    pub fn fraction(&self) -> f64 {
        if self.linkable == 0 {
            0.0
        } else {
            self.recovered as f64 / self.linkable as f64
        }
    }
}

/// Score linked `tracks` against the `labels` their detections carry.
pub fn recall(tracks: &[Track], tracklets: &[Tracklet], labels: &HashMap<i64, String>) -> Recall {
    let label_of = |t: &Tracklet| t.ids.iter().find_map(|id| labels.get(id));
    let mut nights: HashMap<&String, HashSet<i64>> = HashMap::new();
    for t in tracklets {
        if let Some(name) = label_of(t) {
            nights.entry(name).or_default().insert(night_of(t.jd_ref));
        }
    }
    let linkable: HashSet<&String> = nights
        .into_iter()
        .filter(|(_, n)| n.len() >= 2)
        .map(|(name, _)| name)
        .collect();

    let mut report = Recall {
        linkable: linkable.len(),
        ..Recall::default()
    };
    let mut recovered: HashSet<&String> = HashSet::new();
    for track in tracks {
        let names: HashSet<&String> = track
            .members
            .iter()
            .filter_map(|&m| label_of(&tracklets[m]))
            .collect();
        match names.len() {
            0 => {}
            1 => {
                report.pure_tracks += 1;
                recovered.extend(names.into_iter().filter(|n| linkable.contains(n)));
            }
            _ => report.mixed_tracks += 1,
        }
    }
    report.recovered = recovered.len();
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::heliolinc::Hypothesis;

    fn tracklet(ids: &[i64], jd: f64) -> Tracklet {
        Tracklet::from_motion(ids.to_vec(), jd, 10.0, 5.0, 0.2, 0.0, 0.1)
    }

    fn track(members: &[usize]) -> Track {
        Track {
            members: members.to_vec(),
            hypothesis: Hypothesis {
                r_au: 2.5,
                rdot_au_per_day: 0.0,
            },
            state: State {
                pos: [2.5, 0.0, 0.0],
                vel: [0.0, 0.01, 0.0],
            },
            nights: 2,
            rms_au: 0.0,
            residual_arcsec: Some(0.5),
        }
    }

    /// Recall counts an object linkable once its tracklets span two nights,
    /// and recovered only when a track holds that object alone.
    #[test]
    fn test_recall_counts_linkable_and_recovered_objects() {
        let tracklets = vec![
            tracklet(&[1, 2], 2460010.8),
            tracklet(&[3, 4], 2460012.8),
            tracklet(&[5, 6], 2460010.8),
            tracklet(&[7, 8], 2460012.8),
            tracklet(&[9, 10], 2460010.8),
        ];
        let labels: HashMap<i64, String> = [(1, "A"), (3, "A"), (5, "B"), (7, "B"), (9, "C")]
            .into_iter()
            .map(|(id, name)| (id, name.to_string()))
            .collect();
        // A found alone, B only mixed in with A, C seen one night only.
        let tracks = vec![track(&[0, 1]), track(&[1, 2])];
        let r = recall(&tracks, &tracklets, &labels);
        assert_eq!(r.linkable, 2, "A and B span two nights, C does not");
        assert_eq!(r.recovered, 1, "only A has a track of its own");
        assert_eq!(r.pure_tracks, 1);
        assert_eq!(r.mixed_tracks, 1);
        assert!((r.fraction() - 0.5).abs() < 1e-12);
    }

    #[test]
    fn test_reference_epoch_is_the_middle_of_the_span() {
        let tracklets = vec![tracklet(&[1], 2460010.0), tracklet(&[2], 2460014.0)];
        assert_eq!(reference_epoch(&tracklets), 2460012.0);
    }

    #[test]
    fn test_track_detections_are_ascending_and_distinct() {
        let tracklets = vec![tracklet(&[5, 3], 2460010.8), tracklet(&[3, 9], 2460012.8)];
        assert_eq!(track_detections(&track(&[0, 1]), &tracklets), vec![3, 5, 9]);
    }
}
