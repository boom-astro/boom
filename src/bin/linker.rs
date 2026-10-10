//! Run moving-object discovery once each ZTF night is over.
//!
//! For the night just finished, the linker reads the last
//! `linker.window_nights` nights of unassociated detections, links tracklets
//! across nights, recovers objects seen once a night with THOR over the most
//! recent `linker.thor_window_nights`, matches what it finds to the MPC
//! catalog so a known object is stored as a recovery, and stores the tracks.
//! Nothing is sent anywhere else: this is the loop that shows how fast and how
//! complete the search is before anything depends on it.
//!
//! It then links the detections IPAC already identified over the most recent
//! `linker.recall_window_nights`, stores nothing, and logs how many of those
//! objects the search recovered. That recall says whether a change to the
//! search misses things.
//!
//! The last night it finished is recorded in the database, so a restart does
//! not search it again, and a night it could not finish is retried.

use boom::conf::{load_dotenv, AppConfig, LinkerConfig};
use boom::utils::discovery::{
    designations, load_window, persist_clusters, persist_tracks, recall, reference_epoch,
    thor_clusters, track_detections, tracklets_per_night, ThorSearch,
};
use boom::utils::heliolinc::{link_tracklets, LinkConfig, Track};
use boom::utils::identify::{IdentifyConfig, KnownRule, OrbitEntry};
use boom::utils::linking::{night_of, Detection, Tracklet, TrackletConfig};
use boom::utils::mpcorb::load_catalogue;
use boom::utils::o11y::logging::build_subscriber;
use boom::utils::tracks::COUNTERS_COLLECTION;
use clap::Parser;
use mongodb::bson::{doc, Document};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{error, info, warn};

/// Where the last finished night is kept, in `COUNTERS_COLLECTION`.
const LAST_NIGHT_ID: &str = "linker_last_night";

/// How long to wait before trying an unfinished night again.
const RETRY_AFTER: Duration = Duration::from_secs(30 * 60);

#[derive(Parser)]
#[command(about = "Run moving-object discovery once each ZTF night is over")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Search the most recently finished night, then exit.
    #[arg(long, default_value_t = false)]
    once: bool,

    /// Search the window ending with this night, then exit. A night is
    /// numbered as `night_of` numbers it: the integer JD at noon UTC before
    /// the UTC day it covers.
    #[arg(long)]
    night: Option<i64>,
}

/// The current Julian date.
fn now_jd() -> f64 {
    flare::Time::now().to_jd()
}

/// The last night that is over at `jd`: the latest UTC day whose
/// `run_after_utc_hour` has passed.
fn last_finished_night(jd: f64, run_after_utc_hour: u32) -> i64 {
    night_of(jd - run_after_utc_hour as f64 / 24.0)
}

/// When `night` counts as over, JD.
fn finished_at(night: i64, run_after_utc_hour: u32) -> f64 {
    night as f64 + 0.5 + run_after_utc_hour as f64 / 24.0
}

/// The `nights` nights ending with `night`: the window's first JD and its
/// length in days.
fn window(night: i64, nights: u32) -> (f64, f64) {
    let nights = nights.max(1);
    ((night - nights as i64 + 1) as f64 + 0.5, nights as f64)
}

/// Tracklets per night, linked across nights with the default search.
fn link(detections: &[Detection]) -> (Vec<Tracklet>, Vec<Track>) {
    let tracklets = tracklets_per_night(detections, &TrackletConfig::default());
    if tracklets.is_empty() {
        return (tracklets, Vec::new());
    }
    let cfg = LinkConfig {
        reference_jd: reference_epoch(&tracklets),
        ..LinkConfig::default()
    };
    let tracks = link_tracklets(&tracklets, detections, &cfg);
    (tracklets, tracks)
}

/// The designation of each group, or none at all without a catalog.
fn known_objects(
    groups: &[Vec<i64>],
    detections: &[Detection],
    catalog: Option<&[OrbitEntry]>,
) -> Vec<Option<String>> {
    match catalog {
        Some(orbits) => designations(
            groups,
            detections,
            orbits,
            &IdentifyConfig::default(),
            &KnownRule::default(),
        ),
        None => Vec::new(),
    }
}

/// The last night a run finished, if any.
async fn last_recorded_night(db: &mongodb::Database) -> Option<i64> {
    match db
        .collection::<Document>(COUNTERS_COLLECTION)
        .find_one(doc! { "_id": LAST_NIGHT_ID })
        .await
    {
        Ok(found) => found.and_then(|d| d.get_i64("night").ok()),
        Err(error) => {
            error!(%error, "could not read the last finished night");
            None
        }
    }
}

async fn record_night(db: &mongodb::Database, night: i64) {
    if let Err(error) = db
        .collection::<Document>(COUNTERS_COLLECTION)
        .replace_one(
            doc! { "_id": LAST_NIGHT_ID },
            doc! { "_id": LAST_NIGHT_ID, "night": night },
        )
        .upsert(true)
        .await
    {
        error!(%error, night, "could not record the finished night");
    }
}

/// Search the window ending with `night`, store what it finds, and report the
/// recall on objects IPAC identified.
///
/// True when the night is done: its window loaded, and every pass that stores
/// tracks stored them. A night that is not done is worth trying again.
async fn run_night(
    db: &mongodb::Database,
    cfg: &LinkerConfig,
    night: i64,
    stop: &AtomicBool,
) -> bool {
    let started = Instant::now();
    let (jd_start, span) = window(night, cfg.window_nights);
    info!(
        night,
        jd_start, span, "searching the window ending with this night"
    );

    let (detections, labels) = match load_window(db, jd_start, span, cfg.drb, false, None).await {
        Ok(found) => found,
        Err(error) => {
            error!(%error, night, "could not load the window");
            return false;
        }
    };
    info!(
        detections = detections.len(),
        seconds = started.elapsed().as_secs_f64(),
        "loaded the unassociated detections"
    );

    // One read of the catalog serves both passes. Without it every known
    // object IPAC missed would be stored as a discovery candidate, so a run
    // that should match and cannot does not store anything.
    let catalog = if cfg.match_known && (cfg.link || cfg.thor) && !detections.is_empty() {
        match load_catalogue(db).await {
            Ok(orbits) if orbits.is_empty() => {
                error!("MPC_orbits is empty, so tracks cannot be matched; run mpcorb_ingest");
                return false;
            }
            Ok(orbits) => Some(orbits),
            Err(error) => {
                error!(%error, "could not read MPC_orbits, so tracks cannot be matched");
                return false;
            }
        }
    } else {
        None
    };
    let dry_run = !cfg.persist;
    let mut done = true;

    if cfg.link && !detections.is_empty() && !stop.load(Ordering::Relaxed) {
        let pass = Instant::now();
        let (tracklets, tracks) = link(&detections);
        let groups: Vec<Vec<i64>> = tracks
            .iter()
            .map(|t| track_detections(t, &tracklets))
            .collect();
        let known = known_objects(&groups, &detections, catalog.as_deref());
        let report = persist_tracks(
            db,
            &tracks,
            &tracklets,
            &detections,
            &labels,
            &known,
            dry_run,
            TrackletConfig::default().min_detections,
            LinkConfig::default().min_nights,
            stop,
        )
        .await;
        done &= report.complete();
        info!(
            tracklets = tracklets.len(),
            tracks = tracks.len(),
            known = known.iter().filter(|k| k.is_some()).count(),
            stored = report.stored,
            unchanged = report.unchanged,
            wrote = report.ran && !dry_run,
            seconds = pass.elapsed().as_secs_f64(),
            "linking pass done"
        );
    }

    if cfg.thor && !detections.is_empty() && !stop.load(Ordering::Relaxed) {
        let pass = Instant::now();
        let (thor_start, _) = window(night, cfg.thor_window_nights);
        let recent: Vec<Detection> = detections
            .iter()
            .filter(|d| d.jd >= thor_start)
            .copied()
            .collect();
        let search = ThorSearch::default();
        let clusters = thor_clusters(&recent, &search);
        let groups: Vec<Vec<i64>> = clusters.iter().map(|(c, _)| c.ids.clone()).collect();
        let known = known_objects(&groups, &recent, catalog.as_deref());
        let report = persist_clusters(
            db,
            &clusters,
            &recent,
            &labels,
            &known,
            dry_run,
            search.config.min_detections,
            search.config.min_nights,
            stop,
        )
        .await;
        done &= report.complete();
        info!(
            detections = recent.len(),
            clusters = clusters.len(),
            known = known.iter().filter(|k| k.is_some()).count(),
            stored = report.stored,
            unchanged = report.unchanged,
            wrote = report.ran && !dry_run,
            seconds = pass.elapsed().as_secs_f64(),
            "THOR pass done"
        );
    }

    if stop.load(Ordering::Relaxed) {
        info!(night, "asked to stop, leaving the night unfinished");
        return false;
    }

    // Diagnostic only, so a recall that fails does not hold the night back.
    if cfg.recall {
        let pass = Instant::now();
        let (start, span) = window(night, cfg.recall_window_nights);
        match load_window(db, start, span, cfg.drb, true, None).await {
            Ok((identified, names)) => {
                let (tracklets, tracks) = link(&identified);
                let r = recall(&tracks, &tracklets, &names);
                info!(
                    detections = identified.len(),
                    linkable = r.linkable,
                    recovered = r.recovered,
                    recall = r.fraction(),
                    mixed_tracks = r.mixed_tracks,
                    seconds = pass.elapsed().as_secs_f64(),
                    "recall on objects IPAC identified"
                );
            }
            Err(error) => error!(%error, "could not load identified detections for recall"),
        }
    }

    if detections.is_empty() {
        warn!(night, "no unassociated detections in the window");
    }
    info!(
        night,
        done,
        seconds = started.elapsed().as_secs_f64(),
        "night over"
    );
    done
}

/// Set `stop` and wake `wake` on SIGTERM or Ctrl-C.
///
/// Listening from the start, not only while waiting, so a shutdown mid-run is
/// seen between tracks and the tracks lock is released rather than left held
/// for its lease when Docker gives up and kills the process.
fn listen_for_stop(stop: Arc<AtomicBool>, wake: Arc<Notify>) {
    use tokio::signal::unix::{signal, SignalKind};
    tokio::spawn(async move {
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                warn!(%error, "cannot listen for SIGTERM, so only Ctrl-C stops the linker");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
        info!("asked to stop");
        stop.store(true, Ordering::Relaxed);
        // A permit, so a wait that starts after this still returns at once.
        wake.notify_one();
    });
}

/// Sleep for `duration`, or until asked to stop. True when asked to stop.
async fn sleep_or_stop(duration: Duration, stop: &AtomicBool, wake: &Notify) -> bool {
    if stop.load(Ordering::Relaxed) {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => stop.load(Ordering::Relaxed),
        _ = wake.notified() => true,
    }
}

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    load_dotenv();

    let stop = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Notify::new());
    listen_for_stop(stop.clone(), wake.clone());

    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(|| "config.yaml".to_string());
    let config = AppConfig::from_path(&config_path).expect("failed to load config");
    let db = config.build_db().await.expect("failed to connect to mongo");
    let cfg = config.linker;
    info!(?cfg, "linker starting");

    if let Some(night) = cli.night {
        run_night(&db, &cfg, night, &stop).await;
        return;
    }
    if cli.once {
        let night = last_finished_night(now_jd(), cfg.run_after_utc_hour);
        if run_night(&db, &cfg, night, &stop).await {
            record_night(&db, night).await;
        }
        return;
    }

    let mut done = last_recorded_night(&db).await;
    if let Some(night) = done {
        info!(night, "resuming after the last finished night");
    }
    loop {
        let night = last_finished_night(now_jd(), cfg.run_after_utc_hour);
        let wait = if done.is_some_and(|d| d >= night) {
            let days = (finished_at(night + 1, cfg.run_after_utc_hour) - now_jd()).max(0.0);
            info!(
                next_night = night + 1,
                wait_hours = days * 24.0,
                "waiting for the next night to end"
            );
            // A minute late, so the night is over by any clock's reckoning.
            Duration::from_secs_f64(days * 86_400.0 + 60.0)
        } else if run_night(&db, &cfg, night, &stop).await {
            record_night(&db, night).await;
            done = Some(night);
            continue;
        } else {
            info!(
                night,
                retry_minutes = RETRY_AFTER.as_secs() / 60,
                "night not finished, will try again"
            );
            RETRY_AFTER
        };
        if sleep_or_stop(wait, &stop, &wake).await {
            info!("stopping");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// JD of 2026-09-29 00:00 UTC; night 2461312 is that UTC day.
    const SEPT_29: f64 = 2461312.5;

    #[test]
    fn test_a_night_is_over_once_its_hour_has_passed() {
        // 13:59 UTC on the 29th: Palomar's night on the 29th is not yet done.
        assert_eq!(last_finished_night(SEPT_29 + 13.99 / 24.0, 14), 2461311);
        // 14:01 UTC: it is.
        assert_eq!(last_finished_night(SEPT_29 + 14.01 / 24.0, 14), 2461312);
        // Just before midnight, the next night has not started.
        assert_eq!(last_finished_night(SEPT_29 + 23.9 / 24.0, 14), 2461312);
    }

    #[test]
    fn test_the_next_run_is_when_the_next_night_is_over() {
        let at = finished_at(2461312, 14);
        assert!((at - (SEPT_29 + 14.0 / 24.0)).abs() < 1e-9);
        assert_eq!(last_finished_night(at + 1e-6, 14), 2461312);
        assert_eq!(last_finished_night(at - 1e-6, 14), 2461311);
    }

    #[test]
    fn test_the_window_ends_with_the_night() {
        let (start, span) = window(2461312, 14);
        assert_eq!(span, 14.0);
        assert_eq!(night_of(start), 2461312 - 13);
        assert_eq!(night_of(start + span - 1e-6), 2461312);
        assert_eq!(
            night_of(start + span),
            2461313,
            "the window stops at the night"
        );
    }
}
