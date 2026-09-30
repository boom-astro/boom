//! Run moving-object discovery once each ZTF night is over.
//!
//! For the night just finished, the linker reads the last
//! `linker.window_nights` nights of unassociated detections, links tracklets
//! across nights, recovers objects seen once a night with THOR over the most
//! recent `linker.thor_window_nights`, matches what it finds to the MPC
//! catalogue so a known object is stored as a recovery, and stores the tracks.
//! Nothing is sent anywhere else: this is the loop that shows how fast and how
//! complete the search is before anything depends on it.
//!
//! It then links the detections IPAC already identified over the most recent
//! `linker.recall_window_nights`, stores nothing, and logs how many of those
//! objects the search recovered. That recall says whether a change to the
//! search misses things.

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
use clap::Parser;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

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
    chrono::Utc::now().timestamp_millis() as f64 / 86_400_000.0 + 2_440_587.5
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

/// The designation of each group, or none at all without a catalogue.
fn known_objects(
    groups: &[Vec<i64>],
    detections: &[Detection],
    catalogue: Option<&[OrbitEntry]>,
) -> Vec<Option<String>> {
    match catalogue {
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

/// Search the window ending with `night`, store what it finds, and report the
/// recall on objects IPAC identified.
async fn run_night(db: &mongodb::Database, cfg: &LinkerConfig, night: i64) {
    let started = Instant::now();
    let (jd_start, span) = window(night, cfg.window_nights);
    info!(
        night,
        jd_start, span, "searching the window ending with this night"
    );

    let (detections, labels) = match load_window(db, jd_start, span, cfg.drb, false, None).await {
        Ok(found) => found,
        Err(error) => {
            error!(%error, night, "could not load the window, skipping this night");
            return;
        }
    };
    info!(
        detections = detections.len(),
        seconds = started.elapsed().as_secs_f64(),
        "loaded the unassociated detections"
    );

    // One read of the catalogue serves both passes.
    let catalogue = if cfg.match_known && (cfg.link || cfg.thor) {
        match load_catalogue(db).await {
            Ok(orbits) => Some(orbits),
            Err(error) => {
                error!(%error, "could not read MPC_orbits, so tracks are not matched");
                None
            }
        }
    } else {
        None
    };
    let dry_run = !cfg.persist;

    if cfg.link && !detections.is_empty() {
        let pass = Instant::now();
        let (tracklets, tracks) = link(&detections);
        let groups: Vec<Vec<i64>> = tracks
            .iter()
            .map(|t| track_detections(t, &tracklets))
            .collect();
        let known = known_objects(&groups, &detections, catalogue.as_deref());
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
        )
        .await;
        info!(
            tracklets = tracklets.len(),
            tracks = tracks.len(),
            known = known.iter().filter(|k| k.is_some()).count(),
            stored = report.stored,
            wrote = report.ran && !dry_run,
            seconds = pass.elapsed().as_secs_f64(),
            "linking pass done"
        );
    }

    if cfg.thor && !detections.is_empty() {
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
        let known = known_objects(&groups, &recent, catalogue.as_deref());
        let report = persist_clusters(
            db,
            &clusters,
            &recent,
            &labels,
            &known,
            dry_run,
            search.config.min_detections,
            search.config.min_nights,
        )
        .await;
        info!(
            detections = recent.len(),
            clusters = clusters.len(),
            known = known.iter().filter(|k| k.is_some()).count(),
            stored = report.stored,
            wrote = report.ran && !dry_run,
            seconds = pass.elapsed().as_secs_f64(),
            "THOR pass done"
        );
    }

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
        seconds = started.elapsed().as_secs_f64(),
        "night done"
    );
}

/// Sleep for `duration`, or until asked to stop. True when asked to stop.
async fn sleep_or_stop(duration: Duration) -> bool {
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(error) => {
            warn!(%error, "cannot listen for SIGTERM, so only Ctrl-C stops a wait");
            tokio::select! {
                _ = tokio::time::sleep(duration) => return false,
                _ = tokio::signal::ctrl_c() => return true,
            }
        }
    };
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = tokio::signal::ctrl_c() => true,
        _ = terminate.recv() => true,
    }
}

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    load_dotenv();

    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(|| "config.yaml".to_string());
    let config = AppConfig::from_path(&config_path).expect("failed to load config");
    let db = config.build_db().await.expect("failed to connect to mongo");
    let cfg = config.linker;
    info!(?cfg, "linker starting");

    if let Some(night) = cli.night {
        run_night(&db, &cfg, night).await;
        return;
    }

    let mut done: Option<i64> = None;
    loop {
        let night = last_finished_night(now_jd(), cfg.run_after_utc_hour);
        if done != Some(night) {
            run_night(&db, &cfg, night).await;
            done = Some(night);
            if cli.once {
                return;
            }
        }
        let wait_days = (finished_at(night + 1, cfg.run_after_utc_hour) - now_jd()).max(0.0);
        info!(
            next_night = night + 1,
            wait_hours = wait_days * 24.0,
            "waiting for the next night to end"
        );
        // A minute late, so the night is over by any clock's reckoning.
        if sleep_or_stop(Duration::from_secs_f64(wait_days * 86_400.0 + 60.0)).await {
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
