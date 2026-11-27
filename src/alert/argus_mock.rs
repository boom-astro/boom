use crate::alert::argus::{
    ArgusCandidate, ArgusEventAssociation, ArgusLane, ArgusRawAvroAlert, ArgusSsoAssociation,
    ArgusSsoHistoryEntry,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::f64::consts::TAU;
use std::sync::{Arc, Mutex};

const ZEROPOINT: f32 = 25.0;
const OBLIQUITY_DEG: f64 = 23.439;
const MAX_OBJECTS_PER_LANE: usize = 2_000_000;
const MAX_SHARED_PER_LANE: usize = 200_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockSettings {
    pub history_depth: usize,
    pub cutout_bytes: usize,
    pub new_object_fraction: f64,
    #[serde(default)]
    pub anchored_fraction: f64,
    #[serde(default)]
    pub shared_fraction: f64,
}

impl Default for MockSettings {
    fn default() -> Self {
        MockSettings {
            history_depth: 10,
            cutout_bytes: 0,
            new_object_fraction: 0.2,
            anchored_fraction: 0.0,
            shared_fraction: 0.3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Source {
    sequence: u64,
    seed: u64,
    ra: f64,
    dec: f64,
    base_mag: f32,
    amplitude: f32,
    period_days: f32,
}

#[derive(Default)]
pub struct SharedEvents {
    lanes: Mutex<Vec<VecDeque<Source>>>,
}

impl SharedEvents {
    fn publish(&self, lane: ArgusLane, source: Source) {
        let mut lanes = self.lanes.lock().unwrap();
        if lanes.is_empty() {
            lanes.resize_with(ArgusLane::ALL.len(), VecDeque::new);
        }
        let events = &mut lanes[lane_index(lane)];
        if events.len() >= MAX_SHARED_PER_LANE {
            events.pop_front();
        }
        events.push_back(source);
    }

    fn adopt(&self, lane: ArgusLane) -> Option<Source> {
        let cadence = lane.cadence_secs()?;
        let lanes = self.lanes.lock().unwrap();
        let finer: Vec<&VecDeque<Source>> = ArgusLane::ALL
            .iter()
            .filter(|other| other.cadence_secs().is_some_and(|c| c < cadence))
            .filter_map(|other| lanes.get(lane_index(*other)))
            .filter(|events| !events.is_empty())
            .collect();
        let total: usize = finer.iter().map(|events| events.len()).sum();
        if total == 0 {
            return None;
        }
        let mut pick = rand::random_range(0..total);
        for events in finer {
            if pick < events.len() {
                return events.get(pick).copied();
            }
            pick -= events.len();
        }
        None
    }

    pub fn clear(&self) {
        self.lanes.lock().unwrap().clear();
    }
}

struct TrackedObject {
    source: Source,
    rate_ra: f32,
    rate_dec: f32,
    t0_ms: i64,
    epoch: u64,
    detections: u32,
}

impl TrackedObject {
    fn id(&self, is_sso: bool) -> String {
        if is_sso {
            packed_designation(self.source.sequence)
        } else {
            let high = (self.source.seed as u128) << 64;
            uuid::Uuid::from_u128(high | splitmix(self.source.seed, 1) as u128).to_string()
        }
    }
}

pub struct ArgusMock {
    settings: MockSettings,
    pools: [VecDeque<TrackedObject>; ArgusLane::ALL.len()],
    sequence: u64,
    anchors: Arc<Vec<(f64, f64)>>,
    shared: Arc<SharedEvents>,
    adopted: HashSet<(usize, u64)>,
}

fn splitmix(seed: u64, n: u64) -> u64 {
    let mut z = seed.wrapping_add(n.wrapping_mul(0x9E3779B97F4A7C15));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn jitter(seed: u64, n: u64) -> f32 {
    (splitmix(seed, n) as f64 / u64::MAX as f64 * 2.0 - 1.0) as f32
}

fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn unix_ms_to_jd(ms: i64) -> f64 {
    ms as f64 / 86_400_000.0 + 2_440_587.5
}

fn lane_index(lane: ArgusLane) -> usize {
    ArgusLane::ALL.iter().position(|l| *l == lane).unwrap_or(0)
}

fn random_sky_position() -> (f64, f64) {
    let sin_dec = rand::random_range((-30.0_f64).to_radians().sin()..1.0);
    (rand::random_range(0.0..360.0), sin_dec.asin().to_degrees())
}

fn random_ecliptic_position() -> (f64, f64) {
    let lambda = rand::random_range(0.0_f64..360.0).to_radians();
    let beta = (rand::random_range(-1.0_f64..1.0) * rand::random_range(0.0_f64..12.0)).to_radians();
    let eps = OBLIQUITY_DEG.to_radians();
    let sin_dec = beta.sin() * eps.cos() + beta.cos() * eps.sin() * lambda.sin();
    let ra = (lambda.sin() * eps.cos() - beta.tan() * eps.sin()).atan2(lambda.cos());
    (
        ra.to_degrees().rem_euclid(360.0),
        sin_dec.asin().to_degrees(),
    )
}

fn packed_designation(n: u64) -> String {
    let letters = b"ABCDEFGHJKLMNOPQRSTUVWXY";
    let year = 20 + (n / 10_000) % 7;
    format!(
        "K{:02}{}{:02}{}",
        year,
        letters[(n % 24) as usize] as char,
        (n / 24) % 100,
        letters[((n / 2400) % 24) as usize] as char
    )
}

fn magnitude(source: &Source, midpoint_ms: i64, n: u64) -> f32 {
    let days = midpoint_ms as f64 / 86_400_000.0;
    let phase = splitmix(source.seed, 9) as f64 / u64::MAX as f64 * TAU;
    let wave = (days / source.period_days as f64 * TAU + phase).sin() as f32;
    source.base_mag + source.amplitude * wave + 0.05 * jitter(source.seed, n)
}

impl ArgusMock {
    pub fn new(settings: MockSettings, shared: Arc<SharedEvents>) -> Self {
        ArgusMock {
            settings,
            pools: Default::default(),
            sequence: rand::random::<u32>() as u64,
            anchors: Arc::new(Vec::new()),
            shared,
            adopted: HashSet::new(),
        }
    }

    pub fn set_anchors(&mut self, anchors: Arc<Vec<(f64, f64)>>) {
        self.anchors = anchors;
    }

    pub fn settings(&self) -> &MockSettings {
        &self.settings
    }

    pub fn set_settings(&mut self, settings: MockSettings) {
        self.settings = settings;
    }

    pub fn population(&self, lane: ArgusLane) -> usize {
        self.pools[lane_index(lane)].len()
    }

    fn anchored_position(&self) -> Option<(f64, f64)> {
        if self.anchors.is_empty() || !rand::random_bool(self.settings.anchored_fraction) {
            return None;
        }
        let (ra, dec) = self.anchors[rand::random_range(0..self.anchors.len())];
        let offset = 0.5 / 3600.0;
        Some((
            (ra + rand::random_range(-offset..offset) / dec.to_radians().cos().max(0.01))
                .rem_euclid(360.0),
            (dec + rand::random_range(-offset..offset)).clamp(-90.0, 90.0),
        ))
    }

    fn lane_cadence_ms(lane: ArgusLane) -> i64 {
        lane.cadence_secs().unwrap_or(60) as i64 * 1000
    }

    fn new_source(&mut self, lane: ArgusLane) -> Source {
        self.sequence += 1;
        let (ra, dec) = if lane == ArgusLane::Sso {
            random_ecliptic_position()
        } else {
            self.anchored_position().unwrap_or_else(random_sky_position)
        };
        let (mag_low, mag_high) = match lane {
            ArgusLane::Second => (11.0, 16.5),
            ArgusLane::Minute => (14.0, 20.0),
            ArgusLane::Base => (16.0, 21.5),
            ArgusLane::Hour | ArgusLane::Day => (17.0, 22.5),
            ArgusLane::Sso => (15.0, 20.5),
        };
        Source {
            sequence: self.sequence,
            seed: splitmix(self.sequence, rand::random()),
            ra,
            dec,
            base_mag: rand::random_range(mag_low..mag_high),
            amplitude: rand::random_range(0.05..1.5),
            period_days: 10f32.powf(rand::random_range(-1.3..1.3)),
        }
    }

    fn new_object(&mut self, lane: ArgusLane, now_ms: i64) -> TrackedObject {
        let is_sso = lane == ArgusLane::Sso;
        let adopted = if !is_sso && rand::random_bool(self.settings.shared_fraction) {
            (0..4).find_map(|_| {
                self.shared
                    .adopt(lane)
                    .filter(|source| self.adopted.insert((lane_index(lane), source.seed)))
            })
        } else {
            None
        };
        let source = match adopted {
            Some(source) => source,
            None => {
                let source = self.new_source(lane);
                if !is_sso {
                    self.shared.publish(lane, source);
                }
                source
            }
        };
        TrackedObject {
            source,
            rate_ra: if is_sso {
                rand::random_range(-0.05..0.05)
            } else {
                0.0
            },
            rate_dec: if is_sso {
                rand::random_range(-0.02..0.02)
            } else {
                0.0
            },
            t0_ms: now_ms - Self::lane_cadence_ms(lane),
            epoch: 1,
            detections: 1,
        }
    }

    fn next_object(&mut self, lane: ArgusLane, now_ms: i64) -> TrackedObject {
        let cadence_ms = Self::lane_cadence_ms(lane);
        let pool = &mut self.pools[lane_index(lane)];
        let due = pool
            .front()
            .is_some_and(|front| now_ms >= front.t0_ms + (front.epoch as i64 + 1) * cadence_ms);
        match pool.pop_front() {
            Some(mut object) if due => {
                let epoch = ((now_ms - object.t0_ms) / cadence_ms) as u64;
                object.epoch = epoch.max(object.epoch + 1);
                object.detections += 1;
                object
            }
            Some(object) => {
                pool.push_front(object);
                self.new_object(lane, now_ms)
            }
            None => self.new_object(lane, now_ms),
        }
    }

    pub fn next_alert(&mut self, lane: ArgusLane, now_ms: i64) -> ArgusRawAvroAlert {
        let object = self.next_object(lane, now_ms);
        let alert = self.build_alert(&object, lane, now_ms);
        let fades = rand::random_bool(self.settings.new_object_fraction);
        let pool = &mut self.pools[lane_index(lane)];
        if !fades {
            pool.push_back(object);
        }
        while pool.len() > MAX_OBJECTS_PER_LANE {
            pool.pop_front();
        }
        alert
    }

    pub fn sample_alert(&mut self, lane: ArgusLane, now_ms: i64) -> ArgusRawAvroAlert {
        let mut object = self.new_object(lane, now_ms);
        let epochs = self.settings.history_depth as u64 + 1;
        object.t0_ms -= (epochs as i64 - 1) * Self::lane_cadence_ms(lane);
        object.epoch = epochs;
        object.detections = epochs as u32;
        self.build_alert(&object, lane, now_ms)
    }

    fn candidate(object: &TrackedObject, lane: ArgusLane, n: u64, now_ms: i64) -> ArgusCandidate {
        let cadence_ms = Self::lane_cadence_ms(lane);
        let source = &object.source;
        let obs_end = object.t0_ms + n as i64 * cadence_ms;
        let obs_start = obs_end - cadence_ms;
        let midpoint = obs_start + cadence_ms / 2;
        let elapsed_s = (obs_end - object.t0_ms) as f64 / 1000.0;
        let dec = (source.dec + object.rate_dec as f64 * elapsed_s / 3600.0).clamp(-89.9, 89.9);
        let ra = (source.ra + object.rate_ra as f64 * elapsed_s / 3600.0 / dec.to_radians().cos())
            .rem_euclid(360.0);
        let filter = if n.is_multiple_of(2) { 1 } else { 2 };
        let mag_dif = magnitude(source, midpoint, n);
        let flux_dif = 10f32.powf(-0.4 * (mag_dif - ZEROPOINT));
        let mag_err = (0.01 + 0.02 * (mag_dif - 12.0).max(0.0)).min(0.4);
        let flux_err = flux_dif * mag_err / 1.0857;
        let mag_sci = mag_dif - 0.3;
        let flux_sci = 10f32.powf(-0.4 * (mag_sci - ZEROPOINT));
        let healpix_order = 14u8;
        let healpix_id =
            cdshealpix::nested::hash(healpix_order, ra.to_radians(), dec.to_radians()) as i64;
        let dec_band = ((dec + 90.0) * 2.0) as i64;
        let ra_slot = (ra * 4.0) as i64;
        let tanseg_id = dec_band * 1440 + ra_slot;
        let reference_end = unix_ms_to_jd(object.t0_ms) - 30.0;
        ArgusCandidate {
            alert_id: format!(
                "{:x}-{:016x}-{}-{}",
                healpix_id,
                source.seed,
                cadence_ms / 1000,
                n
            ),
            obs_start_time: iso(obs_start),
            obs_end_time: iso(obs_end),
            obs_weighted_midpoint: iso(midpoint),
            integration_time: (cadence_ms / 1000) as f32,
            coadd: cadence_ms > 60_000,
            coadd_spacing: (cadence_ms > 60_000).then_some(cadence_ms / 1000 / 15),
            filter,
            epoch_id: format!("{:016x}-{}", splitmix(obs_end as u64, filter as u64), lane),
            camera_ids: vec![(source.seed % 1200) as i32],
            ota_ids: vec![(source.seed % 1200) as i32],
            filter_ids: vec![filter],
            focuser_ids: vec![(source.seed % 1200) as i32],
            subarray_ids: vec![(source.seed % 8) as i32],
            timestamp: now_ms,
            withdrawal_time: None,
            ra,
            dec,
            ra_err: Some(0.3),
            dec_err: Some(0.3),
            ra_dec_covariance: Some(0.01),
            x: (splitmix(source.seed, 1) % 2048) as f32,
            y: (splitmix(source.seed, 2) % 2048) as f32,
            x_err: Some(0.2),
            y_err: Some(0.2),
            flags: if jitter(source.seed, n + 7) > 0.95 {
                8
            } else {
                0
            },
            tanseg_id,
            minipix_idx: Some((splitmix(source.seed, 3) % 4096) as i32),
            healpix_id: Some(healpix_id),
            healpix_order: Some(healpix_order as i32),
            num_reference_sources: Some(120),
            residual_rms: Some(0.15),
            psf_flux_sci: Some(flux_sci),
            psf_magnitude_sci: Some(mag_sci),
            psf_flux_dif: Some(flux_dif),
            psf_magnitude_dif: Some(mag_dif),
            psf_flux_sci_err: Some(flux_err),
            psf_magnitude_sci_err: Some(mag_err),
            psf_flux_dif_err: Some(flux_err),
            psf_magnitude_dif_err: Some(mag_err),
            psf_chi2: Some(1.0 + 0.2 * jitter(source.seed, n + 1)),
            aper_flux_sci: Some(flux_sci * 1.02),
            aper_magnitude_sci: Some(mag_sci - 0.02),
            aper_flux_dif: Some(flux_dif * 1.02),
            aper_magnitude_dif: Some(mag_dif - 0.02),
            aper_flux_sci_err: Some(flux_err * 1.1),
            aper_magnitude_sci_err: Some(mag_err * 1.1),
            aper_flux_dif_err: Some(flux_err * 1.1),
            aper_magnitude_dif_err: Some(mag_err * 1.1),
            psf_snr: Some(1.0857 / mag_err),
            aper_snr: Some(1.0857 / (mag_err * 1.1)),
            is_flux_positive: true,
            psf_lim_mag: Some(17.0 + 1.25 * ((cadence_ms / 1000) as f32).log10()),
            aper_lim_mag: Some(16.9 + 1.25 * ((cadence_ms / 1000) as f32).log10()),
            sky_background: Some(150.0),
            psf_zeropoint: Some(ZEROPOINT),
            aper_zeropoint: Some(ZEROPOINT),
            psf_zp_err: Some(0.01),
            aper_zp_err: Some(0.01),
            placeholder_photometric_calibration: None,
            star_galaxy_placeholder: None,
            fwhm: Some(2.0),
            ellipticity: Some(0.08),
            n_pixels: Some(25),
            peak: Some(flux_sci / 8.0),
            psf_fwhm: Some(2.0),
            psf_beta: Some(2.5),
            psf_over_ap: Some(0.98),
            artefact_placeholder: None,
            real_bogus_score: Some(0.75 + 0.25 * jitter(source.seed, n + 2)),
            rb_model_version: "mock-0".to_string(),
            real_bogus_confidence: Some(0.9),
            placeholder_catalog_context: None,
            reference_id: tanseg_id,
            reference_start: reference_end - 60.0,
            reference_end,
            reference_depth: 21.5,
        }
    }

    fn cutout(&self) -> Option<Vec<u8>> {
        if self.settings.cutout_bytes == 0 {
            return None;
        }
        let mut bytes = vec![0u8; self.settings.cutout_bytes];
        rand::fill(&mut bytes[..]);
        Some(bytes)
    }

    fn build_alert(
        &self,
        object: &TrackedObject,
        lane: ArgusLane,
        now_ms: i64,
    ) -> ArgusRawAvroAlert {
        let is_sso = lane == ArgusLane::Sso;
        let cadence_ms = Self::lane_cadence_ms(lane);
        let n = object.epoch;
        let depth = self.settings.history_depth as u64;
        let source = &object.source;
        let object_id = object.id(is_sso);

        let candidate = Self::candidate(object, lane, n, now_ms);
        let history: Vec<(u64, ArgusCandidate)> = (n.saturating_sub(depth).max(1)..n)
            .map(|k| (k, Self::candidate(object, lane, k, now_ms)))
            .collect();
        let history = (!history.is_empty()).then_some(history);
        let first_detection = Some(object.t0_ms + cadence_ms);
        let most_recent = Some(object.t0_ms + (n as i64 - 1) * cadence_ms);
        let detections = object.detections as i64;

        let (event_association, sso_association) = if is_sso {
            let reduced = candidate.psf_magnitude_dif.unwrap_or(18.0) - 5.0;
            let sso_history = history.map(|entries| {
                entries
                    .into_iter()
                    .map(|(k, entry)| ArgusSsoHistoryEntry {
                        epoch: object.t0_ms + k as i64 * cadence_ms,
                        eph_offset_ra: 0.3 * jitter(source.seed, k + 11),
                        eph_offset_dec: 0.3 * jitter(source.seed, k + 12),
                        reduced_mag: entry.psf_magnitude_dif.unwrap_or(18.0) - 5.0,
                        candidate: entry,
                    })
                    .collect()
            });
            (
                None,
                Some(ArgusSsoAssociation {
                    sso_id: object_id,
                    spk_id: 20_000_000 + source.sequence as i64,
                    sso_name: (source.seed % 10 < 3).then(|| format!("Mock{}", source.sequence)),
                    orb_source: Some("MPCORB".to_string()),
                    eph_ra: Some(candidate.ra),
                    eph_dec: Some(candidate.dec),
                    eph_vmag: Some(source.base_mag),
                    eph_offset_ra: Some(0.3 * jitter(source.seed, n + 3)),
                    eph_offset_dec: Some(0.3 * jitter(source.seed, n + 4)),
                    eph_offset_along_track: Some(0.2 * jitter(source.seed, n + 5)),
                    eph_offset_cross_track: Some(0.1 * jitter(source.seed, n + 6)),
                    eph_rate_ra: Some(object.rate_ra),
                    eph_rate_dec: Some(object.rate_dec),
                    eph_trail_length: Some(
                        (object.rate_ra.hypot(object.rate_dec)) * candidate.integration_time,
                    ),
                    eph_trail_angle: Some(object.rate_ra.atan2(object.rate_dec).to_degrees()),
                    topo_dist: Some(1.2 + (source.seed % 300) as f32 / 100.0),
                    helio_dist: Some(2.1 + (source.seed % 300) as f32 / 100.0),
                    ecl_lambda: Some(candidate.ra as f32),
                    ecl_beta: Some(0.0),
                    phase_angle: Some(12.0),
                    reduced_mag: Some(reduced),
                    reduced_mag_mean: Some(source.base_mag - 5.0),
                    reduced_mag_min: Some(source.base_mag - 5.0 - source.amplitude),
                    reduced_mag_max: Some(source.base_mag - 5.0 + source.amplitude),
                    reduced_mag_rms: Some(source.amplitude / 2.0),
                    first_detection,
                    most_recent_detection_at_cadence: most_recent,
                    num_detection_at_this_cadence: Some(detections),
                    num_detection_base_cadence: Some(detections),
                    sso_history,
                }),
            )
        } else {
            let history =
                history.map(|entries| entries.into_iter().map(|(_, entry)| entry).collect());
            let flux = candidate.psf_flux_dif;
            let mut event = ArgusEventAssociation {
                event_id: object_id,
                event_ra: source.ra,
                event_ra_err: Some(0.1),
                event_dec: source.dec,
                event_dec_err: Some(0.1),
                summary_start: Some(iso(object.t0_ms)),
                first_detection,
                most_recent_detection_at_cadence: most_recent,
                num_detection_at_this_cadence: Some(detections),
                num_detection_base_cadence: Some(detections),
                beta_psf_flux_mean: flux,
                beta_psf_flux_min: flux.map(|f| f * 0.7),
                beta_psf_flux_max: flux.map(|f| f * 1.3),
                beta_psf_flux_slope_before_max: Some(0.1),
                beta_psf_flux_slope_after_max: Some(-0.05),
                beta_ap_flux_mean: flux,
                beta_ap_flux_min: flux.map(|f| f * 0.7),
                beta_ap_flux_max: flux.map(|f| f * 1.3),
                beta_ap_flux_slope_before_max: Some(0.1),
                beta_ap_flux_slope_after_max: Some(-0.05),
                rho_psf_flux_mean: flux,
                rho_psf_flux_min: flux.map(|f| f * 0.7),
                rho_psf_flux_max: flux.map(|f| f * 1.3),
                rho_psf_flux_slope_before_max: Some(0.1),
                rho_psf_flux_slope_after_max: Some(-0.05),
                rho_ap_flux_mean: flux,
                rho_ap_flux_min: flux.map(|f| f * 0.7),
                rho_ap_flux_max: flux.map(|f| f * 1.3),
                rho_ap_flux_slope_before_max: Some(0.1),
                rho_ap_flux_slope_after_max: Some(-0.05),
                second_cadence_history: None,
                minute_cadence_history: None,
                base_coadd_history: None,
                hour_coadd_history: None,
            };
            match lane {
                ArgusLane::Second => event.second_cadence_history = history,
                ArgusLane::Minute => event.minute_cadence_history = history,
                ArgusLane::Base => event.base_coadd_history = history,
                ArgusLane::Hour => event.hour_coadd_history = history,
                ArgusLane::Day | ArgusLane::Sso => {}
            }
            (Some(event), None)
        };

        ArgusRawAvroAlert {
            schema_version: "3.0".to_string(),
            alert_origin: "argus-mock".to_string(),
            alert_cadence: (cadence_ms / 1000) as i32,
            alert_id: candidate.alert_id.clone(),
            candidate,
            event_association,
            sso_association,
            cutout_science: self.cutout(),
            cutout_template: self.cutout(),
            cutout_difference: self.cutout(),
        }
    }
}
