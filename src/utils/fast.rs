//! Fast movers, 1 to 25 deg/day, chained night to night and kept when one orbit fits.

use crate::utils::discovery::DiscoveryError;
use crate::utils::enums::Survey;
use crate::utils::heliolinc::{unit_vector, State};
use crate::utils::linking::{find_tracklets, predict, Detection, Tracklet, TrackletConfig};
use crate::utils::orbit_fit::{
    converge_orbit, fit_within, rms_arcsec, Observation, OrbitFit, CONVERGE_ITERATIONS,
    GIVE_UP_FACTOR,
};
use crate::utils::outburst::median;
use crate::utils::sso_geometry::{dot, norm, observer_position, Site, ZTF};
use crate::utils::tracks::{member_alert, member_id};
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use rayon::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tracing::info;

pub const FAST_KIND: &str = "fast";

const NEAREST: usize = 48;
const VELOCITY_WEIGHT: f64 = 0.1;
const LSST_EXPOSURE_DAYS: f64 = 30.0 / 86_400.0;
const MIN_TRAILED_RATE_DEG_PER_DAY: f64 = 3.0;
const TRAIL_TOLERANCE: f64 = 0.4;
const SEED_RANGES: usize = 24;
const SEED_RANGES_AU: (f64, f64) = (0.002, 3.0);
const SEED_RANGE_RATES: [f64; 7] = [-0.03, -0.015, -0.006, 0.0, 0.006, 0.015, 0.03];
const SEEDS_FITTED: usize = 3;

#[derive(Debug, Clone)]
pub struct FastSearch {
    pub min_rate_deg_per_day: f64,
    pub max_rate_deg_per_day: f64,
    pub max_span_days: f64,
    pub max_tracklet_rms_arcsec: f64,
    pub dense_detections: usize,
    pub max_gap_nights: i64,
    pub min_nights: usize,
    pub min_detections: usize,
    pub max_chain_tracklets: usize,
    pub max_residual_arcsec: f64,
}

impl Default for FastSearch {
    fn default() -> Self {
        Self {
            min_rate_deg_per_day: 1.0,
            max_rate_deg_per_day: 25.0,
            max_span_days: 2.5 / 24.0,
            max_tracklet_rms_arcsec: 1.0,
            dense_detections: 10_000,
            max_gap_nights: 5,
            min_nights: 3,
            min_detections: 5,
            max_chain_tracklets: 60,
            max_residual_arcsec: 3.0,
        }
    }
}

impl FastSearch {
    fn tracklets(&self, min_detections: usize) -> TrackletConfig {
        TrackletConfig {
            min_rate_deg_per_day: self.min_rate_deg_per_day,
            max_rate_deg_per_day: self.max_rate_deg_per_day,
            max_rms_arcsec: self.max_tracklet_rms_arcsec,
            max_span_days: self.max_span_days,
            min_detections,
            ..TrackletConfig::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct FastTrack {
    pub members: Vec<i64>,
    pub fit: OrbitFit,
}

fn trail_arcsec(rate_deg_per_day: f64) -> f64 {
    rate_deg_per_day * LSST_EXPOSURE_DAYS * 3600.0
}

pub async fn lsst_trails(
    db: &mongodb::Database,
    detections: &[Detection],
) -> Result<HashMap<i64, f64>, DiscoveryError> {
    let ids: Vec<i64> = detections
        .iter()
        .filter_map(|d| match member_alert(d.id) {
            (Survey::Lsst, id) => Some(id),
            _ => None,
        })
        .collect();
    let shortest = trail_arcsec(MIN_TRAILED_RATE_DEG_PER_DAY) * (1.0 - TRAIL_TOLERANCE);
    let mut trails = HashMap::new();
    for chunk in ids.chunks(10_000) {
        let mut cursor = db
            .collection::<Document>(&format!("{}_alerts", Survey::Lsst))
            .find(doc! { "_id": { "$in": chunk }, "candidate.trailLength": { "$gte": shortest } })
            .projection(doc! { "candidate.trailLength": 1 })
            .await?;
        while let Some(doc) = cursor.next().await {
            let doc = doc?;
            let length = doc
                .get_document("candidate")
                .and_then(|c| c.get_f64("trailLength"));
            if let (Ok(id), Ok(length)) = (doc.get_i64("_id"), length) {
                trails.insert(member_id(&Survey::Lsst, id), length);
            }
        }
    }
    Ok(trails)
}

fn trailed_like(t: &Tracklet, trails: &HashMap<i64, f64>) -> bool {
    let rate = t.rate_deg_per_day();
    let expected = trail_arcsec(rate);
    rate >= MIN_TRAILED_RATE_DEG_PER_DAY
        && t.ids.iter().all(|id| {
            trails
                .get(id)
                .is_some_and(|&length| (length - expected).abs() < TRAIL_TOLERANCE * expected)
        })
}

fn fast_tracklets(
    detections: &[Detection],
    trails: &HashMap<i64, f64>,
    search: &FastSearch,
) -> Vec<Tracklet> {
    let mut by_night: BTreeMap<(i64, Option<&str>), Vec<Detection>> = BTreeMap::new();
    for d in detections {
        by_night
            .entry((d.night(), d.site.map(|s| s.mpc_code)))
            .or_default()
            .push(*d);
    }
    let nights: Vec<_> = by_night.into_iter().collect();
    nights
        .par_iter()
        .flat_map(|((night, site), dets)| {
            let dense = dets.len() > search.dense_detections;
            let mut found = tiled(dets, &search.tracklets(if dense { 3 } else { 2 }));
            if dense {
                let trailed: Vec<Detection> = dets
                    .iter()
                    .filter(|d| trails.contains_key(&d.id))
                    .copied()
                    .collect();
                found.extend(
                    tiled(&trailed, &search.tracklets(2))
                        .into_iter()
                        .filter(|t| t.ids.len() == 2 && trailed_like(t, trails)),
                );
            }
            info!(
                night,
                site = site.unwrap_or(ZTF.mpc_code),
                detections = dets.len(),
                dense,
                tracklets = found.len(),
                "fast tracklets"
            );
            found
        })
        .collect()
}

struct Tiles {
    core: f64,
    margin: f64,
    bands: i64,
}

impl Tiles {
    fn new(margin: f64) -> Self {
        let core = (4.0 * margin).clamp(1.0, 90.0);
        Tiles {
            core,
            margin,
            bands: (180.0 / core).ceil() as i64,
        }
    }

    fn band(&self, dec: f64) -> i64 {
        (((dec + 90.0) / self.core).floor() as i64).clamp(0, self.bands - 1)
    }

    fn ring(&self, band: i64) -> (i64, f64) {
        let lo = -90.0 + band as f64 * self.core;
        let hi = (lo + self.core).min(90.0);
        let widest = if lo <= 0.0 && hi >= 0.0 {
            1.0
        } else {
            lo.abs().min(hi.abs()).to_radians().cos()
        };
        let poleward = (lo.abs().max(hi.abs()) + self.margin).min(90.0);
        let cells = ((360.0 * widest / self.core).floor() as i64).max(1);
        (
            cells,
            (self.margin / poleward.to_radians().cos()).min(180.0),
        )
    }

    fn cell(ra: f64, cells: i64) -> i64 {
        ((ra * cells as f64 / 360.0).floor() as i64).rem_euclid(cells)
    }

    fn of(&self, ra: f64, dec: f64) -> (i64, i64) {
        let band = self.band(dec);
        (band, Self::cell(ra, self.ring(band).0))
    }

    fn around(&self, ra: f64, dec: f64) -> Vec<(i64, i64)> {
        let mut keys = Vec::new();
        for band in self.band(dec - self.margin)..=self.band(dec + self.margin) {
            let (cells, ra_margin) = self.ring(band);
            let first = ((ra - ra_margin) * cells as f64 / 360.0).floor() as i64;
            let last = ((ra + ra_margin) * cells as f64 / 360.0).floor() as i64;
            keys.extend((first..=last.min(first + cells - 1)).map(|c| (band, c.rem_euclid(cells))));
        }
        keys
    }
}

fn tiled(detections: &[Detection], cfg: &TrackletConfig) -> Vec<Tracklet> {
    let tiles =
        Tiles::new(cfg.max_rate_deg_per_day * cfg.max_span_days + cfg.match_radius_arcsec / 3600.0);
    let mut members: BTreeMap<(i64, i64), Vec<Detection>> = BTreeMap::new();
    for d in detections {
        for key in tiles.around(d.ra, d.dec) {
            members.entry(key).or_default().push(*d);
        }
    }
    let members: Vec<_> = members.into_iter().collect();
    members
        .par_iter()
        .flat_map_iter(|(key, dets)| {
            find_tracklets(dets, cfg)
                .into_iter()
                .filter(|t| tiles.of(t.ra_ref, t.dec_ref) == *key)
        })
        .collect()
}

fn longest_first(tracklets: Vec<Tracklet>) -> Vec<Tracklet> {
    let mut order: Vec<usize> = (0..tracklets.len()).collect();
    order.sort_by(|&a, &b| {
        let (a, b) = (&tracklets[a], &tracklets[b]);
        b.ids
            .len()
            .cmp(&a.ids.len())
            .then(a.rms_arcsec.total_cmp(&b.rms_arcsec))
    });
    let mut owned: HashSet<i64> = HashSet::new();
    let mut keep = vec![false; tracklets.len()];
    for k in order {
        let ids = &tracklets[k].ids;
        if ids.iter().all(|id| !owned.contains(id)) {
            if ids.len() >= 3 {
                owned.extend(ids.iter().copied());
            }
            keep[k] = true;
        }
    }
    tracklets
        .into_iter()
        .zip(keep)
        .filter_map(|(t, keep)| keep.then_some(t))
        .collect()
}

fn combine(x: &[f64; 3], kx: f64, y: &[f64; 3], ky: f64) -> [f64; 3] {
    [
        kx * x[0] + ky * y[0],
        kx * x[1] + ky * y[1],
        kx * x[2] + ky * y[2],
    ]
}

fn unit(v: [f64; 3]) -> [f64; 3] {
    combine(&v, 1.0 / norm(&v).max(1e-12), &v, 0.0)
}

fn angle_deg(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    dot(a, b).clamp(-1.0, 1.0).acos().to_degrees()
}

struct Motion {
    jd: f64,
    night: i64,
    at: [f64; 3],
    velocity: [f64; 3],
    heading: [f64; 3],
    speed_deg_per_day: f64,
}

fn line_of_sight(t: &Tracklet) -> ([f64; 3], [f64; 3]) {
    let step = 0.01;
    let look = |jd: f64| {
        let (ra, dec) = predict(t, jd);
        unit_vector(ra, dec)
    };
    let turning = combine(
        &look(t.jd_ref + step),
        0.5 / step,
        &look(t.jd_ref - step),
        -0.5 / step,
    );
    (look(t.jd_ref), turning)
}

impl Motion {
    fn of(t: &Tracklet) -> Self {
        let (at, velocity) = line_of_sight(t);
        Motion {
            jd: t.jd_ref,
            night: t.night(),
            at,
            velocity,
            heading: unit(velocity),
            speed_deg_per_day: norm(&velocity).to_degrees(),
        }
    }

    fn key(&self, jd: f64) -> [f64; 6] {
        let p = unit(combine(&self.at, 1.0, &self.velocity, jd - self.jd));
        let w = VELOCITY_WEIGHT;
        [
            p[0],
            p[1],
            p[2],
            w * self.velocity[0],
            w * self.velocity[1],
            w * self.velocity[2],
        ]
    }
}

fn continues(a: &Motion, b: &Motion) -> Option<f64> {
    let dt = b.jd - a.jd;
    let cos = dot(&a.at, &b.at).clamp(-1.0, 1.0);
    let arc =
        (cos.acos().to_degrees() - 0.5 * (a.speed_deg_per_day + b.speed_deg_per_day) * dt).abs();
    let leaving = angle_deg(&unit(combine(&b.at, 1.0, &a.at, -cos)), &a.heading);
    let arriving = angle_deg(&unit(combine(&b.at, cos, &a.at, -1.0)), &b.heading);
    let speed = (b.speed_deg_per_day - a.speed_deg_per_day).abs();
    let (tol_arc, tol_angle, tol_speed) = (0.1 + 0.2 * dt * dt, 3.0 + 3.0 * dt, 1.0 + 1.5 * dt);
    (arc < tol_arc && leaving < tol_angle && arriving < tol_angle && speed < tol_speed)
        .then(|| arc / tol_arc + (leaving + arriving) / tol_angle + speed / tol_speed)
}

struct KdTree {
    points: Vec<[f64; 6]>,
    index: Vec<usize>,
}

impl KdTree {
    fn new(points: Vec<[f64; 6]>) -> Self {
        let mut index: Vec<usize> = (0..points.len()).collect();
        Self::split(&mut index, &points, 0);
        KdTree { points, index }
    }

    fn split(index: &mut [usize], points: &[[f64; 6]], depth: usize) {
        if index.len() < 2 {
            return;
        }
        let (axis, mid) = (depth % 6, index.len() / 2);
        index.select_nth_unstable_by(mid, |&a, &b| points[a][axis].total_cmp(&points[b][axis]));
        let (left, right) = index.split_at_mut(mid);
        Self::split(left, points, depth + 1);
        Self::split(&mut right[1..], points, depth + 1);
    }

    fn nearest(&self, q: &[f64; 6], k: usize, radius: f64) -> Vec<usize> {
        let mut best = Vec::with_capacity(k + 1);
        self.search(&self.index, 0, q, k, radius * radius, &mut best);
        best.into_iter().map(|(_, i)| i).collect()
    }

    fn search(
        &self,
        index: &[usize],
        depth: usize,
        q: &[f64; 6],
        k: usize,
        r2: f64,
        best: &mut Vec<(f64, usize)>,
    ) {
        if index.is_empty() {
            return;
        }
        let (axis, mid) = (depth % 6, index.len() / 2);
        let p = &self.points[index[mid]];
        let bound = |best: &Vec<(f64, usize)>| if best.len() == k { best[k - 1].0 } else { r2 };
        let d2: f64 = p.iter().zip(q).map(|(a, b)| (a - b) * (a - b)).sum();
        if d2 < bound(best) {
            let at = best.partition_point(|&(d, _)| d <= d2);
            best.insert(at, (d2, index[mid]));
            best.truncate(k);
        }
        let diff = q[axis] - p[axis];
        let (near, far) = if diff < 0.0 {
            (&index[..mid], &index[mid + 1..])
        } else {
            (&index[mid + 1..], &index[..mid])
        };
        self.search(near, depth + 1, q, k, r2, best);
        if diff * diff < bound(best) {
            self.search(far, depth + 1, q, k, r2, best);
        }
    }
}

fn mutual_best(
    earlier: &[usize],
    later: &[usize],
    gap: i64,
    motions: &[Motion],
) -> Vec<(usize, usize)> {
    let jds = |ks: &[usize]| ks.iter().map(|&k| motions[k].jd).collect::<Vec<f64>>();
    let epoch = 0.5 * (median(&jds(earlier)).unwrap_or(0.0) + median(&jds(later)).unwrap_or(0.0));
    let tree = KdTree::new(later.iter().map(|&k| motions[k].key(epoch)).collect());
    let g = gap as f64;
    let radius = (1.0 + 0.6 * g * g).to_radians() + VELOCITY_WEIGHT * (3.0 + 3.0 * g).to_radians();
    let scored: Vec<(usize, usize, f64)> = earlier
        .par_iter()
        .enumerate()
        .flat_map_iter(|(i, &a)| {
            tree.nearest(&motions[a].key(epoch), NEAREST, radius)
                .into_iter()
                .filter_map(move |j| continues(&motions[a], &motions[later[j]]).map(|s| (i, j, s)))
        })
        .collect();
    let mut forward: Vec<Option<(usize, f64)>> = vec![None; earlier.len()];
    let mut backward: Vec<Option<(usize, f64)>> = vec![None; later.len()];
    for &(i, j, s) in &scored {
        if forward[i].is_none_or(|(_, best)| s < best) {
            forward[i] = Some((j, s));
        }
        if backward[j].is_none_or(|(_, best)| s < best) {
            backward[j] = Some((i, s));
        }
    }
    forward
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            let (j, _) = (*f)?;
            (backward[j].map(|(b, _)| b) == Some(i)).then_some((earlier[i], later[j]))
        })
        .collect()
}

fn continuations(motions: &[Motion], search: &FastSearch) -> Vec<(usize, usize)> {
    let mut by_night: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for (k, m) in motions.iter().enumerate() {
        by_night.entry(m.night).or_default().push(k);
    }
    let mut pairs: Vec<(&[usize], &[usize], i64)> = Vec::new();
    for (night, earlier) in &by_night {
        for gap in 1..=search.max_gap_nights {
            if let Some(later) = by_night.get(&(night + gap)) {
                pairs.push((earlier, later, gap));
            }
        }
    }
    pairs
        .par_iter()
        .flat_map_iter(|&(earlier, later, gap)| mutual_best(earlier, later, gap, motions))
        .collect()
}

fn chains(motions: &[Motion], edges: &[(usize, usize)], search: &FastSearch) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..motions.len()).collect();
    fn root(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }
    for &(a, b) in edges {
        let (ra, rb) = (root(&mut parent, a), root(&mut parent, b));
        if ra != rb {
            parent[ra] = rb;
        }
    }
    let linked: BTreeSet<usize> = edges.iter().flat_map(|&(a, b)| [a, b]).collect();
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for k in linked {
        groups.entry(root(&mut parent, k)).or_default().push(k);
    }
    let (fitting, oversized): (Vec<Vec<usize>>, Vec<Vec<usize>>) = groups
        .into_values()
        .filter(|g| {
            g.iter()
                .map(|&k| motions[k].night)
                .collect::<HashSet<_>>()
                .len()
                >= search.min_nights
        })
        .partition(|g| g.len() <= search.max_chain_tracklets);
    if !oversized.is_empty() {
        info!(
            oversized = oversized.len(),
            "fast chains too large to fit, skipped"
        );
    }
    fitting
}

fn longest_path(
    chain: &[usize],
    next: &HashMap<usize, Vec<usize>>,
    motions: &[Motion],
    tracklets: &[Tracklet],
) -> Vec<usize> {
    let mut order = chain.to_vec();
    order.sort_by(|&a, &b| motions[a].jd.total_cmp(&motions[b].jd));
    let mut length: HashMap<usize, usize> = chain.iter().map(|&k| (k, 1)).collect();
    let mut previous: HashMap<usize, usize> = HashMap::new();
    for &k in &order {
        for &n in next.get(&k).into_iter().flatten() {
            if length[&k] + 1 > length[&n] {
                length.insert(n, length[&k] + 1);
                previous.insert(n, k);
            }
        }
    }
    let Some(&end) = order
        .iter()
        .max_by_key(|&&k| (length[&k], tracklets[k].ids.len()))
    else {
        return Vec::new();
    };
    let mut path = vec![end];
    let mut at = end;
    while let Some(&p) = previous.get(&at) {
        path.push(p);
        at = p;
    }
    path.reverse();
    path
}

fn seed_states(t: &Tracklet, site: &Site) -> Vec<State> {
    let step = 0.01;
    let (at, turning) = line_of_sight(t);
    let observer = observer_position(t.jd_ref, site);
    let observer_velocity = combine(
        &observer_position(t.jd_ref + step, site),
        0.5 / step,
        &observer_position(t.jd_ref - step, site),
        -0.5 / step,
    );
    let (first, last) = SEED_RANGES_AU;
    let ratio = (last / first).powf(1.0 / (SEED_RANGES - 1) as f64);
    (0..SEED_RANGES)
        .flat_map(|k| {
            let range = first * ratio.powi(k as i32);
            SEED_RANGE_RATES.map(|range_rate| State {
                pos: combine(&observer, 1.0, &at, range),
                vel: combine(
                    &combine(&observer_velocity, 1.0, &at, range_rate),
                    1.0,
                    &turning,
                    range,
                ),
            })
        })
        .collect()
}

fn fit_path(
    path: &[usize],
    tracklets: &[Tracklet],
    by_id: &HashMap<i64, &Detection>,
    search: &FastSearch,
) -> Option<OrbitFit> {
    let ids: BTreeSet<i64> = path
        .iter()
        .flat_map(|&k| tracklets[k].ids.iter().copied())
        .collect();
    let mut observations: Vec<Observation> = ids
        .iter()
        .filter_map(|id| by_id.get(id))
        .map(|d| Observation {
            jd: d.jd,
            ra: d.ra,
            dec: d.dec,
            site: d.site.unwrap_or(ZTF),
        })
        .collect();
    if observations.len() < search.min_detections.max(3) {
        return None;
    }
    observations.sort_by(|a, b| a.jd.total_cmp(&b.jd));
    let seed = &tracklets[path[path.len() / 2]];
    let site = seed
        .ids
        .iter()
        .find_map(|id| by_id.get(id))
        .and_then(|d| d.site)
        .unwrap_or(ZTF);
    let mut seeds: Vec<(f64, State)> = seed_states(seed, &site)
        .into_iter()
        .filter_map(|s| Some((rms_arcsec(&s, seed.jd_ref, &observations)?, s)))
        .filter(|(rms, _)| rms.is_finite())
        .collect();
    seeds.sort_by(|a, b| a.0.total_cmp(&b.0));
    let gate = search.max_residual_arcsec;
    seeds
        .into_iter()
        .take(SEEDS_FITTED)
        .filter_map(|(_, s)| {
            let screened = fit_within(&observations, &s, seed.jd_ref, gate)?;
            if screened.rms_arcsec <= gate || screened.rms_arcsec > GIVE_UP_FACTOR * gate {
                return Some(screened);
            }
            // The screen reads a close object's slow crawl to its minimum as convergence.
            converge_orbit(
                &observations,
                &screened.state,
                seed.jd_ref,
                CONVERGE_ITERATIONS,
            )
            .or(Some(screened))
        })
        .min_by(|a, b| a.rms_arcsec.total_cmp(&b.rms_arcsec))
}

fn fit_chain(
    chain: &[usize],
    next: &HashMap<usize, Vec<usize>>,
    motions: &[Motion],
    tracklets: &[Tracklet],
    by_id: &HashMap<i64, &Detection>,
    search: &FastSearch,
) -> Option<FastTrack> {
    let mut path = longest_path(chain, next, motions, tracklets);
    if path.len() < search.min_nights {
        return None;
    }
    let passes = |fit: &OrbitFit| fit.rms_arcsec <= search.max_residual_arcsec;
    let mut fit = fit_path(&path, tracklets, by_id, search);
    while !fit.as_ref().is_some_and(passes) && path.len() > search.min_nights {
        let (k, better) = (0..path.len())
            .filter_map(|k| {
                let mut fewer = path.clone();
                fewer.remove(k);
                Some((k, fit_path(&fewer, tracklets, by_id, search)?))
            })
            .min_by(|a, b| a.1.rms_arcsec.total_cmp(&b.1.rms_arcsec))?;
        path.remove(k);
        fit = Some(better);
    }
    let fit = fit.filter(passes)?;
    let members: BTreeSet<i64> = path
        .iter()
        .flat_map(|&k| tracklets[k].ids.iter().copied())
        .collect();
    Some(FastTrack {
        members: members.into_iter().collect(),
        fit,
    })
}

pub fn fast_tracks(
    detections: &[Detection],
    trails: &HashMap<i64, f64>,
    search: &FastSearch,
) -> Vec<FastTrack> {
    let started = std::time::Instant::now();
    let tracklets = longest_first(fast_tracklets(detections, trails, search));
    let motions: Vec<Motion> = tracklets.par_iter().map(Motion::of).collect();
    let edges = continuations(&motions, search);
    let mut next: HashMap<usize, Vec<usize>> = HashMap::new();
    for &(a, b) in &edges {
        next.entry(a).or_default().push(b);
    }
    let chains = chains(&motions, &edges, search);
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    let tracks: Vec<FastTrack> = chains
        .par_iter()
        .filter_map(|chain| fit_chain(chain, &next, &motions, &tracklets, &by_id, search))
        .collect();
    info!(
        tracklets = tracklets.len(),
        continuations = edges.len(),
        chains = chains.len(),
        tracks = tracks.len(),
        seconds = started.elapsed().as_secs_f64(),
        "fast movers linked"
    );
    tracks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::heliolinc::state_to_elements;
    use crate::utils::orbit_fit::predict_radec;
    use crate::utils::sso_geometry::earth_position;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    #[test]
    fn test_a_fast_neo_among_random_detections_is_linked_and_fitted() {
        let epoch = 2_461_300.75;
        let step = 0.01;
        let earth = earth_position(epoch);
        let earth_velocity = combine(
            &earth_position(epoch + step),
            0.5 / step,
            &earth_position(epoch - step),
            -0.5 / step,
        );
        let look = unit_vector(40.0, 15.0);
        let west = unit(combine(
            &unit_vector(39.9, 15.0),
            1.0,
            &unit_vector(40.1, 15.0),
            -1.0,
        ));
        let truth = State {
            pos: combine(&earth, 1.0, &look, 0.08),
            vel: combine(
                &combine(&earth_velocity, 1.0, &west, 0.008),
                1.0,
                &look,
                -0.002,
            ),
        };
        assert!(state_to_elements(&truth, epoch).is_some());

        let mut rng = StdRng::seed_from_u64(7);
        let visits = [0.0, 0.02, 0.04, 1.0, 1.03, 3.0, 3.02, 3.04];
        let mut detections: Vec<Detection> = visits
            .iter()
            .enumerate()
            .map(|(k, &dt)| {
                let (ra, dec) = predict_radec(&truth, epoch, epoch + dt, &ZTF).unwrap();
                Detection {
                    id: k as i64 + 1,
                    jd: epoch + dt,
                    ra: ra + rng.random_range(-0.15..0.15) / 3600.0,
                    dec: dec + rng.random_range(-0.15..0.15) / 3600.0,
                    mag: Some(18.5 + rng.random_range(-0.05..0.05)),
                    mag_err: Some(0.05),
                    band: Some('r'),
                    site: None,
                }
            })
            .collect();
        let neo: Vec<i64> = detections.iter().map(|d| d.id).collect();
        for night in 0..4 {
            for visit in 0..5 {
                let jd = epoch + night as f64 + 0.02 * visit as f64;
                for _ in 0..400 {
                    detections.push(Detection {
                        id: 1000 + detections.len() as i64,
                        jd,
                        ra: rng.random_range(10.0..55.0),
                        dec: rng.random_range(0.0..30.0),
                        mag: Some(rng.random_range(17.0..21.0)),
                        mag_err: Some(rng.random_range(0.05..0.15)),
                        band: Some(if rng.random_bool(0.5) { 'g' } else { 'r' }),
                        site: None,
                    });
                }
            }
        }

        let tracks = fast_tracks(&detections, &HashMap::new(), &FastSearch::default());
        assert_eq!(tracks.len(), 1, "{tracks:?}");
        assert_eq!(tracks[0].members, neo);
        assert!(
            tracks[0].fit.rms_arcsec < 1.0,
            "{}",
            tracks[0].fit.rms_arcsec
        );
    }
}
