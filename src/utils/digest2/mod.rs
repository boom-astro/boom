//! Port of the MPC's digest2 and its NEO population model (MIT, see LICENSE).

use crate::utils::linking::Detection;
use crate::utils::sso_geometry::{Site, ZTF};
use std::f64::consts::PI;
use std::sync::LazyLock;

const QX: usize = 29;
const EX: usize = 8;
const IX: usize = 11;
const HX: usize = 18;
const BINS: usize = QX * EX * IX * HX;

const QPART: [f64; QX] = [
    0.4, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.67, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.2,
    3.5, 4.0, 4.5, 5.0, 5.5, 10.0, 20.0, 30.0, 40.0, 100.0,
];
const EPART: [f64; EX] = [0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 0.9, 1.1];
const IPART: [f64; IX] = [
    2.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 40.0, 60.0, 90.0, 180.0,
];
const HPART: [f64; HX] = [
    6.0, 8.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0,
    24.0, 25.5,
];

const MIN_DISTANCE_AU: f64 = 0.05;
const MAX_DISTANCE_AU: f64 = 100.0;
const MIN_DISTANCE_STEP_AU: f64 = 0.2;
const MIN_ANGLE_STEP: f64 = 0.1;
const AGE_LIMIT: u32 = 1;
const K: f64 = 0.01720209895;
const INV_K: f64 = 1.0 / K;
const U: f64 = K * K;
const TWO_PI: f64 = PI * 2.0;
const ARCSEC: f64 = PI / (180.0 * 3600.0);
const LCG_A: u64 = 302_875_106_592_253;
const LCG_MASK: u64 = (1 << 59) - 1;
const INV_LCG_M: f64 = 1.0 / (1u64 << 59) as f64;
const SEED: u64 = 3;
const EARTH_RADIUS_AU: f64 = 6.37814e6 / 149.59787e9;
const DEFAULT_OBS_ERR: f64 = ARCSEC;
const DEFAULT_V: f64 = 21.0;
const NEO_Q_AU: f64 = 1.3;
const SAME_ARC_DAYS: f64 = 0.125;

const OBS_ERR_ARCSEC: [(&str, f64); 35] = [
    ("106", 0.4),
    ("291", 0.4),
    ("568", 0.2),
    ("691", 0.4),
    ("703", 0.7),
    ("704", 0.7),
    ("A50", 0.5),
    ("C51", 0.7),
    ("D29", 0.8),
    ("E12", 0.5),
    ("F51", 0.1),
    ("F52", 0.1),
    ("G96", 0.3),
    ("H15", 0.5),
    ("J75", 0.4),
    ("T05", 0.5),
    ("T08", 0.5),
    ("W94", 0.4),
    ("W16", 0.4),
    ("V00", 0.2),
    ("381", 0.8),
    ("K88", 0.4),
    ("I41", 0.8),
    ("U68", 0.5),
    ("J04", 0.4),
    ("807", 0.4),
    ("H21", 0.4),
    ("T09", 0.1),
    ("T12", 0.1),
    ("T14", 0.1),
    ("J95", 0.5),
    ("Y00", 0.5),
    ("L01", 0.7),
    ("I52", 0.5),
    ("V06", 0.3),
];

static MODEL: LazyLock<Model> = LazyLock::new(|| Model::parse(include_str!("neo_model.csv")));

struct Model {
    all: Vec<f64>,
    unknown: Vec<f64>,
    all_neo: Vec<f64>,
    unknown_neo: Vec<f64>,
}

fn bin(iq: usize, ie: usize, ii: usize, ih: usize) -> usize {
    ((iq * EX + ie) * IX + ii) * HX + ih
}

fn part_of(parts: &[f64], value: &str) -> usize {
    let value: f64 = value.parse().expect("digest2 model bin");
    parts
        .iter()
        .position(|p| (p - value).abs() < 1e-9)
        .expect("digest2 model bin")
}

impl Model {
    fn parse(csv: &str) -> Self {
        let mut model = Model {
            all: vec![0.0; BINS],
            unknown: vec![0.0; BINS],
            all_neo: vec![0.0; BINS],
            unknown_neo: vec![0.0; BINS],
        };
        for line in csv.lines().skip(1) {
            let fields: Vec<&str> = line.split(',').collect();
            let (iq, ie, ii) = (
                part_of(&QPART, fields[2]),
                part_of(&EPART, fields[3]),
                part_of(&IPART, fields[4]),
            );
            let table = match (fields[0], fields[1]) {
                ("All", "SS") => &mut model.all,
                ("Unk", "SS") => &mut model.unknown,
                ("All", "NEO") => &mut model.all_neo,
                ("Unk", "NEO") => &mut model.unknown_neo,
                _ => continue,
            };
            for (ih, value) in fields[5..].iter().enumerate() {
                if !value.is_empty() {
                    table[bin(iq, ie, ii, ih)] = value.parse().expect("digest2 model count");
                }
            }
        }
        model
    }
}

fn part_index(parts: &[f64], x: f64) -> Option<usize> {
    parts.iter().position(|&p| !(x >= p))
}

fn h_bin(h: f64) -> usize {
    HPART[..HX - 1]
        .iter()
        .position(|&p| !(h >= p))
        .unwrap_or(HX - 1)
}

#[derive(Debug, Clone, Copy)]
pub struct Observation {
    pub mjd: f64,
    pub ra: f64,
    pub dec: f64,
    pub vmag: f64,
    pub site: Site,
}

pub fn v_magnitude(band: Option<char>, mag: Option<f64>) -> f64 {
    let Some(mag) = mag.filter(|m| *m > 0.0) else {
        return 0.0;
    };
    mag + match band {
        Some('V') | Some('v') => 0.0,
        Some('B') => -0.8,
        Some('U') => -1.3,
        Some('g') => -0.35,
        Some('r') => 0.14,
        Some('R') | Some('C') | Some('W') => 0.4,
        Some('i') => 0.32,
        Some('z') => 0.26,
        Some('I') => 0.8,
        Some('J') => 1.2,
        Some('w') => -0.13,
        Some('y') => 0.32,
        Some('L') => 0.2,
        Some('H') => 1.4,
        Some('K') => 1.7,
        Some('Y') => 0.7,
        Some('G') => 0.28,
        Some('c') => -0.05,
        Some('o') => 0.33,
        Some('u') => 2.5,
        _ => 0.4,
    }
}

impl From<&Detection> for Observation {
    fn from(d: &Detection) -> Self {
        Observation {
            mjd: d.jd - 2_400_000.5,
            ra: d.ra.to_radians(),
            dec: d.dec.to_radians(),
            vmag: v_magnitude(d.band, d.mag),
            site: d.site.unwrap_or(ZTF),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NeoScore {
    pub noid: f64,
    pub raw: f64,
}

fn dot(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn cross(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn ec_rotate(c: &mut [f64; 3], soe: f64, coe: f64) {
    let e1 = c[2] * soe + c[1] * coe;
    c[2] = c[2] * coe - c[1] * soe;
    c[1] = e1;
}

fn sun_earth(mjd: f64) -> ([f64; 3], f64, f64) {
    let d = mjd - 51544.5;
    let g = 357.529 + 0.98560028 * d;
    let q = 280.459 + 0.98564736 * d;
    let g2 = g + g;
    let l = q + 1.915 * (g * PI / 180.0).sin() + 0.020 * (g2 * PI / 180.0).sin();
    let r = 1.00014 - 0.01671 * (g * PI / 180.0).cos() - 0.00014 * (g2 * PI / 180.0).cos();
    let e = 23.439 - 0.00000036 * d;
    let soe = (e * PI / 180.0).sin();
    let coe = (e * PI / 180.0).cos();
    let x = r * (l * PI / 180.0).cos();
    let y = r * (l * PI / 180.0).sin();
    ([x, y * coe, y * soe], soe, coe)
}

fn lst(j0: f64, longitude: f64) -> f64 {
    let t = (j0 - 15019.5) / 36525.0;
    let th = (6.6460656 + (2400.051262 + 0.00002581 * t) * t) / 24.0;
    let ut = 1.0 % (j0 - 0.5);
    (th + ut + longitude) % TWO_PI
}

fn sphr_to_cart(s: [f64; 2]) -> [f64; 3] {
    let t = s[1].cos();
    [t * s[0].cos(), t * s[0].sin(), s[1].sin()]
}

fn cart_to_sphr(c: [f64; 3]) -> [f64; 2] {
    [(c[1].atan2(c[0]) + TWO_PI) % TWO_PI, c[2].asin()]
}

fn rotate(m: &[[f64; 3]; 3], a: [f64; 3]) -> [f64; 3] {
    let mut out = [0.0; 3];
    for (r, row) in m.iter().enumerate() {
        out[r] = row[0] * a[0] + row[1] * a[1] + row[2] * a[2];
    }
    out
}

struct GreatCircle {
    derotate: [[f64; 3]; 3],
    ra0: f64,
    t0: f64,
    rs: Vec<[f64; 2]>,
    ntime: Vec<f64>,
    r0: f64,
    rr: f64,
    d0: f64,
    dr: f64,
}

impl GreatCircle {
    fn fit(obs: &[Observation]) -> Option<Self> {
        let n = obs.len();
        let cart: Vec<[f64; 3]> = obs.iter().map(|o| sphr_to_cart([o.ra, o.dec])).collect();
        let normal = cross(&cart[0], &cart[n - 1]);
        let nmag2 = dot(&normal, &normal);
        let nmag = nmag2.sqrt();
        let nxy = 1.0 / (normal[0] * normal[0] + normal[1] * normal[1]).sqrt();
        let gcix = normal[1] * nxy;
        let gciy = -normal[0] * nxy;
        let sina = (nmag2 - normal[2] * normal[2]).sqrt() / nmag;
        let cosa = normal[2] / nmag;
        let sinagx = sina * gcix;
        let sinagy = sina * gciy;
        let onemcosa = 1.0 - cosa;
        let onemcosagx = onemcosa * gcix;
        let onemcosagxgy = onemcosagx * gciy;
        let rot = [
            [cosa + onemcosagx * gcix, onemcosagxgy, sinagy],
            [onemcosagxgy, cosa + onemcosa * gciy * gciy, -sinagx],
            [-sinagy, sinagx, cosa],
        ];
        let rotated: Vec<[f64; 3]> = cart.iter().map(|c| rotate(&rot, *c)).collect();
        if rotated[0][2].abs() > ARCSEC || rotated[n - 1][2].abs() > ARCSEC {
            return None;
        }
        let mut derotate = rot;
        for r in 0..3 {
            for c in 0..3 {
                derotate[r][c] = rot[c][r];
            }
        }
        let mut rs: Vec<[f64; 2]> = rotated.iter().map(|c| cart_to_sphr(*c)).collect();
        let ra0 = rs[0][0];
        for s in rs.iter_mut() {
            s[0] = (s[0] + 3.0 * PI - ra0) % TWO_PI - PI;
        }
        let t0 = obs[0].mjd;
        let ntime: Vec<f64> = obs.iter().map(|o| o.mjd - t0).collect();
        let (r0, rr, d0, dr) = if n == 2 {
            (0.0, rs[1][0] / ntime[1], 0.0, 0.0)
        } else {
            let (mut sumt, mut sumra, mut sumdec) = (0.0, 0.0, 0.0);
            let (mut sumt2, mut sumtra, mut sumtdec) = (0.0, 0.0, 0.0);
            for (t, s) in ntime.iter().zip(rs.iter()) {
                sumt += t;
                sumra += s[0];
                sumdec += s[1];
                sumt2 += t * t;
                sumtra += t * s[0];
                sumtdec += t * s[1];
            }
            let nf = n as f64;
            let invd = 1.0 / (nf * sumt2 - sumt * sumt);
            (
                invd * (sumra * sumt2 - sumtra * sumt),
                invd * (nf * sumtra - sumra * sumt),
                invd * (sumdec * sumt2 - sumtdec * sumt),
                invd * (nf * sumtdec - sumdec * sumt),
            )
        };
        Some(GreatCircle {
            derotate,
            ra0,
            t0,
            rs,
            ntime,
            r0,
            rr,
            d0,
            dr,
        })
    }

    fn position(&self, t: f64) -> (f64, f64) {
        let nt = t - self.t0;
        let rsc = [self.r0 + self.rr * nt + self.ra0, self.d0 + self.dr * nt];
        let sc = cart_to_sphr(rotate(&self.derotate, sphr_to_cart(rsc)));
        (sc[0], sc[1])
    }

    fn rms_arcsec(&self) -> f64 {
        let n = self.rs.len();
        let mut s = 0.0;
        for i in 0..n {
            let computed = [
                self.r0 + self.rr * self.ntime[i] + self.ra0,
                self.d0 + self.dr * self.ntime[i],
            ];
            let observed = [self.rs[i][0] + self.ra0, self.rs[i][1]];
            let so = cart_to_sphr(rotate(&self.derotate, sphr_to_cart(observed)));
            let sc = cart_to_sphr(rotate(&self.derotate, sphr_to_cart(computed)));
            let dec = (so[1] - sc[1]) / ARCSEC;
            let ra = (so[0] - sc[0]) * sc[1].cos() / ARCSEC;
            s += ra * ra + dec * dec;
        }
        (s / n as f64).sqrt()
    }
}

fn one_obs(
    o1: usize,
    o2: usize,
    arcs_use_all_obs: bool,
    pt: f64,
    olist: &[Observation],
) -> Option<(Observation, f64)> {
    let mut result = olist[o1];
    if o1 == o2 {
        return Some((result, 0.0));
    }
    if o1 + 1 == o2 {
        if !arcs_use_all_obs {
            let dt = if o1 == 0 {
                result = olist[1];
                result.mjd - pt
            } else {
                pt - result.mjd
            };
            if dt < 0.0 {
                return Some((result, 0.0));
            }
        }
        result = olist[o1];
        let gc = GreatCircle::fit(&[olist[o1], olist[o2]])?;
        result.mjd = if arcs_use_all_obs {
            (olist[o1].mjd + olist[o2].mjd) * 0.5
        } else {
            pt
        };
        (result.ra, result.dec) = gc.position(result.mjd);
        return Some((result, 0.0));
    }
    let tr = if arcs_use_all_obs {
        let is = (o1 + o2) / 2;
        let mut tr = olist[is].mjd;
        if is + is < o1 + o2 {
            tr = (tr + olist[is + 1].mjd) * 0.5;
        }
        tr
    } else {
        let dt = if o1 == 0 {
            result = olist[o2];
            result.mjd - pt
        } else {
            result = olist[o1];
            pt - result.mjd
        };
        if dt < 0.0 {
            result.mjd
        } else {
            pt
        }
    };
    let gc = GreatCircle::fit(&olist[o1..=o2])?;
    result = olist[o1];
    result.mjd = tr;
    (result.ra, result.dec) = gc.position(tr);
    Some((result, gc.rms_arcsec()))
}

fn two_obs(olist: &[Observation]) -> Option<([Observation; 2], [f64; 2])> {
    let n = olist.len();
    let mut pair = [olist[0], olist[n - 1]];
    if n == 2 {
        return Some((pair, [0.0, 0.0]));
    }
    let gc = GreatCircle::fit(olist)?;
    let rms = gc.rms_arcsec();
    let all_same = olist
        .iter()
        .all(|o| o.site.mpc_code == olist[0].site.mpc_code);
    let sixth = (n - 1) as f64 / 6.0;
    let (whole, fs) = (sixth.trunc(), sixth.fract());
    let is = whole as usize;
    let t17 = olist[is].mjd + (olist[is + 1].mjd - olist[is].mjd) * fs;
    let is = n - 1 - is;
    let t83 = olist[is].mjd - (olist[is].mjd - olist[is - 1].mjd) * fs;
    // digest2 tests the arc length against itself, so one site always fits whole.
    if all_same {
        pair[0].mjd = t17;
        (pair[0].ra, pair[0].dec) = gc.position(t17);
        pair[1].mjd = t83;
        (pair[1].ra, pair[1].dec) = gc.position(t83);
        return Some((pair, [rms, rms]));
    }
    let (mut o1, mut o2) = (0, n - 1);
    let (code1, code2) = (olist[o1].site.mpc_code, olist[o2].site.mpc_code);
    let (t1, t2) = (olist[o1].mjd, olist[o2].mjd);
    loop {
        let dt1 = olist[o1 + 1].mjd - t1;
        if olist[o1 + 1].site.mpc_code != code1 || dt1 > SAME_ARC_DAYS {
            loop {
                let o = o2 - 1;
                if o == o1 || olist[o].site.mpc_code != code2 || t2 - olist[o].mjd > SAME_ARC_DAYS {
                    break;
                }
                o2 = o;
            }
            break;
        }
        let dt2 = t2 - olist[o2 - 1].mjd;
        if olist[o2 - 1].site.mpc_code != code2 || dt2 > SAME_ARC_DAYS {
            loop {
                let o = o1 + 1;
                if o == o2 || olist[o].site.mpc_code != code1 || olist[o].mjd - t1 > SAME_ARC_DAYS {
                    break;
                }
                o1 = o;
            }
            break;
        }
        if dt1 < dt2 {
            o1 += 1;
        } else {
            o2 -= 1;
        }
        if o2 <= o1 + 1 {
            break;
        }
    }
    let joined = o2 == o1 + 1;
    let (first, rms0) = one_obs(0, o1, joined, t17, olist)?;
    let (last, rms1) = one_obs(o2, n - 1, joined, t83, olist)?;
    Some(([first, last], [rms0, rms1]))
}

fn site_obs_err(site: &Site) -> f64 {
    OBS_ERR_ARCSEC
        .iter()
        .find(|(code, _)| *code == site.mpc_code)
        .map_or(DEFAULT_OBS_ERR, |(_, err)| err * ARCSEC)
}

fn clip_err(computed_rms_arcsec: f64, site: &Site) -> f64 {
    let default = site_obs_err(site);
    if default == 0.0 {
        return 0.0;
    }
    if computed_rms_arcsec == 0.0 {
        return default;
    }
    let computed = computed_rms_arcsec * ARCSEC;
    if default > computed {
        default
    } else {
        computed
    }
}

struct Search<'a> {
    model: &'a Model,
    pair: [Observation; 2],
    obs_err: [f64; 2],
    no_obs_err: bool,
    rand: u64,
    vmag: f64,
    soe: f64,
    coe: f64,
    invdt: f64,
    invdtsq: f64,
    sun_observer: [[f64; 3]; 2],
    observer_object_unit: [[f64; 3]; 2],
    sun_object0: [f64; 3],
    sun_object0_mag: f64,
    observer1_object0: [f64; 3],
    observer1_object0_mag: f64,
    observer1_object0_magsq: f64,
    tz: f64,
    hmag_bin: usize,
    d_any_tag: bool,
    d_tag: Vec<bool>,
    d_in: Vec<bool>,
    d_out: Vec<bool>,
    tag_in: Vec<bool>,
    tag_out: Vec<bool>,
    sum_all_in: f64,
    sum_unk_in: f64,
    sum_all_out: f64,
    sum_unk_out: f64,
}

impl Search<'_> {
    fn random(&mut self) -> f64 {
        self.rand = self.rand.wrapping_mul(LCG_A) & LCG_MASK;
        self.rand as f64 * INV_LCG_M
    }

    fn offset_motion_vector(&mut self, mut rx: i32, mut dx: i32) {
        for i in 0..2 {
            let err = self.obs_err[i];
            let dec = self.pair[i].dec + dx as f64 * err * 0.5;
            let cosdec = dec.cos();
            let ra = self.pair[i].ra + rx as f64 * err * 0.5 * cosdec;
            let mut v = [ra.cos() * cosdec, ra.sin() * cosdec, dec.sin()];
            ec_rotate(&mut v, self.soe, self.coe);
            self.observer_object_unit[i] = v;
            rx = -rx;
            dx = -dx;
        }
    }

    fn setup_distance(&mut self, d: f64) {
        let mut observer_object0 = [0.0; 3];
        for (i, o) in observer_object0.iter_mut().enumerate() {
            *o = self.observer_object_unit[0][i] * d;
        }
        for i in 0..3 {
            self.sun_object0[i] = self.sun_observer[0][i] + observer_object0[i];
        }
        self.sun_object0_mag = dot(&self.sun_object0, &self.sun_object0).sqrt();
        for i in 0..3 {
            self.observer1_object0[i] = self.sun_object0[i] - self.sun_observer[1][i];
        }
        self.observer1_object0_magsq = dot(&self.observer1_object0, &self.observer1_object0);
        self.observer1_object0_mag = self.observer1_object0_magsq.sqrt();

        let rdelta = d * self.sun_object0_mag;
        let cospsi = dot(&observer_object0, &self.sun_object0) / rdelta;
        let hmag = if cospsi > -0.9999 {
            let tanhalf = (1.0 - cospsi * cospsi).sqrt() / (1.0 + cospsi);
            let phi1 = (-3.33 * tanhalf.powf(0.63)).exp();
            let phi2 = (-1.87 * tanhalf.powf(1.22)).exp();
            self.vmag - 5.0 * rdelta.log10() + 2.5 * (0.85 * phi1 + 0.15 * phi2).log10()
        } else {
            30.0
        };
        self.hmag_bin = h_bin(hmag);
    }

    fn tag_angle(&mut self, an: f64) -> bool {
        let d2 = self.observer1_object0_mag * an.sin() / (PI - an - self.tz).sin();
        let mut v = [0.0; 3];
        for (i, vi) in v.iter_mut().enumerate() {
            *vi = (d2 * self.observer_object_unit[1][i] - self.observer1_object0[i])
                * self.invdt
                * INV_K;
        }
        let hv = cross(&self.sun_object0, &v);
        let hsq = dot(&hv, &hv);
        let hm = hsq.sqrt();
        let vsq = dot(&v, &v);
        let temp = 2.0 - self.sun_object0_mag * vsq;
        if self.sun_object0_mag > temp * 100.0 {
            return false;
        }
        let orbit_a = self.sun_object0_mag / temp;
        let inva = temp / self.sun_object0_mag;
        let orbit_e = (1.0 - hsq * inva).sqrt();
        if orbit_e > 0.99 {
            return false;
        }
        let orbit_i = if hv[2] >= hm {
            0.0
        } else {
            (hv[2] / hm).acos() * 180.0 / PI
        };
        let q = orbit_a * (1.0 - orbit_e);
        let (Some(iq), Some(ie), Some(ii)) = (
            part_index(&QPART, q),
            part_index(&EPART, orbit_e),
            part_index(&IPART, orbit_i),
        ) else {
            return false;
        };
        let b = bin(iq, ie, ii, self.hmag_bin);
        let tags = if q < NEO_Q_AU {
            &mut self.d_in
        } else {
            &mut self.d_out
        };
        if tags[b] {
            return false;
        }
        tags[b] = true;
        self.d_any_tag = true;
        self.d_tag[b] = true;
        true
    }

    fn a_range(&mut self, ang1: f64, ang2: f64, age: u32) {
        let d3 = (ang2 - ang1) / 3.0;
        let mid = ang1 + d3 + d3 * self.random();
        if self.tag_angle(mid) || d3 > MIN_ANGLE_STEP {
            self.a_range(ang1, mid, 0);
            self.a_range(mid, ang2, 0);
            return;
        }
        if age < AGE_LIMIT {
            self.a_range(ang1, mid, age + 1);
            self.a_range(mid, ang2, age + 1);
        }
    }

    fn angle_range(&mut self) -> Option<(f64, f64)> {
        let th = dot(&self.observer1_object0, &self.observer_object_unit[1])
            / self.observer1_object0_mag;
        self.tz = th.acos();
        let aa = self.invdtsq;
        let bb = (-2.0 * self.observer1_object0_mag * th) * aa;
        let cc = self.observer1_object0_magsq * aa - 2.0 * U / self.sun_object0_mag;
        let dsc = bb * bb - 4.0 * aa * cc;
        if !(dsc > 0.0) {
            return None;
        }
        let sd = dsc.sqrt();
        let inv2aa = 0.5 / aa;
        let angle = |sd1: f64| {
            let d2 = (-bb + sd1) * inv2aa;
            let d2s = d2 * d2;
            let nns =
                d2s + self.observer1_object0_magsq - 2.0 * d2 * self.observer1_object0_mag * th;
            let nn = nns.sqrt();
            let ca = (nns + self.observer1_object0_magsq - d2s)
                / (2.0 * nn * self.observer1_object0_mag);
            let sa = d2 * self.tz.sin() / nn;
            2.0 * sa.atan2(1.0 + ca)
        };
        Some((angle(-sd), angle(sd)))
    }

    fn search_angles(&mut self) -> bool {
        let Some((ang1, ang2)) = self.angle_range() else {
            return false;
        };
        self.a_range(ang1, ang2, 0);
        if !self.d_any_tag {
            return false;
        }
        let mut new_tag = false;
        for b in 0..BINS {
            if !self.d_tag[b] {
                continue;
            }
            if self.d_in[b] && !self.tag_in[b] {
                new_tag = true;
                self.tag_in[b] = true;
                self.sum_all_in += self.model.all_neo[b];
                self.sum_unk_in += self.model.unknown_neo[b];
            }
            if self.d_out[b] && !self.tag_out[b] {
                new_tag = true;
                self.tag_out[b] = true;
                self.sum_all_out += self.model.all[b] - self.model.all_neo[b];
                self.sum_unk_out += self.model.unknown[b] - self.model.unknown_neo[b];
            }
        }
        new_tag
    }

    fn search_distance(&mut self, d: f64) -> bool {
        self.d_tag.fill(false);
        self.d_in.fill(false);
        self.d_out.fill(false);
        self.d_any_tag = false;
        let mut new_tag = false;
        for ri in -1..=1 {
            for di in -1..=1 {
                self.offset_motion_vector(ri, di);
                self.setup_distance(d);
                if self.search_angles() {
                    new_tag = true;
                }
                if self.no_obs_err {
                    return new_tag;
                }
            }
        }
        new_tag
    }

    fn d_range(&mut self, d1: f64, d2: f64, age: u32) {
        let dmid = (d1 + d2) * 0.5;
        if self.search_distance(dmid) || d2 - d1 > MIN_DISTANCE_STEP_AU {
            self.d_range(d1, dmid, 0);
            self.d_range(dmid, d2, 0);
            return;
        }
        if age < AGE_LIMIT {
            self.d_range(d1, dmid, age + 1);
            self.d_range(dmid, d2, age + 1);
        }
    }
}

pub fn neo_score(observations: &[Observation]) -> Option<NeoScore> {
    let mut olist = observations.to_vec();
    olist.sort_by(|a, b| a.mjd.total_cmp(&b.mjd));
    let (first, last) = (olist.first()?, olist.last()?);
    if olist.len() < 2 || (first.ra == last.ra && first.dec == last.dec) {
        return None;
    }
    let mags: Vec<f64> = olist.iter().map(|o| o.vmag).filter(|m| *m > 0.0).collect();
    let vmag = if mags.is_empty() {
        DEFAULT_V
    } else {
        mags.iter().sum::<f64>() / mags.len() as f64
    };
    let (pair, rms) = two_obs(&olist)?;
    let dt = pair[1].mjd - pair[0].mjd;
    let invdt = 1.0 / dt;
    let mut search = Search {
        model: &MODEL,
        pair,
        obs_err: [0.0; 2],
        no_obs_err: false,
        rand: SEED,
        vmag,
        soe: 0.0,
        coe: 0.0,
        invdt,
        invdtsq: invdt * invdt,
        sun_observer: [[0.0; 3]; 2],
        observer_object_unit: [[0.0; 3]; 2],
        sun_object0: [0.0; 3],
        sun_object0_mag: 0.0,
        observer1_object0: [0.0; 3],
        observer1_object0_mag: 0.0,
        observer1_object0_magsq: 0.0,
        tz: 0.0,
        hmag_bin: 0,
        d_any_tag: false,
        d_tag: vec![false; BINS],
        d_in: vec![false; BINS],
        d_out: vec![false; BINS],
        tag_in: vec![false; BINS],
        tag_out: vec![false; BINS],
        sum_all_in: 0.0,
        sum_unk_in: 0.0,
        sum_all_out: 0.0,
        sum_unk_out: 0.0,
    };
    for (i, obs) in pair.iter().enumerate() {
        search.obs_err[i] = clip_err(rms[i], &obs.site);
        let (sun_earth, soe, coe) = sun_earth(obs.mjd);
        search.soe = soe;
        search.coe = coe;
        let longitude = obs.site.longitude_deg / 360.0;
        let rho_cos = obs.site.rho_cos_phi * EARTH_RADIUS_AU;
        let rho_sin = obs.site.rho_sin_phi * EARTH_RADIUS_AU;
        let th = lst(obs.mjd, longitude);
        let mut v = [
            rho_cos * th.cos() - sun_earth[0],
            rho_cos * th.sin() - sun_earth[1],
            rho_sin - sun_earth[2],
        ];
        ec_rotate(&mut v, soe, coe);
        search.sun_observer[i] = v;
    }
    search.no_obs_err = search.obs_err[0] == 0.0 && search.obs_err[1] == 0.0;
    search.search_distance(MIN_DISTANCE_AU);
    search.search_distance(MAX_DISTANCE_AU);
    search.d_range(MIN_DISTANCE_AU, MAX_DISTANCE_AU, 0);
    let score = |inside: f64, outside: f64| {
        let d = inside + outside;
        if d > 0.0 {
            100.0 * inside / d
        } else {
            100.0
        }
    };
    Some(NeoScore {
        noid: score(search.sum_unk_in, search.sum_unk_out),
        raw: score(search.sum_all_in, search.sum_all_out),
    })
}

pub fn neo_score_of(detections: &[&Detection]) -> Option<NeoScore> {
    let observations: Vec<Observation> = detections.iter().map(|d| Observation::from(*d)).collect();
    neo_score(&observations)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MT_LEMMON: Site = Site {
        longitude_deg: 249.21128,
        rho_cos_phi: 0.845111,
        rho_sin_phi: 0.533614,
        astrometric_sigma_arcsec: 0.0,
        mpc_code: "G96",
    };

    fn parse(line: &str, site: Site) -> Observation {
        let f = |a: usize, b: usize| line[a..b].trim().parse::<f64>().unwrap_or(0.0);
        let (year, month) = (f(15, 19) as i32, f(20, 22) as i32);
        let z = year + (month - 14) / 12;
        let days = [0, 306, 337, 0, 31, 61, 92, 122, 153, 184, 214, 245, 275][month as usize]
            + 365 * z
            + z / 4
            - z / 100
            + z / 400
            - 678_882;
        let ra = ((f(32, 34) * 60.0 + f(35, 37)) * 60.0 + f(38, 44)) * PI / (12.0 * 3600.0);
        let dec = ((f(45, 47) * 60.0 + f(48, 50)) * 60.0 + f(51, 56)) * PI / (180.0 * 3600.0);
        Observation {
            mjd: days as f64 + f(23, 32),
            ra,
            dec: if &line[44..45] == "-" { -dec } else { dec },
            vmag: v_magnitude(line[70..71].chars().next(), Some(f(65, 70))),
            site,
        }
    }

    fn scores(lines: &[&str], site: Site) -> (f64, f64) {
        let observations: Vec<Observation> = lines.iter().map(|l| parse(l, site)).collect();
        let score = neo_score(&observations).unwrap();
        (score.noid.round(), score.raw.round())
    }

    #[test]
    fn test_scores_match_the_digest2_binary() {
        let sample = [
            "     K16S99K 1C2022 12 25.38496508 32 36.283+17 10 35.94         21.98GV     G96",
            "     K16S99K 1C2022 12 25.39527308 32 35.635+17 10 37.27         21.72GV     G96",
            "     K16S99K 1C2022 12 25.40040208 32 35.473+17 10 37.38         21.31GV     G96",
        ];
        assert_eq!(scores(&sample, MT_LEMMON), (16.0, 10.0));
        let main_belt = [
            "     T000010  C2026 09 22.31803202 23 23.184+13 57 54.94         16.61r      I41",
            "     T000010  C2026 09 22.41520802 23 20.988+13 58 02.07         16.98g      I41",
            "     T000010  C2026 09 22.45300902 23 20.120+13 58 04.65         16.38i      I41",
        ];
        assert_eq!(scores(&main_belt, ZTF), (3.0, 2.0));
    }
}
