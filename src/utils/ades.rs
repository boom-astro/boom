//! Render tracklets as ADES, the astrometry format the Minor Planet Center takes.
//!
//! Unlinked tracklets go to the MPC's Isolated Tracklet File, where each is
//! identified only by a `trkSub` of our own choosing; the MPC does the linking
//! and assigns any designation. So nothing here needs an orbit.

use crate::utils::linking::{Detection, Tracklet};
use std::collections::HashMap;

/// Who is submitting, and through which telescope.
#[derive(Debug, Clone)]
pub struct SubmissionHeader {
    /// MPC observatory code, e.g. `I41` for ZTF at Palomar.
    pub mpc_code: String,
    pub submitter: String,
    pub observers: String,
    pub measurers: String,
    pub telescope_design: String,
    /// Metres.
    pub telescope_aperture: f64,
    pub telescope_detector: String,
    /// Astrometric reference catalogue the positions are on.
    pub ast_cat: String,
}

/// Positional uncertainty quoted per detection, arcseconds.
#[derive(Debug, Clone, Copy)]
pub struct Uncertainty {
    pub rms_ra_arcsec: f64,
    pub rms_dec_arcsec: f64,
}

/// UTC timestamp for a Julian date, to millisecond precision.
pub fn jd_to_iso8601(jd: f64) -> String {
    let unix_seconds = (jd - 2_440_587.5) * 86_400.0;
    let whole = unix_seconds.floor();
    let millis = ((unix_seconds - whole) * 1000.0).round() as u32;
    // A rounded millisecond can reach 1000, which chrono will not accept.
    let (whole, millis) = if millis >= 1000 {
        (whole + 1.0, 0)
    } else {
        (whole, millis)
    };
    let dt = chrono::DateTime::from_timestamp(whole as i64, millis * 1_000_000)
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).expect("epoch is valid"));
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// A tracklet's temporary identifier, unique within a submission.
///
/// The MPC only requires it be consistent across the detections of one
/// tracklet and distinct between them, so the first detection's id serves.
pub fn track_sub(tracklet: &Tracklet) -> String {
    let seed = tracklet.ids.first().copied().unwrap_or_default();
    // Base-36 of the low bits keeps it inside the 8 characters ADES allows.
    let mut n = (seed.unsigned_abs() % 36_u64.pow(7)).max(1);
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    while n > 0 {
        out.push(digits[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    format!("t{}", String::from_utf8_lossy(&out))
}

fn header_block(header: &SubmissionHeader) -> String {
    format!(
        "# version=2017\n\
         # observatory\n\
         ! mpcCode {}\n\
         # submitter\n\
         ! name {}\n\
         # observers\n\
         ! name {}\n\
         # measurers\n\
         ! name {}\n\
         # telescope\n\
         ! design {}\n\
         ! aperture {:.1}\n\
         ! detector {}\n",
        header.mpc_code,
        header.submitter,
        header.observers,
        header.measurers,
        header.telescope_design,
        header.telescope_aperture,
        header.telescope_detector,
    )
}

const COLUMNS: &str =
    "trkSub |mode|stn |obsTime                 |ra          |dec         |rmsRA|rmsDec|mag  |band|astCat";

/// One observation row.
fn row(
    trk_sub: &str,
    detection: &Detection,
    header: &SubmissionHeader,
    uncertainty: Uncertainty,
) -> String {
    let mag = match detection.mag {
        Some(m) => format!("{m:5.2}"),
        None => "     ".to_string(),
    };
    let band = match detection.band {
        Some(b) => format!("{b}   "),
        None => "    ".to_string(),
    };
    format!(
        "{:<7}|{:<4}|{:<4}|{:<24}|{:>12.7}|{:>+12.7}|{:>5.2}|{:>6.2}|{}|{}|{}",
        trk_sub,
        "CCD",
        header.mpc_code,
        jd_to_iso8601(detection.jd),
        detection.ra,
        detection.dec,
        uncertainty.rms_ra_arcsec,
        uncertainty.rms_dec_arcsec,
        mag,
        band,
        header.ast_cat,
    )
}

/// Render tracklets as an ADES PSV submission.
///
/// `detections` supplies the astrometry the tracklets refer to by id; a
/// tracklet whose detections are all missing is skipped rather than reported
/// with holes in it.
pub fn to_psv(
    tracklets: &[Tracklet],
    detections: &[Detection],
    header: &SubmissionHeader,
    uncertainty: Uncertainty,
) -> String {
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();

    let mut out = header_block(header);
    out.push_str(COLUMNS);
    out.push('\n');
    for tracklet in tracklets {
        let trk_sub = track_sub(tracklet);
        let mut rows: Vec<&Detection> = tracklet
            .ids
            .iter()
            .filter_map(|id| by_id.get(id).copied())
            .collect();
        if rows.is_empty() {
            continue;
        }
        rows.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap_or(std::cmp::Ordering::Equal));
        for detection in rows {
            out.push_str(&row(&trk_sub, detection, header, uncertainty));
            out.push('\n');
        }
    }
    out
}

/// ZTF at Palomar, as the MPC lists it.
pub fn ztf_header(submitter: &str, observers: &str) -> SubmissionHeader {
    SubmissionHeader {
        mpc_code: "I41".to_string(),
        submitter: submitter.to_string(),
        observers: observers.to_string(),
        measurers: submitter.to_string(),
        telescope_design: "reflector".to_string(),
        telescope_aperture: 1.2,
        telescope_detector: "CCD".to_string(),
        ast_cat: "Gaia2".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detections() -> Vec<Detection> {
        vec![
            Detection {
                id: 1,
                jd: 2461292.75,
                ra: 258.382572,
                dec: 54.383199,
                mag: Some(19.53),
                band: Some('g'),
            },
            Detection {
                id: 2,
                jd: 2461292.80,
                ra: 258.392572,
                dec: 54.384199,
                mag: Some(19.61),
                band: Some('g'),
            },
            Detection {
                id: 3,
                jd: 2461292.85,
                ra: 258.402572,
                dec: 54.385199,
                mag: None,
                band: None,
            },
        ]
    }

    fn tracklet() -> Tracklet {
        Tracklet::from_motion(
            vec![1, 2, 3],
            2461292.80,
            258.392572,
            54.384199,
            0.2,
            0.02,
            0.1,
        )
    }

    #[test]
    fn test_jd_to_iso8601_matches_a_known_epoch() {
        // JD 2440587.5 is the Unix epoch by definition.
        assert_eq!(jd_to_iso8601(2440587.5), "1970-01-01T00:00:00.000Z");
        assert_eq!(jd_to_iso8601(2451545.0), "2000-01-01T12:00:00.000Z");
    }

    #[test]
    fn test_psv_has_a_row_per_detection() {
        let psv = to_psv(
            &[tracklet()],
            &detections(),
            &ztf_header("M. Coughlin", "ZTF"),
            Uncertainty {
                rms_ra_arcsec: 0.15,
                rms_dec_arcsec: 0.15,
            },
        );
        let rows: Vec<&str> = psv
            .lines()
            .filter(|l| !l.starts_with('#') && !l.starts_with('!') && !l.starts_with("trkSub"))
            .collect();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.contains("I41")));
    }

    #[test]
    fn test_one_tracklet_shares_one_trksub() {
        let psv = to_psv(
            &[tracklet()],
            &detections(),
            &ztf_header("M. Coughlin", "ZTF"),
            Uncertainty {
                rms_ra_arcsec: 0.15,
                rms_dec_arcsec: 0.15,
            },
        );
        let subs: std::collections::HashSet<&str> = psv
            .lines()
            .filter(|l| !l.starts_with('#') && !l.starts_with('!') && !l.starts_with("trkSub"))
            .map(|l| l.split('|').next().unwrap().trim())
            .collect();
        assert_eq!(subs.len(), 1);
        assert!(subs.iter().next().unwrap().len() <= 8);
    }

    #[test]
    fn test_distinct_tracklets_get_distinct_trksubs() {
        let a = tracklet();
        let b = Tracklet::from_motion(vec![2, 3], 2461292.82, 258.4, 54.385, 0.2, 0.02, 0.1);
        assert_ne!(track_sub(&a), track_sub(&b));
    }

    #[test]
    fn test_rows_are_ordered_by_time() {
        let mut shuffled = detections();
        shuffled.reverse();
        let psv = to_psv(
            &[tracklet()],
            &shuffled,
            &ztf_header("M. Coughlin", "ZTF"),
            Uncertainty {
                rms_ra_arcsec: 0.15,
                rms_dec_arcsec: 0.15,
            },
        );
        let times: Vec<String> = psv
            .lines()
            .filter(|l| !l.starts_with('#') && !l.starts_with('!') && !l.starts_with("trkSub"))
            .map(|l| l.split('|').nth(3).unwrap().trim().to_string())
            .collect();
        let mut sorted = times.clone();
        sorted.sort();
        assert_eq!(times, sorted);
    }

    #[test]
    fn test_a_detection_without_photometry_still_reports_astrometry() {
        let psv = to_psv(
            &[tracklet()],
            &detections(),
            &ztf_header("M. Coughlin", "ZTF"),
            Uncertainty {
                rms_ra_arcsec: 0.15,
                rms_dec_arcsec: 0.15,
            },
        );
        // The third detection carries no magnitude, so those columns stay blank.
        let last = psv.lines().last().expect("a row");
        let fields: Vec<&str> = last.split('|').collect();
        assert!(fields[8].trim().is_empty(), "mag should be blank");
        assert!(fields[9].trim().is_empty(), "band should be blank");
        assert!(!fields[4].trim().is_empty(), "ra must still be present");
    }
}
