//! Catalina Sky Survey tracklets, as archived in the PDS bundle.
//!
//! CSS publishes the output of its own moving-object search, not just images:
//! `.mtds` files hold the tracklets its pipeline built, `.dets` the ones that
//! survived validation and `.rjct` the ones that did not. Reading them lets a
//! search link CSS tracklets alongside tracklets found here, which reaches
//! objects neither survey sees often enough on its own.
//!
//! Bundle layout is `tel/yyyy/yyMmmdd/`, where `tel` is the MPC code, so the
//! site an observation came from is known from its path.

use crate::utils::linking::{fit_tracklet, Detection, Tracklet, TrackletConfig};
use crate::utils::sso_geometry::Site;

/// One night's tracklets from one CSS field.
#[derive(Debug, Clone)]
pub struct CssTracklets {
    /// MPC code of the telescope, which fixes the site the astrometry is from.
    pub mpc_code: String,
    pub site: Site,
    pub tracklets: Vec<Tracklet>,
    pub detections: Vec<Detection>,
}

/// Parse a `.mtds`, `.dets` or `.rjct` file. They share the DETSV2 layout.
///
/// `mpc_code` comes from the archive path rather than the file, which carries
/// no site of its own. CSS supplies the grouping and [`fit_tracklet`] supplies
/// the fit, so the result is the same representation the linker already takes.
///
/// Detection ids are synthesised from the source id and the epoch: a CSS source
/// id is unique only within its exposure, so using it alone would collide
/// across a night.
pub fn parse_dets(
    text: &str,
    mpc_code: &str,
    cfg: &TrackletConfig,
) -> Result<CssTracklets, String> {
    let site = Site::from_mpc_code(mpc_code)
        .ok_or_else(|| format!("unknown MPC observatory code {mpc_code}"))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default().trim();
    if !header.starts_with("DETSV") {
        return Err(format!("not a DETS file: first line is {header:?}"));
    }

    let mut detections: Vec<Detection> = Vec::new();
    // Tracklets arrive as consecutive runs sharing a leading id, so members are
    // gathered by that id rather than assumed contiguous.
    let mut groups: Vec<(String, Vec<Detection>)> = Vec::new();
    for line in lines {
        let f: Vec<&str> = line.split_whitespace().collect();
        // The exposure manifest between the header and the detections is
        // `<file> <jd>`, which is two fields; a detection has many more.
        if f.len() < 9 {
            continue;
        }
        let (Ok(jd), Ok(ra_hours), Ok(dec), Ok(source_id)) = (
            f[3].parse::<f64>(),
            f[4].parse::<f64>(),
            f[5].parse::<f64>(),
            f[2].parse::<i64>(),
        ) else {
            continue;
        };
        // RA is in hours here, Dec in degrees; confirmed against the matching
        // `.mpcd`, whose sexagesimal RA is the same angle.
        let ra = ra_hours * 15.0;
        if !(0.0..=360.0).contains(&ra) || !(-90.0..=90.0).contains(&dec) {
            return Err(format!("position out of range on line {line:?}"));
        }
        let det = Detection {
            id: detection_id(source_id, jd),
            jd,
            ra,
            dec,
            mag: f[8].parse::<f64>().ok().filter(|m| *m > 0.0 && *m < 40.0),
            mag_err: None,
            band: None,
            // From the archive path, not the file: this is the whole point.
            site: Some(site),
        };
        detections.push(det);
        let key = f[0].to_string();
        match groups.last_mut() {
            Some((k, members)) if *k == key => members.push(det),
            _ => groups.push((key, vec![det])),
        }
    }

    let tracklets = groups
        .into_iter()
        .filter_map(|(_, mut members)| {
            members.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap_or(std::cmp::Ordering::Equal));
            fit_tracklet(&members, cfg)
        })
        .collect();

    Ok(CssTracklets {
        mpc_code: mpc_code.to_string(),
        site,
        tracklets,
        detections,
    })
}

/// A stable id for one detection.
///
/// CSS source ids repeat between exposures, so the epoch is mixed in. The
/// epoch is quantised to a millisecond, which is far finer than any cadence and
/// keeps the id reproducible across reads of the same file.
fn detection_id(source_id: i64, jd: f64) -> i64 {
    let millis = (jd * 86_400_000.0).round() as i64;
    // Keeps both parts in range: a CSS source id fits well inside 2^24.
    (millis.rem_euclid(1 << 39)) << 24 | (source_id.rem_euclid(1 << 24))
}

/// The public PDS mirror of the CSS archive, which needs no credentials.
const ARCHIVE: &str = "https://pds-css-archive.s3.us-west-2.amazonaws.com";
const BUNDLE: &str = "sbn/gbo.ast.catalina.survey/data_derived";

/// The archive path for one telescope's night, e.g. `G96/2025/25Sep30`.
///
/// Nights are named the way the bundle names them, which is the UTC date of
/// the following morning in `yyMmmdd`.
pub fn archive_prefix(mpc_code: &str, night: chrono::NaiveDate) -> String {
    format!(
        "{BUNDLE}/{mpc_code}/{}/{}",
        night.format("%Y"),
        night.format("%y%b%d")
    )
}

/// What one telescope-night's fetch found and took.
///
/// `found` separates a night the archive does not have yet from one already
/// cached, which otherwise look the same to a caller counting downloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fetched {
    /// Tracklet files the archive holds for the night.
    pub found: usize,
    /// Of those, the ones downloaded now rather than already present.
    pub fetched: usize,
}

/// Download one telescope-night's `.mtds` files into `dir`, skipping any
/// already there.
///
/// A night absent from the archive returns `found: 0` rather than an error:
/// PDS ingest lags observing by days to weeks, so most nights a nightly job
/// asks for will not be there, and that is expected rather than a failure.
/// `dir` is left uncreated in that case, so an empty directory never stands in
/// for a night that was fetched.
pub async fn fetch_night(
    client: &reqwest::Client,
    mpc_code: &str,
    night: chrono::NaiveDate,
    dir: &std::path::Path,
) -> Result<Fetched, String> {
    let prefix = archive_prefix(mpc_code, night);
    let listing = client
        .get(format!(
            "{ARCHIVE}/?list-type=2&max-keys=1000&prefix={prefix}/"
        ))
        .send()
        .await
        .map_err(|e| format!("listing {prefix}: {e}"))?
        .text()
        .await
        .map_err(|e| format!("listing {prefix}: {e}"))?;

    let keys = mtds_keys(&listing);
    if keys.is_empty() {
        return Ok(Fetched {
            found: 0,
            fetched: 0,
        });
    }
    std::fs::create_dir_all(dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    let mut fetched = 0;
    for key in keys.iter() {
        let Some(name) = key.rsplit('/').next() else {
            continue;
        };
        let path = dir.join(name);
        if path.exists() {
            continue;
        }
        let body = client
            .get(format!("{ARCHIVE}/{key}"))
            .send()
            .await
            .map_err(|e| format!("fetching {key}: {e}"))?
            .bytes()
            .await
            .map_err(|e| format!("fetching {key}: {e}"))?;
        std::fs::write(&path, &body).map_err(|e| format!("writing {}: {e}", path.display()))?;
        fetched += 1;
    }
    Ok(Fetched {
        found: keys.len(),
        fetched,
    })
}

/// The `.mtds` keys in an S3 listing.
///
/// Scanned rather than parsed as XML: the listing has one element worth
/// reading and a parser would be a dependency for nothing.
fn mtds_keys(listing: &str) -> Vec<String> {
    listing
        .split("<Key>")
        .skip(1)
        .filter_map(|chunk| chunk.split("</Key>").next())
        .filter(|key| key.ends_with(".mtds"))
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One real field from G96 on 2025-09-30, trimmed to two tracklets. The
    /// positions are the archived ones, so the units are pinned by real data.
    const SAMPLE: &str = "DETSV2.0
4 / 4
G96_20250930_2B_FHAUM2_01_0001.arch.fz 2460948.72339281
G96_20250930_2B_FHAUM2_01_0002.arch.fz 2460948.73249624
0001      1      46753 2460948.72339281 23.970943  42.77386   2826.223   2461.065  20.77     2.33    1.415 -84.2
0001      2      50142 2460948.73249624 23.970900  42.76982   2822.627   2554.892  20.80     1.45    1.125   6.5
0002      1      97313 2460948.72339281 23.897884  43.80877   4732.821   4902.604  21.62     0.54    1.015 -72.4
0002      2     105048 2460948.73249624 23.897713  43.80590   4732.590   4999.934  22.40     0.10    1.999   0.0
";

    /// The archived `.mpcd` for this detection reads 23 58 15.39 +42 46 25.9,
    /// which is the same angle only if the file's RA is in hours.
    #[test]
    fn test_ra_is_read_as_hours() {
        let parsed =
            parse_dets(SAMPLE, "G96", &TrackletConfig::default()).expect("the sample parses");
        let first = &parsed.detections[0];
        let mpcd_ra = (23.0 + 58.0 / 60.0 + 15.39 / 3600.0) * 15.0;
        let mpcd_dec = 42.0 + 46.0 / 60.0 + 25.9 / 3600.0;
        assert!(
            (first.ra - mpcd_ra).abs() < 1e-4,
            "ra {} should match the mpcd angle {mpcd_ra}",
            first.ra
        );
        assert!((first.dec - mpcd_dec).abs() < 1e-4, "dec {}", first.dec);
    }

    #[test]
    fn test_groups_detections_into_tracklets() {
        let parsed =
            parse_dets(SAMPLE, "G96", &TrackletConfig::default()).expect("the sample parses");
        assert_eq!(parsed.detections.len(), 4);
        assert_eq!(parsed.tracklets.len(), 2);
        assert!(parsed.tracklets.iter().all(|t| t.ids.len() == 2));
        assert_eq!(parsed.mpc_code, "G96");
    }

    /// The site comes from the path, and it is not the one ZTF would assume.
    #[test]
    fn test_the_site_is_the_telescope_that_observed() {
        let parsed =
            parse_dets(SAMPLE, "G96", &TrackletConfig::default()).expect("the sample parses");
        assert_eq!(parsed.site, crate::utils::sso_geometry::MT_LEMMON);
        assert_ne!(parsed.site, crate::utils::sso_geometry::ZTF);
        assert!(
            parse_dets(SAMPLE, "ZZZ", &TrackletConfig::default()).is_err(),
            "an unknown code is an error"
        );
    }

    /// A source id repeats between exposures, so ids must not collide.
    #[test]
    fn test_detection_ids_are_unique_across_a_night() {
        let parsed =
            parse_dets(SAMPLE, "G96", &TrackletConfig::default()).expect("the sample parses");
        let unique: std::collections::HashSet<i64> =
            parsed.detections.iter().map(|d| d.id).collect();
        assert_eq!(unique.len(), parsed.detections.len());
        // The same source id at two epochs is two detections.
        assert_ne!(
            detection_id(46753, 2460948.72339281),
            detection_id(46753, 2460948.73249624)
        );
    }

    /// The rate is what links a tracklet to the next night, so it has to carry
    /// the cos(dec) the sky imposes rather than raw RA difference.
    #[test]
    fn test_rates_are_on_the_sky() {
        let parsed =
            parse_dets(SAMPLE, "G96", &TrackletConfig::default()).expect("the sample parses");
        let t = &parsed.tracklets[0];
        let dt = 2460948.73249624 - 2460948.72339281;
        let expected_dec = (42.76982 - 42.77386) / dt;
        assert!(
            (t.dec_rate_deg_per_day - expected_dec).abs() < 1e-4,
            "dec rate {}",
            t.dec_rate_deg_per_day
        );
        let expected_ra = (23.970900 - 23.970943) * 15.0 * 42.77386_f64.to_radians().cos() / dt;
        assert!(
            (t.ra_rate_deg_per_day - expected_ra).abs() < 1e-4,
            "ra rate {}",
            t.ra_rate_deg_per_day
        );
    }

    /// The bundle names a night by the UTC date of the following morning, in
    /// the abbreviated form its directories use.
    #[test]
    fn test_the_archive_path_matches_the_bundle_layout() {
        let night = chrono::NaiveDate::from_ymd_opt(2025, 9, 30).expect("a date");
        assert_eq!(
            archive_prefix("G96", night),
            "sbn/gbo.ast.catalina.survey/data_derived/G96/2025/25Sep30"
        );
        let january = chrono::NaiveDate::from_ymd_opt(2026, 1, 5).expect("a date");
        assert_eq!(
            archive_prefix("703", january),
            "sbn/gbo.ast.catalina.survey/data_derived/703/2026/26Jan05"
        );
    }

    /// A listing carries every product for the night; only the tracklets are
    /// wanted, and a truncated or empty one must not panic.
    #[test]
    fn test_only_tracklet_keys_are_taken_from_a_listing() {
        let listing = "<ListBucketResult>\
            <Contents><Key>a/b/G96_1.mtds</Key></Contents>\
            <Contents><Key>a/b/G96_1.arch.fz</Key></Contents>\
            <Contents><Key>a/b/G96_1.rjct</Key></Contents>\
            <Contents><Key>a/b/G96_2.mtds</Key></Contents>\
            </ListBucketResult>";
        assert_eq!(mtds_keys(listing), vec!["a/b/G96_1.mtds", "a/b/G96_2.mtds"]);
        assert!(mtds_keys("").is_empty());
        assert!(mtds_keys("<ListBucketResult></ListBucketResult>").is_empty());
    }

    #[test]
    fn test_a_non_dets_file_is_refused() {
        assert!(parse_dets("something else\n1 2 3\n", "G96", &TrackletConfig::default()).is_err());
    }
}
