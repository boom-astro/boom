//! MPCORB parsing: fixed-width orbital elements from the Minor Planet Center.
//!
//! Format: <https://minorplanetcenter.net/iau/info/MPOrbitFormat.html>

use mongodb::bson::{doc, Document};

use crate::utils::sso_geometry::OrbitalElements;

/// One MPCORB record.
#[derive(Debug, Clone, PartialEq)]
pub struct MpcorbEntry {
    /// Unpacked designation, matching the convention ZTF uses in `ssnamenr`
    /// (`"1"`, `"363205"`, `"2014 WF524"`).
    pub designation: String,
    /// Absolute magnitude.
    pub h: Option<f64>,
    /// Slope parameter.
    pub g: Option<f64>,
    pub elements: OrbitalElements,
}

/// Order letters within a half-month, omitting I.
const ORDER_LETTERS: &str = "ABCDEFGHJKLMNOPQRSTUVWXYZ";
/// The old two-character cycle tops out at 2026 CZ619; the extended scheme
/// resumes at the next object.
const EXTENDED_FIRST_ORDINAL: u64 = 15_501;

/// Value of a packed character: `0-9`, then `A-Z` = 10-35, then `a-z` = 36-61.
fn packed_value(c: char) -> Option<u32> {
    match c {
        '0'..='9' => c.to_digit(10),
        'A'..='Z' => Some(c as u32 - 'A' as u32 + 10),
        'a'..='z' => Some(c as u32 - 'a' as u32 + 36),
        _ => None,
    }
}

/// Packed epoch (`"K2669"` = 2026 June 9) to Julian Date at 0h TT.
pub fn unpack_epoch(packed: &str) -> Option<f64> {
    let c: Vec<char> = packed.trim().chars().collect();
    if c.len() != 5 {
        return None;
    }
    let century = match c[0] {
        'I' => 18,
        'J' => 19,
        'K' => 20,
        _ => return None,
    };
    let year = century * 100 + c[1].to_digit(10)? * 10 + c[2].to_digit(10)?;
    let month = packed_value(c[3])?;
    let day = packed_value(c[4])?;
    julian_date(year as i64, month as i64, day as i64)
}

/// Julian Date at 0h for a Gregorian calendar date.
fn julian_date(year: i64, month: i64, day: i64) -> Option<f64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (y, m) = if month <= 2 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };
    let a = y.div_euclid(100);
    let b = 2 - a + a.div_euclid(4);
    Some(
        (365.25 * (y + 4716) as f64).floor()
            + (30.6001 * (m + 1) as f64).floor()
            + day as f64
            + b as f64
            - 1524.5,
    )
}

/// Packed designation to the unpacked form.
///
/// Numbered objects lose their zero padding (`"00001"` -> `"1"`); the
/// letter-prefixed form covers 100000 and above (`"u5784"` -> `"565784"`).
/// Provisional designations unpack to `"2014 WF524"` style.
pub fn unpack_designation(packed: &str) -> Option<String> {
    let s = packed.trim();
    let c: Vec<char> = s.chars().collect();

    // Very high numbers: '~' plus four base-62 characters.
    if c.first() == Some(&'~') && c.len() == 5 {
        let mut n: u64 = 0;
        for ch in &c[1..] {
            n = n * 62 + packed_value(*ch)? as u64;
        }
        return Some((n + 620_000).to_string());
    }

    if c.len() == 5 {
        // Numbered: five digits, or a letter for the leading two digits.
        if s.chars().all(|ch| ch.is_ascii_digit()) {
            return Some(s.trim_start_matches('0').to_string());
        }
        let high = packed_value(c[0])?;
        let low: String = c[1..].iter().collect();
        let low: u32 = low.parse().ok()?;
        return Some((high * 10_000 + low).to_string());
    }

    // Extended packed provisional: '_' + year + half-month + four base-62 digits,
    // counting on from the first object the old two-character cycle cannot reach.
    // <https://minorplanetcenter.net/mpcops/documentation/provisional-designation-definition/>
    if c.len() == 7 && c[0] == '_' {
        let year = 2000 + packed_value(c[1])?;
        let half_month = c[2];
        let mut offset: u64 = 0;
        for ch in &c[3..7] {
            offset = offset * 62 + packed_value(*ch)? as u64;
        }
        let index = EXTENDED_FIRST_ORDINAL + offset - 1;
        let letter = ORDER_LETTERS.chars().nth((index % 25) as usize)?;
        let cycle = index / 25;
        return Some(if cycle == 0 {
            format!("{year} {half_month}{letter}")
        } else {
            format!("{year} {half_month}{letter}{cycle}")
        });
    }

    if c.len() == 7 {
        // Palomar-Leiden and Trojan survey designations: "PLS4847" -> "4847 P-L".
        let survey = match &s[0..3] {
            "PLS" => Some("P-L"),
            "T1S" => Some("T-1"),
            "T2S" => Some("T-2"),
            "T3S" => Some("T-3"),
            _ => None,
        };
        if let Some(code) = survey {
            let number = s[3..7].trim_start_matches('0');
            if !number.is_empty() && number.chars().all(|ch| ch.is_ascii_digit()) {
                return Some(format!("{number} {code}"));
            }
            return None;
        }

        // Provisional: century, year, half-month letter, cycle count, order letter.
        let century = match c[0] {
            'I' => 18,
            'J' => 19,
            'K' => 20,
            _ => return None,
        };
        let year = century * 100 + c[1].to_digit(10)? * 10 + c[2].to_digit(10)?;
        let half_month = c[3];
        let cycle = packed_value(c[4])? * 10 + c[5].to_digit(10)?;
        let order = c[6];
        return Some(if cycle == 0 {
            format!("{year} {half_month}{order}")
        } else {
            format!("{year} {half_month}{order}{cycle}")
        });
    }

    None
}

fn field(line: &str, start: usize, end: usize) -> Option<&str> {
    line.get(start..end).map(str::trim)
}

fn parse_f64(line: &str, start: usize, end: usize) -> Option<f64> {
    field(line, start, end).and_then(|s| s.parse().ok())
}

/// Parse one MPCORB line. Returns `None` for headers, blank lines and any
/// record whose orbital elements are unusable.
pub fn parse_line(line: &str) -> Option<MpcorbEntry> {
    if line.len() < 103 || line.trim().is_empty() {
        return None;
    }
    let designation = unpack_designation(field(line, 0, 7)?)?;
    let elements = OrbitalElements {
        epoch_jd: unpack_epoch(field(line, 20, 25)?)?,
        mean_anomaly: parse_f64(line, 26, 35)?,
        peri: parse_f64(line, 37, 46)?,
        node: parse_f64(line, 48, 57)?,
        incl: parse_f64(line, 59, 68)?,
        e: parse_f64(line, 70, 79)?,
        a: parse_f64(line, 92, 103)?,
    };
    // A non-elliptical or degenerate orbit is not usable here. Written as a
    // positive test so a NaN fails it rather than slipping through a negation.
    let elliptical = elements.a > 0.0 && (0.0..1.0).contains(&elements.e);
    if !elliptical {
        return None;
    }
    Some(MpcorbEntry {
        designation,
        h: parse_f64(line, 8, 13),
        g: parse_f64(line, 14, 19),
        elements,
    })
}

/// Collection `mpcorb_ingest` writes and enrichment reads.
pub const ORBITS_COLLECTION: &str = "MPC_orbits";

/// Render one entry as the stored document. Kept next to the reader below so
/// the two halves of this collection's schema cannot drift apart.
pub fn to_document(entry: &MpcorbEntry, updated_at: f64) -> Document {
    let el = &entry.elements;
    doc! {
        "_id": &entry.designation,
        "epoch_jd": el.epoch_jd,
        "a": el.a,
        "e": el.e,
        "incl": el.incl,
        "node": el.node,
        "peri": el.peri,
        "mean_anomaly": el.mean_anomaly,
        "h": entry.h,
        "g": entry.g,
        "updated_at": updated_at,
    }
}

/// Read elements back out of a stored document.
pub fn elements_from_document(doc: &Document) -> Option<OrbitalElements> {
    Some(OrbitalElements {
        epoch_jd: doc.get_f64("epoch_jd").ok()?,
        a: doc.get_f64("a").ok()?,
        e: doc.get_f64("e").ok()?,
        incl: doc.get_f64("incl").ok()?,
        node: doc.get_f64("node").ok()?,
        peri: doc.get_f64("peri").ok()?,
        mean_anomaly: doc.get_f64("mean_anomaly").ok()?,
    })
}

/// Rewrite a ZTF `ssnamenr` into the designation MPCORB is keyed by.
///
/// IPAC has used three forms over the survey's life, and they do not all match
/// MPCORB as written:
///
/// - `"9816"` — a permanent number. Already the key; the common case today.
/// - `"(100)Hekate"`, `"(57996)2002RV107"` — number and name run together, used
///   until roughly JD 2458300 (mid-2018). The parenthesised number is the key.
/// - `"2015TW415"` — a provisional designation with the space removed. MPCORB
///   writes `"2015 TW415"`.
///
/// Returns `None` for anything not resolvable to an MPCORB key, including
/// comets (`"C/2026O1"`), which MPCORB does not carry at all.
pub fn normalize_ztf_ssnamenr(ssnamenr: &str) -> Option<String> {
    let s = ssnamenr.trim();
    if s.is_empty() {
        return None;
    }

    // "(57996)2002RV107" -> "57996". The number is permanent, so the trailing
    // name or provisional designation adds nothing.
    if let Some(rest) = s.strip_prefix('(') {
        let (number, _) = rest.split_once(')')?;
        return (!number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()))
            .then(|| number.to_string());
    }

    if s.bytes().all(|b| b.is_ascii_digit()) {
        return Some(s.to_string());
    }

    // Provisional: four-digit year, then the half-month and order letters, then
    // an optional cycle count. Anything else (comet prefixes, survey forms we
    // have not seen from IPAC) is left alone rather than guessed at.
    let b = s.as_bytes();
    if b.len() >= 6
        && b[..4].iter().all(|c| c.is_ascii_digit())
        && b[4].is_ascii_uppercase()
        && b[5].is_ascii_uppercase()
        && b[6..].iter().all(|c| c.is_ascii_digit())
    {
        return Some(format!("{} {}", &s[..4], &s[4..]));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real lines from MPCORB.DAT.
    const CERES: &str = "00001    3.34  0.15 K2669 274.41935   73.29420   80.24863   10.58803  0.0796923  0.21430445   2.7655526  0 MPO980521  7297 126 1801-2026 0.83 M-v 30k Veres      4000      (1) Ceres              20260103";
    const PALLAS: &str = "00002    4.12  0.15 K2669 254.24963  310.96993  172.88661   34.93279  0.2307001  0.21383960   2.7695590  0 E2026-O67  9066 124 1804-2026 0.77 M-c 28k MPCORBFIT  4000      (2) Pallas             20260718";

    #[test]
    fn test_parses_ceres() {
        let e = parse_line(CERES).expect("Ceres must parse");
        assert_eq!(e.designation, "1");
        assert_eq!(e.h, Some(3.34));
        assert_eq!(e.g, Some(0.15));
        assert_eq!(e.elements.mean_anomaly, 274.41935);
        assert_eq!(e.elements.peri, 73.29420);
        assert_eq!(e.elements.node, 80.24863);
        assert_eq!(e.elements.incl, 10.58803);
        assert_eq!(e.elements.e, 0.0796923);
        assert_eq!(e.elements.a, 2.7655526);
    }

    #[test]
    fn test_parses_pallas() {
        let e = parse_line(PALLAS).expect("Pallas must parse");
        assert_eq!(e.designation, "2");
        assert_eq!(e.elements.incl, 34.93279);
        assert_eq!(e.elements.a, 2.7695590);
    }

    // The file carries mean daily motion as well as semimajor axis. Deriving one
    // from the other catches a column misalignment that still parses as a number.
    #[test]
    fn test_semimajor_axis_agrees_with_the_files_mean_motion() {
        for (line, n_file) in [(CERES, 0.21430445), (PALLAS, 0.21383960)] {
            let e = parse_line(line).unwrap();
            let n_derived = 0.985_607_668_6 / e.elements.a.powf(1.5);
            assert!(
                (n_derived - n_file).abs() < 1e-6,
                "derived {n_derived} vs file {n_file}"
            );
        }
    }

    #[test]
    fn test_unpacks_epoch() {
        // K2669 = 2026 June 9.
        assert_eq!(unpack_epoch("K2669"), Some(2_461_200.5));
        // Month and day above 9 use letters: A=10 .. V=31.
        assert_eq!(unpack_epoch("K26C1"), julian_date(2026, 12, 1));
        assert_eq!(unpack_epoch("J9611"), julian_date(1996, 1, 1));
        assert_eq!(unpack_epoch("nope"), None);
    }

    #[test]
    fn test_unpacks_numbered_designations() {
        assert_eq!(unpack_designation("00001").as_deref(), Some("1"));
        assert_eq!(unpack_designation("09816").as_deref(), Some("9816"));
        assert_eq!(unpack_designation("A0000").as_deref(), Some("100000"));
        // 565784 = 56 * 10000 + 5784, and 56 packs to 'u'.
        assert_eq!(unpack_designation("u5784").as_deref(), Some("565784"));
    }

    // Documented MPC examples.
    #[test]
    fn test_unpacks_provisional_designations() {
        assert_eq!(unpack_designation("J95X00A").as_deref(), Some("1995 XA"));
        assert_eq!(unpack_designation("J95X01L").as_deref(), Some("1995 XL1"));
        assert_eq!(unpack_designation("K14Wq4F").as_deref(), Some("2014 WF524"));
    }

    // ZTF reports ssnamenr unpacked, so these must match without further work.
    #[test]
    fn test_designations_match_the_ztf_convention() {
        for (packed, ztf) in [("09816", "9816"), ("21949", "21949"), ("u5784", "565784")] {
            assert_eq!(unpack_designation(packed).as_deref(), Some(ztf));
        }
    }

    // Every case below is a literal value taken from ZTF_alerts in production;
    // the three forms are not documented anywhere, only observed.
    #[test]
    fn test_normalizes_bare_numbers() {
        assert_eq!(normalize_ztf_ssnamenr("9816").as_deref(), Some("9816"));
        assert_eq!(
            normalize_ztf_ssnamenr("  305164 ").as_deref(),
            Some("305164")
        );
    }

    #[test]
    fn test_normalizes_the_early_parenthesised_form() {
        assert_eq!(
            normalize_ztf_ssnamenr("(100)Hekate").as_deref(),
            Some("100")
        );
        assert_eq!(
            normalize_ztf_ssnamenr("(57996)2002RV107").as_deref(),
            Some("57996")
        );
        assert_eq!(
            normalize_ztf_ssnamenr("(190564)2000SU128").as_deref(),
            Some("190564")
        );
    }

    #[test]
    fn test_normalizes_provisional_designations() {
        assert_eq!(
            normalize_ztf_ssnamenr("2015TW415").as_deref(),
            Some("2015 TW415")
        );
        assert_eq!(normalize_ztf_ssnamenr("2014YB").as_deref(), Some("2014 YB"));
        assert_eq!(
            normalize_ztf_ssnamenr("1998QF28").as_deref(),
            Some("1998 QF28")
        );
    }

    // Round-trip against the packed side: whatever IPAC reports must land on the
    // key mpcorb_ingest actually wrote.
    #[test]
    fn test_normalized_ssnamenr_matches_unpacked_mpcorb_keys() {
        for (packed, ssnamenr) in [
            ("09816", "9816"),
            ("K14Wq4F", "2014WF524"),
            ("J95X00A", "1995XA"),
            ("J95X01L", "1995XL1"),
        ] {
            assert_eq!(
                normalize_ztf_ssnamenr(ssnamenr),
                unpack_designation(packed),
                "ssnamenr {} did not resolve to the key for {}",
                ssnamenr,
                packed
            );
        }
    }

    // MPCORB carries no comets, so a comet designation must miss rather than
    // resolve to something else.
    #[test]
    fn test_rejects_what_it_cannot_resolve() {
        assert_eq!(normalize_ztf_ssnamenr("C/2026O1"), None);
        assert_eq!(normalize_ztf_ssnamenr("73P-C"), None);
        assert_eq!(normalize_ztf_ssnamenr(""), None);
        assert_eq!(normalize_ztf_ssnamenr("()"), None);
    }

    // Roughly 4,000 objects from the Palomar-Leiden surveys use their own packed
    // form, which is neither numbered nor provisional.
    #[test]
    fn test_unpacks_survey_designations() {
        assert_eq!(unpack_designation("PLS4847").as_deref(), Some("4847 P-L"));
        assert_eq!(unpack_designation("PLS6331").as_deref(), Some("6331 P-L"));
        assert_eq!(unpack_designation("T1S0123").as_deref(), Some("123 T-1"));
        assert_eq!(unpack_designation("T2S2040").as_deref(), Some("2040 T-2"));
        assert_eq!(unpack_designation("T3S3141").as_deref(), Some("3141 T-3"));
    }

    // The three worked examples in the MPC specification.
    #[test]
    fn test_unpacks_extended_provisional_designations() {
        assert_eq!(unpack_designation("_QC0000").as_deref(), Some("2026 CA620"));
        assert_eq!(
            unpack_designation("_QC0aEM").as_deref(),
            Some("2026 CZ6190")
        );
        assert_eq!(
            unpack_designation("_QCzzzz").as_deref(),
            Some("2026 CL591673")
        );
    }

    #[test]
    fn test_rejects_headers_and_junk() {
        assert!(parse_line("").is_none());
        assert!(parse_line("Des'n     H     G   Epoch     M        Peri.").is_none());
        assert!(parse_line("-----------------").is_none());
    }
}
