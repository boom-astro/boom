//! MPC's comet file, reduced to the same elements MPCORB feeds.

use crate::utils::sso_geometry::OrbitalElements;

/// Where MPC publishes comet elements.
pub const DEFAULT_COMETELS_URL: &str = "https://www.minorplanetcenter.net/iau/MPCORB/CometEls.txt";

/// A parsed comet, keyed as ZTF's `ssnamenr` writes it.
#[derive(Debug, Clone, PartialEq)]
pub struct CometEntry {
    pub designation: String,
    pub h: Option<f64>,
    pub g: Option<f64>,
    pub elements: OrbitalElements,
}

fn column(line: &str, from: usize, to: usize) -> Option<&str> {
    line.get(from..to.min(line.len())).map(str::trim)
}

fn number(line: &str, from: usize, to: usize) -> Option<f64> {
    column(line, from, to)?.parse().ok()
}

/// Julian date from a Gregorian calendar date with a fractional day.
pub fn julian_date(year: i64, month: i64, day: f64) -> f64 {
    let (y, m) = if month <= 2 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };
    let a = y.div_euclid(100);
    let b = 2 - a + a.div_euclid(4);
    (365.25 * (y + 4716) as f64).floor() + (30.6001 * (m + 1) as f64).floor() + day + b as f64
        - 1524.5
}

/// The key `ssnamenr` carries: `1P` when numbered, else `C/2025Q3`.
fn designation_for(line: &str) -> Option<String> {
    let name = column(line, 102, line.len())?;
    let designation = name.split('(').next()?.trim();
    if designation.is_empty() {
        return None;
    }
    if let Some((head, _)) = designation.split_once('/') {
        if head.starts_with(|c: char| c.is_ascii_digit()) {
            return Some(head.to_string());
        }
    }
    Some(designation.replace(' ', ""))
}

/// Parse one record, or `None` when it is a header or unusable.
pub fn parse_line(line: &str) -> Option<CometEntry> {
    if line.len() < 100 || line.trim().is_empty() {
        return None;
    }

    let tp = julian_date(
        number(line, 14, 18)? as i64,
        number(line, 19, 21)? as i64,
        number(line, 22, 29)?,
    );
    let q = number(line, 30, 39)?;
    let e = number(line, 40, 49)?;
    if !(q > 0.0 && e >= 0.0 && q.is_finite() && e.is_finite()) {
        return None;
    }

    // Only filled for perturbed solutions; elements refer to perihelion anyway.
    let epoch_jd = match column(line, 81, 89).filter(|s| s.len() == 8) {
        Some(stamp) => julian_date(
            stamp[0..4].parse().ok()?,
            stamp[4..6].parse().ok()?,
            stamp[6..8].parse().ok()?,
        ),
        None => tp,
    };

    Some(CometEntry {
        designation: designation_for(line)?,
        h: number(line, 90, 95),
        g: number(line, 96, 100),
        elements: OrbitalElements {
            epoch_jd,
            // Meaningless above e = 1; the propagator reads `q` and `e`.
            a: if e < 1.0 { q / (1.0 - e) } else { 0.0 },
            e,
            incl: number(line, 70, 79)?,
            node: number(line, 60, 69)?,
            peri: number(line, 50, 59)?,
            mean_anomaly: 0.0,
            q,
            tp,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::sso_geometry::{geometry_at, true_anomaly};

    // Real lines from CometEls.txt.
    const HALLEY: &str = "0001P         2061 08  2.8739  0.571098  0.968021  112.1899   59.2896  162.1880  20260905   5.5  3.2  1P/Halley";
    const HALE_BOPP: &str = "    CJ95O010  1997 03 29.0338  0.924783  0.994898  130.7211  281.8006   89.7380  20260905  -2.0  4.0  C/1995 O1 (Hale-Bopp)";
    const NEARLY_PARABOLIC: &str = "    CJ47X01b  1947 10 21.2095  0.124034  0.999440  196.9183  338.0351  140.7380  20260905   9.0  4.0  C/1947 X1-B (Southern comet)";

    #[test]
    fn test_parses_halley() {
        let c = parse_line(HALLEY).expect("Halley parses");
        assert_eq!(c.designation, "1P");
        assert!((c.elements.q - 0.571098).abs() < 1e-9);
        assert!((c.elements.e - 0.968021).abs() < 1e-9);
        assert!((c.elements.incl - 162.1880).abs() < 1e-6);
        assert_eq!(c.h, Some(5.5));

        // Perihelion 2061 Aug 2.8739.
        assert!((c.elements.tp - julian_date(2061, 8, 2.8739)).abs() < 1e-9);
        // Halley's semimajor axis is about 17.8 au.
        assert!((c.elements.a - 17.8).abs() < 0.5, "a was {}", c.elements.a);
    }

    /// A key that misses IPAC's form finds no orbit for the object.
    #[test]
    fn test_designations_match_the_alert_form() {
        assert_eq!(parse_line(HALLEY).unwrap().designation, "1P");
        assert_eq!(parse_line(HALE_BOPP).unwrap().designation, "C/1995O1");
    }

    #[test]
    fn test_a_near_parabolic_comet_parses_and_propagates() {
        let c = parse_line(NEARLY_PARABOLIC).expect("parses");
        assert!(c.elements.e > 0.999);

        let g = geometry_at(&c.elements, c.elements.tp);
        assert!(
            (g.helio_dist - c.elements.q).abs() < 1e-4,
            "at perihelion r was {} against q {}",
            g.helio_dist,
            c.elements.q
        );
        assert!(true_anomaly(&c.elements, c.elements.tp).abs() < 1e-6);
    }

    /// True anomaly signs about perihelion, and tp matches the elements.
    #[test]
    fn test_geometry_reports_perihelion_axes() {
        let c = parse_line(HALLEY).expect("parses");
        let tp = c.elements.tp;

        let at_perihelion = geometry_at(&c.elements, tp);
        assert!(at_perihelion.true_anomaly.abs() < 1e-4);
        assert!((at_perihelion.perihelion_time - tp).abs() < 1e-9);

        // Inbound is negative, outbound positive, so the legs separate.
        assert!(geometry_at(&c.elements, tp - 400.0).true_anomaly < 0.0);
        assert!(geometry_at(&c.elements, tp + 400.0).true_anomaly > 0.0);

        // And the object is further out on both sides than at perihelion.
        assert!(geometry_at(&c.elements, tp - 400.0).helio_dist > c.elements.q);
        assert!(geometry_at(&c.elements, tp + 400.0).helio_dist > c.elements.q);
    }

    /// Headers, rules and short lines appear in the file and are not records.
    #[test]
    fn test_non_records_are_skipped() {
        assert!(parse_line("").is_none());
        assert!(parse_line("   ").is_none());
        assert!(parse_line("too short to be a record").is_none());
    }

    #[test]
    fn test_julian_date_matches_known_epochs() {
        // J2000.0
        assert!((julian_date(2000, 1, 1.5) - 2_451_545.0).abs() < 1e-9);
        // MPC's own epoch stamp format, 2026 Sep 5.
        assert!((julian_date(2026, 9, 5.0) - 2_461_288.5).abs() < 1e-9);
    }
}
