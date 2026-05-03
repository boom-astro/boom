//! Helpers for assembling `evaluate_cadence` inputs from a per-aux-doc
//! bundle.
//!
//! The `lightcurve_binner` binary reads a per-source bundle out of
//! MongoDB — the latest classifier scores from `<survey>_alerts`, the
//! source's `cross_matches` and existing `binned_lightcurve` arrays from
//! `<survey>_alerts_aux`. This module turns that bundle into the typed
//! inputs [`evaluate_cadence`](crate::utils::cadence::evaluate_cadence)
//! consumes:
//!
//! - `tags: Vec<String>` from classifier-score rules + catalog hits
//! - `score_promotion: bool` derived from the same rules
//! - per-band outburst signal computed against the new bins + bin history
//!
//! Pure module: no MongoDB, no async, no global state.

use crate::conf::ClassifierScoreRule;
use crate::utils::binning::BinnedPoint;
use crate::utils::cadence::is_outburst;
use mongodb::bson::{Bson, Document};
use std::collections::HashMap;

/// Walk `scores` against `rules`. For each rule whose `field` is present on
/// `scores` and crosses (`>=`) `threshold`, append `tags` (deduped,
/// order-preserving) and OR `promote` into the returned bool.
///
/// `scores = None` (no classifications doc on the alert — enrichment hasn't
/// run yet, or this is a pre-classification source) returns
/// `(vec![], false)`. Fields whose value is not a number on `scores` are
/// skipped silently — they would either be a serialization bug worth a
/// dedicated alert or genuinely missing, and either way the binner
/// shouldn't crash.
pub fn tags_from_classifier_scores(
    scores: Option<&Document>,
    rules: &[ClassifierScoreRule],
) -> (Vec<String>, bool) {
    let Some(doc) = scores else {
        return (Vec::new(), false);
    };
    let mut tags: Vec<String> = Vec::new();
    let mut promote = false;
    for rule in rules {
        let Some(score) = bson_field_as_f64(doc, &rule.field) else {
            continue;
        };
        if !score.is_finite() || score < rule.threshold {
            continue;
        }
        for t in &rule.tags {
            if !tags.iter().any(|x| x == t) {
                tags.push(t.clone());
            }
        }
        promote |= rule.promote;
    }
    (tags, promote)
}

/// Read a numeric BSON field as `f64`. Accepts Double, Int32, Int64.
/// Returns `None` for missing fields or non-numeric types — callers treat
/// missing as "rule does not fire".
fn bson_field_as_f64(doc: &Document, field: &str) -> Option<f64> {
    match doc.get(field) {
        Some(Bson::Double(d)) => Some(*d),
        Some(Bson::Int32(i)) => Some(*i as f64),
        Some(Bson::Int64(i)) => Some(*i as f64),
        _ => None,
    }
}

/// For each catalog name in `rules`, if `cross_matches[name]` is a
/// non-empty array, emit the named synthetic tag. Tags are
/// order-preserving but unique (a tag named by two different catalogs is
/// only emitted once). `cross_matches = None` returns empty.
///
/// Note that the ordering of emitted tags depends on `rules`'s iteration
/// order (`HashMap` is unordered), so callers should not rely on a stable
/// order across emissions — only on tag presence. The downstream
/// `resolve_tag_policy` is order-sensitive but that ordering comes from
/// `tag_rules`, not from this list.
pub fn tags_from_cross_matches(
    cross_matches: Option<&Document>,
    rules: &HashMap<String, String>,
) -> Vec<String> {
    let Some(doc) = cross_matches else {
        return Vec::new();
    };
    let mut tags: Vec<String> = Vec::new();
    for (catalog, tag) in rules {
        let Ok(arr) = doc.get_array(catalog) else {
            continue;
        };
        if arr.is_empty() {
            continue;
        }
        if !tags.iter().any(|x| x == tag) {
            tags.push(tag.clone());
        }
    }
    tags
}

/// Outburst signal for a single band: compares `new_bin.flux_med` against
/// the most recent `k` finite-flux entries of `history`, calling through
/// to [`is_outburst`].
///
/// Skips upper-only bins in `history` (their `flux_med == None`). Returns
/// `false` if `new_bin` is upper-only or if history has fewer than 3
/// finite-flux entries (matches `is_outburst`'s minimum).
pub fn band_outburst_signal(
    new_bin: &BinnedPoint,
    history: &[BinnedPoint],
    k: usize,
    sigma: f64,
) -> bool {
    let Some(latest_flux) = new_bin.flux_med else {
        return false;
    };
    // `history` is in append order (oldest first). Take the most recent k
    // finite-flux bins by reversing, filtering upper-only, then capping.
    let history_fluxes: Vec<f64> = history
        .iter()
        .rev()
        .filter_map(|b| b.flux_med)
        .take(k)
        .collect();
    is_outburst(latest_flux, new_bin.flux_err, &history_fluxes, sigma)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::binning::SrcKind;
    use crate::utils::lightcurves::Band;
    use mongodb::bson::doc;

    fn rule(field: &str, threshold: f64, tags: &[&str], promote: bool) -> ClassifierScoreRule {
        ClassifierScoreRule {
            field: field.to_string(),
            threshold,
            tags: tags.iter().map(|s| s.to_string()).collect(),
            promote,
        }
    }

    fn bin_with_flux(flux: f64, err: f64, band: Band) -> BinnedPoint {
        BinnedPoint {
            jd: 0.0,
            n: 1,
            flux_med: Some(flux),
            flux_err: err,
            flux_mad: 0.0,
            has_nondet: false,
            src_kinds: vec![SrcKind::Psf],
            window_start_jd: 0.0,
            window_days: 1.0,
            band,
        }
    }

    fn upper_only_bin(band: Band) -> BinnedPoint {
        BinnedPoint {
            jd: 0.0,
            n: 0,
            flux_med: None,
            flux_err: 30.0,
            flux_mad: 0.0,
            has_nondet: true,
            src_kinds: vec![SrcKind::Upper],
            window_start_jd: 0.0,
            window_days: 1.0,
            band,
        }
    }

    // ─── tags_from_classifier_scores ─────────────────────────────────────────

    #[test]
    fn classifier_scores_none_returns_empty() {
        let (tags, promote) = tags_from_classifier_scores(None, &[rule("btsbot", 0.5, &["explosive"], true)]);
        assert!(tags.is_empty());
        assert!(!promote);
    }

    #[test]
    fn classifier_scores_empty_rules_returns_empty() {
        let scores = doc! { "btsbot": 0.9 };
        let (tags, promote) = tags_from_classifier_scores(Some(&scores), &[]);
        assert!(tags.is_empty());
        assert!(!promote);
    }

    #[test]
    fn classifier_score_above_threshold_fires() {
        let scores = doc! { "btsbot": 0.9 };
        let (tags, promote) = tags_from_classifier_scores(
            Some(&scores),
            &[rule("btsbot", 0.5, &["explosive", "supernova"], true)],
        );
        assert_eq!(tags, vec!["explosive".to_string(), "supernova".to_string()]);
        assert!(promote);
    }

    #[test]
    fn classifier_score_at_threshold_inclusive() {
        // `>=` semantics — equality fires.
        let scores = doc! { "btsbot": 0.5 };
        let (tags, promote) =
            tags_from_classifier_scores(Some(&scores), &[rule("btsbot", 0.5, &["explosive"], true)]);
        assert_eq!(tags, vec!["explosive".to_string()]);
        assert!(promote);
    }

    #[test]
    fn classifier_score_below_threshold_does_not_fire() {
        let scores = doc! { "btsbot": 0.4 };
        let (tags, promote) =
            tags_from_classifier_scores(Some(&scores), &[rule("btsbot", 0.5, &["explosive"], true)]);
        assert!(tags.is_empty());
        assert!(!promote);
    }

    #[test]
    fn classifier_missing_field_does_not_fire() {
        // Common shape during early enrichment — only some classifiers ran.
        let scores = doc! { "acai_h": 0.9 };
        let (tags, promote) =
            tags_from_classifier_scores(Some(&scores), &[rule("btsbot", 0.5, &["explosive"], true)]);
        assert!(tags.is_empty());
        assert!(!promote);
    }

    #[test]
    fn classifier_non_numeric_field_skipped() {
        let scores = doc! { "btsbot": "broken" };
        let (tags, promote) =
            tags_from_classifier_scores(Some(&scores), &[rule("btsbot", 0.5, &["explosive"], true)]);
        assert!(tags.is_empty());
        assert!(!promote);
    }

    #[test]
    fn classifier_int_score_accepted() {
        // Defensive: if a classifier emits an int 1 or 0, treat it as a number.
        let scores = doc! { "btsbot": 1_i32 };
        let (tags, _) = tags_from_classifier_scores(
            Some(&scores),
            &[rule("btsbot", 0.5, &["explosive"], true)],
        );
        assert_eq!(tags, vec!["explosive".to_string()]);
    }

    #[test]
    fn classifier_multiple_rules_union_tags_or_promote() {
        // Co-flagging: two rules fire at once. Tags merge (no duplicates),
        // promote is OR'd. Mirrors btsbot=0.7 + acai_v=0.6 simultaneously.
        let scores = doc! { "btsbot": 0.7, "acai_v": 0.6 };
        let (tags, promote) = tags_from_classifier_scores(
            Some(&scores),
            &[
                rule("btsbot", 0.5, &["explosive", "supernova"], true),
                rule("acai_v", 0.5, &["variable"], false),
            ],
        );
        assert_eq!(
            tags,
            vec!["explosive".to_string(), "supernova".to_string(), "variable".to_string()]
        );
        assert!(promote);
    }

    #[test]
    fn classifier_duplicate_tags_across_rules_deduped() {
        let scores = doc! { "btsbot": 0.9, "acai_h": 0.9 };
        let (tags, _) = tags_from_classifier_scores(
            Some(&scores),
            &[
                rule("btsbot", 0.5, &["explosive"], true),
                rule("acai_h", 0.5, &["explosive", "hostless"], true),
            ],
        );
        // `explosive` appears once even though two rules emit it.
        assert_eq!(tags, vec!["explosive".to_string(), "hostless".to_string()]);
    }

    #[test]
    fn classifier_promote_false_does_not_set_bool() {
        // Tags add but promote stays false — the acai_v case from the design.
        let scores = doc! { "acai_v": 0.9 };
        let (tags, promote) =
            tags_from_classifier_scores(Some(&scores), &[rule("acai_v", 0.5, &["variable"], false)]);
        assert_eq!(tags, vec!["variable".to_string()]);
        assert!(!promote);
    }

    // ─── tags_from_cross_matches ─────────────────────────────────────────────

    #[test]
    fn cross_matches_none_returns_empty() {
        let mut rules = HashMap::new();
        rules.insert("milliquas_v6".to_string(), "agn".to_string());
        let tags = tags_from_cross_matches(None, &rules);
        assert!(tags.is_empty());
    }

    #[test]
    fn cross_matches_empty_rules_returns_empty() {
        let xm = doc! { "milliquas_v6": [doc! { "name": "QSO" }] };
        let tags = tags_from_cross_matches(Some(&xm), &HashMap::new());
        assert!(tags.is_empty());
    }

    #[test]
    fn cross_matches_non_empty_array_emits_tag() {
        let xm = doc! { "milliquas_v6": [doc! { "name": "QSO" }] };
        let mut rules = HashMap::new();
        rules.insert("milliquas_v6".to_string(), "agn".to_string());
        let tags = tags_from_cross_matches(Some(&xm), &rules);
        assert_eq!(tags, vec!["agn".to_string()]);
    }

    #[test]
    fn cross_matches_empty_array_does_not_emit_tag() {
        // Common shape: the xmatch worker writes `cross_matches.<catalog>: []`
        // for catalogs it queried but found no match in.
        let xm = doc! { "milliquas_v6": Vec::<Bson>::new() };
        let mut rules = HashMap::new();
        rules.insert("milliquas_v6".to_string(), "agn".to_string());
        let tags = tags_from_cross_matches(Some(&xm), &rules);
        assert!(tags.is_empty());
    }

    #[test]
    fn cross_matches_catalog_not_in_doc_skipped() {
        let xm = doc! { "ps1_dr1": [doc! {}] };
        let mut rules = HashMap::new();
        rules.insert("milliquas_v6".to_string(), "agn".to_string());
        let tags = tags_from_cross_matches(Some(&xm), &rules);
        assert!(tags.is_empty());
    }

    #[test]
    fn cross_matches_multiple_catalogs_union_tags() {
        let xm = doc! {
            "milliquas_v6": [doc! { "name": "QSO" }],
            "kilonova_candidates": [doc! { "name": "host_a" }],
        };
        let mut rules = HashMap::new();
        rules.insert("milliquas_v6".to_string(), "agn".to_string());
        rules.insert("kilonova_candidates".to_string(), "kilonova-candidate".to_string());
        let mut tags = tags_from_cross_matches(Some(&xm), &rules);
        tags.sort(); // HashMap iteration order is unstable
        assert_eq!(tags, vec!["agn".to_string(), "kilonova-candidate".to_string()]);
    }

    #[test]
    fn cross_matches_two_catalogs_naming_same_tag_dedupe() {
        // Two different catalogs both flag a source as AGN — the tag list
        // shouldn't double up.
        let xm = doc! {
            "milliquas_v6": [doc! {}],
            "agn_supplemental": [doc! {}],
        };
        let mut rules = HashMap::new();
        rules.insert("milliquas_v6".to_string(), "agn".to_string());
        rules.insert("agn_supplemental".to_string(), "agn".to_string());
        let tags = tags_from_cross_matches(Some(&xm), &rules);
        assert_eq!(tags, vec!["agn".to_string()]);
    }

    // ─── band_outburst_signal ────────────────────────────────────────────────

    #[test]
    fn band_outburst_upper_only_new_bin_returns_false() {
        let history = (0..5)
            .map(|_| bin_with_flux(100.0, 5.0, Band::R))
            .collect::<Vec<_>>();
        let new_bin = upper_only_bin(Band::R);
        assert!(!band_outburst_signal(&new_bin, &history, 10, 4.0));
    }

    #[test]
    fn band_outburst_clear_excess_fires() {
        let history: Vec<_> = [100.0_f64, 105.0, 95.0, 102.0, 98.0]
            .iter()
            .map(|f| bin_with_flux(*f, 5.0, Band::R))
            .collect();
        let new_bin = bin_with_flux(1000.0, 5.0, Band::R);
        assert!(band_outburst_signal(&new_bin, &history, 10, 4.0));
    }

    #[test]
    fn band_outburst_within_baseline_does_not_fire() {
        let history: Vec<_> = [100.0_f64, 105.0, 95.0, 102.0, 98.0]
            .iter()
            .map(|f| bin_with_flux(*f, 5.0, Band::R))
            .collect();
        let new_bin = bin_with_flux(110.0, 5.0, Band::R);
        assert!(!band_outburst_signal(&new_bin, &history, 10, 4.0));
    }

    #[test]
    fn band_outburst_short_history_does_not_fire() {
        // Fewer than 3 finite-flux history bins → is_outburst returns false.
        let history = vec![bin_with_flux(100.0, 5.0, Band::R), bin_with_flux(105.0, 5.0, Band::R)];
        let new_bin = bin_with_flux(1000.0, 5.0, Band::R);
        assert!(!band_outburst_signal(&new_bin, &history, 10, 4.0));
    }

    #[test]
    fn band_outburst_skips_upper_only_history_bins() {
        // History has 2 finite + 4 upper-only; only the 2 finite count, so
        // there are < 3 finite history points and the test should not fire.
        let history = vec![
            bin_with_flux(100.0, 5.0, Band::R),
            bin_with_flux(105.0, 5.0, Band::R),
            upper_only_bin(Band::R),
            upper_only_bin(Band::R),
            upper_only_bin(Band::R),
            upper_only_bin(Band::R),
        ];
        let new_bin = bin_with_flux(1000.0, 5.0, Band::R);
        assert!(!band_outburst_signal(&new_bin, &history, 10, 4.0));
    }

    #[test]
    fn band_outburst_takes_only_most_recent_k() {
        // 12 bins of clean baseline followed by `k=3` recent bins of
        // dramatically-shifted flux. With k=3, the rolling-MAD baseline is
        // computed only from the most recent 3, all near 1000 — so a new
        // bin near 1000 should NOT be an outburst even though it's far from
        // the long-history mean.
        let mut history: Vec<BinnedPoint> = (0..12)
            .map(|_| bin_with_flux(100.0, 5.0, Band::R))
            .collect();
        for _ in 0..3 {
            history.push(bin_with_flux(1000.0, 5.0, Band::R));
        }
        let new_bin = bin_with_flux(1010.0, 5.0, Band::R);
        // With k=3, only the recent 3 (all near 1000) drive the baseline.
        assert!(!band_outburst_signal(&new_bin, &history, 3, 4.0));
        // With k=20, the older 100-baseline dominates and the new 1010 IS
        // an outburst against it.
        assert!(band_outburst_signal(&new_bin, &history, 20, 4.0));
    }

    #[test]
    fn band_outburst_negative_excursion_fires() {
        let history: Vec<_> = [100.0_f64, 105.0, 95.0, 102.0, 98.0]
            .iter()
            .map(|f| bin_with_flux(*f, 5.0, Band::R))
            .collect();
        let new_bin = bin_with_flux(-1000.0, 5.0, Band::R);
        assert!(band_outburst_signal(&new_bin, &history, 10, 4.0));
    }
}
