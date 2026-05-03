//! Cadence-tier state machine for the binned light-curve infrastructure.
//!
//! Decides which cadence tier each source's bins are computed at, when to
//! promote a source to a finer tier (outburst detection, classifier-score
//! crossing, class change), and when to demote (dwell-gated). See
//! `docs/binned-lightcurves.md` for the schema and design rationale.
//!
//! Pure module: no MongoDB, no async, no config-file parsing. Callers
//! assemble inputs from the aux document, the resolved tdtax tag set, and
//! the cadence config, then call [`evaluate_cadence`] to produce the new
//! state.
//!
//! # Design
//!
//! - **Two ladders.** Transient sources walk H ↔ N then dark; variable
//!   sources walk N → W → M (aperiodic) or have no time-bins at all
//!   (periodic; Argus owns the canonical store).
//! - **Tag-driven resolution.** tdtax tags select ladder, sub-track, and
//!   the class-specific H-tier `h_cadence_hours`. Most-specific tag wins.
//!   Catalog hits add synthetic tags upstream; this module sees only the
//!   final tag set.
//! - **Promotion is unconditional.** Outburst (>4σ deviation from rolling
//!   baseline) or classifier score crossing → tier := H, immediately.
//! - **Demotion is dwell-gated and one-step-at-a-time.** H→N: 14d, N→W:
//!   60d, W→M: 180d. Untouched sources don't auto-demote — that's the job
//!   of the weekly full-sweep, which calls [`evaluate_cadence`] with
//!   `outburst = false` and `score_promotion = false`.
//! - **Append-only history.** The state machine never rewrites past bins;
//!   on tier change, only future windows use the new cadence.

use crate::utils::binning::{median_absolute_deviation, median_f64};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Which ladder a source is on.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Ladder {
    Transient,
    Variable,
}

/// Cadence tier within a ladder. Not all combinations are legal:
/// - Transient ladder: `H` or `N`.
/// - Aperiodic-variable ladder: `N`, `W`, or `M`.
/// - Periodic-variable ladder: no tier (`tier = None` on `BinCadence`).
///
/// `evaluate_cadence` enforces these constraints; constructing an illegal
/// (ladder, tier) pair externally is a usage bug.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, Deserialize, Serialize)]
pub enum Tier {
    H,
    N,
    W,
    M,
}

impl Tier {
    /// Default bin window in days for the tier (excluding H, which is
    /// per-class-cadence). Used to seed window construction in the binner.
    pub fn default_window_days(&self) -> Option<f64> {
        match self {
            Tier::H => None,
            Tier::N => Some(1.0),
            Tier::W => Some(7.0),
            Tier::M => Some(30.0),
        }
    }
}

/// Cause of the most recent cadence change. Persisted on the aux doc to make
/// the state machine's history auditable.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// First time this source has been evaluated.
    Init,
    /// Promoted to H by outburst detection on the binned light curve.
    Outburst,
    /// Promoted to H by a classifier score crossing its class-specific threshold.
    ScoreCrossing,
    /// Ladder switched because the resolved tag set changed (e.g., classifier
    /// reclassified the source, or a new crossmatch hit added a synthetic tag).
    LadderSwitch,
    /// Tier demoted by elapsed dwell with no outburst.
    DwellDemote,
    /// Tier clamped by min/max derived from the resolved tag set.
    TagClamp,
}

/// Per-source cadence state. Stored as `bin_cadence` on the aux doc.
///
/// `tier = None` means the source is on the periodic-variable sub-track of
/// the variable ladder — no time-bins are written, only the hot-recent raw
/// retention applies. All other ladder/tier combinations carry a `Some`
/// tier.
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct BinCadence {
    pub ladder: Ladder,
    pub tier: Option<Tier>,
    /// Set when `tier == Some(H)` on the transient ladder; otherwise `None`.
    pub h_cadence_hours: Option<f64>,
    /// JD at which the current `(ladder, tier)` was entered. Drives dwell
    /// calculations.
    pub since_jd: f64,
    /// JD at which `evaluate_cadence` last ran for this source.
    pub last_evaluated_jd: f64,
    pub reason: Reason,
    /// For H-tier transients promoted via outburst, the JD at which the
    /// active window expires (after which dwell-gated demotion can fire).
    pub active_until_jd: Option<f64>,
}

impl BinCadence {
    /// Initial state for a source seen for the first time. Defaults to the
    /// transient ladder at N tier — the safest place to put an unclassified
    /// source (we don't yet know if it's a transient or variable, so we
    /// store nightly bins until we do).
    pub fn initial(now_jd: f64) -> Self {
        Self {
            ladder: Ladder::Transient,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: now_jd,
            last_evaluated_jd: now_jd,
            reason: Reason::Init,
            active_until_jd: None,
        }
    }
}

/// What sub-track a tag selects within the variable ladder.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VariableSub {
    Periodic,
    Aperiodic,
}

/// What a single tag implies for cadence policy. Read by tag resolution; the
/// caller assembles a list of these in priority order (most-specific tags
/// last) and `resolve_tag_policy` walks them in order, with later rules
/// overriding earlier ones field by field.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TagRule {
    pub tag: String,
    pub ladder: Option<Ladder>,
    pub variable_sub: Option<VariableSub>,
    /// Class-specific H-tier cadence in hours. Only meaningful on the
    /// transient ladder.
    pub h_cadence_hours: Option<f64>,
    /// Coarseness ceiling: tier may not be coarser than this. Used for
    /// classes where finer cadence is required (kilonova, GRB afterglow,
    /// AGN whose flares need nightly bins).
    pub coarsest_allowed: Option<Tier>,
    /// Coarseness floor: tier may not be finer than this. Used for classes
    /// where finer cadence wastes storage (YSO, LPV, bogus/artifact pinned
    /// to monthly).
    pub finest_allowed: Option<Tier>,
}

/// Dwell windows in days for each one-step demotion. Promotion is
/// unconditional; demotion requires the source to have spent at least
/// this many days at the current tier with no outburst signal.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DwellConfig {
    pub h_to_n_days: f64,
    pub n_to_w_days: f64,
    pub w_to_m_days: f64,
}

impl Default for DwellConfig {
    fn default() -> Self {
        Self {
            h_to_n_days: 14.0,
            n_to_w_days: 60.0,
            w_to_m_days: 180.0,
        }
    }
}

/// Top-level cadence config. Most fields are populated from
/// `binning.<survey>` in `config.yaml`; this struct is the in-memory shape
/// passed to `evaluate_cadence`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct CadenceConfig {
    /// Named cadence presets (e.g., `fast → 0.5`, `medium → 12.0`,
    /// `slow → 48.0`). Tag rules can reference these by name through the
    /// caller's tag-resolution layer; this module sees only the resolved
    /// numeric `h_cadence_hours`.
    pub presets: HashMap<String, f64>,
    pub dwell: DwellConfig,
    /// Sigma threshold for the outburst rule. Default 4.0 — see
    /// `is_outburst`.
    pub outburst_sigma: f64,
    /// Number of recent bins to use as the rolling baseline for outburst
    /// detection. Default 10.
    pub rolling_window_k: usize,
    /// Number of days the H tier persists after an outburst-triggered
    /// promotion. Default 30.
    pub active_window_days: f64,
    /// Default H-tier cadence (hours) for unclassified sources. Default
    /// `12.0` — matches the `medium` preset, which is calibrated to
    /// AppleCider's internal 12-hour merge window.
    pub default_h_cadence_hours: f64,
}

impl Default for CadenceConfig {
    fn default() -> Self {
        let mut presets = HashMap::new();
        presets.insert("fast".to_string(), 0.5);
        presets.insert("medium".to_string(), 12.0);
        presets.insert("slow".to_string(), 48.0);
        Self {
            presets,
            dwell: DwellConfig::default(),
            outburst_sigma: 4.0,
            rolling_window_k: 10,
            active_window_days: 30.0,
            default_h_cadence_hours: 12.0,
        }
    }
}

/// Result of resolving a tag set: the policy that should be applied to a
/// source classified with these tags. Pure function of the input tags and
/// the rule list; no source state involved.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTagPolicy {
    pub ladder: Ladder,
    pub variable_sub: Option<VariableSub>,
    pub h_cadence_hours: Option<f64>,
    /// Coarseness ceiling: tier may not be coarser than this.
    pub coarsest_allowed: Option<Tier>,
    /// Coarseness floor: tier may not be finer than this.
    pub finest_allowed: Option<Tier>,
}

impl Default for ResolvedTagPolicy {
    /// Default policy when no tag matched any rule: transient ladder, N
    /// tier, medium H cadence (12 h). Safe over-storage until classified.
    fn default() -> Self {
        Self {
            ladder: Ladder::Transient,
            variable_sub: None,
            h_cadence_hours: None,
            coarsest_allowed: None,
            finest_allowed: None,
        }
    }
}

/// Walk the tag list against the rule list, applying matching rules
/// in-order. Later rules override earlier ones field-by-field; the caller
/// orders rules from least-specific to most-specific so the most-specific
/// tag wins on each field.
///
/// Tag matching is exact string comparison.
pub fn resolve_tag_policy(tags: &[String], rules: &[TagRule]) -> ResolvedTagPolicy {
    let mut policy = ResolvedTagPolicy::default();
    for rule in rules {
        if !tags.iter().any(|t| t == &rule.tag) {
            continue;
        }
        if let Some(ladder) = rule.ladder {
            policy.ladder = ladder;
        }
        if let Some(sub) = rule.variable_sub {
            policy.variable_sub = Some(sub);
        }
        if let Some(h) = rule.h_cadence_hours {
            policy.h_cadence_hours = Some(h);
        }
        if let Some(t) = rule.coarsest_allowed {
            policy.coarsest_allowed = Some(t);
        }
        if let Some(t) = rule.finest_allowed {
            policy.finest_allowed = Some(t);
        }
    }
    policy
}

/// Outburst detection on a per-band binned light curve.
///
/// Returns `true` if the latest bin's flux deviates from the rolling-median
/// baseline by more than `sigma_thresh × (rolling_MAD + latest_err)`.
///
/// The added `latest_err` term ensures a noisy single bin doesn't trigger
/// purely on its own internal scatter. With fewer than 3 history bins the
/// baseline is too small to be meaningful; the function returns `false`
/// (insufficient evidence to call an outburst).
///
/// Caller computes per-band and ORs across bands — outburst in any band
/// triggers promotion.
pub fn is_outburst(
    latest_flux: f64,
    latest_err: f64,
    history_fluxes: &[f64],
    sigma_thresh: f64,
) -> bool {
    if history_fluxes.len() < 3 {
        return false;
    }
    if !latest_flux.is_finite() || !latest_err.is_finite() {
        return false;
    }
    let cleaned: Vec<f64> = history_fluxes
        .iter()
        .copied()
        .filter(|f| f.is_finite())
        .collect();
    if cleaned.len() < 3 {
        return false;
    }
    let median = median_f64(&cleaned);
    let mad = median_absolute_deviation(&cleaned, median);
    let denom = mad + latest_err.max(0.0);
    if denom <= 0.0 {
        // Pathological case — both baseline and latest err are zero; no
        // sensible outburst test.
        return false;
    }
    (latest_flux - median).abs() > sigma_thresh * denom
}

/// Apply the resolved tag policy's tier clamps to a candidate tier.
///
/// Tier ordering uses *finest-first* indexing: H < N (transient ladder),
/// N < W < M (variable ladder). Smaller index = finer cadence; larger index
/// = coarser. The clamps are:
///
/// - `coarsest_allowed`: tier may not be coarser than this. `tier_idx` is
///   capped above at `idx(coarsest_allowed)`. Used for AGN, kilonova, GRB
///   afterglow — classes where finer cadence is required.
/// - `finest_allowed`: tier may not be finer than this. `tier_idx` is capped
///   below at `idx(finest_allowed)`. Used for YSO, LPV, bogus/artifact —
///   classes where finer cadence wastes storage.
///
/// Setting both to the same tier pins the source to that tier exactly.
fn clamp_tier(tier: Tier, ladder: Ladder, policy: &ResolvedTagPolicy) -> Tier {
    let order: &[Tier] = match ladder {
        Ladder::Transient => &[Tier::H, Tier::N],
        Ladder::Variable => &[Tier::N, Tier::W, Tier::M],
    };
    let idx = |t: Tier| order.iter().position(|x| *x == t);

    let mut out_idx = match idx(tier) {
        Some(i) => i,
        // Tier not legal on this ladder — clamp to ladder's finest tier
        // (caller is in the middle of a ladder switch).
        None => 0,
    };
    if let Some(coarsest_idx) = policy.coarsest_allowed.and_then(idx) {
        out_idx = out_idx.min(coarsest_idx);
    }
    if let Some(finest_idx) = policy.finest_allowed.and_then(idx) {
        out_idx = out_idx.max(finest_idx);
    }
    order[out_idx]
}

/// Core state machine. Pure function of `(now_jd, current state, tag policy,
/// outburst signal, classifier-score promotion signal, config)`.
///
/// `current = None` indicates the source has never been evaluated;
/// `BinCadence::initial` is used as the prior state.
///
/// Rules, in order:
/// 1. Resolve target ladder / sub-track from tags.
/// 2. If the resolved ladder differs from `current.ladder`, ladder-switch:
///    enter the new ladder at its finest legal tier (subject to clamps).
/// 3. On the transient ladder, if `outburst_detected` or `score_promotion`,
///    promote (or stay at) H, set `active_until_jd = now + active_window`.
/// 4. On the transient ladder at H tier, if `active_until_jd < now` and
///    `H→N` dwell elapsed and no promotion signal, demote to N.
/// 5. On the variable ladder (aperiodic), if dwell elapsed and no promotion
///    signal, demote one tier (N → W → M).
/// 6. Apply tag clamps (`min_tier`, `max_tier`) at the end.
///
/// `last_evaluated_jd` is always updated to `now_jd` on every call.
/// `since_jd` is updated only when the (ladder, tier) pair actually changes;
/// `active_until_jd` is updated whenever an outburst extends the active
/// window.
pub fn evaluate_cadence(
    now_jd: f64,
    current: Option<&BinCadence>,
    tag_policy: &ResolvedTagPolicy,
    outburst_detected: bool,
    score_promotion: bool,
    config: &CadenceConfig,
) -> BinCadence {
    let prior = current
        .cloned()
        .unwrap_or_else(|| BinCadence::initial(now_jd));
    let target_ladder = tag_policy.ladder;
    let promotion_signal = outburst_detected || score_promotion;

    // ── Case 1: variable ladder + periodic sub-track → no time-bins. ──────
    if target_ladder == Ladder::Variable && tag_policy.variable_sub == Some(VariableSub::Periodic)
    {
        let changed = prior.ladder != Ladder::Variable || prior.tier.is_some();
        return BinCadence {
            ladder: Ladder::Variable,
            tier: None,
            h_cadence_hours: None,
            since_jd: if changed { now_jd } else { prior.since_jd },
            last_evaluated_jd: now_jd,
            reason: if changed {
                Reason::LadderSwitch
            } else {
                prior.reason
            },
            active_until_jd: None,
        };
    }

    // ── Case 2: ladder switch to a new ladder/sub-track. ─────────────────
    let ladder_changed = prior.ladder != target_ladder
        || (target_ladder == Ladder::Variable && prior.tier.is_none());
    if ladder_changed {
        // Enter the new ladder at its finest legal tier.
        let entry_tier = match target_ladder {
            Ladder::Transient => Tier::N, // start at N; outburst signal below may bump to H
            Ladder::Variable => Tier::N,
        };
        let clamped = clamp_tier(entry_tier, target_ladder, tag_policy);
        let mut state = BinCadence {
            ladder: target_ladder,
            tier: Some(clamped),
            h_cadence_hours: None,
            since_jd: now_jd,
            last_evaluated_jd: now_jd,
            reason: Reason::LadderSwitch,
            active_until_jd: None,
        };
        // Apply promotion signal in the same call so we don't drop an
        // outburst that arrived simultaneously with a class change.
        if target_ladder == Ladder::Transient && promotion_signal {
            apply_h_promotion(&mut state, now_jd, tag_policy, config, outburst_detected);
        }
        return state;
    }

    // ── Case 3: transient ladder, no ladder change. ──────────────────────
    if target_ladder == Ladder::Transient {
        let mut state = prior.clone();
        state.last_evaluated_jd = now_jd;

        if promotion_signal {
            apply_h_promotion(&mut state, now_jd, tag_policy, config, outburst_detected);
        } else if state.tier == Some(Tier::H) {
            // Possibly demote H → N if active window expired and dwell elapsed.
            let active_expired = state
                .active_until_jd
                .map(|t| now_jd >= t)
                .unwrap_or(true);
            let dwell_ok = (now_jd - state.since_jd) >= config.dwell.h_to_n_days;
            if active_expired && dwell_ok {
                state.tier = Some(clamp_tier(Tier::N, Ladder::Transient, tag_policy));
                state.h_cadence_hours = None;
                state.active_until_jd = None;
                state.since_jd = now_jd;
                state.reason = Reason::DwellDemote;
            }
        }
        // Always re-clamp the tier after any change.
        if let Some(t) = state.tier {
            let clamped = clamp_tier(t, Ladder::Transient, tag_policy);
            if clamped != t {
                state.tier = Some(clamped);
                state.since_jd = now_jd;
                state.reason = Reason::TagClamp;
            }
        }
        return state;
    }

    // ── Case 4: variable ladder, aperiodic sub-track, no ladder change. ──
    let mut state = prior.clone();
    state.last_evaluated_jd = now_jd;

    if promotion_signal {
        // Outburst on a variable means the source is misclassified — the
        // classifier should reclassify it as a transient and the next
        // evaluation will ladder-switch. Here we defer to tags: stay on the
        // variable ladder, but log the outburst by promoting one tier
        // (M→W→N) so the next aux update is denser. Skip if at finest tier.
        let new_tier = match state.tier {
            Some(Tier::M) => Some(Tier::W),
            Some(Tier::W) => Some(Tier::N),
            other => other,
        };
        if new_tier != state.tier {
            state.tier = new_tier;
            state.since_jd = now_jd;
            state.reason = Reason::Outburst;
        }
    } else {
        // Dwell-gated demotion N → W → M.
        let dwell_ok = match state.tier {
            Some(Tier::N) => (now_jd - state.since_jd) >= config.dwell.n_to_w_days,
            Some(Tier::W) => (now_jd - state.since_jd) >= config.dwell.w_to_m_days,
            _ => false,
        };
        if dwell_ok {
            let new_tier = match state.tier {
                Some(Tier::N) => Some(Tier::W),
                Some(Tier::W) => Some(Tier::M),
                other => other,
            };
            if new_tier != state.tier {
                state.tier = new_tier;
                state.since_jd = now_jd;
                state.reason = Reason::DwellDemote;
            }
        }
    }
    // Apply ladder clamps at the end.
    if let Some(t) = state.tier {
        let clamped = clamp_tier(t, Ladder::Variable, tag_policy);
        if clamped != t {
            state.tier = Some(clamped);
            state.since_jd = now_jd;
            state.reason = Reason::TagClamp;
        }
    }
    state
}

/// Mutates `state` in place to reflect promotion to (or refresh of) H tier
/// on the transient ladder. Sets `tier`, `h_cadence_hours`, `active_until_jd`,
/// `since_jd`, and `reason` per the promotion rules.
fn apply_h_promotion(
    state: &mut BinCadence,
    now_jd: f64,
    tag_policy: &ResolvedTagPolicy,
    config: &CadenceConfig,
    outburst_detected: bool,
) {
    let h_cadence = tag_policy
        .h_cadence_hours
        .unwrap_or(config.default_h_cadence_hours);
    let new_active_until = now_jd + config.active_window_days;
    // Apply tier clamps — if max_tier disallows H, fall through to N.
    let target_tier = clamp_tier(Tier::H, Ladder::Transient, tag_policy);
    if state.tier != Some(target_tier) {
        state.since_jd = now_jd;
    }
    state.tier = Some(target_tier);
    state.h_cadence_hours = if target_tier == Tier::H {
        Some(h_cadence)
    } else {
        None
    };
    state.active_until_jd = if target_tier == Tier::H {
        Some(new_active_until)
    } else {
        None
    };
    state.reason = if outburst_detected {
        Reason::Outburst
    } else {
        Reason::ScoreCrossing
    };
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CadenceConfig {
        CadenceConfig::default()
    }

    fn no_policy() -> ResolvedTagPolicy {
        ResolvedTagPolicy::default()
    }

    fn transient_policy(h_hours: f64) -> ResolvedTagPolicy {
        ResolvedTagPolicy {
            ladder: Ladder::Transient,
            variable_sub: None,
            h_cadence_hours: Some(h_hours),
            coarsest_allowed: None,
            finest_allowed: None,
        }
    }

    fn periodic_policy() -> ResolvedTagPolicy {
        ResolvedTagPolicy {
            ladder: Ladder::Variable,
            variable_sub: Some(VariableSub::Periodic),
            h_cadence_hours: None,
            coarsest_allowed: None,
            finest_allowed: None,
        }
    }

    fn aperiodic_policy(coarsest_allowed: Option<Tier>) -> ResolvedTagPolicy {
        ResolvedTagPolicy {
            ladder: Ladder::Variable,
            variable_sub: Some(VariableSub::Aperiodic),
            h_cadence_hours: None,
            coarsest_allowed,
            finest_allowed: None,
        }
    }

    // ─── outburst detection ──────────────────────────────────────────────────

    #[test]
    fn outburst_returns_false_with_too_few_history() {
        assert!(!is_outburst(1000.0, 5.0, &[100.0, 100.0], 4.0));
        assert!(!is_outburst(1000.0, 5.0, &[], 4.0));
    }

    #[test]
    fn outburst_detects_clear_excess() {
        let history = vec![100.0, 105.0, 95.0, 102.0, 98.0];
        assert!(is_outburst(1000.0, 5.0, &history, 4.0));
    }

    #[test]
    fn outburst_ignores_within_baseline_scatter() {
        let history = vec![100.0, 105.0, 95.0, 102.0, 98.0];
        assert!(!is_outburst(110.0, 5.0, &history, 4.0));
    }

    #[test]
    fn outburst_detects_negative_excursion() {
        // A drop is also an outburst (e.g., eclipse, dip) — uses |diff|.
        let history = vec![100.0, 105.0, 95.0, 102.0, 98.0];
        assert!(is_outburst(-1000.0, 5.0, &history, 4.0));
    }

    #[test]
    fn outburst_filters_nonfinite_history() {
        let history = vec![100.0, f64::NAN, 95.0, f64::INFINITY, 98.0, 102.0];
        // 4 finite points remain; outburst test should still work.
        assert!(is_outburst(1000.0, 5.0, &history, 4.0));
    }

    #[test]
    fn outburst_returns_false_on_nonfinite_latest() {
        let history = vec![100.0, 105.0, 95.0];
        assert!(!is_outburst(f64::NAN, 5.0, &history, 4.0));
    }

    // ─── tag resolution ──────────────────────────────────────────────────────

    #[test]
    fn empty_tags_get_default_policy() {
        let policy = resolve_tag_policy(&[], &[]);
        assert_eq!(policy, ResolvedTagPolicy::default());
        assert_eq!(policy.ladder, Ladder::Transient);
    }

    #[test]
    fn matching_tag_sets_ladder_and_cadence() {
        let rules = vec![TagRule {
            tag: "explosive".to_string(),
            ladder: Some(Ladder::Transient),
            variable_sub: None,
            h_cadence_hours: Some(12.0),
            coarsest_allowed: None,
            finest_allowed: None,
        }];
        let policy = resolve_tag_policy(&["explosive".to_string()], &rules);
        assert_eq!(policy.ladder, Ladder::Transient);
        assert_eq!(policy.h_cadence_hours, Some(12.0));
    }

    #[test]
    fn later_rule_overrides_earlier_field_by_field() {
        // Rule order = priority order. Both tags match; the later one wins on
        // h_cadence_hours but the earlier one's ladder persists if the later
        // doesn't override it.
        let rules = vec![
            TagRule {
                tag: "explosive".to_string(),
                ladder: Some(Ladder::Transient),
                variable_sub: None,
                h_cadence_hours: Some(24.0),
                coarsest_allowed: None,
                finest_allowed: None,
            },
            TagRule {
                tag: "gravitational waves".to_string(),
                ladder: None, // intentionally not set
                variable_sub: None,
                h_cadence_hours: Some(0.5),
                coarsest_allowed: None,
                finest_allowed: None,
            },
        ];
        let policy = resolve_tag_policy(
            &["explosive".to_string(), "gravitational waves".to_string()],
            &rules,
        );
        assert_eq!(policy.ladder, Ladder::Transient); // from earlier rule
        assert_eq!(policy.h_cadence_hours, Some(0.5)); // overridden by later
    }

    #[test]
    fn variable_periodic_tag_sets_sub_track() {
        let rules = vec![TagRule {
            tag: "instability strip".to_string(),
            ladder: Some(Ladder::Variable),
            variable_sub: Some(VariableSub::Periodic),
            h_cadence_hours: None,
            coarsest_allowed: None,
            finest_allowed: None,
        }];
        let policy = resolve_tag_policy(&["instability strip".to_string()], &rules);
        assert_eq!(policy.ladder, Ladder::Variable);
        assert_eq!(policy.variable_sub, Some(VariableSub::Periodic));
    }

    // ─── tier clamping ───────────────────────────────────────────────────────

    #[test]
    fn coarsest_allowed_caps_demotion() {
        // Aperiodic variable with coarsest_allowed = W. Demotion logic
        // produces M (coarser than W) → clamped back to W.
        let policy = aperiodic_policy(Some(Tier::W));
        assert_eq!(clamp_tier(Tier::M, Ladder::Variable, &policy), Tier::W);
    }

    #[test]
    fn finest_allowed_pins_to_minimum_coarseness() {
        // YSO-like: finest_allowed = M means tier may not be finer than M.
        // Even N (finest) gets pulled to M.
        let policy = ResolvedTagPolicy {
            ladder: Ladder::Variable,
            variable_sub: Some(VariableSub::Aperiodic),
            h_cadence_hours: None,
            coarsest_allowed: None,
            finest_allowed: Some(Tier::M),
        };
        assert_eq!(clamp_tier(Tier::N, Ladder::Variable, &policy), Tier::M);
    }

    #[test]
    fn coarsest_and_finest_at_same_tier_pins_exactly() {
        // Kilonova-like: both clamps at H pin to H regardless of input tier.
        let policy = ResolvedTagPolicy {
            ladder: Ladder::Transient,
            variable_sub: None,
            h_cadence_hours: None,
            coarsest_allowed: Some(Tier::H),
            finest_allowed: Some(Tier::H),
        };
        assert_eq!(clamp_tier(Tier::N, Ladder::Transient, &policy), Tier::H);
        assert_eq!(clamp_tier(Tier::H, Ladder::Transient, &policy), Tier::H);
    }

    #[test]
    fn clamp_no_change_when_within_bounds() {
        // coarsest_allowed = M is trivial on the variable ladder (M is
        // coarsest), so a tier of W stays at W.
        let policy = aperiodic_policy(Some(Tier::M));
        assert_eq!(clamp_tier(Tier::W, Ladder::Variable, &policy), Tier::W);
    }

    // ─── state machine: initial / unclassified ───────────────────────────────

    #[test]
    fn initial_state_is_transient_n_unclassified() {
        let state = evaluate_cadence(2459000.0, None, &no_policy(), false, false, &cfg());
        assert_eq!(state.ladder, Ladder::Transient);
        assert_eq!(state.tier, Some(Tier::N));
        assert_eq!(state.h_cadence_hours, None);
        assert_eq!(state.reason, Reason::Init);
        assert_eq!(state.last_evaluated_jd, 2459000.0);
        assert_eq!(state.since_jd, 2459000.0);
    }

    // ─── state machine: transient ladder promotion ───────────────────────────

    #[test]
    fn outburst_promotes_transient_n_to_h() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459000.0,
            reason: Reason::Init,
            active_until_jd: None,
        };
        let policy = transient_policy(0.5);
        let state = evaluate_cadence(2459005.0, Some(&prior), &policy, true, false, &cfg());
        assert_eq!(state.tier, Some(Tier::H));
        assert_eq!(state.h_cadence_hours, Some(0.5));
        assert_eq!(state.active_until_jd, Some(2459005.0 + 30.0));
        assert_eq!(state.reason, Reason::Outburst);
        assert_eq!(state.since_jd, 2459005.0);
    }

    #[test]
    fn outburst_at_h_extends_active_window_no_since_change() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::H),
            h_cadence_hours: Some(0.5),
            since_jd: 2459000.0,
            last_evaluated_jd: 2459003.0,
            reason: Reason::Outburst,
            active_until_jd: Some(2459030.0),
        };
        let state = evaluate_cadence(
            2459010.0,
            Some(&prior),
            &transient_policy(0.5),
            true,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::H));
        assert_eq!(state.active_until_jd, Some(2459010.0 + 30.0));
        // `since_jd` does not change because the (ladder, tier) pair didn't change.
        assert_eq!(state.since_jd, 2459000.0);
    }

    #[test]
    fn score_promotion_promotes_to_h() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459000.0,
            reason: Reason::Init,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459005.0,
            Some(&prior),
            &transient_policy(12.0),
            false,
            true,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::H));
        assert_eq!(state.h_cadence_hours, Some(12.0));
        assert_eq!(state.reason, Reason::ScoreCrossing);
    }

    // ─── state machine: transient ladder demotion ────────────────────────────

    #[test]
    fn h_demotes_to_n_after_active_expired_and_dwell_elapsed() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::H),
            h_cadence_hours: Some(0.5),
            since_jd: 2459000.0,
            last_evaluated_jd: 2459014.0,
            reason: Reason::Outburst,
            active_until_jd: Some(2459013.0),
        };
        let now = 2459015.0; // active_until_jd passed (t > 2459013); dwell 15 > 14
        let state = evaluate_cadence(
            now,
            Some(&prior),
            &transient_policy(0.5),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::N));
        assert_eq!(state.h_cadence_hours, None);
        assert_eq!(state.active_until_jd, None);
        assert_eq!(state.since_jd, now);
        assert_eq!(state.reason, Reason::DwellDemote);
    }

    #[test]
    fn h_does_not_demote_while_active_window_open() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::H),
            h_cadence_hours: Some(0.5),
            since_jd: 2459000.0,
            last_evaluated_jd: 2459014.0,
            reason: Reason::Outburst,
            active_until_jd: Some(2459030.0),
        };
        // Dwell satisfied (15 > 14) but active window still open.
        let state = evaluate_cadence(
            2459020.0,
            Some(&prior),
            &transient_policy(0.5),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::H));
        assert_eq!(state.h_cadence_hours, Some(0.5));
        assert_eq!(state.since_jd, 2459000.0);
    }

    #[test]
    fn h_does_not_demote_while_dwell_unmet() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::H),
            h_cadence_hours: Some(0.5),
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::Outburst,
            active_until_jd: Some(2459003.0),
        };
        // Active expired but dwell only 7 days (< 14).
        let state = evaluate_cadence(
            2459007.0,
            Some(&prior),
            &transient_policy(0.5),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::H));
    }

    // ─── state machine: ladder switch ────────────────────────────────────────

    #[test]
    fn transient_to_periodic_variable_drops_tier() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::Init,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459010.0,
            Some(&prior),
            &periodic_policy(),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.ladder, Ladder::Variable);
        assert_eq!(state.tier, None);
        assert_eq!(state.h_cadence_hours, None);
        assert_eq!(state.active_until_jd, None);
        assert_eq!(state.since_jd, 2459010.0);
        assert_eq!(state.reason, Reason::LadderSwitch);
    }

    #[test]
    fn periodic_variable_stays_periodic_no_change() {
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: None,
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::LadderSwitch,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459010.0,
            Some(&prior),
            &periodic_policy(),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.ladder, Ladder::Variable);
        assert_eq!(state.tier, None);
        // since_jd preserved; reason preserved.
        assert_eq!(state.since_jd, 2459000.0);
        assert_eq!(state.reason, Reason::LadderSwitch);
        assert_eq!(state.last_evaluated_jd, 2459010.0);
    }

    #[test]
    fn transient_to_aperiodic_variable_lands_at_n_subject_to_clamp() {
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::Init,
            active_until_jd: None,
        };
        // YSO-like aperiodic variable: finest_allowed = M means tier may
        // not be finer than M, so the entry at N is clamped to M.
        let policy = ResolvedTagPolicy {
            ladder: Ladder::Variable,
            variable_sub: Some(VariableSub::Aperiodic),
            h_cadence_hours: None,
            coarsest_allowed: None,
            finest_allowed: Some(Tier::M),
        };
        let state = evaluate_cadence(2459010.0, Some(&prior), &policy, false, false, &cfg());
        assert_eq!(state.ladder, Ladder::Variable);
        assert_eq!(state.tier, Some(Tier::M));
        assert_eq!(state.since_jd, 2459010.0);
    }

    #[test]
    fn ladder_switch_with_concurrent_outburst_lands_at_h() {
        // Class change to a transient class (e.g., CV in outburst gets
        // reclassified) AND outburst signal. Should ladder-switch and
        // promote to H in the same evaluation.
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: Some(Tier::W),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::DwellDemote,
            active_until_jd: None,
        };
        let policy = transient_policy(0.5);
        let state = evaluate_cadence(2459010.0, Some(&prior), &policy, true, false, &cfg());
        assert_eq!(state.ladder, Ladder::Transient);
        assert_eq!(state.tier, Some(Tier::H));
        assert_eq!(state.h_cadence_hours, Some(0.5));
        assert_eq!(state.active_until_jd, Some(2459010.0 + 30.0));
        assert_eq!(state.reason, Reason::Outburst);
    }

    // ─── state machine: aperiodic variable demotion ──────────────────────────

    #[test]
    fn aperiodic_n_to_w_after_60d_dwell() {
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459059.0,
            reason: Reason::LadderSwitch,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459061.0,
            Some(&prior),
            &aperiodic_policy(None),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::W));
        assert_eq!(state.since_jd, 2459061.0);
        assert_eq!(state.reason, Reason::DwellDemote);
    }

    #[test]
    fn aperiodic_w_to_m_after_180d_dwell() {
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: Some(Tier::W),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459179.0,
            reason: Reason::DwellDemote,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459181.0,
            Some(&prior),
            &aperiodic_policy(None),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::M));
    }

    #[test]
    fn aperiodic_m_does_not_demote_further() {
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: Some(Tier::M),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459200.0,
            reason: Reason::DwellDemote,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2460000.0,
            Some(&prior),
            &aperiodic_policy(None),
            false,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::M));
        // since_jd preserved.
        assert_eq!(state.since_jd, 2459000.0);
    }

    #[test]
    fn aperiodic_demotion_stops_at_coarsest_allowed_clamp() {
        // coarsest_allowed = W → cannot demote past W to M.
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: Some(Tier::W),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459180.0,
            reason: Reason::DwellDemote,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459181.0,
            Some(&prior),
            &aperiodic_policy(Some(Tier::W)),
            false,
            false,
            &cfg(),
        );
        // Demotion logic computes M, then clamp pulls it back to W.
        assert_eq!(state.tier, Some(Tier::W));
    }

    #[test]
    fn aperiodic_outburst_promotes_one_tier_finer() {
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: Some(Tier::M),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::DwellDemote,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459010.0,
            Some(&prior),
            &aperiodic_policy(None),
            true,
            false,
            &cfg(),
        );
        assert_eq!(state.tier, Some(Tier::W));
        assert_eq!(state.reason, Reason::Outburst);
    }

    #[test]
    fn aperiodic_outburst_at_finest_tier_no_op() {
        let prior = BinCadence {
            ladder: Ladder::Variable,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::LadderSwitch,
            active_until_jd: None,
        };
        let state = evaluate_cadence(
            2459010.0,
            Some(&prior),
            &aperiodic_policy(None),
            true,
            false,
            &cfg(),
        );
        // Already at N; no further to promote on the variable ladder.
        assert_eq!(state.tier, Some(Tier::N));
    }

    // ─── state machine: clamps interact with promotion ───────────────────────

    #[test]
    fn promotion_to_h_blocked_by_finest_allowed_n() {
        // A class flagged finest_allowed=N (e.g., a slow transient where H
        // is overkill) should not get promoted to H even on outburst.
        let policy = ResolvedTagPolicy {
            ladder: Ladder::Transient,
            variable_sub: None,
            h_cadence_hours: Some(0.5),
            coarsest_allowed: None,
            finest_allowed: Some(Tier::N),
        };
        let prior = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::N),
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459000.0,
            reason: Reason::Init,
            active_until_jd: None,
        };
        let state = evaluate_cadence(2459005.0, Some(&prior), &policy, true, false, &cfg());
        assert_eq!(state.tier, Some(Tier::N));
        assert_eq!(state.h_cadence_hours, None);
        assert_eq!(state.active_until_jd, None);
    }

    // ─── round-trip serialization ────────────────────────────────────────────

    #[test]
    fn bin_cadence_round_trips_through_bson() {
        let state = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::H),
            h_cadence_hours: Some(0.5),
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::Outburst,
            active_until_jd: Some(2459030.0),
        };
        let doc = mongodb::bson::to_document(&state).unwrap();
        let round_tripped: BinCadence = mongodb::bson::from_document(doc).unwrap();
        assert_eq!(state, round_tripped);
    }

    #[test]
    fn periodic_variable_state_round_trips_with_tier_none() {
        let state = BinCadence {
            ladder: Ladder::Variable,
            tier: None,
            h_cadence_hours: None,
            since_jd: 2459000.0,
            last_evaluated_jd: 2459005.0,
            reason: Reason::LadderSwitch,
            active_until_jd: None,
        };
        let doc = mongodb::bson::to_document(&state).unwrap();
        let round_tripped: BinCadence = mongodb::bson::from_document(doc).unwrap();
        assert_eq!(state, round_tripped);
    }
}
