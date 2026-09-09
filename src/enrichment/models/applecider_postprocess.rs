//! Turns the AppleCiDER evidential head's `alpha` into the calibrated,
//! hierarchical output written to Mongo.
//!
//! The model has a single leaf head. Levels above the leaf are not separate
//! predictions: a Dirichlet is closed under grouping of categories, so the
//! `domain` and `family` posteriors are obtained by summing `alpha` over each
//! group. Everything here is therefore a pure function of `alpha` plus the
//! deployment constants below, which is why the raw `alpha` is stored
//! alongside the derived fields.

use crate::enrichment::ztf::AppleCiderClassProbs;

/// The five uncertainty scores the bundle fits abstention gates against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UncertaintyScore {
    Vacuity,
    Entropy,
    ExpectedEntropy,
    MutualInformation,
    TraceUncertainty,
}

impl UncertaintyScore {
    fn name(&self) -> &'static str {
        match self {
            Self::Vacuity => "vacuity",
            Self::Entropy => "entropy",
            Self::ExpectedEntropy => "expected_entropy",
            Self::MutualInformation => "mi",
            Self::TraceUncertainty => "trace_uncertainty",
        }
    }
}

// -- deployment constants ---------------------------------------------------
//
// Fitted on validation data after training, not learned with the network:
// these are deployment policy, and they describe only the .onnx they were
// fitted against. Swapping the model file without updating them yields
// silently wrong probabilities.
//
// All of it comes from the bundle's
// `model_bundles/<bundle>/runtime/single_object_runtime_cache.json`, which
// consolidates that bundle's `report/` directory:
//
//   TEMPERATURE                  temperature        temperature_scaler.json
//   PI_TRAIN, PI_DEPLOY          pi_train,          priors.json
//                                pi_deploy_val
//   LEAF_PROB_THRESHOLDS         prob_thresholds    postprocess.json
//   LEAF_GATE_*                  leaf_chosen        postprocess.json
//   FUSION_*                     uncertainty_fusion uncertainty_fusion.json
//   OOD_*                        ood_reference      uncertainty_metrics.json
//   {DOMAIN,FAMILY,CLASS}_*      hierarchy_levels   taxonomy + fitted gates
//
// Thresholds were chosen by minimising AURC at 80% target coverage, the
// temperature by fitting NLL on validation, the priors from the empirical
// class balance of the train and validation splits.
//
// On recalibration, copy the values across from the new cache and rerun the
// tests: `constants_are_self_consistent` and `hierarchy_maps_are_nested`
// check the structural assumptions the code below relies on. Stored `alpha`
// lets existing documents be replayed against the new values.
//
// These constants assume the bundle's `strategy` is `classwise_pred` at every
// level: gates compare a score against a threshold picked by *predicted*
// class. A bundle fitted with a global threshold would need different code,
// not just different numbers.

/// Leaf classes, in ONNX output order.
pub const CLASS_NAMES: [&str; 8] = [
    "AGN-like",
    "Accreting WD Var",
    "Other Stellar Var",
    "TDE",
    "Ia-like SN",
    "Stripped Envelope SN",
    "H-rich CCSN",
    "Superluminous SN",
];

/// Evidence temperature: alpha_cal = max(alpha - 1, 0) / T + 1.
pub const TEMPERATURE: f64 = 0.2931840121746063;

/// Training class prior, per leaf class.
pub const PI_TRAIN: [f64; 8] = [
    0.4695878762943153,
    0.05252691490091202,
    0.04909826510320236,
    0.0036343687855722416,
    0.3187958581910444,
    0.026400603442364397,
    0.07563601453747515,
    0.004320098745114174,
];

/// Deployment class prior, per leaf class.
pub const PI_DEPLOY: [f64; 8] = [
    0.48938948743062355,
    0.050299270867341386,
    0.04903689193601045,
    0.0037218413320274243,
    0.3011862008923713,
    0.026270540864076614,
    0.07565567526390249,
    0.004440091413646752,
];

/// Per-class probability thresholds for the thresholded leaf prediction.
pub const LEAF_PROB_THRESHOLDS: [f64; 8] = [
    0.0554321508387317,
    0.3764948034637756,
    0.5485397331253957,
    0.973562620870883,
    0.05983348749762674,
    0.8061888299643429,
    0.4314323326299494,
    0.9034318922426955,
];

/// Uncertainty score gating the leaf prediction.
pub const LEAF_GATE_SCORE: UncertaintyScore = UncertaintyScore::ExpectedEntropy;
/// Leaf gate thresholds, indexed by predicted class.
pub const LEAF_GATE_THRESHOLDS: [f64; 8] = [
    0.6801652669906616,
    1.1790732860565185,
    1.12724871635437,
    0.740708601474762,
    1.3442620515823367,
    1.148768424987793,
    1.3122942447662356,
    1.1459175109863282,
];

/// Robust z-sum fusion of the five base uncertainty scores, in the order
/// [vacuity, entropy, expected_entropy, mi, trace_uncertainty].
pub const FUSION_MEDIANS: [f64; 5] = [
    0.14666756987571716,
    0.6707055568695068,
    0.6144042015075684,
    0.05465555191040039,
    0.06863231211900711,
];
pub const FUSION_SCALES: [f64; 5] = [
    0.08299863871335983,
    0.38957943967580794,
    0.3540520219802856,
    0.0315231148481369,
    0.04369819862768054,
];
pub const FUSION_WEIGHTS: [f64; 5] = [
    0.2750530067932444,
    0.29647147438488697,
    0.29675419110223855,
    0.27750650613299166,
    0.292766940096312,
];

/// Out-of-distribution reference, over the five base scores plus the fused one.
pub const OOD_CENTER: [f64; 6] = [
    0.1231747530400753,
    0.5559840202331543,
    0.5100808143615723,
    0.045643746852874756,
    0.055803922936320305,
    -0.28759790405295893,
];
pub const OOD_SCALE: [f64; 6] = [
    0.05655655253604054,
    0.22673382863402367,
    0.20485197448432443,
    0.02110972261726856,
    0.026231652719154952,
    0.6282843149275595,
];
pub const OOD_FEATURE_THRESHOLDS: [f64; 6] = [
    0.322670423090458,
    1.4301585376262664,
    1.3277344697713855,
    0.12120104849338532,
    0.15064530313014984,
    1.8899948008175884,
];
pub const OOD_SCORE_THRESHOLD: f64 = 8.63369142811076;
pub const OOD_MIN_VOTES: usize = 2;

/// `domain` level: 2 nodes, level_index 0.
pub const DOMAIN_NODES: [&str; 2] = ["Variable", "Transient"];
pub const DOMAIN_LEAF_TO_NODE: [usize; 8] = [0, 0, 0, 1, 1, 1, 1, 1];
pub const DOMAIN_GATE_SCORE: UncertaintyScore = UncertaintyScore::TraceUncertainty;
pub const DOMAIN_GATE_THRESHOLDS: [f64; 2] = [0.06626571267843247, 0.07595909982919694];

/// `family` level: 4 nodes, level_index 1.
pub const FAMILY_NODES: [&str; 4] = [
    "NuclearVariable",
    "StellarVariable",
    "NuclearTransient",
    "Supernova",
];
pub const FAMILY_LEAF_TO_NODE: [usize; 8] = [0, 1, 1, 2, 3, 3, 3, 3];
pub const FAMILY_GATE_SCORE: UncertaintyScore = UncertaintyScore::ExpectedEntropy;
pub const FAMILY_GATE_THRESHOLDS: [f64; 4] = [
    0.5290095567703247,
    0.6950922012329102,
    0.912900173664093,
    0.4874100387096405,
];

/// `class` level: 8 nodes, level_index 2.
pub const CLASS_NODES: [&str; 8] = [
    "AGN-like",
    "Accreting WD Var",
    "Other Stellar Var",
    "TDE",
    "Ia-like SN",
    "Stripped Envelope SN",
    "H-rich CCSN",
    "Superluminous SN",
];
pub const CLASS_LEAF_TO_NODE: [usize; 8] = [0, 1, 2, 3, 4, 5, 6, 7];
pub const CLASS_GATE_SCORE: UncertaintyScore = UncertaintyScore::ExpectedEntropy;
pub const CLASS_GATE_THRESHOLDS: [f64; 8] = [
    0.6441427230834962,
    1.152501106262207,
    1.1866893768310547,
    1.1013262748718262,
    1.2202317714691162,
    1.3987396717071534,
    1.357124924659729,
    1.3397242069244384,
];

/// ψ(x) for x > 0, via upward recurrence into the asymptotic expansion.
///
/// `expected_entropy` needs a digamma and no Mongo expression has one, so the
/// score has to be computed here rather than left to a filter.
///
/// Recurring up to 10 rather than 6 keeps the worst-case error near 1e-12
/// instead of 1e-10, for the cost of four divisions.
fn digamma(x: f64) -> f64 {
    let mut x = x;
    let mut acc = 0.0;
    while x < 10.0 {
        acc -= 1.0 / x;
        x += 1.0;
    }
    let inv = 1.0 / x;
    let inv2 = inv * inv;
    acc + x.ln()
        - 0.5 * inv
        - inv2 * (1.0 / 12.0 - inv2 * (1.0 / 120.0 - inv2 * (1.0 / 252.0 - inv2 / 240.0)))
}

/// The five scores, all derived from one Dirichlet.
#[derive(Debug, Clone, Copy)]
pub struct Scores {
    pub vacuity: f64,
    pub entropy: f64,
    pub expected_entropy: f64,
    pub mi: f64,
    pub trace: f64,
}

impl Scores {
    fn get(&self, which: UncertaintyScore) -> f64 {
        match which {
            UncertaintyScore::Vacuity => self.vacuity,
            UncertaintyScore::Entropy => self.entropy,
            UncertaintyScore::ExpectedEntropy => self.expected_entropy,
            UncertaintyScore::MutualInformation => self.mi,
            UncertaintyScore::TraceUncertainty => self.trace,
        }
    }
}

/// Uncertainty of a Dirichlet with concentration `alpha`.
///
/// `entropy` is the total, which splits as `expected_entropy` (aleatoric) plus
/// `mi` (epistemic). `vacuity` is `K/S`: the share of the posterior still
/// supplied by the uniform prior rather than by evidence.
pub fn scores(alpha: &[f64]) -> Scores {
    let s: f64 = alpha.iter().sum();
    let k = alpha.len() as f64;

    let mut entropy = 0.0;
    let mut expected_entropy = 0.0;
    let mut var_sum = 0.0;
    let digamma_s1 = digamma(s + 1.0);
    for &a in alpha {
        let p = a / s;
        entropy -= p * p.max(1e-12).ln();
        expected_entropy -= p * (digamma(a + 1.0) - digamma_s1);
        var_sum += a * (s - a) / (s * s * (s + 1.0));
    }

    Scores {
        vacuity: k / s,
        entropy,
        expected_entropy,
        mi: entropy - expected_entropy,
        trace: var_sum.max(0.0).sqrt(),
    }
}

/// Temperature-scale the evidence: `alpha_cal = max(alpha - 1, 0) / T + 1`.
///
/// `T < 1` inflates the evidence, i.e. the network was trained under-confident.
fn calibrate_alpha(alpha: &[f64]) -> Vec<f64> {
    alpha
        .iter()
        .map(|&a| (a - 1.0).max(0.0) / TEMPERATURE + 1.0)
        .collect()
}

/// Label-shift correction: `p_adj ∝ p · pi_deploy / pi_train`.
fn adjust_for_priors(probs: &[f64]) -> Vec<f64> {
    let weighted: Vec<f64> = probs
        .iter()
        .enumerate()
        .map(|(i, &p)| p * (PI_DEPLOY[i] + 1e-12) / (PI_TRAIN[i] + 1e-12))
        .collect();
    let total: f64 = weighted.iter().sum();
    weighted.iter().map(|w| w / total).collect()
}

fn normalize(alpha: &[f64]) -> Vec<f64> {
    let s: f64 = alpha.iter().sum();
    alpha.iter().map(|a| a / s).collect()
}

/// Sum leaf columns into hierarchy nodes.
///
/// Applied to `alpha` this is the Dirichlet marginalisation; applied to
/// probabilities it is the same regrouping of the mean.
fn aggregate(leaf: &[f64], leaf_to_node: &[usize], n_nodes: usize) -> Vec<f64> {
    let mut out = vec![0.0; n_nodes];
    for (i, &v) in leaf.iter().enumerate() {
        out[leaf_to_node[i]] += v;
    }
    out
}

fn argmax(values: &[f64]) -> usize {
    values
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(i, _)| i)
        .unwrap_or(0)
}

/// Predicted leaf class under the bundle's per-class probability thresholds:
/// the highest-probability class among those clearing their own threshold, or
/// plain argmax when none do.
///
/// Note this is tuned for classification F-beta, not for follow-up triage: the
/// rare classes are gated near certainty (TDE at 0.974), so a strong rare-class
/// candidate can be reported as a commoner class. Triage should read `probs`
/// and `alpha`, not this field.
fn thresholded_prediction(probs: &[f64]) -> usize {
    let clearing: Vec<usize> = (0..probs.len())
        .filter(|&i| probs[i] >= LEAF_PROB_THRESHOLDS[i])
        .collect();
    match clearing
        .iter()
        .max_by(|&&a, &&b| probs[a].partial_cmp(&probs[b]).unwrap())
    {
        Some(&i) => i,
        None => argmax(probs),
    }
}

/// Weighted robust z-sum over the five base scores.
fn fused_uncertainty(s: &Scores) -> f64 {
    let base = [s.vacuity, s.entropy, s.expected_entropy, s.mi, s.trace];
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..5 {
        num += FUSION_WEIGHTS[i] * ((base[i] - FUSION_MEDIANS[i]) / FUSION_SCALES[i]);
        den += FUSION_WEIGHTS[i];
    }
    num / den
}

/// Mahalanobis-style distance from the in-distribution reference, plus the
/// per-feature vote count. Either exceeding its limit raises the flag.
fn ood(s: &Scores, fused: f64) -> (f64, usize, bool) {
    let features = [
        s.vacuity,
        s.entropy,
        s.expected_entropy,
        s.mi,
        s.trace,
        fused,
    ];
    let mut sum_sq = 0.0;
    let mut votes = 0;
    for (i, &f) in features.iter().enumerate() {
        let z = (f - OOD_CENTER[i]) / OOD_SCALE[i].max(1e-8);
        sum_sq += z * z;
        if f > OOD_FEATURE_THRESHOLDS[i] {
            votes += 1;
        }
    }
    let score = sum_sq.sqrt();
    (
        score,
        votes,
        score > OOD_SCORE_THRESHOLD || votes >= OOD_MIN_VOTES,
    )
}

/// One hierarchy level, resolved.
struct Level {
    pred: usize,
    probs: Vec<f64>,
    gate: UncertaintyScore,
    gate_value: f64,
    gate_threshold: f64,
    kept: bool,
}

fn resolve_level(
    alpha: &[f64],
    probs_final: &[f64],
    leaf_to_node: &[usize],
    n_nodes: usize,
    gate: UncertaintyScore,
    thresholds: &[f64],
) -> Level {
    let alpha_level: Vec<f64> = aggregate(alpha, leaf_to_node, n_nodes)
        .iter()
        // marginalize_alpha clamps, so a group can never fall below the prior
        .map(|a| a.max(1.0 + 1e-6))
        .collect();
    let probs = aggregate(probs_final, leaf_to_node, n_nodes);
    let pred = argmax(&probs);

    // Gates were fitted against uncalibrated-alpha scores, so score the raw
    // Dirichlet here even though the probabilities reported are calibrated.
    let gate_value = scores(&alpha_level).get(gate);
    let gate_threshold = thresholds[pred];

    Level {
        pred,
        probs,
        gate,
        gate_value,
        gate_threshold,
        kept: gate_value <= gate_threshold,
    }
}

/// Build the `applecider_outputs` document from the head's raw `alpha`.
///
/// Returns `None` if `alpha` is not the expected 8 classes.
pub fn build(alpha_raw: &[f32]) -> Option<AppleCiderOutputs> {
    if alpha_raw.len() != 8 {
        return None;
    }
    let alpha: Vec<f64> = alpha_raw.iter().map(|&a| a as f64).collect();

    let alpha_cal = calibrate_alpha(&alpha);
    let probs_final = adjust_for_priors(&normalize(&alpha_cal));

    let leaf_scores = scores(&alpha);
    let fused = fused_uncertainty(&leaf_scores);
    let (ood_score, ood_votes, ood_flag) = ood(&leaf_scores, fused);

    let domain = resolve_level(
        &alpha,
        &probs_final,
        &DOMAIN_LEAF_TO_NODE,
        DOMAIN_NODES.len(),
        DOMAIN_GATE_SCORE,
        &DOMAIN_GATE_THRESHOLDS,
    );
    let family = resolve_level(
        &alpha,
        &probs_final,
        &FAMILY_LEAF_TO_NODE,
        FAMILY_NODES.len(),
        FAMILY_GATE_SCORE,
        &FAMILY_GATE_THRESHOLDS,
    );
    let class = resolve_level(
        &alpha,
        &probs_final,
        &CLASS_LEAF_TO_NODE,
        CLASS_NODES.len(),
        CLASS_GATE_SCORE,
        &CLASS_GATE_THRESHOLDS,
    );

    // The leaf gate is fitted separately from the identical-membership `class`
    // level and can disagree with it, so both are reported.
    let pred_thresholded = thresholded_prediction(&probs_final);
    let leaf_gate_value = leaf_scores.get(LEAF_GATE_SCORE);
    let leaf_gate_threshold = LEAF_GATE_THRESHOLDS[pred_thresholded];
    let leaf_kept = leaf_gate_value <= leaf_gate_threshold;

    // Report at the deepest level that survives its gate; abstain if none do.
    let (label, label_level) = if leaf_kept {
        (
            Some(CLASS_NAMES[pred_thresholded].to_string()),
            Some("class".to_string()),
        )
    } else if class.kept {
        (
            Some(CLASS_NODES[class.pred].to_string()),
            Some("class".to_string()),
        )
    } else if family.kept {
        (
            Some(FAMILY_NODES[family.pred].to_string()),
            Some("family".to_string()),
        )
    } else if domain.kept {
        (
            Some(DOMAIN_NODES[domain.pred].to_string()),
            Some("domain".to_string()),
        )
    } else {
        (None, None)
    };

    let f = |v: f64| v as f32;
    Some(AppleCiderOutputs {
        alpha: AppleCiderClassProbs::from_slice_f64(&alpha)?,
        evidence_total: f(alpha.iter().sum()),
        domain: AppleCiderDomainLevel {
            pred: DOMAIN_NODES[domain.pred].to_string(),
            prob: f(domain.probs[domain.pred]),
            probs: AppleCiderDomainProbs {
                variable: f(domain.probs[0]),
                transient: f(domain.probs[1]),
            },
            gate_name: domain.gate.name().to_string(),
            gate: f(domain.gate_value),
            gate_threshold: f(domain.gate_threshold),
            kept: domain.kept,
        },
        family: AppleCiderFamilyLevel {
            pred: FAMILY_NODES[family.pred].to_string(),
            prob: f(family.probs[family.pred]),
            probs: AppleCiderFamilyProbs {
                nuclear_variable: f(family.probs[0]),
                stellar_variable: f(family.probs[1]),
                nuclear_transient: f(family.probs[2]),
                supernova: f(family.probs[3]),
            },
            gate_name: family.gate.name().to_string(),
            gate: f(family.gate_value),
            gate_threshold: f(family.gate_threshold),
            kept: family.kept,
        },
        // No `probs` here: this level's membership map is the identity, so its
        // probabilities are `applecider_fusion` verbatim.
        class: AppleCiderClassLevel {
            pred: CLASS_NODES[class.pred].to_string(),
            prob: f(class.probs[class.pred]),
            gate_name: class.gate.name().to_string(),
            gate: f(class.gate_value),
            gate_threshold: f(class.gate_threshold),
            kept: class.kept,
        },
        leaf_gate: AppleCiderGate {
            gate_name: LEAF_GATE_SCORE.name().to_string(),
            gate: f(leaf_gate_value),
            gate_threshold: f(leaf_gate_threshold),
            kept: leaf_kept,
        },
        label,
        label_level,
        abstain: !(leaf_kept || class.kept || family.kept || domain.kept),
        uncertainty: AppleCiderUncertainty {
            vacuity: f(leaf_scores.vacuity),
            epistemic: f(leaf_scores.mi),
            aleatoric: f(leaf_scores.expected_entropy),
            entropy: f(leaf_scores.entropy),
            trace: f(leaf_scores.trace),
            fused: f(fused),
        },
        ood: AppleCiderOod {
            score: f(ood_score),
            votes: ood_votes as i32,
            flag: ood_flag,
        },
    })
}

/// Both stored documents for one alert, from its raw `alpha`.
///
/// The graph's own `probs` are the uncalibrated Dirichlet mean, so the leaf
/// probabilities are recomputed here with the deployment temperature and
/// priors instead.
pub fn derive(alpha: &[f32]) -> Option<(AppleCiderClassProbs, AppleCiderOutputs)> {
    Some((
        AppleCiderClassProbs::from_probs(&calibrated_probs(alpha)?)?,
        build(alpha)?,
    ))
}

/// [`derive`] over a batched forward pass, whose `alpha` arrives flattened as
/// `(n_rows, 8)`.
///
/// Postprocessing is independent per alert — nothing is normalised across the
/// batch — so this is a row-wise map. It exists to validate the row stride
/// rather than infer it by division: a wrong stride would not fail, it would
/// attribute one alert's classification to another.
///
/// Returns `None` if the buffer is not exactly `n_rows` rows of 8.
pub fn derive_batch(
    alpha_batch: &[f32],
    n_rows: usize,
) -> Option<Vec<(AppleCiderClassProbs, AppleCiderOutputs)>> {
    if n_rows == 0 || alpha_batch.len() != n_rows * CLASS_NAMES.len() {
        return None;
    }
    alpha_batch
        .chunks_exact(CLASS_NAMES.len())
        .map(derive)
        .collect()
}

/// Calibrated, prior-adjusted leaf probabilities, for `applecider_fusion`.
pub fn calibrated_probs(alpha_raw: &[f32]) -> Option<Vec<f32>> {
    if alpha_raw.len() != 8 {
        return None;
    }
    let alpha: Vec<f64> = alpha_raw.iter().map(|&a| a as f64).collect();
    let probs = adjust_for_priors(&normalize(&calibrate_alpha(&alpha)));
    Some(probs.iter().map(|&p| p as f32).collect())
}

// -- output types -----------------------------------------------------------

use apache_avro_derive::AvroSchema;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderDomainProbs {
    #[serde(rename = "Variable")]
    pub variable: f32,
    #[serde(rename = "Transient")]
    pub transient: f32,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderFamilyProbs {
    #[serde(rename = "NuclearVariable")]
    pub nuclear_variable: f32,
    #[serde(rename = "StellarVariable")]
    pub stellar_variable: f32,
    #[serde(rename = "NuclearTransient")]
    pub nuclear_transient: f32,
    #[serde(rename = "Supernova")]
    pub supernova: f32,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderDomainLevel {
    pub pred: String,
    pub prob: f32,
    pub probs: AppleCiderDomainProbs,
    pub gate_name: String,
    pub gate: f32,
    pub gate_threshold: f32,
    pub kept: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderFamilyLevel {
    pub pred: String,
    pub prob: f32,
    pub probs: AppleCiderFamilyProbs,
    pub gate_name: String,
    pub gate: f32,
    pub gate_threshold: f32,
    pub kept: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderClassLevel {
    pub pred: String,
    pub prob: f32,
    pub gate_name: String,
    pub gate: f32,
    pub gate_threshold: f32,
    pub kept: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderGate {
    pub gate_name: String,
    pub gate: f32,
    pub gate_threshold: f32,
    pub kept: bool,
}

/// Leaf uncertainty. `entropy` splits into `aleatoric` + `epistemic`; a high
/// `vacuity` means more photometry may help, while a high `aleatoric` at low
/// `vacuity` means the classes genuinely overlap at this evidence level.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderUncertainty {
    pub vacuity: f32,
    pub epistemic: f32,
    pub aleatoric: f32,
    pub entropy: f32,
    pub trace: f32,
    pub fused: f32,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderOod {
    pub score: f32,
    pub votes: i32,
    pub flag: bool,
}

/// Everything derived from `alpha`. The leaf probabilities live in
/// `applecider_fusion`; this block carries the evidence, the hierarchy and the
/// abstention decision.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct AppleCiderOutputs {
    /// Dirichlet concentration, `1 + evidence`, per leaf class. Every other
    /// field here is a function of this, so a recalibration can be replayed
    /// over stored documents without re-running the model.
    pub alpha: AppleCiderClassProbs,
    pub evidence_total: f32,
    pub domain: AppleCiderDomainLevel,
    pub family: AppleCiderFamilyLevel,
    pub class: AppleCiderClassLevel,
    pub leaf_gate: AppleCiderGate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_level: Option<String>,
    pub abstain: bool,
    pub uncertainty: AppleCiderUncertainty,
    pub ood: AppleCiderOod,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `alpha` for ZTF25abtzltn, recovered from the reference notebook's
    /// published `probs_base` and `vacuity` (S = K / vacuity). Expected values
    /// below are that notebook's own printed output.
    fn ztf25abtzltn_alpha() -> Vec<f32> {
        let s = 8.0 / 0.210472_f64;
        let probs_base = [
            0.026309, 0.026309, 0.026309, 0.026309, 0.815578, 0.026309, 0.026568, 0.026309,
        ];
        probs_base.iter().map(|p| (p * s) as f32).collect()
    }

    fn assert_close(got: f32, want: f64, tol: f64, what: &str) {
        let diff = (got as f64 - want).abs();
        assert!(
            diff < tol,
            "{}: got {}, want {}, diff {}",
            what,
            got,
            want,
            diff
        );
    }

    #[test]
    fn digamma_matches_known_values() {
        // psi(1) = -gamma, psi(2) = 1 - gamma, psi(0.5) = -gamma - 2ln2
        let gamma = 0.5772156649015329_f64;
        assert!((digamma(1.0) + gamma).abs() < 1e-11);
        assert!((digamma(2.0) - (1.0 - gamma)).abs() < 1e-11);
        assert!((digamma(0.5) + gamma + 2.0 * 2.0_f64.ln()).abs() < 1e-11);
        assert!((digamma(10.0) - 2.251752589066721_f64).abs() < 1e-11);
    }

    #[test]
    fn leaf_probabilities_match_reference_notebook() {
        let probs = calibrated_probs(&ztf25abtzltn_alpha()).unwrap();
        // notebook prob_final column
        assert_close(probs[4], 0.932382, 1e-5, "Ia-like SN");
        assert_close(probs[0], 0.009954, 1e-5, "AGN-like");
        assert_close(probs[1], 0.009146, 1e-5, "Accreting WD Var");
        assert_close(probs[2], 0.009539, 1e-5, "Other Stellar Var");
        assert_close(probs[3], 0.009781, 1e-5, "TDE");
        assert_close(probs[5], 0.009504, 1e-5, "Stripped Envelope SN");
        assert_close(probs[6], 0.009875, 1e-5, "H-rich CCSN");
        assert_close(probs[7], 0.009817, 1e-5, "Superluminous SN");
        let total: f32 = probs.iter().sum();
        assert!((total - 1.0).abs() < 1e-5, "probabilities sum to {}", total);
    }

    #[test]
    fn uncertainty_scores_match_reference_notebook() {
        let out = build(&ztf25abtzltn_alpha()).unwrap();
        // notebook "Class uncertainty metrics"
        assert_close(out.uncertainty.vacuity, 0.210472, 1e-5, "vacuity");
        assert_close(out.uncertainty.entropy, 0.836901, 1e-5, "entropy");
        assert_close(
            out.uncertainty.aleatoric,
            0.759036,
            1e-5,
            "expected_entropy",
        );
        assert_close(out.uncertainty.epistemic, 0.077866, 1e-5, "mi");
        assert_close(out.uncertainty.trace, 0.091971, 1e-5, "trace_uncertainty");
        assert_close(out.uncertainty.fused, 0.569903, 1e-4, "fused_uncertainty");
    }

    #[test]
    fn entropy_decomposes_into_aleatoric_and_epistemic() {
        let out = build(&ztf25abtzltn_alpha()).unwrap();
        let sum = out.uncertainty.aleatoric + out.uncertainty.epistemic;
        assert_close(sum, out.uncertainty.entropy as f64, 1e-6, "aleatoric + mi");
    }

    #[test]
    fn hierarchy_matches_reference_notebook() {
        let out = build(&ztf25abtzltn_alpha()).unwrap();

        assert_eq!(out.domain.pred, "Transient");
        assert_close(out.domain.prob, 0.971360, 1e-5, "domain top prob");
        assert_close(out.domain.probs.variable, 0.028640, 1e-5, "P(Variable)");
        assert_eq!(out.domain.gate_name, "trace_uncertainty");
        assert_close(out.domain.gate, 0.061050, 1e-5, "domain gate");
        assert!(out.domain.kept);

        assert_eq!(out.family.pred, "Supernova");
        assert_close(out.family.prob, 0.961579, 1e-5, "family top prob");
        assert_close(
            out.family.probs.stellar_variable,
            0.018686,
            1e-5,
            "P(StellarVar)",
        );
        assert_eq!(out.family.gate_name, "expected_entropy");
        assert_close(out.family.gate, 0.411531, 1e-5, "family gate");
        assert!(out.family.kept);

        assert_eq!(out.class.pred, "Ia-like SN");
        assert_close(out.class.prob, 0.932382, 1e-5, "class top prob");
        assert_close(out.class.gate, 0.759036, 1e-5, "class gate");
        assert!(out.class.kept);

        assert_eq!(out.label.as_deref(), Some("Ia-like SN"));
        assert_eq!(out.label_level.as_deref(), Some("class"));
        assert!(!out.abstain);
    }

    #[test]
    fn hierarchy_probabilities_sum_to_one_at_every_level() {
        let out = build(&ztf25abtzltn_alpha()).unwrap();
        let d = out.domain.probs.variable + out.domain.probs.transient;
        assert!((d - 1.0).abs() < 1e-5, "domain sums to {}", d);
        let f = out.family.probs.nuclear_variable
            + out.family.probs.stellar_variable
            + out.family.probs.nuclear_transient
            + out.family.probs.supernova;
        assert!((f - 1.0).abs() < 1e-5, "family sums to {}", f);
    }

    #[test]
    fn alpha_is_preserved_and_totals_the_evidence() {
        let out = build(&ztf25abtzltn_alpha()).unwrap();
        assert_close(out.alpha.ia_like_sn, 31.0, 1e-3, "alpha Ia-like SN");
        assert_close(out.alpha.agn_like, 1.0, 1e-3, "alpha AGN-like");
        assert_close(out.evidence_total, 38.0098, 1e-2, "evidence_total");
        // vacuity is exactly K/S, so the two must agree
        assert_close(
            out.uncertainty.vacuity,
            8.0 / out.evidence_total as f64,
            1e-6,
            "vacuity == K/S",
        );
    }

    #[test]
    fn ood_matches_reference_notebook() {
        let out = build(&ztf25abtzltn_alpha()).unwrap();
        assert_close(out.ood.score, 3.389418, 1e-3, "ood score");
        assert_eq!(out.ood.votes, 0);
        assert!(!out.ood.flag);
    }

    /// No evidence at all: alpha is the bare prior, so the posterior is uniform
    /// and every gate must reject.
    #[test]
    fn zero_evidence_abstains_completely() {
        let out = build(&[1.0; 8]).unwrap();
        assert_close(out.uncertainty.vacuity, 1.0, 1e-6, "vacuity");
        assert_close(out.evidence_total, 8.0, 1e-6, "evidence_total");
        assert!(out.abstain, "no evidence must abstain");
        assert!(out.label.is_none());
        assert!(!out.domain.kept && !out.family.kept && !out.class.kept);
    }

    /// A rare-class candidate the classification thresholds refuse to call:
    /// TDE leads on probability but is gated at 0.974, so the thresholded
    /// prediction falls to AGN-like. The stored `probs` must still show TDE
    /// ahead, or triage on this document would be misled.
    #[test]
    fn rare_class_candidate_keeps_its_probability_mass() {
        let alpha = [4.0, 1.0, 1.0, 9.0, 2.0, 1.0, 1.0, 1.0];
        let probs = calibrated_probs(&alpha).unwrap();
        let out = build(&alpha).unwrap();
        assert!(
            probs[3] > probs[0],
            "TDE {} should lead AGN-like {}",
            probs[3],
            probs[0]
        );
        assert_eq!(out.class.pred, "TDE", "argmax reports the rare class");
        assert_eq!(out.family.pred, "NuclearTransient");
    }

    /// Guards the shapes the code above indexes into. A recalibrated bundle
    /// that changes a level's node count or reorders the fusion features would
    /// otherwise produce plausible-looking but wrong numbers.
    #[test]
    fn constants_are_self_consistent() {
        assert_eq!(CLASS_NAMES.len(), 8);
        assert_eq!(CLASS_NODES.len(), CLASS_NAMES.len());
        assert_eq!(
            CLASS_NODES, CLASS_NAMES,
            "leaf level must be the leaf classes"
        );

        let pi_train: f64 = PI_TRAIN.iter().sum();
        let pi_deploy: f64 = PI_DEPLOY.iter().sum();
        assert!(
            (pi_train - 1.0).abs() < 1e-6,
            "pi_train sums to {}",
            pi_train
        );
        assert!(
            (pi_deploy - 1.0).abs() < 1e-6,
            "pi_deploy sums to {}",
            pi_deploy
        );
        assert!(
            PI_TRAIN.iter().all(|&p| p > 0.0),
            "a zero prior would divide by ~0"
        );

        assert!(TEMPERATURE > 0.0, "temperature must be positive");
        assert!(
            LEAF_PROB_THRESHOLDS
                .iter()
                .all(|&t| (0.0..=1.0).contains(&t)),
            "probability thresholds must be probabilities"
        );

        for (nodes, thresholds) in [
            (DOMAIN_NODES.len(), DOMAIN_GATE_THRESHOLDS.len()),
            (FAMILY_NODES.len(), FAMILY_GATE_THRESHOLDS.len()),
            (CLASS_NODES.len(), CLASS_GATE_THRESHOLDS.len()),
        ] {
            assert_eq!(nodes, thresholds, "one gate threshold per node");
        }
        assert_eq!(LEAF_GATE_THRESHOLDS.len(), CLASS_NAMES.len());

        // The fusion and OOD arrays are indexed positionally against
        // [vacuity, entropy, expected_entropy, mi, trace] (+ fused for OOD).
        assert_eq!(FUSION_MEDIANS.len(), 5);
        assert_eq!(FUSION_SCALES.len(), 5);
        assert_eq!(FUSION_WEIGHTS.len(), 5);
        assert!(
            FUSION_SCALES.iter().all(|&s| s > 0.0),
            "fusion scales divide"
        );
        assert!(
            FUSION_WEIGHTS.iter().sum::<f64>() > 0.0,
            "weights normalise the sum"
        );
        assert_eq!(OOD_CENTER.len(), 6);
        assert_eq!(OOD_SCALE.len(), 6);
        assert_eq!(OOD_FEATURE_THRESHOLDS.len(), 6);
    }

    /// Each level must partition the leaves, and the levels must nest: leaves
    /// sharing a family must share a domain, or `aggregate` would be summing
    /// probabilities across branches of the taxonomy that do not belong
    /// together.
    #[test]
    fn hierarchy_maps_are_nested() {
        for (name, map, n_nodes) in [
            ("domain", &DOMAIN_LEAF_TO_NODE, DOMAIN_NODES.len()),
            ("family", &FAMILY_LEAF_TO_NODE, FAMILY_NODES.len()),
            ("class", &CLASS_LEAF_TO_NODE, CLASS_NODES.len()),
        ] {
            assert!(
                map.iter().all(|&n| n < n_nodes),
                "{}: node index out of range",
                name
            );
            for node in 0..n_nodes {
                assert!(
                    map.contains(&node),
                    "{}: node {} has no leaves, so its probability is always zero",
                    name,
                    node
                );
            }
        }

        assert!(
            CLASS_LEAF_TO_NODE
                .iter()
                .enumerate()
                .all(|(leaf, &node)| leaf == node),
            "the class level must be the identity, which is why it stores no probs"
        );

        for a in 0..CLASS_NAMES.len() {
            for b in 0..CLASS_NAMES.len() {
                if FAMILY_LEAF_TO_NODE[a] == FAMILY_LEAF_TO_NODE[b] {
                    assert_eq!(
                        DOMAIN_LEAF_TO_NODE[a], DOMAIN_LEAF_TO_NODE[b],
                        "{} and {} share a family but not a domain",
                        CLASS_NAMES[a], CLASS_NAMES[b]
                    );
                }
            }
        }
    }

    #[test]
    fn rejects_wrong_length_alpha() {
        assert!(build(&[1.0; 5]).is_none());
        assert!(calibrated_probs(&[1.0; 5]).is_none());
        assert!(derive(&[1.0; 5]).is_none());
    }

    /// Three alerts with clearly different evidence, so a row mix-up shows up
    /// as a changed label rather than a small numeric drift.
    fn three_distinct_alphas() -> (Vec<f32>, Vec<Vec<f32>>) {
        let rows = vec![
            ztf25abtzltn_alpha(),                          // Ia-like SN
            vec![4.0, 1.0, 1.0, 9.0, 2.0, 1.0, 1.0, 1.0],  // TDE
            vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 20.0], // Superluminous SN
        ];
        (rows.concat(), rows)
    }

    #[test]
    fn batch_matches_row_by_row_processing() {
        let (flat, rows) = three_distinct_alphas();
        let batched = derive_batch(&flat, rows.len()).unwrap();
        assert_eq!(batched.len(), rows.len());

        for (i, row) in rows.iter().enumerate() {
            let (probs_single, out_single) = derive(row).unwrap();
            let (probs_batched, out_batched) = &batched[i];
            assert_eq!(out_batched.label, out_single.label, "row {} label", i);
            assert_eq!(
                out_batched.class.pred, out_single.class.pred,
                "row {} class",
                i
            );
            assert_eq!(
                out_batched.domain.pred, out_single.domain.pred,
                "row {} domain",
                i
            );
            assert_eq!(
                probs_batched.ia_like_sn, probs_single.ia_like_sn,
                "row {} leaf probability",
                i
            );
            assert_eq!(
                out_batched.uncertainty.aleatoric, out_single.uncertainty.aleatoric,
                "row {} uncertainty",
                i
            );
        }
    }

    /// Guards against an off-by-one in the stride: each row must carry its own
    /// alert's evidence, not its neighbour's.
    #[test]
    fn batch_preserves_row_order() {
        let (flat, rows) = three_distinct_alphas();
        let batched = derive_batch(&flat, rows.len()).unwrap();
        assert_eq!(batched[0].1.class.pred, "Ia-like SN");
        assert_eq!(batched[1].1.class.pred, "TDE");
        assert_eq!(batched[2].1.class.pred, "Superluminous SN");
        assert_close(batched[0].1.alpha.ia_like_sn, 31.0, 1e-3, "row 0 alpha");
        assert_close(batched[1].1.alpha.tde, 9.0, 1e-6, "row 1 alpha");
        assert_close(
            batched[2].1.alpha.superluminous_sn,
            20.0,
            1e-6,
            "row 2 alpha",
        );
    }

    #[test]
    fn batch_rejects_a_ragged_buffer() {
        let (flat, rows) = three_distinct_alphas();
        // A miscounted batch must fail rather than silently re-stride the rows.
        assert!(derive_batch(&flat, rows.len() + 1).is_none());
        assert!(derive_batch(&flat, rows.len() - 1).is_none());
        assert!(derive_batch(&flat[..flat.len() - 1], rows.len()).is_none());
        assert!(derive_batch(&[], 0).is_none());
    }

    #[test]
    fn batch_of_one_matches_the_single_alert_path() {
        let alpha = ztf25abtzltn_alpha();
        let batched = derive_batch(&alpha, 1).unwrap();
        assert_eq!(batched.len(), 1);
        assert_eq!(batched[0].1.label.as_deref(), Some("Ia-like SN"));
    }
}
