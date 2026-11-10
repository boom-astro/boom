# AppleCiDER fusion classifications

Every ZTF alert that BOOM enriches, and whose light curve the model can accept,
gets a multimodal transient classification attached to it under two keys:
`classifications.applecider_fusion` and `classifications.applecider_outputs`.
This page explains what the fields mean, when they are absent, and how to use
them — whether you are writing a BOOM filter, querying the alert database, or
deciding what to follow up.

AppleCiDER is an evidential, hierarchical classifier. It reads three modalities
at once — the ZTF light curve, the alert metadata, and the science/template/
difference cutout triplet — and returns a *distribution over* class
probabilities rather than a single probability vector. That distinction is what
the `alpha` and `uncertainty` fields capture, and it is what lets the model say
"I do not know" in a way a plain softmax cannot.

## Quick reference

Everything a filter normally needs is in `applecider_fusion`:

| Field | Meaning | Type |
| --- | --- | --- |
| `applecider_fusion.decision_label` | The class, family, or domain reported | string |
| `applecider_fusion.decision_level` | Which level that label came from: `class`, `family`, or `domain` | string |
| `applecider_fusion.decision_probability` | Probability of the reported label at its level | float |
| `applecider_fusion.abstain_completely` | True when no level was confident enough to report | bool |
| `applecider_fusion.class_probabilities.<class>` | Calibrated probability per leaf class, summing to 1 | float |

`applecider_outputs` carries the evidence, the full hierarchy, and the
diagnostics:

| Field | Meaning | Type |
| --- | --- | --- |
| `model`, `model_version`, `taxonomy_version` | Provenance of the weights and label set | string |
| `alpha.<class>` | Dirichlet concentration per class; `1 + evidence` | float |
| `evidence_total` | Sum of `alpha`; the belief's effective sample size | float |
| `domain`, `family`, `class` | One block per hierarchy level — see [The hierarchy](#the-hierarchy) | object |
| `decision` | Same decision as in `applecider_fusion`, nested | object |
| `uncertainty` | Six scalars — see [Uncertainty](#uncertainty) | object |
| `ood` | Out-of-distribution score, votes, and flag | object |
| `modalities_used` | Which inputs the model actually consumed | object |
| `n_detections_used` | Detections fed to the model after trimming | int |
| `horizon_days` | Light-curve window the model was trained for (100) | float |

Classifications are stored **per alert**, not per object. A well-observed object
has one classification per `candid`, each computed from the light curve as it
stood at that epoch.

## When the fields are absent

When the model did not run, these keys are **omitted entirely** — there is no
placeholder value standing in for a missing classification. A query must
therefore test for existence rather than for a sentinel:

```js
{"classifications.applecider_outputs": {$exists: true}}
```

Both keys are absent together, for any of:

- **Fewer than two photometry points.** The bundle declares
  `min_prefix_events: 2`; a single detection is not a light curve.
- **A light curve spanning more than 100 days.** AppleCiDER is a *mid-stage*
  classifier, trained on the first 100 days after discovery. Older objects are
  out of scope rather than mis-scored.
- **Inference failed.** Logged at `warn`; ACAI and BTSbot scores are still
  written, so a missing AppleCiDER block never costs you the other classifiers.

Expect a substantial fraction of alerts to have no classification. On a typical
ZTF night roughly 60% of enriched alerts clear the gate; the rest are
single-detection or long-history objects.

## The hierarchy

The taxonomy has three levels. Every leaf class belongs to exactly one family
and one domain:

| Domain | Family | Class |
| --- | --- | --- |
| Variable | NuclearVariable | AGN-like |
| Variable | StellarVariable | Accreting WD Var |
| Variable | StellarVariable | Other Stellar Var |
| Transient | NuclearTransient | TDE |
| Transient | Supernova | Ia-like SN |
| Transient | Supernova | Stripped Envelope SN |
| Transient | Supernova | H-rich CCSN |
| Transient | Supernova | Superluminous SN |

The model has a single head: it predicts the eight leaf classes, and the coarser
levels are obtained by *summing* — a Dirichlet is closed under grouping, so the
family and domain posteriors are exact aggregates rather than separate
predictions. `P(Supernova)` is literally the sum of the four supernova classes'
probabilities, and the levels can never contradict each other.

Each level block has the same shape:

| Field | Meaning |
| --- | --- |
| `pred` | Highest-probability node at this level |
| `prob` | That node's probability |
| `probs.<node>` | Probability per node, summing to 1. On `class` these are the same eight values as `applecider_fusion.class_probabilities`, repeated so each level block is self-contained |
| `gate_name` | Which uncertainty score gates this level |
| `gate` | That score's value for this alert |
| `gate_threshold` | The value it had to stay under, chosen by the predicted node |
| `kept` | Whether this level is confident enough to report |

Each level is gated independently, with its own metric and thresholds fitted to
80% coverage. Accuracy on kept predictions rises sharply as the level coarsens:

| Level | Gate metric | Macro F-beta on kept |
| --- | --- | --- |
| `domain` | `trace_uncertainty` | 0.99 |
| `family` | `expected_entropy` | 0.83 |
| `class` | `expected_entropy` | 0.64 |

**The reported label is the deepest level that passes its own gate.** If the
leaf is too uncertain, the model falls back to "Supernova" or "Transient" rather
than guessing a class. If nothing passes, `abstain_completely` is true and the
three `decision_*` fields are absent.

Because the levels are gated independently, they are not monotonic: a rejected
`domain` above a kept `family` is possible, and the deepest kept level still
wins.

## Alpha, and why it is stored

`alpha` is the Dirichlet concentration vector, and it decomposes as
`1 + evidence`. The `1` is a uniform prior — total ignorance — and the rest is
what the model found. Read `alpha_k - 1` as the weight of evidence for class
`k`, in units the network learned rather than counts of anything physical.

Two consequences worth knowing:

- **`alpha_k == 1` means "no information about this class"**, not "unlikely".
  Those classes still get a small probability, but it comes entirely from the
  prior, not from the data. Their relative ordering is meaningless — do not rank
  rare-class candidates on probabilities near the floor.
- **Every other field here is a function of `alpha`.** Probabilities,
  uncertainties, hierarchy levels, the OOD score — all of it. Storing `alpha`
  means a recalibration can be replayed over existing documents without
  re-running the model.

`evidence_total` is `sum(alpha)`. Larger means more accumulated evidence and a
tighter posterior; `vacuity` is just `8 / evidence_total`.

## Uncertainty

Six scalars, all computed from `alpha`:

| Field | Meaning |
| --- | --- |
| `vacuity` | Share of the answer still coming from the prior. High means little evidence yet. |
| `entropy` | Total uncertainty. Splits exactly into `aleatoric + epistemic`. |
| `aleatoric` | Class overlap at the current evidence level. |
| `epistemic` | The model's own ignorance — resolvable with more data. |
| `trace` | Scalar spread of the probability vector. |
| `fused` | Weighted combination of the five, used for OOD scoring. |

The useful distinction for follow-up is between **high `vacuity`** — thin
evidence, so more photometry should help — and **high `aleatoric` at low
`vacuity`** — the classes genuinely overlap at this evidence level, so more of
the same photometry will not resolve it.

Note that these are computed from the *uncalibrated* `alpha` while the
probabilities are calibrated. That is deliberate: the gate thresholds were
fitted against uncalibrated scores, so comparing a calibrated value against them
would silently change the coverage.

## Probabilities are calibrated

`class_probabilities` are not the raw model output. Two corrections are applied:

1. **Temperature scaling** of the evidence (`T = 0.293`). Because `T < 1`, this
   *increases* confidence — the network was trained under-confident.
2. **Prior adjustment** from the training class balance to the deployment one.

The raw values remain recoverable from `alpha` if you need them. The difference
is not cosmetic: on a typical confident alert the leaf probability moves from
about 0.82 to 0.93.

## A caution for follow-up

`decision_label` is the model's *classification*, chosen to maximize
classification accuracy. It is not a follow-up recommendation, and the two can
disagree sharply, because the bundle gates rare classes at very high
probabilities (TDE at 0.974, SLSN at 0.903). An object whose highest probability
is TDE can therefore be reported as something commoner, or abstained on.

For triage, read `class_probabilities` and `alpha` directly, apply your own
thresholds, and treat `abstain_completely: true` as a *signal* rather than a
filter: an alert the model cannot classify, which still carries probability mass
on a rare class, is often exactly the one worth a spectrum.

## Example

A confident Type Ia supernova, 34 detections:

```json
{
  "applecider_fusion": {
    "decision_label": "Ia-like SN",
    "decision_level": "class",
    "decision_probability": 0.93238,
    "abstain_completely": false,
    "class_probabilities": {
      "AGN-like": 0.00995, "Accreting WD Var": 0.00915,
      "Other Stellar Var": 0.00954, "TDE": 0.00978,
      "Ia-like SN": 0.93238, "Stripped Envelope SN": 0.00950,
      "H-rich CCSN": 0.00987, "Superluminous SN": 0.00982
    }
  },
  "applecider_outputs": {
    "model": "cider_mid",
    "model_version": "prod8_mid",
    "taxonomy_version": "cider_8class_v1",
    "alpha": {
      "AGN-like": 1.0, "Accreting WD Var": 1.0, "Other Stellar Var": 1.0,
      "TDE": 1.0, "Ia-like SN": 31.0, "Stripped Envelope SN": 1.0,
      "H-rich CCSN": 1.0098, "Superluminous SN": 1.0
    },
    "evidence_total": 38.0098,
    "domain": {
      "pred": "Transient", "prob": 0.97136,
      "probs": { "Variable": 0.02864, "Transient": 0.97136 },
      "gate_name": "trace_uncertainty", "gate": 0.06105,
      "gate_threshold": 0.07596, "kept": true
    },
    "family": {
      "pred": "Supernova", "prob": 0.96158,
      "probs": {
        "NuclearVariable": 0.00995, "StellarVariable": 0.01869,
        "NuclearTransient": 0.00978, "Supernova": 0.96158
      },
      "gate_name": "expected_entropy", "gate": 0.41153,
      "gate_threshold": 0.48741, "kept": true
    },
    "class": {
      "pred": "Ia-like SN", "prob": 0.93238,
      "probs": {
        "AGN-like": 0.00995, "Accreting WD Var": 0.00915,
        "Other Stellar Var": 0.00954, "TDE": 0.00978,
        "Ia-like SN": 0.93238, "Stripped Envelope SN": 0.00950,
        "H-rich CCSN": 0.00987, "Superluminous SN": 0.00982
      },
      "gate_name": "expected_entropy", "gate": 0.75904,
      "gate_threshold": 1.22023, "kept": true
    },
    "decision": {
      "label": "Ia-like SN", "level": "class",
      "probability": 0.93238, "abstain_completely": false
    },
    "uncertainty": {
      "vacuity": 0.21047, "epistemic": 0.07787, "aleatoric": 0.75903,
      "entropy": 0.83690, "trace": 0.09197, "fused": 0.56990
    },
    "ood": { "score": 3.38942, "votes": 0, "flag": false },
    "modalities_used": {
      "photometry": true, "science_stamp": true, "reference_stamp": true,
      "metadata": true, "redshift": false
    },
    "n_detections_used": 34,
    "horizon_days": 100.0
  }
}
```

Note `alpha` for the six classes sitting at exactly 1.0: the model found no
evidence for any of them. Their differing final probabilities come from the
prior adjustment, not from anything the model saw.

## Querying

Objects the model called a supernova, at any level:

```js
db.ZTF_alerts.find({"classifications.applecider_fusion.decision_label":
  {$in: ["Ia-like SN", "Stripped Envelope SN", "H-rich CCSN",
         "Superluminous SN", "Supernova"]}})
```

Confident nuclear transients:

```js
db.ZTF_alerts.find({
  "classifications.applecider_outputs.family.pred": "NuclearTransient",
  "classifications.applecider_outputs.family.kept": true
})
```

Alerts the model could not classify but which carry TDE probability — candidates
for a spectrum:

```js
db.ZTF_alerts.find({
  "classifications.applecider_fusion.abstain_completely": true,
  "classifications.applecider_fusion.class_probabilities.TDE": {$gt: 0.3}
})
```

Note that class names contain spaces and hyphens, so they must be quoted in
dotted paths.

## Other fields

`modalities_used` records which inputs the model consumed. `photometry`,
`metadata`, `science_stamp` and `reference_stamp` are true whenever a
classification exists, since an alert cannot reach the model without them.
`redshift` is always false: the deployed export has no redshift input, so the
redshift-conditioned bundles cannot be driven through it.

`n_detections_used` is the count after the 100-day trim and the 384-event cap —
usually every point in the light curve, since the eligibility gate already
restricts the span. `horizon_days` records the window the model was trained for.

`ood` reports distance from the training distribution: `score` is a scaled
distance, `votes` counts how many of six uncertainty features exceed their
individual limits, and `flag` is raised when either the score or the vote count
crosses its threshold. A flagged alert is not necessarily wrong — it may simply
be unusual, which can make it more interesting rather than less — but its class
probabilities deserve less trust.

## Provenance

`model_version` identifies the calibration bundle, not just the weights. The
temperature, class priors, gate thresholds and OOD reference are all fitted on
validation data *after* training, and they describe only the `.onnx` they were
fitted against. They live in
`src/enrichment/models/applecider_postprocess.rs`, with a table mapping each
constant back to the bundle file it came from. Swapping the model file without
updating them produces silently wrong probabilities.
