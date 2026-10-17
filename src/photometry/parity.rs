use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::store::ZtfHistory;
use crate::{
    enrichment::{
        deserialize_ztf_alert_lightcurve, deserialize_ztf_forced_lightcurve, ZtfPhotometry,
    },
    utils::lightcurves::{
        analyze_photometry, prepare_photometry, summarise_detections, EPISODE_GAP_DAYS,
    },
};

#[derive(Debug, Serialize)]
pub struct ParityReport {
    pub revision: u64,
    pub histories_equal: bool,
    pub features_equal: bool,
    pub source_counts: [usize; 3],
    pub target_counts: [usize; 3],
    pub write_ms: f64,
    pub read_ms: f64,
    pub mongo_snapshot_ms: f64,
    pub source_features: Value,
    pub target_features: Value,
    pub differences: Vec<Value>,
}

fn counts(h: &ZtfHistory) -> [usize; 3] {
    [
        h.prv_candidates.len(),
        h.prv_nondetections.len(),
        h.fp_hists.len(),
    ]
}

fn normalized(h: &ZtfHistory) -> Value {
    fn sort(records: &[Value]) -> Vec<Value> {
        let mut records = records.to_vec();
        records.sort_by(|a, b| {
            a["jd"]
                .as_f64()
                .partial_cmp(&b["jd"].as_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a["band"].as_str().cmp(&b["band"].as_str()))
                .then_with(|| a["programid"].as_i64().cmp(&b["programid"].as_i64()))
        });
        records
    }
    json!({"prv_candidates": sort(&h.prv_candidates), "prv_nondetections": sort(&h.prv_nondetections), "fp_hists": sort(&h.fp_hists)})
}

fn differences(source: &Value, target: &Value, path: &str, result: &mut Vec<Value>) {
    if source == target || result.len() >= 20 {
        return;
    }
    match (source, target) {
        (Value::Object(a), Value::Object(b)) => {
            let keys: std::collections::BTreeSet<_> = a.keys().chain(b.keys()).collect();
            for key in keys {
                if result.len() >= 20 {
                    break;
                }
                match (a.get(key),b.get(key)) {
                    (Some(source),Some(target)) => differences(source,target,&format!("{path}.{key}"),result),
                    _ => result.push(json!({"path":format!("{path}.{key}"),"source_present":a.contains_key(key),"target_present":b.contains_key(key)})),
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                result.push(json!({"path":path,"source_length":a.len(),"target_length":b.len()}));
            }
            for (i, (a, b)) in a.iter().zip(b).enumerate() {
                differences(a, b, &format!("{path}[{i}]"), result);
            }
        }
        _ => result.push(json!({"path":path,"source":source,"target":target})),
    }
}

/// The same deserializers, SNR/magnitude selection, preparation and light-curve
/// analysis used by ZTF enrichment. No model inference or cross-survey features.
pub fn features(h: &ZtfHistory, ref_jd: f64) -> Result<Value> {
    #[derive(Deserialize)]
    struct Input {
        #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
        prv_candidates: Vec<ZtfPhotometry>,
        #[serde(deserialize_with = "deserialize_ztf_forced_lightcurve")]
        fp_hists: Vec<ZtfPhotometry>,
    }
    let input: Input = serde_json::from_value(serde_json::to_value(h)?)?;
    let mut photometry = input
        .prv_candidates
        .iter()
        .chain(&input.fp_hists)
        .filter(|p| p.jd <= ref_jd)
        .filter_map(|p| p.to_photometry_mag(None))
        .collect();
    prepare_photometry(&mut photometry);
    let (detections, episodes) = summarise_detections(
        input
            .prv_candidates
            .iter()
            .map(|p| (p.jd, p.flux.filter(|f| !f.is_nan()).map(|f| f < 0.0))),
        input
            .fp_hists
            .iter()
            .filter(|p| p.magpsf.is_some())
            .map(|p| p.jd),
        ref_jd,
        EPISODE_GAP_DAYS,
    );
    let stats = if photometry.is_empty() {
        Value::Null
    } else {
        let (per_band, all_bands, _) = analyze_photometry(&photometry);
        json!({"photstats": per_band, "all_bands": all_bands})
    };
    Ok(
        json!({"reference_jd": ref_jd, "usable_points": photometry.len(), "stats": stats,
        "detection_history": detections, "episode_history": episodes}),
    )
}

pub fn compare(
    source: &ZtfHistory,
    target: &ZtfHistory,
    revision: u64,
    write_ms: f64,
    read_ms: f64,
) -> Result<ParityReport> {
    let ref_jd = source
        .prv_candidates
        .iter()
        .chain(&source.fp_hists)
        .chain(&source.prv_nondetections)
        .filter_map(|p| p["jd"].as_f64())
        .reduce(f64::max)
        .unwrap_or(0.0);
    let source_features = features(source, ref_jd)?;
    let target_features = features(target, ref_jd)?;
    let mut raw_differences = Vec::new();
    differences(
        &normalized(source),
        &normalized(target),
        "history",
        &mut raw_differences,
    );
    Ok(ParityReport {
        revision,
        histories_equal: normalized(source) == normalized(target),
        features_equal: source_features == target_features,
        source_counts: counts(source),
        target_counts: counts(target),
        write_ms,
        read_ms,
        mongo_snapshot_ms: 0.0,
        source_features,
        target_features,
        differences: raw_differences,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stored_payload_retains_binary_float_precision() {
        let source = json!({"forcediffimfluxunc":46.038883209228516_f64,
            "magzpsci":26.152799606323242_f64,"pid":9007199254740993_i64});
        let row =
            json!({"kind":"forced","payload":serde_json::to_string(&source).unwrap()}).to_string();
        let restored = ZtfHistory::from_json_each_row(&row).unwrap();
        assert_eq!(restored.fp_hists[0], source);
    }
    #[test]
    fn parity_checks_raw_fields_and_actual_photometry_features() {
        let record = json!({"jd":2461000.5,"band":"g","programid":1,"magpsf":20.0,
            "sigmapsf":0.1,"diffmaglim":21.0,"psfFlux":10.0,"psfFluxErr":1.0,"snr_psf":10.0});
        let source = ZtfHistory {
            prv_candidates: vec![record],
            ..Default::default()
        };
        let mut target: ZtfHistory =
            serde_json::from_value(serde_json::to_value(&source).unwrap()).unwrap();
        let equal = compare(&source, &target, 1, 0.0, 0.0).unwrap();
        assert!(equal.histories_equal && equal.features_equal);
        assert_eq!(equal.source_features["usable_points"], 1);
        target.prv_candidates[0]["programid"] = json!(3);
        assert!(
            !compare(&source, &target, 1, 0.0, 0.0)
                .unwrap()
                .histories_equal
        );
        target.prv_candidates[0]["magpsf"] = json!(19.0);
        assert!(
            !compare(&source, &target, 1, 0.0, 0.0)
                .unwrap()
                .features_equal
        );
        assert_eq!(features(&source, 2460000.0).unwrap()["usable_points"], 0);
    }

    #[test]
    fn diagnostics_distinguish_missing_fields_and_bound_array_output() {
        let mut output = Vec::new();
        differences(&json!({}), &json!({"missing":null}), "root", &mut output);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["source_present"], false);
        output.clear();
        differences(&json!([1, 2]), &json!([1]), "root", &mut output);
        assert_eq!(output[0]["source_length"], 2);
        assert!(output[0].get("source").is_none());
    }
}
