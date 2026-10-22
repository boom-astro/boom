//! Migration-only adapter from accepted aux snapshots to the points contract.
use super::{History, PhotometryStore};
use crate::{
    enrichment::{create_ztf_alert_pipeline, ZtfAlertForEnrichment},
    utils::lightcurves::{
        analyze_photometry, prepare_photometry, summarise_detections, EPISODE_GAP_DAYS,
    },
};
use anyhow::{ensure, Context, Result};
use futures::TryStreamExt;
use mongodb::{
    bson::{self, doc, Bson, DateTime, Document},
    Database,
};
use serde::Serialize;
use serde_json::{json, Value};

pub const STATUS: &str = "ZTF_photometry_points_comparisons";

#[derive(Debug, Serialize)]
pub struct Comparison {
    pub revision: i64,
    pub histories_equal: bool,
    pub enrichment_equal: bool,
    pub features_equal: bool,
    pub counts: [usize; 3],
}

impl Comparison {
    pub fn equal(&self) -> bool {
        self.histories_equal && self.enrichment_equal && self.features_equal
    }
}

pub async fn accepted_snapshot(db: &Database, object_id: &str) -> Result<(i64, History)> {
    fn finite(value: &Bson) -> bool {
        match value {
            Bson::Double(n) => n.is_finite(),
            Bson::Array(items) => items.iter().all(finite),
            Bson::Document(fields) => fields.values().all(finite),
            _ => true,
        }
    }
    let source = db
        .collection::<Document>("ZTF_alerts_aux")
        .find_one(doc! {"_id":object_id})
        .projection(doc! {"version":1,"prv_candidates":1,"prv_nondetections":1,"fp_hists":1})
        .await?
        .context("missing authoritative aux history")?;
    let revision = match source.get("version") {
        None | Some(Bson::Null) => 0,
        Some(Bson::Int32(n)) => i64::from(*n),
        Some(Bson::Int64(n)) => *n,
        _ => anyhow::bail!("invalid aux revision"),
    };
    ensure!(revision >= 0, "negative aux revision");
    // JSON cannot represent NaN/Infinity; reject rather than turn them into null.
    ensure!(
        source.values().all(finite),
        "non-finite accepted history field"
    );
    let mut history: History = serde_json::from_value(serde_json::to_value(source)?)?;
    history.measurements(object_id, revision)?;
    history.sort();
    Ok((revision, history))
}

fn features(alert: &ZtfAlertForEnrichment) -> Value {
    let epoch = alert.candidate.candidate.jd;
    let mut photometry = alert
        .prv_candidates
        .iter()
        .chain(&alert.fp_hists)
        .filter(|p| p.jd <= epoch)
        .filter_map(|p| p.to_photometry_mag(None))
        .collect();
    prepare_photometry(&mut photometry);
    let (detections, episodes) = summarise_detections(
        alert
            .prv_candidates
            .iter()
            .map(|p| (p.jd, p.flux.filter(|f| !f.is_nan()).map(|f| f < 0.))),
        alert
            .fp_hists
            .iter()
            .filter(|p| p.magpsf.is_some())
            .map(|p| p.jd),
        epoch,
        EPISODE_GAP_DAYS,
    );
    let stats = if photometry.is_empty() {
        Value::Null
    } else {
        let (bands, all_bands, _) = analyze_photometry(&photometry);
        json!({"bands":bands,"all_bands":all_bands})
    };
    json!({"stats":stats,"detections":detections,"episodes":episodes})
}

/// Both inputs pass through BOOM's actual enrichment deserializers. Cross-survey
/// histories and catalog context stay identical while ZTF arrays are replaced.
pub fn compare_input(source: Document, history: &History) -> Result<(bool, bool)> {
    let baseline: ZtfAlertForEnrichment = bson::from_document(source.clone())?;
    let mut hydrated = source;
    hydrated.extend(bson::to_document(history)?);
    let hydrated: ZtfAlertForEnrichment = bson::from_document(hydrated)?;
    Ok((
        serde_json::to_value(&baseline)? == serde_json::to_value(&hydrated)?,
        features(&baseline) == features(&hydrated),
    ))
}

/// Synchronous, opt-in comparison before Redis acknowledgement. A retry through
/// the existing-alert shortcut repairs a partially written points batch.
pub async fn mirror_and_compare(
    db: &Database,
    store: &dyn PhotometryStore,
    candid: i64,
    object_id: &str,
) -> Result<Comparison> {
    let result = compare_stable_snapshot(db, store, candid, object_id).await;
    let state = match &result {
        Ok(report) if report.equal() => "pass",
        Ok(_) => "mismatch",
        Err(_) => "error",
    };
    db.collection::<Document>(STATUS).update_one(doc! {"_id":candid}, doc! {"$set":{
        "object_id":object_id,"state":state,"report":bson::to_bson(&result.as_ref().ok())?,
        "error":result.as_ref().err().map(|error|format!("{error:#}")),"compared_at":DateTime::now()
    }}).upsert(true).await?;
    let report = result?;
    ensure!(
        report.equal(),
        "points history or enrichment parity mismatch for {object_id}"
    );
    Ok(report)
}

async fn compare_stable_snapshot(
    db: &Database,
    store: &dyn PhotometryStore,
    candid: i64,
    object_id: &str,
) -> Result<Comparison> {
    for _ in 0..3 {
        let (revision, source) = accepted_snapshot(db, object_id).await?;
        store
            .write_points(&source.measurements(object_id, revision)?)
            .await?;
        let restored = store.read_history(object_id).await?;
        let mut pipeline = create_ztf_alert_pipeline(false);
        pipeline[0] = doc! {"$match":{"_id":candid,"objectId":object_id}};
        let baseline = db
            .collection::<Document>("ZTF_alerts")
            .aggregate(pipeline)
            .await?
            .try_next()
            .await?
            .context("comparison requires a completed source alert")?;
        // A new accepted aux update can race both the points and pipeline reads.
        let (current_revision, current) = accepted_snapshot(db, object_id).await?;
        if current_revision != revision || current != source || restored.max_revision > revision {
            continue;
        }
        let (enrichment_equal, features_equal) = compare_input(baseline, &restored.history)?;
        let report = Comparison {
            revision,
            histories_equal: source == restored.history,
            enrichment_equal,
            features_equal,
            counts: [
                source.prv_candidates.len(),
                source.prv_nondetections.len(),
                source.fp_hists.len(),
            ],
        };
        return Ok(report);
    }
    anyhow::bail!("aux changed during points comparison; retry the completed alert")
}
