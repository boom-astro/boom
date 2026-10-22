//! ZTF points-store contract. Aux remains authoritative during comparison.
pub mod compare;
pub mod mongo;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const SCHEMA_VERSION: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Detection,
    UpperLimit,
    Forced,
}

/// Version 1 follows BOOM's existing per-kind JD identity. It must not be
/// generalized to surveys with distinct exposures at the same epoch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Measurement {
    pub object_id: String,
    pub kind: Kind,
    pub jd: f64,
    pub revision: i64,
    pub record: Value,
}

impl Measurement {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.object_id.is_empty(), "empty measurement object ID");
        ensure!(
            self.jd.is_finite() && self.jd > 0.,
            "invalid measurement JD"
        );
        ensure!(self.revision >= 0, "negative accepted revision");
        ensure!(
            self.record["jd"].as_f64() == Some(self.jd),
            "record JD differs from identity"
        );
        ensure!(self.record["band"].is_string(), "missing measurement band");
        ensure!(
            self.record["programid"].as_i64().is_some(),
            "missing program ID"
        );
        Ok(())
    }

    /// A portable key, independent of Mongo ObjectId and BSON field ordering.
    pub fn id(&self) -> Result<String> {
        self.validate()?;
        let key = serde_json::to_vec(&(SCHEMA_VERSION, &self.object_id, self.kind, self.jd))?;
        Ok(Sha256::digest(key)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct History {
    pub prv_candidates: Vec<Value>,
    pub prv_nondetections: Vec<Value>,
    pub fp_hists: Vec<Value>,
}

impl History {
    pub fn measurements(&self, object_id: &str, revision: i64) -> Result<Vec<Measurement>> {
        let mut points = Vec::new();
        for (kind, records) in [
            (Kind::Detection, &self.prv_candidates),
            (Kind::UpperLimit, &self.prv_nondetections),
            (Kind::Forced, &self.fp_hists),
        ] {
            let mut epochs = std::collections::BTreeSet::new();
            for record in records {
                let point = Measurement {
                    object_id: object_id.to_owned(),
                    kind,
                    jd: record["jd"]
                        .as_f64()
                        .ok_or_else(|| anyhow::anyhow!("missing JD"))?,
                    revision,
                    record: record.clone(),
                };
                point.validate()?;
                ensure!(
                    epochs.insert(point.jd.to_bits()),
                    "duplicate per-kind epoch in source"
                );
                points.push(point);
            }
        }
        Ok(points)
    }

    pub fn push(&mut self, point: Measurement) {
        match point.kind {
            Kind::Detection => &mut self.prv_candidates,
            Kind::UpperLimit => &mut self.prv_nondetections,
            Kind::Forced => &mut self.fp_hists,
        }
        .push(point.record);
    }

    pub fn sort(&mut self) {
        for records in [
            &mut self.prv_candidates,
            &mut self.prv_nondetections,
            &mut self.fp_hists,
        ] {
            records.sort_by(|a, b| {
                a["jd"]
                    .as_f64()
                    .unwrap()
                    .total_cmp(&b["jd"].as_f64().unwrap())
            });
        }
    }
}

#[derive(Debug, Default)]
pub struct StoredHistory {
    pub history: History,
    pub max_revision: i64,
}

/// Internal, unrestricted history reads. Public callers must enforce program
/// permissions separately; this is not an API authorization boundary.
#[async_trait::async_trait]
pub trait PhotometryStore: Send + Sync {
    async fn write_points(&self, points: &[Measurement]) -> Result<()>;
    async fn read_history(&self, object_id: &str) -> Result<StoredHistory>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn identity_is_epoch_based_and_revision_independent() {
        let mut point = Measurement {
            object_id: "ZTF_test".into(),
            kind: Kind::Detection,
            jd: 2460000.5,
            revision: 0,
            record: json!({"jd":2460000.5,"band":"g","programid":1}),
        };
        let id = point.id().unwrap();
        point.revision = 50;
        point.record["programid"] = json!(2);
        assert_eq!(id, point.id().unwrap());
        point.kind = Kind::Forced;
        assert_ne!(id, point.id().unwrap());
        point.jd = f64::NAN;
        assert!(point.id().is_err());
    }

    #[test]
    fn duplicate_source_epochs_are_rejected_and_float_payloads_round_trip() {
        let record = json!({"jd":2460000.5,"band":"g","programid":1,
            "psfFluxErr":46.038883209228516_f64,"pid":9007199254740993_i64});
        let history = History {
            prv_candidates: vec![record.clone()],
            ..Default::default()
        };
        let restored: History =
            serde_json::from_str(&serde_json::to_string(&history).unwrap()).unwrap();
        assert_eq!(history, restored);
        let duplicate = History {
            prv_candidates: vec![record.clone(), record],
            ..Default::default()
        };
        assert!(duplicate.measurements("ZTF_test", 0).is_err());
    }
}
