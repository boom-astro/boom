use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use super::TaskError;
use crate::utils::mpcorb::{
    drop_epochs, load_mpcorb, propagate_to_epochs, stored_epochs, PropagationError,
    MAX_EPOCH_GAP_DAYS, PROPAGATED_COLLECTION,
};
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "propagate_orbits";

const MAX_EPOCHS: usize = 400;
const FIRST_JD: f64 = 2_396_759.0;
const LAST_JD: f64 = 2_506_352.0;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PropagateOrbitsParams {
    /// First epoch, JD.
    pub start_jd: f64,
    /// Last date to cover, JD.
    pub end_jd: f64,
    /// Days between epochs.
    #[serde(default = "default_step_days")]
    pub step_days: f64,
    /// Remove the stored epochs in the range instead.
    #[serde(default)]
    pub drop: bool,
}

fn default_step_days() -> f64 {
    2.0 * MAX_EPOCH_GAP_DAYS
}

impl PropagateOrbitsParams {
    pub fn epochs(&self) -> Vec<f64> {
        let n = ((self.end_jd - self.start_jd) / self.step_days).ceil() as usize;
        (0..=n)
            .map(|k| self.start_jd + k as f64 * self.step_days)
            .collect()
    }

    pub fn validate_params(&self) -> Result<(), String> {
        if ![self.start_jd, self.end_jd, self.step_days]
            .iter()
            .all(|v| v.is_finite())
        {
            return Err("start_jd, end_jd and step_days must be numbers".to_string());
        }
        if self.end_jd < self.start_jd {
            return Err("end_jd must not precede start_jd".to_string());
        }
        if self.start_jd < FIRST_JD || self.end_jd > LAST_JD {
            return Err(format!(
                "the planetary ephemeris covers JD {FIRST_JD} to {LAST_JD}"
            ));
        }
        let max_step = default_step_days();
        if !(1.0..=max_step).contains(&self.step_days) {
            return Err(format!("step_days must be between 1 and {max_step}"));
        }
        if self.epochs().len() > MAX_EPOCHS {
            return Err(format!("at most {MAX_EPOCHS} epochs per run"));
        }
        Ok(())
    }
}

pub async fn run(
    ctx: &TaskContext,
    params: PropagateOrbitsParams,
) -> Result<serde_json::Value, TaskError> {
    let db = ctx.db();
    let target = MutationTarget {
        database: db.name().to_string(),
        collection: PROPAGATED_COLLECTION.to_string(),
        catalog: None,
        survey: None,
    };

    if params.drop {
        let epochs = stored_epochs(db, params.start_jd, params.end_jd, false)
            .await
            .map_err(|e| TaskError::Failed(e.to_string()))?;
        let removed = drop_epochs(db, &epochs)
            .await
            .map_err(|e| TaskError::Failed(e.to_string()))?;
        ctx.info(format!("dropped {} epochs, {removed} orbits", epochs.len()));
        ctx.record_mutation(
            target,
            Operation::Delete,
            doc! { "epochs": epochs.clone(), "orbits": removed as i64 },
        )
        .await;
        return Ok(serde_json::json!({ "dropped_epochs": epochs, "orbits": removed }));
    }

    let epochs = params.epochs();
    let orbits = load_mpcorb(db)
        .await
        .map_err(|e| TaskError::Failed(e.to_string()))?;
    if orbits.is_empty() {
        return Err(TaskError::Failed(
            "MPC_orbits is empty, run mpcorb_ingest first".to_string(),
        ));
    }
    ctx.info(format!(
        "propagating {} orbits to {} epochs",
        orbits.len(),
        epochs.len()
    ));
    let written = propagate_to_epochs(db, &orbits, &epochs, false, 0, |done, total| async move {
        ctx.progress(done, total, "orbits propagated").await;
        !ctx.is_canceled()
    })
    .await
    .map_err(|e| match e {
        PropagationError::Canceled => TaskError::Canceled,
        e => TaskError::Failed(e.to_string()),
    })?;

    ctx.info(format!("stored {written} propagated orbits"));
    ctx.record_mutation(
        target,
        Operation::Recompute,
        doc! {
            "epochs": epochs.clone(),
            "orbits": written as i64,
            "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                .unwrap_or(mongodb::bson::Bson::Null),
        },
    )
    .await;
    Ok(serde_json::json!({ "epochs": epochs, "orbits": written }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(start_jd: f64, end_jd: f64) -> PropagateOrbitsParams {
        PropagateOrbitsParams {
            start_jd,
            end_jd,
            step_days: default_step_days(),
            drop: false,
        }
    }

    #[test]
    fn every_date_in_the_range_is_near_an_epoch() {
        let p = params(2_459_000.5, 2_459_400.5);
        let epochs = p.epochs();
        let mut jd = p.start_jd;
        while jd <= p.end_jd {
            assert!(epochs.iter().any(|e| (e - jd).abs() <= MAX_EPOCH_GAP_DAYS));
            jd += 0.25;
        }
    }
}
