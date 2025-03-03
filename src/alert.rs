use crate::spatial;
use crate::types;
use flare::time::Time;
use mongodb::bson::doc;
use tracing::{error, info};

#[derive(thiserror::Error, Debug)]
pub enum AlertError {
    #[error("failed to decode alert")]
    DecodeError(#[from] Box<dyn std::error::Error>),
    #[error("failed to find candid in the alert collection")]
    FindCandIdError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert collection")]
    InsertAlertError(#[source] mongodb::error::Error),
    #[error("failed to find objectid in the aux alert collection")]
    FindObjectIdError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert aux collection")]
    InsertAuxAlertError(#[source] mongodb::error::Error),
    #[error("failed to update the alert aux collection")]
    UpdateAuxAlertError(#[source] mongodb::error::Error),
}

pub async fn process_alert(
    avro_bytes: &[u8],
    xmatch_configs: &Vec<types::CatalogXmatchConfig>,
    db: &mongodb::Database,
    alert_collection: &mongodb::Collection<mongodb::bson::Document>,
    alert_aux_collection: &mongodb::Collection<mongodb::bson::Document>,
    schema: &apache_avro::Schema,
) -> Result<Option<i64>, AlertError> {
    // decode the alert
    let alert = types::Alert::from_avro_bytes_unsafe(avro_bytes, schema)?;

    // check if the alert already exists in the alerts collection
    if let Some(_) = alert_collection
        .find_one(doc! { "candid": &alert.candid })
        .await
        .map_err(AlertError::FindCandIdError)?
    {
        // we return early if there is already an alert with the same candid
        info!("alert with candid {} already exists", &alert.candid);
        return Ok(None);
    }

    // get the object_id, candid, ra, dec from the alert
    let object_id = alert.object_id.clone();
    let candid = alert.candid;
    let ra = alert.candidate.ra;
    let dec = alert.candidate.dec;

    // separate the alert and its history components
    let (alert_no_history, prv_candidates, fp_hist) = alert.pop_history();

    // insert the alert into the alerts collection (with a created_at timestamp)
    let mut alert_doc = alert_no_history.mongify();
    alert_doc.insert("created_at", Time::now().to_jd());
    alert_collection
        .insert_one(alert_doc)
        .await
        .map_err(AlertError::InsertAlertError)?;

    // - new objects - new entry with prv_candidates, fp_hists, xmatches
    // - existing objects - update prv_candidates, fp_hists
    // (with created_at and updated_at timestamps)

    let prv_candidates_doc = prv_candidates
        .unwrap_or(vec![])
        .into_iter()
        .map(|x| x.mongify())
        .collect::<Vec<_>>();
    let fp_hist_doc = fp_hist
        .unwrap_or(vec![])
        .into_iter()
        .map(|x| x.mongify())
        .collect::<Vec<_>>();

    match alert_aux_collection
        .find_one(doc! { "_id": &object_id })
        .await
        .map_err(AlertError::FindObjectIdError)?
    {
        None => {
            let jd_timestamp = Time::now().to_jd();
            let mut doc = doc! {
                "_id": &object_id,
                "prv_candidates": prv_candidates_doc,
                "fp_hists": fp_hist_doc,
                "created_at": jd_timestamp,
                "updated_at": jd_timestamp,
                "coordinates": {
                    "radec_geojson": {
                        "type": "Point",
                        "coordinates": [ra - 180.0, dec],
                    },
                },
            };
            doc.insert(
                "cross_matches",
                spatial::xmatch(ra, dec, xmatch_configs, &db).await,
            );

            alert_aux_collection
                .insert_one(doc)
                .await
                .map_err(AlertError::InsertAuxAlertError)?;
        }
        Some(_) => {
            let update_doc = doc! {
                "$addToSet": {
                    "prv_candidates": { "$each": prv_candidates_doc },
                    "fp_hists": { "$each": fp_hist_doc }
                },
                "$set": {
                    "updated_at": Time::now().to_jd(),
                }
            };

            alert_aux_collection
                .update_one(doc! { "_id": &object_id }, update_doc)
                .await
                .map_err(AlertError::UpdateAuxAlertError)?;
        }
    }

    Ok(Some(candid))
}
