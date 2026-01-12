use anyhow::{Error, Result};
use boom::{
    alert::{
        deserialize_fp_hists, deserialize_prv_candidate, deserialize_prv_candidates, ZtfForcedPhot,
        ZtfObject, ZtfPrvCandidate,
    },
    utils::o11y::logging::build_subscriber,
    utils::spatial::Coordinates,
};
use clap::Parser;
use futures::stream::StreamExt;
use mongodb::{bson::doc, Client, Collection};
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{error, warn};

#[derive(Parser)]
struct Cli {
    #[arg(long, help = "IDs of objects to import.", required = true)]
    obj_ids: Vec<String>,
    #[arg(
        long,
        help = "Kowalski MongoDB URI (should use read-only credentials).",
        env = "KOWALSKI_MONGODB_URI"
    )]
    kowalski_uri: String,
    #[arg(
        long,
        help = "Boom MongoDB URI (should use write-enabled credentials).",
        env = "BOOM_MONGODB_URI"
    )]
    boom_uri: String,
}

#[derive(Debug, Deserialize)]
struct KowalskiObjId {
    #[serde(rename = "_id")]
    object_id: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct KowalskiZtfObject {
    #[serde(rename = "_id")]
    object_id: String,
    #[serde(deserialize_with = "deserialize_prv_candidates")]
    prv_candidates: Option<Vec<ZtfPrvCandidate>>,
    #[serde(deserialize_with = "deserialize_fp_hists")]
    fp_hists: Option<Vec<ZtfForcedPhot>>,
}

#[derive(Debug, Deserialize)]
struct KowalskiZtfAlert {
    #[serde(rename = "objectId")]
    object_id: String,
    #[serde(deserialize_with = "deserialize_prv_candidate")]
    candidate: ZtfPrvCandidate,
}

// #[derive(Debug, Deserialize, Serialize)]
// struct ZtfObject {
//     #[serde(rename = "_id")]
//     object_id: String,
//     prv_candidates: Vec<ZtfPrvCandidate>,
//     #[serde(default)]
//     prv_nondetections: Vec<ZtfPrvCandidate>,
//     fp_hists: Vec<ZtfForcedPhot>,
//     cross_matches: Option<HashMap<String, Vec<String>>>,
//     coordinates: Coordinates,
//     created_at: f64,
//     updated_at: f64,
// }

fn process_object(
    ztf_obj: KowalskiZtfObject,
    alerts: &HashMap<String, Vec<KowalskiZtfAlert>>,
) -> Result<ZtfObject, Error> {
    let mut prv_candidates_doc = vec![];
    let mut prv_nondetections_doc = vec![];

    let mut brightest_candidate_mag = std::f32::MAX;
    let mut brightest_candidate_ra = 0.0;
    let mut brightest_candidate_dec = 0.0;
    for prv_candidate in ztf_obj.prv_candidates.unwrap() {
        if prv_candidate.prv_candidate.magpsf.is_some() {
            if brightest_candidate_mag > prv_candidate.prv_candidate.magpsf.unwrap() {
                brightest_candidate_mag = prv_candidate.prv_candidate.magpsf.unwrap();
                brightest_candidate_ra = prv_candidate.prv_candidate.ra.unwrap();
                brightest_candidate_dec = prv_candidate.prv_candidate.dec.unwrap();
            }
            prv_candidates_doc.push(prv_candidate);
        } else {
            prv_nondetections_doc.push(prv_candidate);
        }
    }
    for alert in alerts.get(&ztf_obj.object_id).unwrap_or(&vec![]) {
        let candidate = &alert.candidate;
        if brightest_candidate_mag > candidate.prv_candidate.magpsf.unwrap() {
            brightest_candidate_mag = candidate.prv_candidate.magpsf.unwrap();
            brightest_candidate_ra = candidate.prv_candidate.ra.unwrap();
            brightest_candidate_dec = candidate.prv_candidate.dec.unwrap();
        }
        prv_candidates_doc.push(candidate.clone());
    }
    if brightest_candidate_mag == std::f32::MAX {
        // throw an error
        return Err(anyhow::anyhow!(
            "No valid candidates with magpsf found for object {}",
            ztf_obj.object_id
        ));
    }
    // deduplicate the prv_candidates by jd and band, and sort by jd ascending
    prv_candidates_doc.sort_by(|a, b| {
        a.prv_candidate
            .jd
            .partial_cmp(&b.prv_candidate.jd)
            .unwrap()
            .then(a.band.cmp(&b.band))
    });
    prv_candidates_doc
        .dedup_by(|a, b| a.prv_candidate.jd == b.prv_candidate.jd && a.band == b.band);
    // same with the prv_nondetections
    prv_nondetections_doc.sort_by(|a, b| {
        a.prv_candidate
            .jd
            .partial_cmp(&b.prv_candidate.jd)
            .unwrap()
            .then(a.band.cmp(&b.band))
    });
    prv_nondetections_doc
        .dedup_by(|a, b| a.prv_candidate.jd == b.prv_candidate.jd && a.band == b.band);
    // and finally, with the fp_hists inside the ztf_obj.fp_hists
    let mut obj_fp_hists = vec![];
    if let Some(fp_hists) = ztf_obj.fp_hists {
        for fp_hist in fp_hists {
            obj_fp_hists.push(fp_hist);
        }
        obj_fp_hists.sort_by(|a, b| {
            a.fp_hist
                .jd
                .partial_cmp(&b.fp_hist.jd)
                .unwrap()
                .then(a.band.cmp(&b.band))
        });
        obj_fp_hists.dedup_by(|a, b| a.fp_hist.jd == b.fp_hist.jd && a.band == b.band);
    }
    let now = flare::Time::now().to_jd();
    Ok(ZtfObject {
        object_id: ztf_obj.object_id,
        prv_candidates: prv_candidates_doc,
        prv_nondetections: prv_nondetections_doc,
        fp_hists: obj_fp_hists,
        cross_matches: None,
        aliases: None,
        coordinates: Coordinates::new(brightest_candidate_ra, brightest_candidate_dec),
        created_at: now,
        updated_at: now,
    })
}

async fn get_kowalski_alerts_by_object_id(
    obj_ids: &Vec<String>,
    kowalski_alert_collection: &Collection<KowalskiZtfAlert>,
) -> HashMap<String, Vec<KowalskiZtfAlert>> {
    let mut alerts: HashMap<String, Vec<KowalskiZtfAlert>> = HashMap::new();
    let filter = doc! { "objectId": { "$in": obj_ids } };
    let mut cursor = kowalski_alert_collection
        .find(filter)
        .projection(doc! {"objectId": 1, "candid": 1, "candidate": 1, "_id": 0})
        .await
        .unwrap();
    while let Some(result) = cursor.next().await {
        let alert = result.unwrap();
        alerts
            .entry(alert.object_id.clone())
            .or_insert(vec![])
            .push(alert);
    }
    alerts
}

async fn insert_boom_objects(boom_objs: &Vec<ZtfObject>, boom_collection: &Collection<ZtfObject>) {
    for chunk in boom_objs.chunks(1000.min(boom_objs.len())) {
        match boom_collection.insert_many(chunk).ordered(false).await {
            Ok(_result) => {}
            Err(e) => {
                match *e.kind {
                    // if it's just duplicate errors (11000), we can ignore them
                    mongodb::error::ErrorKind::InsertMany(insert_many_error) => {
                        if let Some(write_errors) = insert_many_error.write_errors {
                            for write_error in write_errors {
                                if write_error.code != 11000 {
                                    error!("Error inserting document: {:?}", write_error);
                                }
                            }
                        }
                    }
                    _ => {
                        error!("Error inserting documents: {}", e);
                    }
                }
            }
        }
    }
}

async fn get_kowalski_collections(
    kowalski_uri: &str,
) -> (
    Collection<KowalskiZtfAlert>,
    Collection<KowalskiZtfObject>,
    Collection<KowalskiObjId>,
) {
    let kowalski_client = Client::with_uri_str(kowalski_uri).await.unwrap();
    let kowalski_alert_collection: Collection<KowalskiZtfAlert> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts");
    let kowalski_obj_collection: Collection<KowalskiZtfObject> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts_aux");
    let kowalski_obj_collection_idonly: Collection<KowalskiObjId> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts_aux");
    (
        kowalski_alert_collection,
        kowalski_obj_collection,
        kowalski_obj_collection_idonly,
    )
}

async fn get_boom_obj_collection(boom_uri: &str) -> Collection<ZtfObject> {
    let boom_client = Client::with_uri_str(boom_uri).await.unwrap();
    let boom_collection: Collection<ZtfObject> =
        boom_client.database("boom").collection("ZTF_alerts_aux");
    // create indexes if they do not exist
    boom_collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "coordinates.radec_geojson": "2dsphere", "_id": 1 })
                .options(None)
                .build(),
        )
        .await
        .unwrap();
    boom_collection
}

// in some instances, we may not have an entry in Kowalski for the object itself, but we may have alerts for it
// when that happens, we need a method to create a ZtfObject from just the Kowalski alerts
fn make_obj_from_alerts(candidates: &Vec<KowalskiZtfAlert>) -> Result<ZtfObject, Error> {
    if candidates.is_empty() {
        return Err(anyhow::anyhow!("No candidates provided"));
    }
    let mut prv_candidates_doc = vec![];

    let mut brightest_candidate_mag = std::f32::MAX;
    let mut brightest_candidate_ra = 0.0;
    let mut brightest_candidate_dec = 0.0;
    for alert in candidates {
        let candidate = &alert.candidate;
        if brightest_candidate_mag > candidate.prv_candidate.magpsf.unwrap() {
            brightest_candidate_mag = candidate.prv_candidate.magpsf.unwrap();
            brightest_candidate_ra = candidate.prv_candidate.ra.unwrap();
            brightest_candidate_dec = candidate.prv_candidate.dec.unwrap();
        }
        prv_candidates_doc.push(candidate.clone());
    }
    if brightest_candidate_mag == std::f32::MAX {
        return Err(anyhow::anyhow!(
            "No valid candidates with magpsf found for object {}",
            candidates[0].object_id
        ));
    }
    // deduplicate the prv_candidates by jd and band, and sort by jd ascending
    prv_candidates_doc.sort_by(|a, b| {
        a.prv_candidate
            .jd
            .partial_cmp(&b.prv_candidate.jd)
            .unwrap()
            .then(a.band.cmp(&b.band))
    });
    prv_candidates_doc
        .dedup_by(|a, b| a.prv_candidate.jd == b.prv_candidate.jd && a.band == b.band);

    let now = flare::Time::now().to_jd();
    Ok(ZtfObject {
        object_id: candidates[0].object_id.clone(),
        prv_candidates: prv_candidates_doc,
        prv_nondetections: vec![],
        fp_hists: vec![],
        cross_matches: None,
        aliases: None,
        coordinates: Coordinates::new(brightest_candidate_ra, brightest_candidate_dec),
        created_at: now,
        updated_at: now,
    })
}

async fn make_boom_objs_from_kowalski(
    obj_ids: &Vec<String>,
    kowalski_obj_collection: &Collection<KowalskiZtfObject>,
    kowalski_alert_collection: &Collection<KowalskiZtfAlert>,
) -> Vec<ZtfObject> {
    let mut alerts = get_kowalski_alerts_by_object_id(&obj_ids, &kowalski_alert_collection).await;
    println!(
        "Retrieved alerts from Kowalski for {} objects.",
        alerts.len()
    );

    let mut boom_objs = vec![];
    let mut cursor = kowalski_obj_collection
        .find(doc! { "_id": { "$in": obj_ids } })
        .projection(doc! { "prv_candidates": 1, "fp_hists": 1 })
        .await
        .unwrap();
    println!("Retrieved objects from Kowalski. Processing...");
    while let Some(result) = cursor.next().await {
        match result {
            Ok(ztf_obj) => {
                let obj_id = ztf_obj.object_id.clone();
                match process_object(ztf_obj, &alerts) {
                    Ok(boom_obj) => {
                        boom_objs.push(boom_obj);
                    }
                    Err(e) => {
                        error!("Error processing object {}: {}", obj_id, e);
                    }
                }
                // remove the alerts for this object id to save memory
                alerts.remove(&obj_id);
            }
            Err(e) => {
                error!("Error fetching document: {}", e);
            }
        }
    }

    // process remaining alerts that did not have a corresponding Kowalski object
    for (obj_id, alert_list) in alerts.iter() {
        match make_obj_from_alerts(alert_list) {
            Ok(boom_obj) => {
                boom_objs.push(boom_obj);
                warn!("Created Boom object from alerts only for object {}", obj_id);
            }
            Err(e) => {
                error!("Error processing alerts for object {}: {}", obj_id, e);
            }
        }
    }
    boom_objs
}

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let args = Cli::parse();
    let kowalski_uri = args.kowalski_uri;
    let boom_uri = args.boom_uri;
    let obj_ids = args.obj_ids;

    let (kowalski_alert_collection, kowalski_obj_collection, _) =
        get_kowalski_collections(&kowalski_uri).await;
    println!(
        "Retrieved Kowalski collections: alerts and objects. Processing {} object IDs.",
        obj_ids.len()
    );
    let boom_obj_collection = get_boom_obj_collection(&boom_uri).await;
    println!("Retrieved Boom collection for ZTF objects.");

    // Process remaining documents
    let boom_objs = make_boom_objs_from_kowalski(
        &obj_ids,
        &kowalski_obj_collection,
        &kowalski_alert_collection,
    )
    .await;
    insert_boom_objects(&boom_objs, &boom_obj_collection).await;
}
