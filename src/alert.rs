use mongodb::bson::doc;

use crate::spatial;
use crate::types;
use crate::time;

pub async fn process_alert(
    avro_bytes: Vec<u8>,
    xmatch_configs: &Vec<types::CatalogXmatchConfig>,
    db: &mongodb::Database
) -> Result<Option<i64>, Box<dyn std::error::Error>> {

    let start = std::time::Instant::now();

    // decode the alert
    let alert = match types::Alert::from_avro_bytes(avro_bytes) {
        Ok(alert) => alert,
        Err(e) => {
            println!("Error reading alert packet: {}", e);
            return Ok(None);
        }
    };

    println!("Decoded alert in {:.6} seconds", start.elapsed().as_secs_f64());

    let start = std::time::Instant::now();

    // check if the alert already exists in the alerts collection
    let collection_alert = db.collection("alerts");
    if !collection_alert.find_one(doc! { "candid": &alert.candid }).await.unwrap().is_none() {
        // we return early if there is already an alert with the same candid
        println!("alert with candid {} already exists", &alert.candid);
        return Ok(None);
    }

    println!("Checked for existing alert in {:.6} seconds", start.elapsed().as_secs_f64());

    let start = std::time::Instant::now();

    // get the object_id, candid, ra, dec from the alert
    let object_id = alert.object_id.clone();
    let candid = alert.candid;
    let ra = alert.candidate.ra;
    let dec = alert.candidate.dec;

    // separate the alert and its history components
    let (
        alert_no_history,
        prv_candidates,
        fp_hist
    ) = alert.pop_history();

    println!("Separated alert from history in {:.6} seconds", start.elapsed().as_secs_f64());

    let start = std::time::Instant::now();
    
    // insert the alert into the alerts collection (with a created_at timestamp)
    let mut alert_doc = alert_no_history.mongify();
    alert_doc.insert("created_at", time::jd_now());
    collection_alert.insert_one(alert_doc).await.unwrap();

    println!("Inserted alert into alerts collection in {:.6} seconds", start.elapsed().as_secs_f64());

    // - new objects - new entry with prv_candidates, fp_hists, xmatches
    // - existing objects - update prv_candidates, fp_hists
    // (with created_at and updated_at timestamps)
    let collection_alert_aux: mongodb::Collection<mongodb::bson::Document> = db.collection("alerts_aux");

    let start = std::time::Instant::now();

    let prv_candidates_doc = prv_candidates.unwrap_or(vec![]).into_iter().map(|x| x.mongify()).collect::<Vec<_>>();
    let fp_hist_doc = fp_hist.unwrap_or(vec![]).into_iter().map(|x| x.mongify()).collect::<Vec<_>>();

    println!("Converted prv_candidates and fp_hists in {:.6} seconds", start.elapsed().as_secs_f64());

    let start = std::time::Instant::now();

    if collection_alert_aux.find_one(doc! { "_id": &object_id }).await.unwrap().is_none() {
        let start_new = std::time::Instant::now();
        let jd_timestamp = time::jd_now();
        let mut doc = doc! {
            "_id": &object_id,
            "prv_candidates": prv_candidates_doc,
            "fp_hists": fp_hist_doc,
            "created_at": jd_timestamp,
            "updated_at": jd_timestamp,
        };
        let start_xmatch = std::time::Instant::now();
        doc.insert("cross_matches", spatial::xmatch(ra, dec, xmatch_configs, &db).await);

        println!("Cross matched in {:.6} seconds", start_xmatch.elapsed().as_secs_f64());

        collection_alert_aux.insert_one(doc).await.unwrap();

        println!("Inserted new alert_aux in {:.6} seconds", start_new.elapsed().as_secs_f64());
    } else {
        let start_update = std::time::Instant::now();
        let update_doc = doc! {
            "$addToSet": {
                "prv_candidates": { "$each": prv_candidates_doc },
                "fp_hists": { "$each": fp_hist_doc }
            },
            "$set": {
                "updated_at": time::jd_now(),
            }
        };

        collection_alert_aux.update_one(doc! { "_id": &object_id }, update_doc).await.unwrap();

        println!("Updated alert_aux in {:.6} seconds", start_update.elapsed().as_secs_f64());
    }

    println!("Inserted or updated alert_aux in {:.6} seconds\n", start.elapsed().as_secs_f64());

    Ok(Some(candid))
}