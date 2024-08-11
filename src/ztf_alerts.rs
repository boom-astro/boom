use actix_web::{get, web, HttpResponse};
use mongodb::{bson::doc, Client, Collection};
use futures::TryStreamExt;

const DB_NAME: &str = "boom";
const ALERTS_COLLECTION: &str = "ztf_alerts";
const ALERTS_AUX_COLLECTION: &str = "ztf_alerts_aux";

#[get("/alerts/ztf/{object_id}")]
pub async fn get_object(client: web::Data<Client>, object_id: web::Path<String>) -> HttpResponse {
    let db = client.database(DB_NAME);
    let collection = db.collection(ALERTS_COLLECTION);

    // first, get the brightest alert
    let find_options = mongodb::options::FindOptions::builder()
        .sort(doc! {
            "candidate.magpsf": 1,
        })
        .projection(
            doc!{ "_id": 0 }
        )
        .limit(1)
        .build();

    // get the brightest alert for the object, where the object is likely most visible
    let mut brightest_alert_cursor = collection.find(doc! {
        "objectId": object_id.to_string(),
    }).with_options(find_options).await.expect("failed to execute find query");
    let brightest_alert = brightest_alert_cursor.try_next().await.expect("failed to get next document").expect("no document found");

    // next, get all of the alerts for the object but without the cutoutScience, cutoutTemplate, and cutoutDifference fields
    let find_options_alerts = mongodb::options::FindOptions::builder()
        .projection(
            doc! {
                "_id": 0,
                "cutoutScience": 0,
                "cutoutTemplate": 0,
                "cutoutDifference": 0,
            }
        )
        .sort(
            doc! {
                "candidate.jd": 1,
            }
        )
        .build();

    let cursor_alerts = collection.find(doc!{
        "objectId": object_id.to_string(),
    }).with_options(find_options_alerts).await.unwrap();
    let alerts = cursor_alerts.try_collect::<Vec<mongodb::bson::Document>>().await.unwrap();

    // then fetch the entry for that object in the aux collection
    let aux_collection: Collection<mongodb::bson::Document> = db.collection(ALERTS_AUX_COLLECTION);
    let aux_entry = aux_collection.find_one(doc! {
        "_id": object_id.to_string(),
    }).await.expect("failed to execute find_one query").expect("no aux entry found");

    let mut data = doc! { };

    data.insert("objectId", object_id.to_string());
    // brighest alert has the cutoutScience, cutoutTemplate, and cutoutDifference fields
    // that are byte strings
    match brightest_alert.get_document("cutoutScience") {
        Ok(cutout) => {
            data.insert("cutoutScience", cutout);
        },
        Err(error) => {
            println!("cutoutScience not found: {}", error);
            // print the type of the cutoutScience field
            data.insert("cutoutsScience", "");
        }
    }
    data.insert("alerts", alerts);

    let prv_candidates = aux_entry.get_array("prv_candidates").unwrap();
    data.insert("prv_candidates", prv_candidates);

    // crossmatches is a Result<>, so we check if it's Ok or Err
    match aux_entry.get_document("cross_matches") {
        Ok(crossmatches) => {
            data.insert("cross_matches", crossmatches);
        },
        Err(_) => {
            data.insert("cross_matches",  doc! { });
        }
    }

    HttpResponse::Ok().json(data)
}