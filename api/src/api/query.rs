use crate::models::{query_models::*, response};
use actix_web::{HttpResponse, get, web};
use futures::TryStreamExt;
use mongodb::{
    Client, Collection, IndexModel,
    bson::{Document, doc},
};
use std::collections::HashMap;

const DB_NAME: &str = "boom";

// builds find options for mongo query
pub fn build_options(
    projection: Option<mongodb::bson::Document>,
    kwargs: QueryKwargs,
) -> mongodb::options::FindOptions {
    let mut find_options = mongodb::options::FindOptions::default();
    if kwargs.limit.is_some() {
        find_options.limit = Some(kwargs.limit.unwrap());
    }
    if kwargs.skip.is_some() {
        find_options.skip = Some(kwargs.skip.unwrap());
    }
    if kwargs.sort.is_some() {
        find_options.sort = Some(kwargs.sort.unwrap());
    }
    if kwargs.max_time_ms.is_some() {
        find_options.max_time = Some(std::time::Duration::from_millis(
            kwargs.max_time_ms.unwrap(),
        ));
    }
    if projection.is_some() {
        find_options.projection = Some(projection.unwrap());
    }

    find_options
}

pub fn build_cone_search_filter(
    mut filter: mongodb::bson::Document,
    radec: (f64, f64),
    mut radius: f64,
    unit: Unit,
) -> mongodb::bson::Document {
    let ra = radec.0 - 180.0;
    let dec = radec.1;
    // convert radius to radians based on unit
    match unit {
        Unit::Degrees => radius = radius.to_radians(),
        Unit::Arcseconds => radius = radius.to_radians() / 3600.0,
        Unit::Arcminutes => radius = radius.to_radians() / 60.0,
        Unit::Radians => {}
    }
    let center_sphere = doc! {
        "$centerSphere": [[ra, dec], radius]
    };
    let geo_within = doc! {
        "$geoWithin": center_sphere
    };
    filter.insert("coordinates.radec_geojson", geo_within);
    filter
}

pub async fn get_catalog_names(
    db: mongodb::Database,
) -> Result<Vec<String>, mongodb::error::Error> {
    // get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => return Err(e),
    };
    let mut data = collection_names
        .iter()
        .filter(|name| !name.starts_with("system."))
        .cloned()
        .collect::<Vec<String>>();
    data.sort();
    return Ok(data);
}

pub async fn get_catalog_info(
    db: mongodb::Database,
    catalogs: Vec<String>,
) -> Result<Vec<Document>, mongodb::error::Error> {
    let mut data = Vec::new();
    for catalog in catalogs {
        match db
            .run_command(doc! {
                "collstats": catalog
            })
            .await
        {
            Ok(d) => data.push(d),
            Err(e) => return Err(e),
        };
    }
    return Ok(data);
}

pub async fn get_index_info(
    db: mongodb::Database,
    catalogs: Vec<String>,
) -> Result<Vec<Vec<IndexModel>>, mongodb::error::Error> {
    let mut out_data = Vec::new();
    for i in 0..catalogs.len() {
        let collection: Collection<Document> = db.collection(&catalogs[i]);
        let cursor = match collection.list_indexes().await {
            Ok(c) => c,
            Err(e) => return Err(e),
        };
        let data = match cursor.try_collect::<Vec<mongodb::IndexModel>>().await {
            Ok(d) => d,
            Err(e) => return Err(e),
        };
        out_data.push(data);
    }
    return Ok(out_data);
}

pub async fn get_db_info(db: mongodb::Database) -> Result<Document, mongodb::error::Error> {
    let data = match db.run_command(doc! { "dbstats": 1 }).await {
        Ok(d) => d,
        Err(e) => return Err(e),
    };
    return Ok(data);
}

// retrieves a sample of a database collection
pub async fn get_collection_sample(
    collection: Collection<Document>,
    size: i64,
) -> Result<Option<Vec<Document>>, mongodb::error::Error> {
    if size > 1000 || size < 0 {
        return Err(mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "sample size must be between 0 and 1000",
        )));
    }
    let kwargs_sample = QueryKwargs {
        limit: Some(size),
        ..Default::default()
    };
    // use find to get a sample of the collection
    let options = build_options(None, kwargs_sample);
    let cursor = match collection.find(doc! {}).with_options(options).await {
        Ok(c) => c,
        Err(e) => {
            return Err(e);
        }
    };
    let docs = match cursor.try_collect::<Vec<mongodb::bson::Document>>().await {
        Ok(d) => d,
        Err(e) => {
            return Err(e);
        }
    };
    return Ok(Some(docs));
}

#[get("/query/info")]
pub async fn get_info(client: web::Data<Client>, body: web::Json<InfoQueryBody>) -> HttpResponse {
    let db = client.database(DB_NAME);
    let command = match body.command.clone() {
        Some(c) => c,
        None => {
            return response::bad_request("command required for info query");
        }
    };
    // get collection names in alphabetical order
    if command == "catalog_names" {
        let data = match get_catalog_names(db.clone()).await {
            Ok(d) => d,
            Err(e) => {
                return response::internal_error(&format!("Error getting catalog names: {:?}", e));
            }
        };
        return response::ok("Catalog names", serde_json::json!(data));
    // get collection statistics for catalog(s)
    } else if command == "catalog_info" {
        let catalogs = match body.catalogs.clone() {
            Some(c) => c,
            None => {
                return response::bad_request("catalog(s) required for catalog_info");
            }
        };
        let data = match get_catalog_info(db.clone(), catalogs.clone()).await {
            Ok(d) => d,
            Err(e) => {
                return response::internal_error(&format!("Error getting catalog info: {:?}", e));
            }
        };
        return response::ok(
            &format!("Catalog info for {:?}", catalogs),
            serde_json::json!(data),
        );
    // get list of indexes on the collection
    } else if command == "index_info" {
        let catalogs = match body.catalogs.clone() {
            Some(c) => c,
            None => {
                return response::bad_request("catalog(s) required for index_info");
            }
        };
        let data = get_index_info(db.clone(), catalogs.clone()).await;
        match data {
            Ok(d) => {
                return response::ok(
                    &format!("Index info for {:?}", catalogs),
                    serde_json::json!(d),
                );
            }
            Err(e) => {
                return response::internal_error(&format!("Error getting index info: {:?}", e));
            }
        }
    } else if command == "db_info" {
        let data = match get_db_info(db.clone()).await {
            Ok(d) => d,
            Err(e) => {
                return response::internal_error(&format!("Error getting database info: {:?}", e));
            }
        };
        return response::ok("Database info", serde_json::json!(data));
    } else {
        return response::bad_request(&format!("Unknown command: {}", command));
    }
}

#[get("/query/sample")]
pub async fn sample(client: web::Data<Client>, body: web::Json<QueryBody>) -> HttpResponse {
    let this_query = body.query.clone().unwrap_or_default();
    let catalog = match this_query.catalog {
        Some(c) => c,
        None => return response::bad_request("catalog name required for sample"),
    };

    let collection: Collection<Document> = client.database(DB_NAME).collection(&catalog);
    let size = this_query.size.unwrap_or(1);
    let docs = match get_collection_sample(collection, size).await {
        Ok(d) => d,
        Err(e) => {
            return response::internal_error(&format!("Error getting sample: {:?}", e));
        }
    };
    return response::ok(
        &format!("Sample of collection: {}", catalog),
        serde_json::json!(docs),
    );
}

#[get("/query/count_documents")]
pub async fn count_documents(
    client: web::Data<Client>,
    body: web::Json<QueryBody>,
) -> HttpResponse {
    let this_query = body.query.clone().unwrap_or_default();
    let catalog = match this_query.catalog {
        Some(c) => c,
        None => return response::bad_request("catalog name required for count_documents"),
    };
    let collection: Collection<Document> = client.database(DB_NAME).collection(&catalog);
    let filter = this_query.filter.unwrap_or(doc! {});
    let doc_count = collection.count_documents(filter).await;
    match doc_count {
        Err(e) => {
            return response::internal_error(&format!("Error counting documents: {:?}", e));
        }
        Ok(x) => {
            return response::ok(
                &format!("Count of documents in collection: {}", catalog),
                serde_json::json!(x),
            );
        }
    }
}

#[get("/query/find")]
pub async fn find(client: web::Data<Client>, body: web::Json<QueryBody>) -> HttpResponse {
    let this_query = body.query.clone().unwrap_or_default();
    let filter = match this_query.filter {
        Some(f) => f,
        None => {
            return response::bad_request("filter required for find");
        }
    };
    let catalog = match this_query.catalog {
        Some(c) => c,
        None => {
            return response::bad_request("catalog name required for find");
        }
    };
    let find_options = build_options(
        this_query.projection,
        body.kwargs.clone().unwrap_or_default(),
    );
    let collection: Collection<Document> = client.database(DB_NAME).collection(&catalog);
    let cursor = match collection.find(filter).with_options(find_options).await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error finding documents: {:?}", e));
        }
    };

    let docs = match cursor.try_collect::<Vec<mongodb::bson::Document>>().await {
        Ok(d) => d,
        Err(e) => {
            return response::internal_error(&format!("Error collecting documents: {:?}", e));
        }
    };
    return response::ok(
        &format!("Found document(s) in {}", catalog),
        serde_json::json!(docs),
    );
}

#[get("/query/cone_search")]
pub async fn cone_search(
    client: web::Data<Client>,
    body: web::Json<ConeSearchBody>,
) -> HttpResponse {
    let this_body = body.clone();
    let radius = match this_body.radius {
        Some(r) => r,
        None => return response::bad_request("radius required for cone_search"),
    };
    let unit = match this_body.unit {
        Some(u) => u,
        None => return response::bad_request("unit required for cone_search"),
    };
    let object_coordinates = match this_body.object_coordinates {
        Some(o) => o,
        None => {
            return response::bad_request("object_coordinates required for cone_search");
        }
    };
    let catalog_details = match this_body.catalog {
        Some(c) => c,
        None => {
            return response::bad_request("catalog(s) required for cone_search");
        }
    };
    let catalog = match catalog_details.catalog_name {
        Some(c) => c,
        None => {
            return response::bad_request("catalog_name required for catalog_details");
        }
    };

    let collection: Collection<Document> = client.database(DB_NAME).collection(&catalog);

    let projection = catalog_details.projection;
    let input_filter = catalog_details.filter.unwrap_or(doc! {});

    let kwargs = this_body.kwargs.unwrap_or_default();
    let find_options = build_options(projection, kwargs);

    // perform cone search over each set of object coordinates
    let mut docs: HashMap<String, Vec<mongodb::bson::Document>> = HashMap::new();
    for (object_name, radec) in object_coordinates {
        let filter = build_cone_search_filter(
            input_filter.clone(),
            (radec[0], radec[1]),
            radius,
            unit.clone(),
        );
        let cursor = match collection
            .find(filter)
            .with_options(find_options.clone())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return response::internal_error(&format!("Error finding documents: {:?}", e));
            }
        };
        // create map entry for this object's cone search
        let data = match cursor.try_collect::<Vec<mongodb::bson::Document>>().await {
            Ok(d) => d,
            Err(e) => {
                return response::internal_error(&format!("Error collecting documents: {:?}", e));
            }
        };
        docs.insert(object_name, data);
    }
    return response::ok(
        &format!("Cone Search on {} completed", catalog),
        serde_json::json!(docs),
    );
}
