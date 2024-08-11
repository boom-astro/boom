use actix_web::{post, web, HttpResponse};
use mongodb::{bson::doc, Client, Collection};
use futures::TryStreamExt;

const DB_NAME: &str = "boom";

const SUPPORTED_QUERY_TYPES: [&str; 4] = ["find", "cone_search", "sample", "info"];
const SUPPORTED_INFO_COMMANDS: [&str; 4] = ["catalog_names", "catalog_info", "index_info", "db_info"];

#[derive(serde::Deserialize, Clone)]
enum Unit {
    Degrees,
    Radians,
    Arcseconds,
    Arcminutes,
}

#[derive(serde::Deserialize, Clone)]
struct ObjectCoordinates {
    ra: f64,
    dec: f64,
    radius: f64,
    unit: Option<Unit>,
}

#[derive(serde::Deserialize, Clone)]
struct Query {
    object_coordinates: Option<ObjectCoordinates>,
    command: Option<String>,
    catalog: Option<String>,
    filter: Option<mongodb::bson::Document>,
    projection: Option<mongodb::bson::Document>,
    size: Option<i64>,
}

#[derive(serde::Deserialize, Clone)]
struct QueryKwargs {
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<mongodb::bson::Document>,
    max_time_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
struct QueryBody {
    query_type: String,
    query: Option<Query>,
    kwargs: Option<QueryKwargs>,
}

async fn build_options(projection: Option<mongodb::bson::Document>, kwargs: QueryKwargs) -> mongodb::options::FindOptions {
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
        find_options.max_time = Some(std::time::Duration::from_millis(kwargs.max_time_ms.unwrap()));
    }

    if projection.is_some() {
        find_options.projection = Some(projection.unwrap());
    }

    find_options
}

async fn build_cone_search_filter(mut filter: mongodb::bson::Document, object_coordinates: ObjectCoordinates) -> mongodb::bson::Document {
    let mut ra = object_coordinates.ra;
    let dec = object_coordinates.dec;
    let mut radius = object_coordinates.radius;
    let unit = object_coordinates.unit.unwrap_or(Unit::Degrees);

    // make ra geojson (ra - 180)
    ra = ra - 180.0;

    // convert radius to radians based on unit
    match unit {
        Unit::Degrees => {
            radius = radius.to_radians();
        }
        Unit::Arcseconds => {
            radius = radius.to_radians() / 3600.0;
        }
        Unit::Arcminutes => {
            radius = radius.to_radians() / 60.0;
        }
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

/// Get on raw document from any collection
#[post("/query")]
pub async fn query(client: web::Data<Client>, body: web::Json<QueryBody>) -> HttpResponse {
    let query_type = body.query_type.as_str();
    let query = body.query.clone().unwrap_or(Query {
        object_coordinates: None,
        command: None,
        catalog: None,
        filter: None,
        projection: None,
        size: None,
    });
    let kwargs = body.kwargs.clone().unwrap_or(QueryKwargs {
        limit: None,
        skip: None,
        sort: None,
        max_time_ms: None,
    });

    if !SUPPORTED_QUERY_TYPES.contains(&query_type) {
        return HttpResponse::BadRequest().body(format!("Unknown query type {query_type}"));
    }

    if query_type == "info" {
        let command = query.command.clone().expect("command is required for info queries");
        if !SUPPORTED_INFO_COMMANDS.contains(&command.as_str()) {
            return HttpResponse::BadRequest().body(format!("Unknown command {command}"));
        }
        if command == "catalog_names" {
            let catalog_names = client.database(DB_NAME).list_collection_names().await.unwrap();
            let mut data = catalog_names.iter().filter(|name| !name.starts_with("system.")).collect::<Vec<&String>>();
            // return the catalog names alphabetically sorted
            data.sort();
            return HttpResponse::Ok().json(data);
        } else if command == "catalog_info" {
            let catalog = query.catalog.expect("catalog is required for catalog_info");
            let data = client.database(DB_NAME).run_command(doc! { "collstats": catalog }).await.unwrap();
            return HttpResponse::Ok().json(data);
        } else if command == "index_info" {
            let catalog = query.catalog.expect("catalog is required for index_info");
            let collection: Collection<mongodb::bson::Document> = client.database(DB_NAME).collection(&catalog);
            let cursor = collection.list_indexes().await.unwrap();
            let data = cursor.try_collect::<Vec<mongodb::IndexModel>>().await.unwrap();
            return HttpResponse::Ok().json(data);
        } else if command == "db_info" {
            let data = client.database(DB_NAME).run_command(doc! { "dbstats": 1 }).await.unwrap();
            return HttpResponse::Ok().json(data);
        } else {
            return HttpResponse::BadRequest().body(format!("Unknown command {command}"));
        }
    }

    let catalog = query.catalog.unwrap();
    let collection: Collection<mongodb::bson::Document> = client.database(DB_NAME).collection(&catalog);

    if query_type == "sample" {
        let size = query.size.unwrap_or(1);
        if size > 100 {
            return HttpResponse::BadRequest().body("size must be less than 1000");
        }
        let kwargs_sample = QueryKwargs {
            limit: Some(size),
            skip: None,
            sort: None,
            max_time_ms: None,
        };
        // just use a find_one to get a single document as a sample of the collection
        let options = build_options(None, kwargs_sample).await;
        let cursor = collection.find(doc! {}).with_options(options).await.unwrap();
        let docs = cursor.try_collect::<Vec<mongodb::bson::Document>>().await.unwrap();
        HttpResponse::Ok().json(docs)
    } else if query_type == "find" {
        let filter = query.filter.expect("filter is required for find");
        let projection = query.projection;

        let find_options = build_options(projection, kwargs).await;

        let cursor = collection.find(filter).with_options(find_options).await.unwrap();
        let docs = cursor.try_collect::<Vec<mongodb::bson::Document>>().await.unwrap();
        HttpResponse::Ok().json(docs)
    } else if query_type == "cone_search" {
        let mut filter = query.filter.expect("filter is required for cone_search");
        let projection = query.projection;
        // get the object coordinates, but have a try catch in case it's not there and return an error
        let object_coordinates = query.object_coordinates.expect("object_coordinates is required for cone_search");

        filter = build_cone_search_filter(filter, object_coordinates).await;

        let find_options = build_options(projection, kwargs).await;

        let cursor = collection.find(filter).with_options(find_options).await.unwrap();
        let docs = cursor.try_collect::<Vec<mongodb::bson::Document>>().await.unwrap();
        HttpResponse::Ok().json(docs)
    } else {
        HttpResponse::BadRequest().body(format!("Unknown query type {query_type}"))
    }
}