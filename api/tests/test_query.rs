use actix_web::{
    test::{self, TestRequest},
    web,
};
#[cfg(test)]
use boom_api::{
    api::{query, query::build_options},
    models::query_models::{QueryKwargs, Unit},
};
use mongodb::{
    Client,
    bson::{Document, doc},
    options::FindOptions,
};

// TODO: put in config
const DB_NAME: &str = "boom";
const CATALOG_NAME: &str = "ZTF";

// TODO: get info for client from the config file
pub async fn get_web_client() -> web::Data<Client> {
    let user = "mongoadmin";
    let pass = "mongoadminsecret";
    let uri = std::env::var("MONGODB_URI")
        .unwrap_or_else(|_| format!("mongodb://{user}:{pass}@localhost:27017").into());
    let client = Client::with_uri_str(uri).await.expect("failed to connect");
    web::Data::new(client)
}

// returns mongodb collection
pub async fn get_database_collection() -> mongodb::Collection<Document> {
    let client = get_web_client().await;
    let collection = client
        .database(DB_NAME)
        .collection(&format!("{}_alerts", CATALOG_NAME));
    collection
}

// checks if two FindOptions structs have equal member values.
// only checks members which are accessed by boom_api::api::query::build_options
pub fn check_find_options_equal(a: FindOptions, b: FindOptions) -> bool {
    if a.limit != b.limit || a.skip != b.skip || a.sort != b.sort || a.max_time != b.max_time {
        return false;
    }
    return true;
}

// UNIT TESTS

#[actix_rt::test]
async fn test_build_options() {
    let test_projection_good = Some(doc! {
        "$project": {
            "objectId": 1,
            "candid": 1,
            "candidate": 1,
        }
    });

    let test_kwargs = QueryKwargs {
        limit: Some(5),
        ..Default::default()
    };

    // test default
    let default_options_test = build_options(
        None,
        QueryKwargs {
            ..Default::default()
        },
    );
    let default_options = mongodb::options::FindOptions::default();
    assert!(check_find_options_equal(
        default_options,
        default_options_test
    ));

    // test sample projection and default kwargs
    let proj_options_test = build_options(
        test_projection_good.clone(),
        QueryKwargs {
            ..Default::default()
        },
    );
    let mut proj_options = mongodb::options::FindOptions::default();
    proj_options.projection = test_projection_good.clone();
    assert!(check_find_options_equal(proj_options_test, proj_options));

    // test sample projection with sample kwargs
    let full_options_test = build_options(test_projection_good.clone(), test_kwargs.clone());
    let mut full_options = mongodb::options::FindOptions::default();
    full_options.projection = test_projection_good.clone();
    full_options.limit = test_kwargs.limit;
    assert!(check_find_options_equal(full_options, full_options_test));
}

#[test]
fn test_build_cone_search_filter() {
    let radec = (91.0, 188.0);
    let unit = Unit::Degrees;
    let radius: f64 = 16.0;

    let init_filter = doc! {
        "$project": doc! {
            "cutoutScience": 0,
            "cutoutDifference": 0,
            "cutoutTemplate": 0,
            "publisher": 0,
            "schemavsn": 0
        }
    };

    let filter_correct = doc! {
        "$project": doc! {
            "cutoutScience": 0,
            "cutoutDifference": 0,
            "cutoutTemplate": 0,
            "publisher": 0,
            "schemavsn": 0
        },
        "coordinates.radec_geojson": doc! {
            "$geoWithin": doc! {
                "$centerSphere": [[radec.0 - 180.0, radec.1], radius.to_radians()]
            }
        }
    };
    let built_filter = query::build_cone_search_filter(init_filter, radec, radius, unit);
    assert_eq!(built_filter, filter_correct);
}

#[actix_rt::test]
async fn test_get_catalog_names() {
    let client = get_web_client().await;
    let _ = query::get_catalog_names(client.database(DB_NAME)).await;
}

#[actix_rt::test]
async fn test_get_catalog_info() {
    let client = get_web_client().await;
    let catalog_names = query::get_catalog_names(client.database(DB_NAME))
        .await
        .unwrap();
    let catalog_name = if catalog_names.len() > 0 {
        vec![catalog_names[0].clone()]
    } else {
        return;
    };
    let _ = query::get_catalog_info(client.database(DB_NAME), catalog_name);
}

#[actix_rt::test]
async fn test_get_index_info() {
    let client = get_web_client().await;
    let catalog_names = query::get_catalog_names(client.database(DB_NAME))
        .await
        .unwrap();
    let catalog_name = if catalog_names.len() > 0 {
        vec![catalog_names[0].clone()]
    } else {
        return;
    };
    let _ = query::get_index_info(client.database(DB_NAME), catalog_name);
}

#[actix_rt::test]
async fn test_get_db_info() {
    let client = get_web_client().await;
    let _ = query::get_db_info(client.database(DB_NAME)).await;
}

#[actix_rt::test]
async fn test_get_collection_sample() {
    let collection = get_database_collection().await;
    let _ = query::get_collection_sample(collection, 2).await;
}

#[actix_rt::test]
#[should_panic]
async fn test_get_collection_sample_negative_size() {
    let collection = get_database_collection().await;
    let _ = query::get_collection_sample(collection, -1).await.unwrap();
}

#[actix_rt::test]
#[should_panic]
async fn test_get_collection_sample_size_too_large() {
    let collection = get_database_collection().await;
    let _ = query::get_collection_sample(collection, 1001)
        .await
        .unwrap();
}

#[actix_rt::test]
async fn test_count_documents() {
    let collection = get_database_collection().await;
    let filter = doc! {};
    let _ = collection.count_documents(filter).await.unwrap();
}
