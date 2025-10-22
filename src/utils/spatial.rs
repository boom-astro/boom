use crate::{conf, utils::o11y::logging::as_error};

use flare::spatial::great_circle_distance;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use tracing::{instrument, warn};

#[derive(thiserror::Error, Debug)]
pub enum XmatchError {
    #[error("value access error from bson")]
    BsonValueAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("error from mongodb")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("distance_key field is null")]
    NullDistanceKey,
    #[error("distance_max field is null")]
    NullDistanceMax,
    #[error("distance_max_near field is null")]
    NullDistanceMaxNear,
    #[error("failed to convert the bson data into a document")]
    AsDocumentError,
}

fn get_f64_from_doc(doc: &mongodb::bson::Document, key: &str) -> Option<f64> {
    let value = match doc.get(key) {
        Some(mongodb::bson::Bson::Double(v)) => *v,
        Some(mongodb::bson::Bson::Int32(v)) => *v as f64,
        Some(mongodb::bson::Bson::Int64(v)) => *v as f64,
        _ => {
            warn!("no valid {} in doc", key);
            return None;
        }
    };
    // if the value is out of bounds, return None
    if value.is_nan() || value.is_infinite() {
        warn!("{} is NaN or infinite", key);
        return None;
    }
    Some(value)
}

pub fn catalog_xmatch_pipeline(
    ra_geojson: f64,
    dec_geojson: f64,
    xmatch_config: &conf::CatalogXmatchConfig,
) -> Vec<mongodb::bson::Document> {
    match xmatch_config.n_max {
        Some(n_max) => {
            vec![
                doc! {
                    "$match": {
                        "coordinates.radec_geojson": {
                            "$nearSphere": [ra_geojson, dec_geojson],
                            "$maxDistance": xmatch_config.radius
                        }
                    }
                },
                doc! {
                    "$project": &xmatch_config.projection
                },
                doc! {
                    "$limit": n_max
                },
                doc! {
                    "$group": {
                        "_id": mongodb::bson::Bson::Null,
                        "matches": {
                            "$push": "$$ROOT"
                        }
                    }
                },
                doc! {
                    "$project": {
                        "_id": 0,
                        "matches": 1,
                        "catalog": &xmatch_config.catalog
                    }
                },
            ]
        }
        None => {
            vec![
                doc! {
                    "$match": {
                        "coordinates.radec_geojson": {
                            "$geoWithin": {
                                "$centerSphere": [[ra_geojson, dec_geojson], xmatch_config.radius]
                            }
                        }
                    }
                },
                doc! {
                    "$project": &xmatch_config.projection
                },
                doc! {
                    "$group": {
                        "_id": mongodb::bson::Bson::Null,
                        "matches": {
                            "$push": "$$ROOT"
                        }
                    }
                },
                doc! {
                    "$project": {
                        "_id": 0,
                        "matches": 1,
                        "catalog": &xmatch_config.catalog
                    }
                },
            ]
        }
    }
}

#[instrument(skip(xmatch_configs, db), fields(database = db.name()), err)]
pub async fn xmatch(
    ra: f64,
    dec: f64,
    xmatch_configs: &Vec<conf::CatalogXmatchConfig>,
    db: &mongodb::Database,
) -> Result<mongodb::bson::Document, XmatchError> {
    // TODO, make the xmatch config a hashmap for faster access
    // while looping over the xmatch results of the batched queries
    if xmatch_configs.len() == 0 {
        return Ok(doc! {});
    }
    let ra_geojson = ra - 180.0;
    let dec_geojson = dec;

    // first build the aggregation pipeline for the first catalog
    let mut x_matches_pipeline =
        catalog_xmatch_pipeline(ra_geojson, dec_geojson, &xmatch_configs[0]);

    // then for all the other xmatch_configs, use a unionWith stage
    for xmatch_config in xmatch_configs.iter().skip(1) {
        x_matches_pipeline.push(doc! {
            "$unionWith": {
                "coll": &xmatch_config.catalog,
                "pipeline": catalog_xmatch_pipeline(ra_geojson, dec_geojson, xmatch_config)
            }
        });
    }

    let collection: mongodb::Collection<mongodb::bson::Document> =
        db.collection(&xmatch_configs[0].catalog);
    let mut cursor = collection
        .aggregate(x_matches_pipeline)
        .await
        .inspect_err(as_error!("failed to aggregate"))?;

    let mut xmatch_docs = doc! {};
    // pre add the catalogs + empty vec to the xmatch_docs
    // this allows us to have a consistent output structure
    for xmatch_config in xmatch_configs.iter() {
        xmatch_docs.insert(&xmatch_config.catalog, mongodb::bson::Bson::Array(vec![]));
    }

    while let Some(result) = cursor.next().await {
        let doc = result.inspect_err(as_error!("failed to get next document"))?;
        let catalog = doc
            .get_str("catalog")
            .inspect_err(as_error!("failed to get catalog"))?;
        let matches = doc
            .get_array("matches")
            .inspect_err(as_error!("failed to get matches"))?;

        let xmatch_config = xmatch_configs
            .iter()
            .find(|x| x.catalog == catalog)
            .expect("this should never panic, the doc was derived from the catalogs");

        if !xmatch_config.use_distance {
            xmatch_docs.insert(catalog, matches);
        } else {
            let distance_key = xmatch_config
                .distance_key
                .as_ref()
                .ok_or(XmatchError::NullDistanceKey)?;
            let distance_max = xmatch_config
                .distance_max
                .ok_or(XmatchError::NullDistanceMax)?;
            let distance_max_near = xmatch_config
                .distance_max_near
                .ok_or(XmatchError::NullDistanceMaxNear)?;

            let mut matches_filtered: Vec<mongodb::bson::Document> = vec![];
            for xmatch_doc in matches.iter() {
                let xmatch_doc = xmatch_doc
                    .as_document()
                    .ok_or(XmatchError::AsDocumentError)?;

                let xmatch_ra = match get_f64_from_doc(&xmatch_doc, "ra") {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };
                let xmatch_dec = match get_f64_from_doc(&xmatch_doc, "dec") {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };
                let doc_z = match get_f64_from_doc(&xmatch_doc, distance_key) {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };

                let cm_radius_arcsec = if doc_z < 0.01 {
                    distance_max_near // in arcsec
                } else {
                    distance_max * (0.05 / doc_z) // in arcsec
                };
                let distance_arcsec =
                    great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0; // convert to arcsec

                if distance_arcsec < cm_radius_arcsec {
                    // calculate the distance between objs in kpc
                    // let distance_kpc = angular_separation * (doc_z / 0.05);
                    let distance_kpc = if doc_z > 0.005 {
                        distance_arcsec * (doc_z / 0.05)
                    } else {
                        -1.0
                    };

                    // we make a mutable copy of the xmatch_doc
                    let mut xmatch_doc = xmatch_doc.clone();
                    // add the distance fields to the xmatch_doc
                    xmatch_doc.insert("distance_arcsec", distance_arcsec);
                    xmatch_doc.insert("distance_kpc", distance_kpc);
                    matches_filtered.push(xmatch_doc);
                }
            }
            xmatch_docs.insert(&xmatch_config.catalog, matches_filtered);
        }
    }

    Ok(xmatch_docs)
}
