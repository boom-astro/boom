use flare::spatial::great_circle_distance;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use tracing::warn;

use crate::types;

pub async fn xmatch(
    ra: f64,
    dec: f64,
    xmatch_configs: &Vec<types::CatalogXmatchConfig>,
    db: &mongodb::Database,
) -> mongodb::bson::Document {
    let ra_geojson = ra - 180.0;
    let dec_geojson = dec;

    let mut xmatch_docs = doc! {};

    let mut x_matches_pipeline = vec![
        doc! {
            "$match": {
                "coordinates.radec_geojson": {
                    "$geoWithin": {
                        "$centerSphere": [[ra_geojson, dec_geojson], xmatch_configs[0].radius]
                    }
                }
            }
        },
        doc! {
            "$project": xmatch_configs[0].projection.clone()
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
                xmatch_configs[0].catalog.clone(): "$matches"
            }
        },
    ];

    // then for all the other xmatch_configs, use a unionWith stage
    for xmatch_config in xmatch_configs.iter().skip(1) {
        x_matches_pipeline.push(doc! {
            "$unionWith": {
                "coll": xmatch_config.catalog.clone(),
                "pipeline": [
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
                        "$project": xmatch_config.projection.clone()
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
                            xmatch_config.catalog.clone(): "$matches"
                        }
                    }
                ]
            }
        });
    }

    let collection: mongodb::Collection<mongodb::bson::Document> =
        db.collection(&xmatch_configs[0].catalog.clone());
    let mut cursor = collection.aggregate(x_matches_pipeline).await.unwrap();

    while let Some(doc) = cursor.next().await {
        let doc = doc.unwrap();
        for xmatch_config in xmatch_configs.iter() {
            if doc.contains_key(&xmatch_config.catalog) {
                xmatch_docs.insert(
                    xmatch_config.catalog.clone(),
                    doc.get_array(&xmatch_config.catalog).unwrap(),
                );
            }
        }
    }

    for xmatch_config in xmatch_configs {
        if !xmatch_docs.contains_key(&xmatch_config.catalog) {
            xmatch_docs.insert::<&str, mongodb::bson::Array>(
                &xmatch_config.catalog,
                mongodb::bson::Array::new(),
            );
        }
        // if we are using a distance field, we project the source at ra,dec to the distance of
        // the crossmatch, then compute the distance between the two points in kpc

        if xmatch_config.use_distance {
            let distance_key = xmatch_config.distance_key.clone().unwrap();
            let distance_max = xmatch_config.distance_max.clone().unwrap();
            let distance_max_near = xmatch_config.distance_max_near.clone().unwrap();

            let matches = xmatch_docs.get_array_mut(&xmatch_config.catalog).unwrap();
            let mut matches_filtered: Vec<mongodb::bson::Bson> = vec![];
            for xmatch_doc in matches.iter_mut() {
                let xmatch_doc = xmatch_doc.as_document_mut().unwrap();
                let xmatch_ra = match xmatch_doc.get_f64("ra") {
                    Ok(x) => x,
                    _ => {
                        warn!("No ra in xmatch doc");
                        continue;
                    }
                };
                let xmatch_dec = match xmatch_doc.get_f64("dec") {
                    Ok(x) => x,
                    _ => {
                        warn!("No dec in xmatch doc");
                        continue;
                    }
                };
                let doc_z_option = match xmatch_doc.get(&distance_key) {
                    Some(z) => z.as_f64(),
                    _ => {
                        warn!("No z in xmatch doc");
                        continue;
                    }
                };
                let doc_z = match doc_z_option {
                    Some(z) => z,
                    None => {
                        warn!("No z in xmatch doc");
                        continue;
                    }
                };

                let cm_radius = if doc_z < 0.01 {
                    distance_max_near / 3600.0 // to degrees
                } else {
                    distance_max * (0.05 / doc_z) / 3600.0 // to degrees
                };
                let angular_separation =
                    great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0;

                if angular_separation < cm_radius {
                    // calculate the distance between objs in kpc
                    // let distance_kpc = angular_separation * (doc_z / 0.05);
                    let distance_kpc = if doc_z > 0.005 {
                        angular_separation * (doc_z / 0.05)
                    } else {
                        -1.0
                    };
                    // overwrite doc_copy with doc_copy + the angular separation and the distance in kpc
                    xmatch_doc.insert("angular_separation", angular_separation);
                    xmatch_doc.insert("distance_kpc", distance_kpc);
                    matches_filtered.push(mongodb::bson::Bson::from(xmatch_doc.clone()));
                }
            }
            *matches = mongodb::bson::Array::from(matches_filtered);
        }
    }

    xmatch_docs
}
