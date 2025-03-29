use flare::spatial::great_circle_distance;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use tracing::warn;

use crate::conf;

pub async fn xmatch(
    ra: f64,
    dec: f64,
    xmatch_configs: &Vec<conf::CatalogXmatchConfig>,
    db: &mongodb::Database,
) -> mongodb::bson::Document {
    // TODO, make the xmatch config a hashmap for faster access
    // while looping over the xmatch results of the batched queries
    if xmatch_configs.len() == 0 {
        return doc! {};
    }
    let ra_geojson = ra - 180.0;
    let dec_geojson = dec;

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
            "$project": &xmatch_configs[0].projection
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
                &xmatch_configs[0].catalog: "$matches"
            }
        },
    ];

    // then for all the other xmatch_configs, use a unionWith stage
    for xmatch_config in xmatch_configs.iter().skip(1) {
        x_matches_pipeline.push(doc! {
            "$unionWith": {
                "coll": &xmatch_config.catalog,
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
                    }
                ]
            }
        });
    }

    let collection: mongodb::Collection<mongodb::bson::Document> =
        db.collection(&xmatch_configs[0].catalog);
    let mut cursor = collection.aggregate(x_matches_pipeline).await.unwrap();

    let mut xmatch_docs = doc! {};
    // pre add the catalogs + empty vec to the xmatch_docs
    // this allows us to have a consistent output structure
    for xmatch_config in xmatch_configs.iter() {
        xmatch_docs.insert(&xmatch_config.catalog, mongodb::bson::Bson::Array(vec![]));
    }

    while let Some(doc) = cursor.next().await {
        let doc = doc.unwrap();
        let catalog = doc.get_str("catalog").unwrap();
        let matches = doc.get_array("matches").unwrap();

        let xmatch_config = xmatch_configs
            .iter()
            .find(|x| x.catalog == catalog)
            .unwrap();

        if !xmatch_config.use_distance {
            xmatch_docs.insert(catalog, matches);
        } else {
            let distance_key = xmatch_config.distance_key.as_ref().unwrap();
            let distance_max = xmatch_config.distance_max.unwrap();
            let distance_max_near = xmatch_config.distance_max_near.unwrap();

            let mut matches_filtered: Vec<mongodb::bson::Document> = vec![];
            for xmatch_doc in matches.iter() {
                let xmatch_doc = xmatch_doc.as_document().unwrap();
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

                    // we make a mutable copy of the xmatch_doc
                    let mut xmatch_doc = xmatch_doc.clone();
                    // overwrite doc_copy with doc_copy + the angular separation and the distance in kpc
                    xmatch_doc.insert("angular_separation", angular_separation);
                    xmatch_doc.insert("distance_kpc", distance_kpc);
                    matches_filtered.push(xmatch_doc);
                }
            }
            xmatch_docs.insert(&xmatch_config.catalog, matches_filtered);
        }
    }

    xmatch_docs
}
