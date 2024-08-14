use futures::stream::StreamExt;
use mongodb::bson::doc;

use crate::types;

const DEGRA: f64 = std::f64::consts::PI / 180.0;

pub fn great_circle_distance(ra1_deg: f64, dec1_deg: f64, ra2_deg: f64, dec2_deg: f64) -> f64 {
    let ra1 = ra1_deg * DEGRA;
    let dec1 = dec1_deg * DEGRA;
    let ra2 = ra2_deg * DEGRA;
    let dec2 = dec2_deg * DEGRA;
    let delta_ra = (ra2 - ra1).abs();
    let mut distance = (dec2.cos() * delta_ra.sin()).powi(2) // let mut distance = (dec2.sin() * delta_ra.cos()).powi(2)
        + (dec1.cos() * dec2.sin() - dec1.sin() * dec2.cos() * delta_ra.cos()).powi(2);
    distance = distance
        .sqrt()
        .atan2(dec1.sin() * dec2.sin() + dec1.cos() * dec2.cos() * delta_ra.cos());
    distance * 180.0 / std::f64::consts::PI
}

pub fn in_ellipse(
    alpha: f64,
    delta0: f64,
    alpha1: f64,
    delta01: f64,
    d0: f64,
    axis_ratio: f64,
    pao: f64,
) -> bool {
    let d_alpha = (alpha1 - alpha) * DEGRA;
    let delta1 = delta01 * DEGRA;
    let delta = delta0 * DEGRA;
    let pa = pao * DEGRA;
    let d = d0 * DEGRA;
    // e is the sqrt of 1.0 - axis_ratio^2
    let e = (1.0 - axis_ratio.powi(2)).sqrt();

    let t1 = d_alpha.cos();
    let t22 = d_alpha.sin();
    let t3 = delta1.cos();
    let t32 = delta1.sin();
    let t6 = delta.cos();
    let t26 = delta.sin();
    let t9 = d.cos();
    let t55 = d.sin();

    if t3 * t6 * t1 + t32 * t26 < 0.0 {
        return false;
    }

    let t2 = t1 * t1;
    let t4 = t3 * t3;
    let t5 = t2 * t4;
    let t7 = t6 * t6;
    let t8 = t5 * t7;
    let t10 = t9 * t9;
    let t11 = t7 * t10;
    let t13 = pa.cos();
    let t14 = t13 * t13;
    let t15 = t14 * t10;
    let t18 = t7 * t14;
    let t19 = t18 * t10;

    let t24 = pa.sin();

    let t31 = t1 * t3;

    let t36 = 2.0 * t31 * t32 * t26 * t6;
    let t37 = t31 * t32;
    let t38 = t26 * t6;
    let t45 = t4 * t10;

    let t56 = t55 * t55;
    let t57 = t4 * t7;

    let t60 = -t8 + t5 * t11 + 2.0 * t5 * t15
        - t5 * t19
        - 2.0 * t1 * t4 * t22 * t10 * t24 * t13 * t26
        - t36
        + 2.0 * t37 * t38 * t10
        - 2.0 * t37 * t38 * t15
        - t45 * t14
        - t45 * t2
        + 2.0 * t22 * t3 * t32 * t6 * t24 * t10 * t13
        - t56
        + t7
        - t11
        + t4
        - t57
        + t57 * t10
        + t19
        - t18 * t45;

    let t61 = e * e;
    let t63 = t60 * t61 + t8 + t57 - t4 - t7 + t56 + t36;

    let inside = t63 > 0.0;
    inside
}

pub async fn xmatch(
    ra: f64,
    dec: f64,
    xmatch_configs: &Vec<types::CatalogXmatchConfig>,
    db: &mongodb::Database
) -> mongodb::bson::Document {

    let ra_geojson = ra - 180.0;
    let dec_geojson = dec;


    let mut xmatch_docs = doc! {
    };

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
        }
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

    let collection: mongodb::Collection<mongodb::bson::Document> = db.collection(&xmatch_configs[0].catalog.clone());
    let mut cursor = collection.aggregate(x_matches_pipeline).await.unwrap();

    while let Some(doc) = cursor.next().await {
        let doc = doc.unwrap();
        for xmatch_config in xmatch_configs.iter() {
            if doc.contains_key(&xmatch_config.catalog) {
                xmatch_docs.insert(xmatch_config.catalog.clone(), doc.get_array(&xmatch_config.catalog).unwrap());
            }
        }
    }

    for xmatch_config in xmatch_configs {
        if !xmatch_docs.contains_key(&xmatch_config.catalog) {
            xmatch_docs.insert::<&str, mongodb::bson::Array>(&xmatch_config.catalog, mongodb::bson::Array::new());
        }
        // if we are using a distance field, we project the source at ra,dec to the distance of
        // the crossmatch, then compute the distance between the two points in kpc
        
        if xmatch_config.use_distance {
            let distance_key = xmatch_config.distance_key.clone().unwrap();
            let distance_unit = xmatch_config.distance_unit.clone().unwrap();
            let distance_max = xmatch_config.distance_max.clone().unwrap();
            let distance_max_near = xmatch_config.distance_max_near.clone().unwrap();

            let matches = xmatch_docs.get_array_mut(&xmatch_config.catalog).unwrap();
            let mut matches_filtered: Vec<mongodb::bson::Bson> = vec![];
            for xmatch_doc in matches.iter_mut() {
                let xmatch_doc = xmatch_doc.as_document_mut().unwrap();
                if !xmatch_doc.get_f64("ra").is_ok() || !xmatch_doc.get_f64("dec").is_ok() {
                    continue;
                }
                let xmatch_ra = xmatch_doc.get_f64("ra").unwrap();
                let xmatch_dec = xmatch_doc.get_f64("dec").unwrap();
                if distance_unit == types::DistanceUnit::Redshift {
                    let doc_z_option = match xmatch_doc.get(&distance_key) {
                        Some(z) => z.as_f64(),
                        _ => {
                            continue;
                        }
                    };
                    // check if it's not none instead of just unwrapping
                    if doc_z_option.is_none() {
                        continue;
                    }
                    let doc_z = doc_z_option.unwrap();

                    let cm_radius = if doc_z < 0.01 {
                        distance_max_near / 3600.0 // to degrees
                    } else {
                        distance_max * (0.05 / doc_z) / 3600.0 // to degrees
                    };
                    if in_ellipse(ra, dec, xmatch_ra, xmatch_dec, cm_radius, 1.0, 0.0) {
                        // calculate the angular separation
                        let angular_separation =
                            great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0;
                        // calculate the distance between objs in kpc
                        //let distance_kpc = angular_separation * (doc_z / 0.05);
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
                } else if distance_unit == types::DistanceUnit::Mpc {
                    let doc_mpc_option = match xmatch_doc.get(&distance_key) {
                        // mpc could be f64 or i32, so try both
                        Some(mpc) => {
                            let mpc_f64 = mpc.as_f64();
                            if mpc_f64.is_none() {
                                let mpc_i32 = mpc.as_i32();
                                if mpc_i32.is_none() {
                                    None
                                } else {
                                    Some(mpc_i32.unwrap() as f64)
                                }
                            } else {
                                mpc_f64
                            }
                        }
                        _ => {
                            println!("No mpc");
                            continue;
                        }
                    };
                    if doc_mpc_option.is_none() {
                        // also print the distance key we are using
                        println!("Mpc is none using {}", distance_key);
                        // print the document _id
                        println!("{:?}", xmatch_doc.get("_id"));
                        continue;
                    }
                    let doc_mpc = doc_mpc_option.unwrap();
                    let cm_radius = if doc_mpc < 40.0 {
                        distance_max_near / 3600.0 // to degrees
                    } else {
                        (distance_max / (doc_mpc * 1000.0)) // 10**3
                            .atan()
                            .to_degrees()
                    };
                    if in_ellipse(ra, dec, xmatch_ra, xmatch_dec, cm_radius, 1.0, 0.0) {
                        // here we don't * 3600.0 yet because we need to calculate the distance in kpc first
                        let angular_separation = great_circle_distance(ra, dec, xmatch_ra, xmatch_dec);
                        // calculate the distance between objs in kpc
                        let distance_kpc = if doc_mpc > 0.005 {
                            angular_separation.to_radians() * (doc_mpc * 1000.0)
                        } else {
                            -1.0
                        };
                        xmatch_doc.insert("distance_arcsec", angular_separation * 3600.0);
                        xmatch_doc.insert("distance_kpc", distance_kpc);
                        matches_filtered.push(mongodb::bson::Bson::from(xmatch_doc.clone()));
                    }
                }
            }
            *matches = mongodb::bson::Array::from(matches_filtered);
        }
    }

    xmatch_docs
}

pub async fn xmatch_partitioned(
    ra: f64,
    dec: f64,
    xmatch_config: &types::CatalogXmatchConfig,
    db: &mongodb::Database
) -> mongodb::bson::Document {

    let ra_geojson = ra - 180.0;
    let dec_geojson = dec;

    // so, first we want to figure out the healpix indexes that are within the search radius of the ra,dec
    let healpix_indexes = ra_dec_to_healpix(ra, dec, xmatch_config.radius, xmatch_config.healpix_partition_order.unwrap());
    
    // then generate the catalog names based on the catalog name and the healpix index
    // like: catalog_1, catalog_2, catalog_3, etc
    let catalog_names = healpix_indexes.iter().map(|x| format!("{}_{}", xmatch_config.catalog, x)).collect::<Vec<_>>();

    let collection: mongodb::Collection<mongodb::bson::Document> = db.collection(catalog_names[0].as_str());

    let mut xmatch_docs = doc! {
        xmatch_config.catalog.clone(): mongodb::bson::Array::new()
    };

    let mut x_matches_pipeline = vec![
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
    ];

    for catalog_name in catalog_names.iter().skip(1) {
        x_matches_pipeline.push(doc! {
            "$unionWith": {
                "coll": catalog_name.clone(),
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

    let mut cursor = collection.aggregate(x_matches_pipeline).await.unwrap();

    while let Some(doc) = cursor.next().await {
        let mut doc = doc.unwrap();
        if doc.contains_key(&xmatch_config.catalog) {
            // extend the array
            let matches = xmatch_docs.get_array_mut(&xmatch_config.catalog).unwrap();
            let new_matches = doc.get_array_mut(&xmatch_config.catalog).unwrap();
            matches.extend(new_matches.iter().cloned());
        }
    }

    xmatch_docs
}

// let's implement some healpix utilities
// we want to be able to:
// - get the list of healpix indexes overlap with a given ra,dec and radius at a given order

fn get_flat_cells(bmoc: cdshealpix::nested::bmoc::BMOC) -> (Vec<u64>, Vec<u8>, Vec<u8>) {
    let len = bmoc.deep_size();
    let mut ipix = Vec::<u64>::with_capacity(len);
    let mut depth = Vec::<u8>::with_capacity(len);
    let mut fully_covered = Vec::<u8>::with_capacity(len);
  
    for c in bmoc.flat_iter_cell() {
      ipix.push(c.hash);
      depth.push(c.depth);
      fully_covered.push(c.is_full as u8);
    }
  
    depth.shrink_to_fit();
    ipix.shrink_to_fit();
    fully_covered.shrink_to_fit();
  
    (ipix.into(), depth.into(), fully_covered.into())
  }

pub fn ra_dec_to_healpix(ra: f64, dec: f64, radius: f64, order: i32) -> Vec<u64> {
    // convert ra, dec to radians
    let lon = ra * DEGRA;
    let lat = dec * DEGRA;
    // let mut delta_order = order + 2;
    // let mut ipix = vec![];
    // while delta_order < 30 {
    //     let bmoc = cdshealpix::nested::cone_coverage_approx_custom(order as u8, (delta_order) as u8, lon, lat, radius);
    //     (ipix, _, _) = get_flat_cells(bmoc);
    //     if ipix.len() > 0 {
    //         break;
    //     }
    //     delta_order += 1;
    // }
    // ipix
    let delta_order = 16; // at 16, the angular resolution is 3.221" per pixel, so with a radius of 2" = diameter of 4", we should be good
    let bmoc = cdshealpix::nested::cone_coverage_approx_custom(order as u8, (delta_order) as u8, lon, lat, radius);
    let (ipix, _, _) = get_flat_cells(bmoc);
    ipix
}