use ndarray::Array;
use ort::{
	inputs,
	session::{Session, SessionOutputs, builder::GraphOptimizationLevel},
    value::Tensor,
    execution_providers::{CUDAExecutionProvider, ExecutionProvider},
};

use mongodb::bson::{doc, Document};
use boom::{
    conf,
    utils::fits::prepare_triplet,
};
use futures::StreamExt;

const ACAI_FEATURES: [&str; 25] = [
    "drb", "diffmaglim", "ra", "dec", "magpsf", "sigmapsf", 
    "chipsf", "fwhm", "sky", "chinr", "sharpnr", 
    "sgscore1", "distpsnr1", 
    "sgscore2", "distpsnr2", 
    "sgscore3", "distpsnr3",
    "ndethist", "ncovhist",
    "scorr", 
    "nmtchps",
    "clrcoeff",
    "clrcounc",
    "neargaia",
    "neargaiabright"
];

async fn fetch_alerts(
    candids: &[i64], // this is a slice of candids to process
    catalog: &str,
    db: &mongodb::Database
) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
    let mut alert_cursor = db
        .collection::<Document>(format!("{}_alerts", catalog).as_str())
        .aggregate(vec![
            doc! {
                "$match": {
                    "_id": {"$in": candids}
                }
            },
            doc! {
                "$project": {
                    "objectId": 1,
                    "candidate": 1,
                }
            },
            doc! {
                "$lookup": {
                    "from": format!("{}_alerts_aux", catalog),
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            },
            doc! {
                "$lookup": {
                    "from": format!("{}_alerts_cutouts", catalog),
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "object"
                }
            },
            doc! {
                "$project": doc! {
                    "objectId": 1,
                    "candidate": 1,
                    "prv_candidates": doc! {
                        "$filter": doc! {
                            "input": doc! {
                                "$arrayElemAt": [
                                    "$aux.prv_candidates",
                                    0
                                ]
                            },
                            "as": "x",
                            "cond": doc! {
                                "$and": [
                                    {
                                        "$lt": [
                                            {
                                                "$subtract": [
                                                    "$candidate.jd",
                                                    "$$x.jd"
                                                ]
                                            },
                                            365
                                        ]
                                    },
                                    {
                                        "$gte": [
                                            {
                                                "$subtract": [
                                                    "$candidate.jd",
                                                    "$$x.jd"
                                                ]
                                            },
                                            0
                                        ]
                                    },

                                ]
                            }
                        }
                    },
                    "cutoutScience": doc! {
                        "$arrayElemAt": [
                            "$object.cutoutScience",
                            0
                        ]
                    },
                    "cutoutTemplate": doc! {
                        "$arrayElemAt": [
                            "$object.cutoutTemplate",
                            0
                        ]
                    },
                    "cutoutDifference": doc! {
                        "$arrayElemAt": [
                            "$object.cutoutDifference",
                            0
                        ]
                    }
                }
            },
        ])
        .await
        .unwrap();

    let mut alerts: Vec<Document> = Vec::new();
    while let Some(result) = alert_cursor.next().await {
        match result {
            Ok(document) => {
                alerts.push(document);
            }
            _ => {
                continue;
            }
        }
    }

    Ok(alerts)
}

async fn process_chunk(
    candids: &[i64], // this is a slice of candids to process
    catalog: &str,
    db: &mongodb::Database,
    model: &Session
) {
    let alerts = match fetch_alerts(candids, catalog, db).await {
        Ok(alerts) => alerts,
        Err(e) => {
            eprintln!("Failed to fetch alerts: {}", e);
            return;
        }
    };
    if alerts.is_empty() {
        eprintln!("No alerts found for the provided candids.");
        return;
    }

    for alert in &alerts {
        let candid = alert.get_i64("_id").unwrap();
        let obj_id = alert.get_str("objectId").unwrap();

        let candidate = match alert.get_document("candidate") {
            Ok(doc) => doc,
            Err(_) => {
                println!("No candidate found for alert with objectId: {}", obj_id);
                continue;
            }
        };
        // match candidate.get_f64("drb") {
        //     Ok(val) => {
        //         if val < 0.8 {
        //             continue;
        //         }
        //     }
        //     Err(_) => {
        //         continue;
        //     }
        // };
        // match candidate.get_bool("isdiffpos") {
        //     Ok(val) => {
        //         if val == false {
        //             continue;
        //         }
        //     }
        //     Err(_) => {
        //         continue;
        //     }
        // };
        // match candidate.get_f64("ssdistnr") {
        //     Ok(val) => {
        //         if val < 12.0 {
        //             continue;
        //         }
        //     }
        //     Err(_) => {}
        // };
        // match candidate.get_i64("ndethist") {
        //     Ok(val) => {
        //         if val < 2 || val > 100 {
        //             continue;
        //         }
        //     }
        //     Err(_) => {}
        // };

        // let start = std::time::Instant::now();
        let (cutout_science, cutout_template, cutout_difference) = prepare_triplet(&alert).unwrap(); // this can fail if the cutouts are missing or malformed, so we handle it gracefully

        // println!("Preparing triplet took: {:?}", 
        //     start.elapsed()
        // );

        // let start = std::time::Instant::now();
        let mut triplets = Array::zeros((1, 63, 63, 3));
        // each cutout is a flattened 63x63 image, so let's fill the triplets array with the data
        for (i, cutout) in [cutout_science, cutout_template, cutout_difference].iter().enumerate() {
            let mut slice = triplets.slice_mut(ndarray::s![0, .., .., i as usize]);
            let cutout_array = Array::from_shape_vec((63, 63), cutout.to_vec()).unwrap();
            slice.assign(&cutout_array);
        }

        let triplets = Tensor::<f32>::from_array(triplets).unwrap();

        // println!("Making triplet tensor took: {:?}", 
        //     start.elapsed()
        // );

        // let start = std::time::Instant::now();

        let mut features = Array::zeros((1, ACAI_FEATURES.len() as usize));

        for (i, field) in ACAI_FEATURES.iter().enumerate() {
            match candidate.get(field) {
                Some(value) => {
                    let val = match value {
                        mongodb::bson::Bson::Double(d) => *d as f32,
                        mongodb::bson::Bson::Int64(i) => *i as f32,
                        mongodb::bson::Bson::Int32(i) => *i as f32,
                        _ => {
                            0.0 // fallback to 0.0
                        }
                    };
                    features[[0, i]] = val;
                },
                None => {
                    features[[0, i]] = 0.0; // fallback to 0.0
                }
            }
        }

        // println!("Extracting features took: {:?}", 
        //     start.elapsed()
        // );

        // let start = std::time::Instant::now();

        let features = Tensor::<f32>::from_array(features).unwrap();

        // println!("Making features tensor took: {:?}", 
        //     start.elapsed()
        // );

        // let start = std::time::Instant::now();

        let inputs = match inputs! {
            // this is the input dictionary for the model, we need to specify the input tensors
            "features" => features,
            "triplets" => triplets,
        } {
            Ok(inputs) => inputs,
            Err(e) => {
                eprintln!("Failed to create inputs for model: {}", e);
                continue; // skip this candidate
            }
        };
         
        // now let's run the model with the actual data
        let outputs: SessionOutputs = match model.run(inputs) {
            Ok(outputs) => outputs,
            Err(e) => {
                eprintln!("Failed to run model for candid {} ({}): {}", candid, obj_id, e);
                continue; // skip this candidate
            }
        };

        // println!("Running model took: {:?}", 
        //     start.elapsed()
        // );

        // let start = std::time::Instant::now();

        // extract the score output
        let score = match outputs["score"].try_extract_tensor::<f32>() {
            Ok(tensor) => tensor,
            Err(e) => {
                eprintln!("Failed to extract score tensor: {}", e);
                continue;
            }
        };
        // if the score is above 0.95, print it out
        if let Some(slice) = score.as_slice() {
            if slice.len() > 0 && slice[0] > 0.95 {
                println!("High score for candid {} ({}): {:?}", candid, obj_id, slice);
            }
        } else {
            eprintln!("Failed to get score slice for candid {} ({})", candid, obj_id);
        }

        // println!("Extracting score took: {:?}", 
        //     start.elapsed()
        // );

    }

}

#[tokio::main]
async fn main() {
    let config_path = "config.yaml";
    let config_file = conf::load_config(&config_path).unwrap();
    let db = conf::build_db(&config_file).await.unwrap();

    let builder = Session::builder().unwrap();

    // if CUDA is not available, fallback to CPU but inform the user
    match CUDAExecutionProvider::default().is_available() {
        Ok(true) => {},
        Ok(false) => {
            println!("CUDA Execution Provider is not available, falling back to CPU.");
        }
        Err(e) => {
            eprintln!("Failed to check CUDA availability: {}, falling back to CPU", e);
        }
    }

    let model = builder
        .with_execution_providers([CUDAExecutionProvider::default().build()]).unwrap()
        .with_optimization_level(GraphOptimizationLevel::Level3).unwrap()
        .with_intra_threads(1).unwrap()
        .commit_from_file("data/models/acai_h.d1_dnn_20201130.onnx").unwrap();

    println!("Model loaded and optimized successfully.");

    // let's grab 200k candids from the database to process, this will be used to test the model
    let mut candids_cursor = db
        .collection::<Document>("ZTF_alerts")
        .find(doc! {})
        .projection(doc! {"_id": 1})
        // .limit(200_000)
        .await
        .unwrap();

    let mut candids: Vec<i64> = Vec::new();
    while let Some(result) = candids_cursor.next().await {
        match result {
            Ok(document) => {
                let candid = document.get_i64("_id").unwrap();
                candids.push(candid);
            }
            Err(e) => {
                eprintln!("Failed to fetch candid: {}", e);
                continue;
            }
        }
    }

    // print how many candids we have to process
    if candids.is_empty() {
        eprintln!("No candids provided to process.");
        return;
    } else {
        println!("Processing {} candids.", candids.len());
    }

    let catalog: String = "ZTF".to_string();

    let start = std::time::Instant::now();

    // loop over batched of 1000 candids at a time to avoid overwhelming the database
    let mut count = 0;
    for chunk in candids.chunks(1000) {
        process_chunk(&chunk, &catalog, &db, &model).await;
        count += chunk.len();
        // print progress every 10000 candids processed
        if count % 10000 == 0 {
            let duration = start.elapsed();
            println!("Processed {} candids so far, elapsed time: {:?}", 
                count, 
                duration
            );
        }
    }

    let duration = start.elapsed();
    println!("Processing {} candids took: {:?}", 
        candids.len(), 
        duration
    );

}

