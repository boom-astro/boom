use ndarray::{Array, Dim};
use ort::{
    execution_providers::CUDAExecutionProvider,
    inputs,
    session::{builder::GraphOptimizationLevel, Session},
};

use boom::{conf, utils::fits::prepare_triplet};
use futures::StreamExt;
use mongodb::bson::{doc, Document};

fn load_model(path: &str) -> Session {
    let builder = Session::builder().unwrap();

    builder
        .with_execution_providers([CUDAExecutionProvider::default().build()])
        .unwrap()
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .unwrap()
        .with_intra_threads(1)
        .unwrap()
        .commit_from_file(path)
        .unwrap()
}

pub trait Model {
    fn new(path: &str) -> Self;
    fn model(&self) -> &Session;
    fn get_metadata(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 2]>>;
    fn get_triplet(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 4]>> {
        let mut triplets = Array::zeros((alerts.len(), 63, 63, 3));
        for i in 0..alerts.len() {
            let (cutout_science, cutout_template, cutout_difference) =
                prepare_triplet(&alerts[i]).unwrap();
            for (j, cutout) in [cutout_science, cutout_template, cutout_difference]
                .iter()
                .enumerate()
            {
                let mut slice = triplets.slice_mut(ndarray::s![i, .., .., j as usize]);
                let cutout_array = Array::from_shape_vec((63, 63), cutout.to_vec()).unwrap();
                slice.assign(&cutout_array);
            }
        }
        triplets
    }
    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Vec<f32>;
}

pub struct AcaiModel {
    model: Session,
}

impl Model for AcaiModel {
    fn model(&self) -> &Session {
        &self.model
    }
    fn new(path: &str) -> Self {
        Self {
            model: load_model(&path),
        }
    }

    fn get_metadata(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 2]>> {
        let mut features_batch: Vec<[f32; 25]> = Vec::new();

        for alert in alerts {
            let candidate = alert.get_document("candidate").unwrap();

            let drb = candidate.get_f64("drb").unwrap() as f32;
            let diffmaglim = candidate.get_f64("diffmaglim").unwrap() as f32;
            let ra = candidate.get_f64("ra").unwrap() as f32;
            let dec = candidate.get_f64("dec").unwrap() as f32;
            let magpsf = candidate.get_f64("magpsf").unwrap() as f32;
            let sigmapsf = candidate.get_f64("sigmapsf").unwrap() as f32;
            let chipsf = candidate.get_f64("chipsf").unwrap() as f32;
            let fwhm = candidate.get_f64("fwhm").unwrap() as f32;
            let sky = candidate.get_f64("sky").unwrap() as f32;
            let chinr = candidate.get_f64("chinr").unwrap() as f32;
            let sharpnr = candidate.get_f64("sharpnr").unwrap() as f32;
            let sgscore1 = candidate.get_f64("sgscore1").unwrap() as f32;
            let distpsnr1 = candidate.get_f64("distpsnr1").unwrap() as f32;
            let sgscore2 = candidate.get_f64("sgscore2").unwrap() as f32;
            let distpsnr2 = candidate.get_f64("distpsnr2").unwrap() as f32;
            let sgscore3 = candidate.get_f64("sgscore3").unwrap() as f32;
            let distpsnr3 = candidate.get_f64("distpsnr3").unwrap() as f32;
            let ndethist = candidate.get_i32("ndethist").unwrap() as f32;
            let ncovhist = candidate.get_i32("ncovhist").unwrap() as f32;
            let scorr = candidate.get_f64("scorr").unwrap() as f32;
            let nmtchps = candidate.get_i32("nmtchps").unwrap() as f32;
            let clrcoeff = candidate.get_f64("clrcoeff").unwrap() as f32;
            let clrcounc = candidate.get_f64("clrcounc").unwrap() as f32;
            let neargaia = candidate.get_f64("neargaia").unwrap() as f32;
            let neargaiabright = candidate.get_f64("neargaiabright").unwrap() as f32;

            // the vec is nested because we need it to be of shape (1, N), not just a flat array
            let alert_features = [
                drb,
                diffmaglim,
                ra,
                dec,
                magpsf,
                sigmapsf,
                chipsf,
                fwhm,
                sky,
                chinr,
                sharpnr,
                sgscore1,
                distpsnr1,
                sgscore2,
                distpsnr2,
                sgscore3,
                distpsnr3,
                ndethist,
                ncovhist,
                scorr,
                nmtchps,
                clrcoeff,
                clrcounc,
                neargaia,
                neargaiabright,
            ];

            features_batch.push(alert_features);
        }

        Array::from(features_batch)
    }

    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Vec<f32> {
        let model_inputs = inputs! {
            "features" =>  metadata_features.clone(),
            "triplets" => image_features.clone(),
        }
        .unwrap();

        let outputs = self.model.run(model_inputs).unwrap();
        let scores = outputs["score"].try_extract_tensor::<f32>().unwrap();

        return scores.as_slice().unwrap().to_vec();
    }
}

pub struct BtsBotModel {
    model: Session,
}

impl Model for BtsBotModel {
    fn model(&self) -> &Session {
        &self.model
    }
    fn new(path: &str) -> Self {
        Self {
            model: load_model(&path),
        }
    }

    fn get_metadata(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 2]>> {
        let mut features_batch: Vec<[f32; 25]> = Vec::new();

        for alert in alerts {
            let candidate = alert.get_document("candidate").unwrap();

            let sgscore1 = candidate.get_f64("sgscore1").unwrap() as f32;
            let distpsnr1 = candidate.get_f64("distpsnr1").unwrap() as f32;
            let sgscore2 = candidate.get_f64("sgscore2").unwrap() as f32;
            let distpsnr2 = candidate.get_f64("distpsnr2").unwrap() as f32;
            let fwhm = candidate.get_f64("fwhm").unwrap() as f32;
            let magpsf = candidate.get_f64("magpsf").unwrap(); // we convert to f32 later
            let sigmapsf = candidate.get_f64("sigmapsf").unwrap() as f32;
            let chipsf = candidate.get_f64("chipsf").unwrap() as f32;
            let ra = candidate.get_f64("ra").unwrap() as f32;
            let dec = candidate.get_f64("dec").unwrap() as f32;
            let diffmaglim = candidate.get_f64("diffmaglim").unwrap() as f32;
            let ndethist = candidate.get_i32("ndethist").unwrap() as f32;
            let nmtchps = candidate.get_i32("nmtchps").unwrap() as f32;

            let drb = candidate.get_f64("drb").unwrap() as f32;
            let ncovhist = candidate.get_i32("ncovhist").unwrap() as f32;

            let chinr = candidate.get_f64("chinr").unwrap() as f32;
            let sharpnr = candidate.get_f64("sharpnr").unwrap() as f32;
            let scorr = candidate.get_f64("scorr").unwrap() as f32;
            let sky = candidate.get_f64("sky").unwrap() as f32;

            // next, we compute some custom features based on the lightcurve
            let jd = candidate.get_f64("jd").unwrap();
            let mut firstdet_jd = jd.clone();
            let mut peakmag_jd = jd.clone();
            let mut peakmag = magpsf.clone();
            let mut maxmag = magpsf.clone();

            for prv_cand in alert.get_array("prv_candidates").unwrap() {
                let prv_cand = prv_cand.as_document().unwrap();
                let prv_cand_magpsf = prv_cand.get_f64("magpsf").unwrap();
                let prv_cand_jd = prv_cand.get_f64("jd").unwrap();
                if prv_cand_magpsf < peakmag {
                    peakmag = prv_cand_magpsf;
                    peakmag_jd = prv_cand_jd;
                }
                if prv_cand_magpsf > maxmag {
                    maxmag = prv_cand_magpsf
                }
                if prv_cand_jd < firstdet_jd {
                    firstdet_jd = prv_cand_jd
                }
            }

            let days_since_peak = (jd - peakmag_jd) as f32;
            let days_to_peak = (peakmag_jd - firstdet_jd) as f32;
            let age = (firstdet_jd - jd) as f32;

            let nnondet = ncovhist - ndethist;

            let alert_features = [
                sgscore1,
                distpsnr1,
                sgscore2,
                distpsnr2,
                fwhm,
                magpsf as f32,
                sigmapsf,
                chipsf,
                ra,
                dec,
                diffmaglim,
                ndethist,
                nmtchps,
                age,
                days_since_peak,
                days_to_peak,
                peakmag as f32,
                drb,
                ncovhist,
                nnondet,
                chinr,
                sharpnr,
                scorr,
                sky,
                maxmag as f32,
            ];

            features_batch.push(alert_features);
        }

        Array::from(features_batch)
    }

    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Vec<f32> {
        let model_inputs = inputs! {
            "triplet" => image_features.clone(),
            "metadata" =>  metadata_features.clone(),
        }
        .unwrap();

        let outputs = self.model.run(model_inputs).unwrap();

        let scores = outputs["fc_out"].try_extract_tensor::<f32>().unwrap();

        return scores.as_slice().unwrap().to_vec();
    }
}

async fn fetch_alerts(
    candids: &[i64], // this is a slice of candids to process
    catalog: &str,
    db: &mongodb::Database,
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

#[tokio::main]
async fn main() {
    let config_path = "config.yaml";
    let config_file = conf::load_config(&config_path).unwrap();
    let db = conf::build_db(&config_file).await.unwrap();

    let acai_h_model = AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx");
    let acai_n_model = AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx");
    let acai_v_model = AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx");
    let acai_o_model = AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx");
    let acai_b_model = AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx");

    let btsbot_model = BtsBotModel::new("data/models/btsbot-v1.0.1.onnx");

    let mut candids_cursor = db
        .collection::<Document>("ZTF_alerts")
        .find(doc! {})
        .projection(doc! {"_id": 1})
        .limit(100)
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

    if candids.is_empty() {
        eprintln!("No candids provided to process.");
        return;
    }

    let catalog: String = "ZTF".to_string();

    // next let's try running on batches
    let alerts = fetch_alerts(&candids, &catalog, &db).await.unwrap();

    let start = std::time::Instant::now();

    let metadata_batch = acai_h_model.get_metadata(&alerts);
    let triplet_batch = acai_h_model.get_triplet(&alerts);

    let _acai_h_scores = acai_h_model.predict(&metadata_batch, &triplet_batch);
    let _acai_n_scores = acai_n_model.predict(&metadata_batch, &triplet_batch);
    let _acai_v_scores = acai_v_model.predict(&metadata_batch, &triplet_batch);
    let _acai_o_scores = acai_o_model.predict(&metadata_batch, &triplet_batch);
    let _acai_b_scores = acai_b_model.predict(&metadata_batch, &triplet_batch);

    let btsbot_metadata_batch = btsbot_model.get_metadata(&alerts);
    let _btsbot_scores = btsbot_model.predict(&btsbot_metadata_batch, &triplet_batch);

    println!("Time taken (batch): {:?}", start.elapsed());

    let alerts = fetch_alerts(&candids, &catalog, &db).await.unwrap();

    let start = std::time::Instant::now();

    for i in 0..alerts.len() {
        let metadata = acai_h_model.get_metadata(&alerts[i..i + 1]);
        let triplet = acai_h_model.get_triplet(&alerts[i..i + 1]);

        let _acai_h_score = acai_h_model.predict(&metadata, &triplet);

        let _acai_n_score = acai_n_model.predict(&metadata, &triplet);

        let _acai_v_score = acai_v_model.predict(&metadata, &triplet);

        let _acai_o_score = acai_o_model.predict(&metadata, &triplet);

        let _acai_b_score = acai_b_model.predict(&metadata, &triplet);

        let metadata_btsbot = btsbot_model.get_metadata(&alerts[i..i + 1]);
        let _btsbot_score = btsbot_model.predict(&metadata_btsbot, &triplet);

        // if acai_h_score > 0.9 {
        //     println!("ACAI H: {} - {} = {}", candid, object_id, acai_h_score);
        // }
        // if acai_n_score > 0.9 {
        //     println!("ACAI N: {} - {} = {}", candid, object_id, acai_n_score);
        // }
    }

    println!("Time taken (one by one): {:?}", start.elapsed());
}
