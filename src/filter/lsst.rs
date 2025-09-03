use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument};

use crate::filter::{
    get_filter_object, run_filter, uses_field_in_filter, validate_filter_pipeline, Alert,
    Classification, Filter, FilterError, FilterResults, FilterWorker, FilterWorkerError, Origin,
    Photometry,
};
use crate::utils::db::{fetch_timeseries_op, get_array_element};
use crate::utils::enums::Survey;

#[instrument(skip_all, err)]
pub async fn build_lsst_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_collection: &mongodb::Collection<mongodb::bson::Document>,
) -> Result<Vec<Alert>, FilterWorkerError> {
    let candids: Vec<i64> = alerts_with_filter_results.keys().cloned().collect();
    let pipeline = vec![
        doc! {
            "$match": {
                "_id": { "$in": &candids }
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "jd": "$candidate.jd",
                "ra": "$candidate.ra",
                "dec": "$candidate.dec",
                "reliability": "$candidate.reliability",
            }
        },
        doc! {
            "$lookup": {
                "from": "LSST_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$lookup": {
                "from": "LSST_alerts_cutouts",
                "localField": "_id",
                "foreignField": "_id",
                "as": "cutouts"
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "jd": 1,
                "ra": 1,
                "dec": 1,
                "prv_candidates": get_array_element("aux.prv_candidates"),
                "fp_hists": get_array_element("aux.fp_hists"),
                "cutoutScience": get_array_element("cutouts.cutoutScience"),
                "cutoutTemplate": get_array_element("cutouts.cutoutTemplate"),
                "cutoutDifference": get_array_element("cutouts.cutoutDifference"),
            }
        },
    ];

    // Execute the aggregation pipeline
    let mut cursor = alert_collection.aggregate(pipeline).await?;

    let mut alerts_output = Vec::new();
    while let Some(alert_document) = cursor.next().await {
        let alert_document = alert_document?;
        let candid = alert_document.get_i64("_id")?;
        let object_id = alert_document.get_str("objectId")?.to_string();
        let jd = alert_document.get_f64("jd")?;
        let ra = alert_document.get_f64("ra")?;
        let dec = alert_document.get_f64("dec")?;
        let cutout_science = alert_document.get_binary_generic("cutoutScience")?.to_vec();
        let cutout_template = alert_document
            .get_binary_generic("cutoutTemplate")?
            .to_vec();
        let cutout_difference = alert_document
            .get_binary_generic("cutoutDifference")?
            .to_vec();

        // let's create the array of photometry (non forced phot only for now)
        let mut photometry = Vec::new();
        for doc in alert_document.get_array("prv_candidates")?.iter() {
            let doc = match doc.as_document() {
                Some(doc) => doc,
                None => continue, // skip if not a document
            };
            let jd = doc.get_f64("jd")?;
            let flux = doc.get_f64("psfFlux")? * 1e-9; // from nJy to Jy
            let flux_err = doc.get_f64("psfFluxErr")? * 1e-9;
            let band = doc.get_str("band")?.to_string();
            let ra = doc.get_f64("ra").ok(); // optional, might not be present
            let dec = doc.get_f64("dec").ok(); // optional, might not be present

            photometry.push(Photometry {
                jd,
                flux: Some(flux),
                flux_err,
                band: format!("lsst{}", band),
                zero_point: 8.9,
                origin: Origin::Alert,
                programid: 1, // only one public stream for LSST
                survey: Survey::Lsst,
                ra,
                dec,
            });
        }

        for doc in alert_document.get_array("fp_hists")?.iter() {
            let doc = match doc.as_document() {
                Some(doc) => doc,
                None => continue, // skip if not a document
            };
            let jd = doc.get_f64("jd")?;
            let flux = doc.get_f64("psfFlux")?;
            let flux_err = doc.get_f64("psfFluxErr")?;
            let band = doc.get_str("band")?.to_string();
            let ra = doc.get_f64("ra").ok(); // optional, might not be present
            let dec = doc.get_f64("dec").ok(); // optional, might not be present

            photometry.push(Photometry {
                jd,
                flux: Some(flux),
                flux_err,
                band: format!("lsst{}", band),
                zero_point: 8.9,
                origin: Origin::ForcedPhot,
                programid: 1, // only one public stream for LSST
                survey: Survey::Lsst,
                ra,
                dec,
            });
        }

        // sort the photometry by jd ascending
        photometry.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap());

        let mut classifications = Vec::new();
        if let Some(rb) = alert_document.get_f64("reliability").ok() {
            classifications.push(Classification {
                classifier: "reliability".to_string(),
                score: rb,
            });
        }

        let alert = Alert {
            candid,
            object_id,
            jd,
            ra,
            dec,
            filters: alerts_with_filter_results
                .get(&candid)
                .cloned()
                .unwrap_or_else(Vec::new),
            classifications,
            photometry,
            cutout_science,
            cutout_template,
            cutout_difference,
            survey: Survey::Lsst,
        };
        alerts_output.push(alert);
    }

    if candids.len() != alerts_output.len() {
        return Err(FilterWorkerError::AlertNotFound);
    }

    Ok(alerts_output)
}

pub struct LsstFilter {
    id: String,
    pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl Filter for LsstFilter {
    #[instrument(skip(filter_collection), err)]
    async fn build(
        filter_id: &str,
        filter_collection: &mongodb::Collection<mongodb::bson::Document>,
    ) -> Result<Self, FilterError> {
        // get filter object
        let filter_obj = get_filter_object(filter_id, "LSST_alerts", filter_collection).await?;

        // filter prefix (with permissions)
        let mut pipeline = vec![
            doc! {
                "$match": doc! {
                    "_id": doc! {
                        "$in": [] // candids will be inserted here
                    }
                }
            },
            doc! {
                "$project": doc! {
                    "objectId": 1,
                    "candidate": 1,
                    "properties": 1,
                    "coordinates": 1,
                }
            },
        ];

        let mut aux_add_fields = doc! {};

        // get filter pipeline as str and convert to Vec<Bson>
        let filter_pipeline = filter_obj
            .get("pipeline")
            .ok_or(FilterError::FilterPipelineError)?
            .as_str()
            .ok_or(FilterError::FilterPipelineError)?;

        let filter_pipeline = serde_json::from_str::<serde_json::Value>(filter_pipeline)?;
        let filter_pipeline = filter_pipeline
            .as_array()
            .ok_or(FilterError::InvalidFilterPipeline)?;

        // validate filter
        validate_filter_pipeline(&filter_pipeline)?;

        let use_prv_candidates_index = uses_field_in_filter(filter_pipeline, "prv_candidates");
        let use_fp_hists_index = uses_field_in_filter(filter_pipeline, "fp_hists");
        let use_cross_matches_index = uses_field_in_filter(filter_pipeline, "cross_matches");
        let mut use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");

        // check if other survey's data is used, in which case we do want to bring in aliases
        let use_ztf_prv_candidates_index =
            uses_field_in_filter(filter_pipeline, "ZTF.prv_candidates");
        let use_ztf_fp_hists_index = uses_field_in_filter(filter_pipeline, "ZTF.fp_hists");

        if use_ztf_prv_candidates_index.is_some() {
            if use_aliases_index.is_none() {
                use_aliases_index = use_ztf_prv_candidates_index;
            } else {
                use_aliases_index = Some(
                    use_aliases_index
                        .unwrap()
                        .min(use_ztf_prv_candidates_index.unwrap()),
                );
            }
        }
        if use_ztf_fp_hists_index.is_some() {
            if use_aliases_index.is_none() {
                use_aliases_index = use_ztf_fp_hists_index;
            } else {
                use_aliases_index = Some(
                    use_aliases_index
                        .unwrap()
                        .min(use_ztf_fp_hists_index.unwrap()),
                );
            }
        }

        if use_prv_candidates_index.is_some() {
            // insert it in aux addFields stage
            aux_add_fields.insert(
                "prv_candidates".to_string(),
                fetch_timeseries_op("aux.prv_candidates", "candidate.jd", 365, None),
            );
        }
        if use_fp_hists_index.is_some() {
            aux_add_fields.insert(
                "fp_hists".to_string(),
                fetch_timeseries_op("aux.fp_hists", "candidate.jd", 365, None),
            );
        }
        if use_cross_matches_index.is_some() {
            aux_add_fields.insert(
                "cross_matches".to_string(),
                get_array_element("aux.cross_matches"),
            );
        }
        if use_aliases_index.is_some() {
            aux_add_fields.insert("aliases".to_string(), get_array_element("aux.aliases"));
        }

        let mut insert_aux_index = usize::MAX;
        if let Some(index) = use_prv_candidates_index {
            insert_aux_index = insert_aux_index.min(index);
        }
        if let Some(index) = use_fp_hists_index {
            insert_aux_index = insert_aux_index.min(index);
        }
        if let Some(index) = use_cross_matches_index {
            insert_aux_index = insert_aux_index.min(index);
        }
        if let Some(index) = use_aliases_index {
            insert_aux_index = insert_aux_index.min(index);
        }
        let mut insert_aux_pipeline = insert_aux_index != usize::MAX;

        // same for ZTF
        let mut ztf_aux_add_fields = doc! {};
        if use_ztf_prv_candidates_index.is_some() {
            ztf_aux_add_fields.insert(
                "ZTF.prv_candidates".to_string(),
                fetch_timeseries_op("ztf_aux.prv_candidates", "candidate.jd", 365, None),
            );
        }
        if use_ztf_fp_hists_index.is_some() {
            ztf_aux_add_fields.insert(
                "ZTF.fp_hists".to_string(),
                fetch_timeseries_op("ztf_aux.fp_hists", "candidate.jd", 365, None),
            );
        }

        let mut ztf_insert_aux_index = usize::MAX;
        if let Some(index) = use_ztf_prv_candidates_index {
            ztf_insert_aux_index = ztf_insert_aux_index.min(index);
        }
        if let Some(index) = use_ztf_fp_hists_index {
            ztf_insert_aux_index = ztf_insert_aux_index.min(index);
        }
        let mut ztf_insert_aux_pipeline = ztf_insert_aux_index != usize::MAX;

        // now we loop over the base_pipeline and insert stages
        for i in 0..filter_pipeline.len() {
            let x = mongodb::bson::to_document(&filter_pipeline[i])?;

            if insert_aux_pipeline && i == insert_aux_index {
                pipeline.push(doc! {
                    "$lookup": doc! {
                        "from": format!("LSST_alerts_aux"),
                        "localField": "objectId",
                        "foreignField": "_id",
                        "as": "aux"
                    }
                });
                pipeline.push(doc! {
                    "$addFields": &aux_add_fields
                });
                pipeline.push(doc! {
                    "$unset": "aux"
                });
                insert_aux_pipeline = false; // only insert once
            }

            if ztf_insert_aux_pipeline {
                pipeline.push(doc! {
                    "$lookup": doc! {
                        "from": format!("ZTF_alerts_aux"),
                        "localField": "aliases.ZTF.0",
                        "foreignField": "_id",
                        "as": "ztf_aux"
                    }
                });
                pipeline.push(doc! {
                    "$addFields": &ztf_aux_add_fields
                });
                pipeline.push(doc! {
                    "$unset": "ztf_aux"
                });
                ztf_insert_aux_pipeline = false; // only insert once
            }

            // push the current stage
            pipeline.push(x);
        }

        let filter = LsstFilter {
            id: filter_id.to_string(),
            pipeline: pipeline,
        };

        Ok(filter)
    }
}

pub struct LsstFilterWorker {
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    input_queue: String,
    output_topic: String,
    filters: Vec<LsstFilter>,
}

#[async_trait::async_trait]
impl FilterWorker for LsstFilterWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        filter_ids: Option<Vec<String>>,
    ) -> Result<Self, FilterWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let alert_collection = db.collection("LSST_alerts");
        let filter_collection = db.collection("filters");

        let input_queue = "LSST_alerts_filter_queue".to_string();
        let output_topic = "LSST_alerts_results".to_string();

        let all_filter_ids: Vec<String> = filter_collection
            .distinct("_id", doc! {"active": true, "catalog": "LSST_alerts"})
            .await?
            .into_iter()
            .map(|x| {
                x.as_str()
                    .map(|s| s.to_string())
                    .ok_or(FilterError::InvalidFilterId)
            })
            .collect::<Result<Vec<String>, FilterError>>()?;

        let mut filters: Vec<LsstFilter> = Vec::new();
        if let Some(filter_ids) = filter_ids {
            // if filter_ids are provided, we only build those filters
            for filter_id in filter_ids {
                if all_filter_ids.contains(&filter_id) {
                    filters.push(LsstFilter::build(&filter_id, &filter_collection).await?);
                } else {
                    return Err(FilterWorkerError::FilterNotFound);
                }
            }
        } else {
            // if no filter_ids are provided, we build all active filters
            for filter_id in all_filter_ids {
                filters.push(LsstFilter::build(&filter_id, &filter_collection).await?);
            }
        }

        Ok(LsstFilterWorker {
            alert_collection,
            input_queue,
            output_topic,
            filters,
        })
    }

    fn survey() -> Survey {
        Survey::Lsst
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_topic_name(&self) -> String {
        self.output_topic.clone()
    }

    fn has_filters(&self) -> bool {
        !self.filters.is_empty()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(&mut self, alerts: &[String]) -> Result<Vec<Alert>, FilterWorkerError> {
        let mut alerts_output = Vec::new();

        // unlike ZTF where we get a tuple of (programid, candid) from redis
        // LSST has only one public stream, meaning there are no programids
        // so we simply convert the array of String to Vec<i64>
        let candids: Vec<i64> = alerts.iter().map(|alert| alert.parse().unwrap()).collect();

        // run the filters
        let mut results_map: HashMap<i64, Vec<FilterResults>> = HashMap::new();
        for filter in &self.filters {
            let out_documents = run_filter(
                candids.clone(),
                &filter.id,
                filter.pipeline.clone(),
                &self.alert_collection,
            )
            .await?;

            // if the array is empty, continue
            if out_documents.is_empty() {
                continue;
            } else {
                // if we have output documents, we need to process them
                // and create filter results for each document (which contain annotations)
                info!(
                    "{} alerts passed lsst filter {}",
                    out_documents.len(),
                    filter.id,
                );
            }

            let now_ts = chrono::Utc::now().timestamp_millis() as f64;

            for doc in out_documents {
                let candid = doc.get_i64("_id")?;
                // might want to have the annotations as an optional field instead of empty
                let annotations =
                    serde_json::to_string(doc.get_document("annotations").unwrap_or(&doc! {}))?;
                let filter_result = FilterResults {
                    filter_id: filter.id.clone(),
                    passed_at: now_ts,
                    annotations,
                };
                let entry = results_map.entry(candid).or_insert(Vec::new());
                entry.push(filter_result);
            }
        }

        let alerts = build_lsst_alerts(&results_map, &self.alert_collection).await?;
        alerts_output.extend(alerts);

        Ok(alerts_output)
    }
}
