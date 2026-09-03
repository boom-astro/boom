use mongodb::bson::{doc, Bson, Document};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, instrument, warn};

use crate::alert::ZtfCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{
    create_ztf_alert_pipeline, deserialize_ztf_alert_lightcurve, deserialize_ztf_forced_lightcurve,
    fetch_alerts, ZtfAlertClassifications, ZtfPhotometry, ZtfSurveyMatches,
};
use crate::filter::{
    build_loaded_filters, build_lsst_aux_data, insert_lsst_aux_pipeline_if_needed,
    parse_programid_candid_tuple, run_filter, update_aliases_index_multiple, uses_field_in_filter,
    validate_filter_pipeline, watchlist_projections, Alert, Classification, Filter, FilterError,
    FilterResults, FilterWorker, FilterWorkerError, LoadedFilter, Origin, Photometry, SurveyMatch,
    SurveyMatches,
};
use crate::utils::cutouts::CutoutStorage;
use crate::utils::db::{fetch_timeseries_op, get_array_dict_element};
use crate::utils::mpcorb::{
    fill_geometry, has_geometry, normalize_ztf_ssnamenr, OrbitCache, ORBITS_COLLECTION,
};
use crate::utils::outburst::MAX_SEPARATION_ARCSEC;
use crate::utils::sso_geometry::OrbitalElements;
use crate::utils::{enums::Survey, o11y::logging::as_error};

/// For a filter running on another survey (e.g., LSST), determine if we need to
/// fetch ZTF auxiliary data (prv_candidates, fp_hists) based on the fields
/// used in the filter pipeline.
///
/// # Arguments
/// * `use_aliases_index` - Current index of the aliases lookup stage, if any.
/// * `filter_pipeline` - The user-defined filter pipeline stages.
///
/// Returns
/// * `Option<usize>` - Updated index of the aliases lookup stage, if any.
/// * `bool` - Whether to insert the ZTF auxiliary data lookup pipeline.
/// * `Document` - The fields to add to the alert documents.
pub fn build_ztf_aux_data(
    use_aliases_index: Option<usize>,
    filter_pipeline: &Vec<serde_json::Value>,
    permissions: &HashMap<Survey, Vec<i32>>,
) -> (Option<usize>, bool, Document) {
    let use_ztf_prv_candidates_index = uses_field_in_filter(filter_pipeline, "ZTF.prv_candidates");
    let use_ztf_fp_hists_index = uses_field_in_filter(filter_pipeline, "ZTF.fp_hists");

    let mut ztf_aux_add_fields = doc! {
        "ztf_aux": mongodb::bson::Bson::Null,
    };

    static DEFAULT_ZTF_PERMS: &[i32] = &[1];
    let ztf_permissions: &[i32] = permissions
        .get(&Survey::Ztf)
        .map(|v| &v[..])
        .unwrap_or(DEFAULT_ZTF_PERMS);

    let permissions_check = Some(vec![doc! {
        "$in": [
            "$$x.programid",
            &ztf_permissions
        ]
    }]);
    if use_ztf_prv_candidates_index.is_some() {
        ztf_aux_add_fields.insert(
            "ZTF.prv_candidates".to_string(),
            fetch_timeseries_op(
                "ztf_aux.prv_candidates",
                "candidate.jd",
                365,
                permissions_check.clone(),
            ),
        );
    }
    if use_ztf_fp_hists_index.is_some() {
        ztf_aux_add_fields.insert(
            "ZTF.fp_hists".to_string(),
            fetch_timeseries_op(
                "ztf_aux.fp_hists",
                "candidate.jd",
                365,
                permissions_check.clone(),
            ),
        );
    }

    let mut ztf_insert_aux_index = usize::MAX;
    if let Some(index) = use_ztf_prv_candidates_index {
        ztf_insert_aux_index = ztf_insert_aux_index.min(index);
    }
    if let Some(index) = use_ztf_fp_hists_index {
        ztf_insert_aux_index = ztf_insert_aux_index.min(index);
    }
    let ztf_insert_aux_pipeline = ztf_insert_aux_index != usize::MAX;

    let updated_use_aliases_index = update_aliases_index_multiple(
        use_aliases_index,
        vec![use_ztf_prv_candidates_index, use_ztf_fp_hists_index],
    );

    (
        updated_use_aliases_index,
        ztf_insert_aux_pipeline,
        ztf_aux_add_fields,
    )
}

/// Inserts the ZTF auxiliary data lookup pipeline into the provided pipeline
/// if needed.
///
/// # Arguments
/// * `pipeline` - The MongoDB aggregation pipeline to modify.
/// * `ztf_insert_aux_pipeline` - Whether to insert the ZTF auxiliary data lookup pipeline.
/// * `ztf_aux_add_fields` - The fields to add to the alert documents.
///
/// Returns
/// * `()` - The function modifies the pipeline in place.
pub fn insert_ztf_aux_pipeline_if_needed(
    pipeline: &mut Vec<Document>,
    ztf_insert_aux_pipeline: &mut bool,
    ztf_aux_add_fields: &Document,
) {
    if *ztf_insert_aux_pipeline {
        pipeline.push(doc! {
            "$lookup": doc! {
                "from": "ZTF_alerts_aux",
                "localField": "aliases.ZTF.0",
                "foreignField": "_id",
                "as": "ztf_aux"
            }
        });
        pipeline.push(doc! {
            "$addFields": ztf_aux_add_fields
        });
        *ztf_insert_aux_pipeline = false; // only insert once
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ZtfAlertEnriched {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    pub classifications: Option<ZtfAlertClassifications>,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_candidates: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_nondetections: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_forced_lightcurve")]
    pub fp_hists: Vec<ZtfPhotometry>,
    pub survey_matches: Option<ZtfSurveyMatches>,
}

/// Builds ZTF Alert objects from the provided filter results and alert collection.
///
/// # Arguments
/// * `alerts_with_filter_results` - A mapping of alert candids to their corresponding filter results.
/// * `alert_pipeline` - The MongoDB aggregation pipeline to fetch alert data, which should be pre-populated with the necessary lookups for auxiliary data.
/// * `alert_collection` - The MongoDB collection containing ZTF alert documents.
/// * `alert_cutout_storage` - The storage for ZTF alert cutouts.

///
/// # Returns
/// * `Result<Vec<Alert>, FilterWorkerError>` - A vector of constructed Alert objects or a FilterWorkerError.
#[instrument(skip_all, err)]
pub async fn build_ztf_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_pipeline: &Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
    alert_cutout_storage: &CutoutStorage,
) -> Result<Vec<Alert>, FilterWorkerError> {
    let candids: Vec<i64> = alerts_with_filter_results.keys().cloned().collect();
    if candids.is_empty() {
        return Ok(Vec::new());
    }

    let alerts: Vec<ZtfAlertEnriched> = fetch_alerts(&candids, &alert_pipeline, alert_collection)
        .await
        .map_err(|e| FilterWorkerError::FetchAlertsError(e.to_string()))?;

    if alerts.len() != candids.len() {
        let nb_total = candids.len();
        let mut missing_candids: Vec<&i64> = candids
            .iter()
            .filter(|c| !alerts.iter().any(|a| a.candid == **c))
            .collect();
        missing_candids.sort();
        warn!(
            "Only fetched {} alerts from {} candids. Missing candids: {:?}",
            alerts.len(),
            nb_total,
            missing_candids
        );
    }

    let mut candid_to_cutouts = alert_cutout_storage
        .retrieve_multiple_cutouts(&candids, false)
        .await?;

    if candid_to_cutouts.len() != alerts.len() {
        let mut missing_cutouts_candids: Vec<&i64> = alerts
            .iter()
            .filter(|a| !candid_to_cutouts.contains_key(&a.candid))
            .map(|a| &a.candid)
            .collect();
        missing_cutouts_candids.sort();
        warn!(
            "Only fetched cutouts for {} alerts from {} candids. Missing cutouts for candids: {:?}",
            candid_to_cutouts.len(),
            alerts.len(),
            missing_cutouts_candids
        );
        return Err(FilterWorkerError::MissingCutoutsBatch(
            missing_cutouts_candids.len(),
        ));
    }

    let mut alerts_output = Vec::new();
    for alert in alerts {
        let candid = alert.candid;

        let mut classifications = Vec::new();
        if let Some(rb) = alert.candidate.candidate.rb {
            classifications.push(Classification {
                classifier: "rb".to_string(),
                score: rb,
                distance_arcsec: None,
            });
        }
        if let Some(drb) = alert.candidate.candidate.drb {
            classifications.push(Classification {
                classifier: "drb".to_string(),
                score: drb,
                distance_arcsec: None,
            });
        }
        if let (Some(sgscore), Some(distpsnr1)) = (
            alert.candidate.candidate.sgscore1,
            alert.candidate.candidate.distpsnr1,
        ) {
            classifications.push(Classification {
                classifier: "sgscore1".to_string(),
                score: sgscore,
                distance_arcsec: Some(distpsnr1),
            });
        }

        if let Some(alert_classifications) = alert.classifications {
            // ACAI (h,n,o,v,b)
            classifications.push(Classification {
                classifier: "acai_h".to_string(),
                score: alert_classifications.acai_h,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_n".to_string(),
                score: alert_classifications.acai_n,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_o".to_string(),
                score: alert_classifications.acai_o,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_v".to_string(),
                score: alert_classifications.acai_v,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_b".to_string(),
                score: alert_classifications.acai_b,
                distance_arcsec: None,
            });
            // BTSbot
            classifications.push(Classification {
                classifier: "btsbot".to_string(),
                score: alert_classifications.btsbot,
                distance_arcsec: None,
            });
        }

        // TODO, get classifications from the alert document

        let mut photometry = Vec::new();
        for doc in alert.prv_candidates.iter() {
            photometry.push(Photometry {
                jd: doc.jd,
                flux: doc.flux,
                flux_err: doc.flux_err,
                band: format!("ztf{}", doc.band),
                origin: Origin::Alert,
                programid: doc.programid,
                survey: Survey::Ztf,
                ra: doc.ra,
                dec: doc.dec,
            });
        }

        for doc in alert.prv_nondetections.iter() {
            photometry.push(Photometry {
                jd: doc.jd,
                flux: None, // for non-detections, flux is None
                flux_err: doc.flux_err,
                band: format!("ztf{}", doc.band),
                origin: Origin::Alert,
                programid: doc.programid,
                survey: Survey::Ztf,
                ra: None,
                dec: None,
            });
        }

        for doc in alert.fp_hists.iter() {
            photometry.push(Photometry {
                jd: doc.jd,
                flux: doc.flux,
                flux_err: doc.flux_err,
                band: format!("ztf{}", doc.band),
                origin: Origin::ForcedPhot,
                programid: doc.programid,
                survey: Survey::Ztf,
                ra: None,
                dec: None,
            });
        }

        photometry.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap());

        let mut survey_matches = SurveyMatches {
            ztf: None,
            lsst: None,
        };
        if let Some(lsst_match) = alert.survey_matches.as_ref().and_then(|m| m.lsst.as_ref()) {
            let mut lsst_photometry = Vec::new();
            for doc in lsst_match.prv_candidates.iter() {
                lsst_photometry.push(Photometry {
                    jd: doc.jd,
                    flux: doc.flux,
                    flux_err: doc.flux_err,
                    band: format!("lsst{}", doc.band),
                    origin: Origin::Alert,
                    programid: 1,
                    survey: Survey::Lsst,
                    ra: doc.ra,
                    dec: doc.dec,
                });
            }
            for doc in lsst_match.fp_hists.iter() {
                lsst_photometry.push(Photometry {
                    jd: doc.jd,
                    flux: doc.flux,
                    flux_err: doc.flux_err,
                    band: format!("lsst{}", doc.band),
                    origin: Origin::ForcedPhot,
                    programid: 1,
                    survey: Survey::Lsst,
                    ra: None,
                    dec: None,
                });
            }

            lsst_photometry.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap());

            survey_matches.lsst = Some(SurveyMatch {
                object_id: lsst_match.object_id.clone(),
                ra: lsst_match.ra,
                dec: lsst_match.dec,
                photometry: lsst_photometry,
            });
        }

        let cutouts = candid_to_cutouts
            .remove(&candid)
            .ok_or_else(|| FilterWorkerError::MissingCutouts(candid))?;

        let alert = Alert {
            candid: alert.candid,
            object_id: alert.object_id,
            jd: alert.candidate.candidate.jd,
            ra: alert.candidate.candidate.ra,
            dec: alert.candidate.candidate.dec,
            filters: alerts_with_filter_results
                .get(&candid)
                .cloned()
                .unwrap_or_else(Vec::new),
            classifications,
            photometry,
            cutout_science: cutouts.cutout_science,
            cutout_template: cutouts.cutout_template,
            cutout_difference: cutouts.cutout_difference,
            survey: Survey::Ztf,
            survey_matches,
        };

        alerts_output.push(alert);
    }

    Ok(alerts_output)
}

/// Builds a MongoDB aggregation pipeline for ZTF filter execution.
///
/// This function validates the provided filter pipeline and augments it with necessary
/// auxiliary data lookups (prv_candidates, fp_hists, cross_matches, aliases) based on
/// which fields are referenced in the filter. The resulting pipeline starts with a match stage
/// to filter by candids, and should be populated with the actual candids before execution.
///
/// # Arguments
/// * `filter_pipeline` - The user-defined filter pipeline stages
///
/// # Returns
/// * `Result<Vec<Document>, FilterError>` - A complete MongoDB aggregation pipeline ready for execution, or a `FilterError` if validation fails.
/// Lookup surfacing a moving object's own photometry, keyed on MPC designation.
///
/// `objectId` cannot be used: it is positional, so a moving object is given a new
/// one on nearly every detection and its aux document describes the sky position
/// rather than the object.
///
/// The outer key is the normalised `properties.sso.designation`; the inner key is
/// the raw `candidate.ssnamenr` it is derived from. Same value, but the raw field
/// is present on the whole archive while the normalised one only exists on alerts
/// enriched since it was introduced.
///
/// Entries further than `MAX_SEPARATION_ARCSEC` from the predicted position are
/// excluded. Roughly one detection in a hundred carrying a designation is a
/// static source near the predicted track that was given that name upstream,
/// some two magnitudes brighter than the object, which is enough to dominate
/// anything measured over the array. The threshold sits at the edge of the
/// upstream search radius rather than at the width of a good match; see its
/// definition for why a tighter one would cut on ephemeris quality instead.
///
/// This is not the thresholding `sso.is_sso` refuses. There, a large separation is
/// a degraded measurement of this object, and hiding it behind a boolean would
/// conceal a drifting ephemeris the consumer needs to see -- so the number is
/// reported instead. Here the entry is a different source that shares only a
/// label, and putting it in this object's light curve is mislabelling rather than
/// recording. `separation_arcsec` is still carried per entry.
///
/// Geometry is read from each historical alert rather than recomputed, so it is
/// null on detections enriched before geometry existed. Recomputing would need
/// the elements here and would close that gap immediately, but any window
/// shorter than the time since geometry shipped fills in on its own.
fn sso_history_lookup(ztf_permissions: &Vec<i32>, window_days: f64) -> Document {
    doc! {
        "$lookup": {
            "from": "ZTF_alerts",
            "let": {
                "desig": "$properties.sso.designation",
                "jd": "$candidate.jd",
            },
            "pipeline": [
                doc! { "$match": { "$expr": { "$and": [
                    { "$eq": ["$candidate.ssnamenr", "$$desig"] },
                    { "$gte": ["$candidate.jd", { "$subtract": ["$$jd", window_days] }] },
                    { "$lte": ["$candidate.jd", "$$jd"] },
                    { "$in": ["$candidate.programid", ztf_permissions] },
                    { "$gte": ["$candidate.ssdistnr", 0.0] },
                    { "$lt": ["$candidate.ssdistnr", MAX_SEPARATION_ARCSEC] },
                ] } } },
                doc! { "$project": {
                    "_id": 0,
                    // Carried per entry so the array is self-describing: geometry
                    // is filled in after the pipeline runs, by which point the
                    // outer document is whatever the filter chose to project.
                    "designation": "$candidate.ssnamenr",
                    "jd": "$candidate.jd",
                    "fid": "$candidate.fid",
                    "magpsf": "$candidate.magpsf",
                    "sigmapsf": "$candidate.sigmapsf",
                    "ra": "$candidate.ra",
                    "dec": "$candidate.dec",
                    "predicted_mag": "$properties.sso.predicted_mag",
                    "separation_arcsec": "$properties.sso.separation_arcsec",
                    // Each point carries the geometry at its own epoch, not the
                    // alert's. Statistics that scale a window to a reference
                    // point (e.g. an outburst statistic) need one value per
                    // point, and these are the only photometry of the object
                    // itself -- the positional light curve holds a single
                    // detection for a mover.
                    "helio_dist": "$properties.sso.helio_dist",
                    "topo_dist": "$properties.sso.topo_dist",
                    "phase_angle": "$properties.sso.phase_angle",
                } },
                doc! { "$sort": { "jd": 1 } },
            ],
            "as": "sso_history",
        }
    }
}

pub async fn build_ztf_filter_pipeline(
    filter_pipeline: &Vec<serde_json::Value>,
    permissions: &HashMap<Survey, Vec<i32>>,
) -> Result<Vec<Document>, FilterError> {
    // validate filter
    validate_filter_pipeline(&filter_pipeline)?;

    let use_prv_candidates_index = uses_field_in_filter(filter_pipeline, "prv_candidates");
    let use_prv_nondetections_index = uses_field_in_filter(filter_pipeline, "prv_nondetections");
    let use_fp_hists_index = uses_field_in_filter(filter_pipeline, "fp_hists");
    let use_cross_matches_index = uses_field_in_filter(filter_pipeline, "cross_matches");
    let use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");
    let use_sso_history_index = uses_field_in_filter(filter_pipeline, "sso_history");

    // LSST data products
    let (use_aliases_index, mut lsst_insert_aux_pipeline, lsst_aux_add_fields) =
        build_lsst_aux_data(use_aliases_index, filter_pipeline);

    let mut aux_add_fields = doc! {
        "aux": mongodb::bson::Bson::Null,
    };

    let ztf_permissions = match permissions.get(&Survey::Ztf) {
        Some(perms) => perms,
        None => {
            return Err(FilterError::InvalidFilterPipeline(
                "No ZTF permissions found for the filter".to_string(),
            ))
        }
    };

    if use_prv_candidates_index.is_some() {
        // insert it in aux addFields stage
        aux_add_fields.insert(
            "prv_candidates".to_string(),
            fetch_timeseries_op(
                "aux.prv_candidates",
                "candidate.jd",
                365,
                Some(vec![doc! {
                    "$in": [
                        "$$x.programid",
                        &ztf_permissions
                    ]
                }]),
            ),
        );
    }
    if use_prv_nondetections_index.is_some() {
        aux_add_fields.insert(
            "prv_nondetections".to_string(),
            fetch_timeseries_op(
                "aux.prv_nondetections",
                "candidate.jd",
                365,
                Some(vec![doc! {
                    "$in": [
                        "$$x.programid",
                        &ztf_permissions
                    ]
                }]),
            ),
        );
    }
    if use_fp_hists_index.is_some() {
        aux_add_fields.insert(
            "fp_hists".to_string(),
            fetch_timeseries_op(
                "aux.fp_hists",
                "candidate.jd",
                365,
                Some(vec![doc! {
                    "$in": [
                        "$$x.programid",
                        &ztf_permissions
                    ]
                }]),
            ),
        );
    }
    if use_cross_matches_index.is_some() {
        aux_add_fields.insert(
            "cross_matches".to_string(),
            get_array_dict_element("aux.cross_matches"),
        );
    }
    if use_aliases_index.is_some() {
        aux_add_fields.insert("aliases".to_string(), get_array_dict_element("aux.aliases"));
    }

    let mut insert_aux_pipeline = use_prv_candidates_index.is_some()
        || use_prv_nondetections_index.is_some()
        || use_cross_matches_index.is_some()
        || use_fp_hists_index.is_some()
        || use_aliases_index.is_some();

    let mut insert_aux_index = usize::MAX;
    if let Some(index) = use_prv_candidates_index {
        insert_aux_index = insert_aux_index.min(index);
    }
    if let Some(index) = use_prv_nondetections_index {
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

    // some sanity checks
    if insert_aux_index == usize::MAX && insert_aux_pipeline {
        return Err(FilterError::InvalidFilterPipeline(
            "could not determine where to insert aux pipeline".to_string(),
        ));
    }

    let mut insert_sso_history = use_sso_history_index.is_some();
    let insert_sso_history_index = use_sso_history_index.unwrap_or(usize::MAX);

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
                "classifications": 1,
                "properties": 1,
                "coordinates": 1,
            }
        },
    ];

    // now we loop over the base_pipeline and insert stages from the filter_pipeline
    // and when i = insert_index, we insert the aux_pipeline before the stage
    for i in 0..filter_pipeline.len() {
        let x = mongodb::bson::to_document(&filter_pipeline[i])?;

        if insert_sso_history && i == insert_sso_history_index {
            // Most alerts have no designation (old alerts, or no MPC match); drop
            // them before the lookup rather than running it to produce an empty
            // array.
            pipeline.push(doc! {
                "$match": {
                    "properties.sso.designation": { "$exists": true, "$ne": Bson::Null }
                }
            });
            pipeline.push(sso_history_lookup(ztf_permissions, 365.0));
            insert_sso_history = false;
        }

        if insert_aux_pipeline && i == insert_aux_index {
            pipeline.push(doc! {
                "$lookup": doc! {
                    "from": "ZTF_alerts_aux",
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            });
            pipeline.push(doc! {
                "$addFields": &aux_add_fields
            });
            insert_aux_pipeline = false; // only insert once

            insert_lsst_aux_pipeline_if_needed(
                &mut pipeline,
                &mut lsst_insert_aux_pipeline,
                &lsst_aux_add_fields,
            );
        }

        // push the current stage
        pipeline.push(x);
    }

    Ok(pipeline)
}

/// Where a filter must project `sso_history` for its geometry to be filled in.
const SSO_HISTORY_ANNOTATION: &str = "sso_history";

pub struct ZtfFilterWorker {
    alert_pipeline: Vec<Document>,
    alert_collection: mongodb::Collection<Document>,
    /// MPC orbital elements, used to derive geometry for history points that
    /// pre-date enrichment writing it.
    mpc_orbits: mongodb::Collection<Document>,
    alert_cutout_storage: CutoutStorage,
    filter_collection: mongodb::Collection<Filter>,
    input_queue: String,
    output_topic: String,
    filter_ids: Option<Vec<String>>,
    filters: Vec<LoadedFilter>,
    filters_by_permission: HashMap<i32, Vec<String>>,
    /// watchlist catalog name -> config projection, used to surface watchlist
    /// fields under `annotations.watchlist` when a filter is bound to one.
    watchlist_projections: HashMap<String, Document>,
}

impl ZtfFilterWorker {
    /// Derive geometry for `sso_history` points that do not carry it.
    ///
    /// Geometry is a pure function of designation and epoch, so a point stored
    /// without it can be derived on read rather than backfilled.
    ///
    /// This runs after the aggregation, so the values reach the output but not
    /// the pipeline: a filter matching on `helio_dist` still sees null for a
    /// point stored without it. Points enriched with geometry are matchable,
    /// since the pipeline reads those from storage.
    ///
    /// Only fills entries under `annotations.sso_history`; a filter that projects
    /// the history elsewhere keeps whatever the stored documents had.
    async fn fill_sso_history_geometry(&self, cache: &mut OrbitCache, documents: &mut [Document]) {
        // Resolve the designations needing elements before touching anything, so
        // the whole batch is one query and each designation is parsed once.
        let keys = sso_history_keys(documents);
        if keys.is_empty() {
            return;
        }

        let wanted: Vec<String> = keys
            .values()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        // Geometry is an enhancement here: the history is still returned, just
        // without derived values on the older points.
        if let Err(e) = cache.load(&self.mpc_orbits, &wanted).await {
            warn!(
                "could not read {} for history geometry: {}",
                ORBITS_COLLECTION, e
            );
            return;
        }

        let mut filled = 0usize;
        for doc in documents.iter_mut() {
            let Ok(annotations) = doc.get_document_mut("annotations") else {
                continue;
            };
            let Ok(history) = annotations.get_array_mut(SSO_HISTORY_ANNOTATION) else {
                continue;
            };
            for entry in history.iter_mut() {
                let Some(entry) = entry.as_document_mut() else {
                    continue;
                };
                if fill_entry_geometry(entry, &keys, cache.elements()) {
                    filled += 1;
                }
            }
        }
        if filled > 0 {
            debug!("derived geometry for {} sso_history points", filled);
        }
    }
}

/// MPCORB key for each designation in the history that still needs geometry.
fn sso_history_keys(documents: &[Document]) -> HashMap<String, String> {
    let mut keys: HashMap<String, String> = HashMap::new();
    for doc in documents {
        for entry in sso_history_entries(doc) {
            if has_geometry(entry) {
                continue;
            }
            let Ok(designation) = entry.get_str("designation") else {
                continue;
            };
            if keys.contains_key(designation) {
                continue;
            }
            if let Some(key) = normalize_ztf_ssnamenr(designation) {
                keys.insert(designation.to_string(), key);
            }
        }
    }
    keys
}

/// Derive geometry for one history entry, reading the designation and epoch the
/// entry carries. A designation absent from `keys` has no MPCORB form.
fn fill_entry_geometry(
    entry: &mut Document,
    keys: &HashMap<String, String>,
    elements: &HashMap<String, OrbitalElements>,
) -> bool {
    let Ok(jd) = entry.get_f64("jd") else {
        return false;
    };
    let Some(key) = entry.get_str("designation").ok().and_then(|d| keys.get(d)) else {
        return false;
    };
    fill_geometry(entry, key, jd, elements)
}

/// The `sso_history` entries a filter projected into its annotations.
fn sso_history_entries(doc: &Document) -> impl Iterator<Item = &Document> {
    doc.get_document("annotations")
        .ok()
        .and_then(|a| a.get_array(SSO_HISTORY_ANNOTATION).ok())
        .into_iter()
        .flatten()
        .filter_map(|e| e.as_document())
}

#[async_trait::async_trait]
impl FilterWorker for ZtfFilterWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        filter_ids: Option<Vec<String>>,
    ) -> Result<Self, FilterWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let alert_collection = db.collection("ZTF_alerts");
        let mpc_orbits = db.collection(ORBITS_COLLECTION);
        let filter_collection = db.collection("filters");
        let alert_cutout_storage = config.build_cutout_storage(&Survey::Ztf).await?;

        let input_queue = "ZTF_alerts_filter_queue".to_string();
        let output_topic = "ZTF_alerts_results".to_string();

        let watchlist_projections = watchlist_projections(&config, &Survey::Ztf);
        let filters = build_loaded_filters(
            &filter_ids,
            &Survey::Ztf,
            &filter_collection,
            &watchlist_projections,
        )
        .await?;

        // Create a hashmap of filters per programid (permissions)
        let mut filters_by_permission: HashMap<i32, Vec<String>> = HashMap::new();
        for filter in &filters {
            for permission in filter.permissions.get(&Survey::Ztf).into_iter().flatten() {
                let entry = filters_by_permission
                    .entry(*permission)
                    .or_insert(Vec::new());
                entry.push(filter.id.clone());
            }
        }

        Ok(ZtfFilterWorker {
            alert_pipeline: create_ztf_alert_pipeline(true),
            alert_collection,
            mpc_orbits,
            alert_cutout_storage,
            filter_collection,
            input_queue,
            output_topic,
            filter_ids,
            filters,
            filters_by_permission,
            watchlist_projections,
        })
    }

    async fn refresh_filters(&mut self) -> Result<(), FilterWorkerError> {
        info!("refreshing ZTF filters from database");
        let filters = build_loaded_filters(
            &self.filter_ids,
            &Survey::Ztf,
            &self.filter_collection,
            &self.watchlist_projections,
        )
        .await?;

        let mut filters_by_permission: HashMap<i32, Vec<String>> = HashMap::new();
        for filter in &filters {
            for permission in filter.permissions.get(&Survey::Ztf).into_iter().flatten() {
                let entry = filters_by_permission
                    .entry(*permission)
                    .or_insert(Vec::new());
                entry.push(filter.id.clone());
            }
        }

        self.filters = filters;
        self.filters_by_permission = filters_by_permission;

        info!(
            "refreshed ZTF filters from database; now tracking {} filters",
            self.filters.len()
        );

        Ok(())
    }

    fn survey() -> Survey {
        Survey::Ztf
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
        // Shared by every (programid, filter) pass below: overlapping alerts
        // resolve to the same designations.
        let mut orbit_cache = OrbitCache::default();

        // retrieve alerts to process and group by programid
        let mut alerts_by_programid: HashMap<i32, Vec<i64>> = HashMap::new();
        for tuple_str in alerts {
            if let Some(tuple) = parse_programid_candid_tuple(&tuple_str) {
                let entry = alerts_by_programid.entry(tuple.0).or_insert(Vec::new());
                entry.push(tuple.1);
            } else {
                warn!("Failed to parse tuple from string: {}", tuple_str);
            }
        }

        // For each programid, get the filters that have that programid in their
        // permissions and run the filters
        for (programid, candids) in alerts_by_programid {
            let mut results_map: HashMap<i64, Vec<FilterResults>> = HashMap::new();

            // No active filter has permission for this programid, so there is
            // nothing to run these alerts through. Skip them rather than
            // treating it as a fatal error: an unmatched programid is a normal
            // condition (e.g. public alerts arriving while only proprietary
            // filters are configured), not a worker failure. Returning an error
            // here would kill the worker and stall the queue indefinitely.
            let filter_ids_with_perms = match self.filters_by_permission.get(&programid) {
                Some(filter_ids) => filter_ids,
                None => {
                    debug!(
                        programid,
                        n_alerts = candids.len(),
                        "no active filter has permission for programid; skipping alerts"
                    );
                    continue;
                }
            };

            for filter in &self.filters {
                // If the filter ID is not in the list of filter IDs for this
                // programid, skip it
                if !filter_ids_with_perms.contains(&filter.id) {
                    continue;
                }

                let out_documents = run_filter(
                    &candids,
                    &filter.id,
                    filter.pipeline.clone(),
                    &self.alert_collection,
                )
                .await?;

                info!(
                    "{}/{} ZTF alerts with programid {} passed filter {}",
                    out_documents.len(),
                    candids.len(),
                    programid,
                    filter.id,
                );

                // If we have output documents, we need to process them
                // and create filter results for each document (which contain annotations)
                // however, if the array is empty, there's nothing to do
                if out_documents.is_empty() {
                    continue;
                }

                let now_ts = chrono::Utc::now().timestamp_millis() as f64;

                // Before the annotations are serialized: most of the archive
                // pre-dates enrichment writing geometry.
                let mut out_documents = out_documents;
                self.fill_sso_history_geometry(&mut orbit_cache, &mut out_documents)
                    .await;

                for doc in out_documents {
                    let candid = doc
                        .get_i64("_id")
                        .inspect_err(as_error!("Failed to get candid from document"))?;
                    // might want to have the annotations as an optional field instead of empty
                    let annotations =
                        serde_json::to_string(doc.get_document("annotations").unwrap_or(&doc! {}))
                            .inspect_err(as_error!("Failed to serialize annotations"))?;
                    let filter_result = FilterResults {
                        filter_id: filter.id.clone(),
                        filter_name: filter.name.clone(),
                        passed_at: now_ts,
                        annotations,
                    };
                    let entry = results_map.entry(candid).or_insert(Vec::new());
                    entry.push(filter_result);
                }
            }

            let alerts = build_ztf_alerts(
                &results_map,
                &self.alert_pipeline,
                &self.alert_collection,
                &self.alert_cutout_storage,
            )
            .await?;
            alerts_output.extend(alerts);

            self.alert_cutout_storage.evict_from_cache(&candids).await;
        }

        Ok(alerts_output)
    }
}

#[cfg(test)]
mod sso_history_tests {
    use super::*;

    fn pipeline_from(json: &str) -> Vec<serde_json::Value> {
        serde_json::from_str(json).unwrap()
    }

    fn perms() -> HashMap<Survey, Vec<i32>> {
        HashMap::from([(Survey::Ztf, vec![1])])
    }

    fn has_sso_lookup(pipeline: &[Document]) -> bool {
        pipeline.iter().any(|s| {
            s.get_document("$lookup")
                .ok()
                .and_then(|l| l.get_str("as").ok())
                == Some("sso_history")
        })
    }

    #[tokio::test]
    async fn test_sso_history_lookup_injected_only_when_referenced() {
        let without = build_ztf_filter_pipeline(
            &pipeline_from(
                r#"[{"$match": {"candidate.drb": {"$gt": 0.5}}},
                 {"$project": {"objectId": 1, "candid": 1}}]"#,
            ),
            &perms(),
        )
        .await
        .unwrap();
        assert!(!has_sso_lookup(&without), "no lookup unless referenced");

        let with = build_ztf_filter_pipeline(
            &pipeline_from(
                r#"[{"$match": {"sso_history.0": {"$exists": true}}},
                 {"$project": {"objectId": 1, "candid": 1}}]"#,
            ),
            &perms(),
        )
        .await
        .unwrap();
        assert!(has_sso_lookup(&with), "referencing sso_history injects it");
    }

    // Joins on designation, never objectId: objectId is positional, so a moving
    // object gets a new one on nearly every detection.
    #[tokio::test]
    async fn test_sso_history_joins_on_designation() {
        let pipeline = build_ztf_filter_pipeline(
            &pipeline_from(
                r#"[{"$match": {"sso_history.0": {"$exists": true}}},
                 {"$project": {"objectId": 1, "candid": 1}}]"#,
            ),
            &perms(),
        )
        .await
        .unwrap();

        let lookup = pipeline
            .iter()
            .find_map(|s| s.get_document("$lookup").ok())
            .expect("lookup present");
        let rendered = format!("{:?}", lookup);

        // Outer key normalised, inner key raw: the raw field covers the whole
        // archive, so no backfill is needed for history to reach back.
        assert!(rendered.contains("properties.sso.designation"));
        assert!(rendered.contains("candidate.ssnamenr"));
        assert!(
            !rendered.contains("objectId"),
            "must not join on the positional objectId"
        );
        assert_eq!(lookup.get_str("from").unwrap(), "ZTF_alerts");
    }

    fn ceres() -> OrbitalElements {
        OrbitalElements {
            epoch_jd: 2_461_200.5,
            a: 2.7655526,
            e: 0.0796923,
            incl: 10.58803,
            node: 80.24863,
            peri: 73.29420,
            mean_anomaly: 274.41935,
        }
    }

    fn elements() -> HashMap<String, OrbitalElements> {
        HashMap::from([("1".to_string(), ceres())])
    }

    /// Keys for the designations a test entry may carry.
    fn keys() -> HashMap<String, String> {
        ["1", "(1)Ceres", "C/2026O1", "999999"]
            .iter()
            .filter_map(|d| Some((d.to_string(), normalize_ztf_ssnamenr(d)?)))
            .collect()
    }

    // The archive pre-dates enrichment writing geometry, so most history points
    // arrive bare and have to be derived from the elements.
    #[test]
    fn test_bare_history_point_gets_geometry() {
        let mut entry = doc! { "designation": "1", "jd": 2_461_272.5, "magpsf": 9.1 };
        assert!(fill_entry_geometry(&mut entry, &keys(), &elements()));

        // Matches the Horizons-validated values in sso_geometry.
        assert!((entry.get_f64("helio_dist").unwrap() - 2.706853).abs() < 1e-3);
        assert!((entry.get_f64("topo_dist").unwrap() - 3.168905).abs() < 1e-3);
        assert!((entry.get_f64("phase_angle").unwrap() - 17.6824).abs() < 0.01);
    }

    // A point enriched with geometry keeps it: recomputing would re-derive the
    // same number from the same elements, and overwriting hides the provenance.
    #[test]
    fn test_stored_geometry_is_not_overwritten() {
        let mut entry = doc! {
            "designation": "1", "jd": 2_461_272.5,
            "helio_dist": 1.0_f64, "topo_dist": 2.0_f64, "phase_angle": 3.0_f64,
        };
        assert!(!fill_entry_geometry(&mut entry, &keys(), &elements()));
        assert_eq!(entry.get_f64("helio_dist").unwrap(), 1.0);
    }

    // An object with no elements (a comet, say) must stay bare rather than pick
    // up someone else's geometry.
    #[test]
    fn test_unknown_object_stays_bare() {
        let mut entry = doc! { "designation": "C/2026O1", "jd": 2_461_272.5 };
        assert!(!fill_entry_geometry(&mut entry, &keys(), &elements()));
        assert!(entry.get_f64("helio_dist").is_err());

        let mut absent = doc! { "designation": "999999", "jd": 2_461_272.5 };
        assert!(!fill_entry_geometry(&mut absent, &keys(), &elements()));
        assert!(absent.get_f64("helio_dist").is_err());
    }

    // Geometry is a function of the point's own epoch; giving every point the
    // same value would defeat the scaling it exists for.
    #[test]
    fn test_each_point_gets_its_own_epoch() {
        let mut a = doc! { "designation": "1", "jd": 2_461_272.5 };
        let mut b = doc! { "designation": "1", "jd": 2_461_202.5 };
        fill_entry_geometry(&mut a, &keys(), &elements());
        fill_entry_geometry(&mut b, &keys(), &elements());
        assert_ne!(
            a.get_f64("helio_dist").unwrap(),
            b.get_f64("helio_dist").unwrap()
        );
    }

    // The designation is what carries across the two eras, so a history entry
    // written in the "(100)Hekate" era must still resolve.
    #[test]
    fn test_legacy_designation_form_resolves() {
        let history = doc! { "annotations": { "sso_history": [
            { "designation": "(1)Ceres", "jd": 2_461_272.5 },
        ] } };
        assert_eq!(
            sso_history_keys(&[history])
                .get("(1)Ceres")
                .map(String::as_str),
            Some("1")
        );

        let mut entry = doc! { "designation": "(1)Ceres", "jd": 2_461_272.5 };
        assert!(fill_entry_geometry(&mut entry, &keys(), &elements()));
    }

    // A point that already carries geometry needs no elements, and one whose
    // designation has no MPCORB form cannot be looked up.
    #[test]
    fn test_only_bare_resolvable_points_are_queried() {
        let history = doc! { "annotations": { "sso_history": [
            { "designation": "1", "jd": 2_461_272.5 },
            { "designation": "C/2026O1", "jd": 2_461_272.5 },
            {
                "designation": "2", "jd": 2_461_272.5,
                "helio_dist": 1.0_f64, "topo_dist": 2.0_f64, "phase_angle": 3.0_f64,
            },
        ] } };
        let keys = sso_history_keys(&[history]);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.get("1").map(String::as_str), Some("1"));
    }

    // A partially written block is completed rather than treated as done.
    #[test]
    fn test_partial_geometry_is_completed() {
        let mut entry = doc! { "designation": "1", "jd": 2_461_272.5, "helio_dist": 1.0_f64 };
        assert!(fill_entry_geometry(&mut entry, &keys(), &elements()));
        assert!(entry.get_f64("topo_dist").is_ok());
        assert!(entry.get_f64("phase_angle").is_ok());
    }

    // The window points are what a scaling statistic consumes, so each one has to
    // carry its own geometry -- the alert's own values describe only the last
    // point in the window.
    #[tokio::test]
    async fn test_sso_history_carries_per_point_geometry() {
        let pipeline = build_ztf_filter_pipeline(
            &pipeline_from(
                r#"[{"$match": {"sso_history.0": {"$exists": true}}},
                 {"$project": {"objectId": 1, "candid": 1}}]"#,
            ),
            &perms(),
        )
        .await
        .unwrap();

        let lookup = pipeline
            .iter()
            .find_map(|s| s.get_document("$lookup").ok())
            .expect("lookup present");
        let projection = lookup
            .get_array("pipeline")
            .expect("inner pipeline")
            .iter()
            .find_map(|s| s.as_document()?.get_document("$project").ok())
            .expect("projection present");

        for field in ["helio_dist", "topo_dist", "phase_angle"] {
            assert_eq!(
                projection.get_str(field).ok(),
                Some(format!("$properties.sso.{}", field).as_str()),
                "{field} must come from the history entry, not the outer alert"
            );
        }
        // Without jd the geometry cannot be matched to a photometry point.
        assert!(projection.contains_key("jd"));
        assert!(projection.contains_key("magpsf"));
    }

    // Alerts with no designation (old alerts, or no MPC match) are dropped before
    // the lookup rather than running it to build an empty array.
    #[tokio::test]
    async fn test_designation_match_precedes_the_lookup() {
        let pipeline = build_ztf_filter_pipeline(
            &pipeline_from(
                r#"[{"$match": {"sso_history.0": {"$exists": true}}},
                 {"$project": {"objectId": 1, "candid": 1}}]"#,
            ),
            &perms(),
        )
        .await
        .unwrap();

        let lookup_at = pipeline
            .iter()
            .position(|s| s.get_document("$lookup").is_ok())
            .expect("lookup present");
        let guard_at = pipeline
            .iter()
            .position(|s| {
                s.get_document("$match")
                    .ok()
                    .is_some_and(|m| m.contains_key("properties.sso.designation"))
            })
            .expect("designation guard present");

        assert!(guard_at < lookup_at, "guard must run before the lookup");
    }

    // A filter must not see programids it lacks permission for.
    #[tokio::test]
    async fn test_sso_history_respects_permissions() {
        let pipeline = build_ztf_filter_pipeline(
            &pipeline_from(
                r#"[{"$match": {"sso_history.0": {"$exists": true}}},
                 {"$project": {"objectId": 1, "candid": 1}}]"#,
            ),
            &HashMap::from([(Survey::Ztf, vec![1, 2])]),
        )
        .await
        .unwrap();

        let lookup = pipeline
            .iter()
            .find_map(|s| s.get_document("$lookup").ok())
            .expect("lookup present");
        let rendered = format!("{:?}", lookup);
        assert!(rendered.contains("candidate.programid"));
    }
}
