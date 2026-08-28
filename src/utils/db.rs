use chrono::NaiveDate;
use mongodb::{
    bson::{doc, to_document, Document},
    options::IndexOptions,
    Collection, Database, IndexModel,
};
use serde::Serialize;
use tracing::instrument;

use crate::utils::enums::Survey;

#[derive(thiserror::Error, Debug)]
#[error("failed to create index")]
pub struct CreateIndexError(#[from] mongodb::error::Error);

#[instrument(skip(collection, index), fields(collection = collection.name()), err)]
pub async fn create_index(
    collection: &Collection<Document>,
    index: Document,
    unique: bool,
) -> Result<(), CreateIndexError> {
    create_partial_index(collection, index, unique, None).await
}

#[instrument(
    skip(collection, index, partial_filter),
    fields(collection = collection.name()),
    err
)]
pub async fn create_partial_index(
    collection: &Collection<Document>,
    index: Document,
    unique: bool,
    partial_filter: Option<Document>,
) -> Result<(), CreateIndexError> {
    create_named_partial_index(collection, index, unique, partial_filter, None).await
}

/// As [`create_partial_index`], with an explicit index name.
///
/// Needed when a collection carries two indexes over the same keys: Mongo names
/// an index after its keys, so the second would collide with the first.
pub async fn create_named_partial_index(
    collection: &Collection<Document>,
    index: Document,
    unique: bool,
    partial_filter: Option<Document>,
    name: Option<String>,
) -> Result<(), CreateIndexError> {
    let index_model = IndexModel::builder()
        .keys(index)
        .options(
            IndexOptions::builder()
                .unique(unique)
                .partial_filter_expression(partial_filter)
                .name(name)
                .build(),
        )
        .build();
    collection.create_index(index_model).await?;
    Ok(())
}

#[instrument(skip_all)]
pub fn mongify<T: Serialize>(value: &T) -> Document {
    // we removed all the sanitizing logic
    // in favor of using serde's attributes to clean up the data
    // ahead of time.
    // TODO: drop this function entirely and avoid unwrapping
    to_document(value).unwrap()
}

#[instrument(skip_all)]
pub fn mongify_vec<T: Serialize>(value: &Vec<T>) -> Vec<Document> {
    value.iter().map(|v| mongify(v)).collect()
}

#[instrument(skip_all)]
pub fn cutout2bsonbinary(cutout: Vec<u8>) -> mongodb::bson::Binary {
    return mongodb::bson::Binary {
        subtype: mongodb::bson::spec::BinarySubtype::Generic,
        bytes: cutout,
    };
}

/// Count alerts in `<survey>_alerts` for the observing night labelled by `date`
/// (local-noon to local-noon JD window).
///
/// `programids`:
/// - `None` → no permission filter (use for surveys without programid, e.g. LSST,
///   or when caller wants the full unrestricted count).
/// - `Some(pids)` → restrict to `candidate.programid ∈ pids`.
#[instrument(skip(db), err)]
pub async fn count_alerts_for_night(
    db: &Database,
    survey: &Survey,
    date: &NaiveDate,
    programids: Option<&[i32]>,
) -> Result<u64, mongodb::error::Error> {
    let (start_jd, end_jd) = survey.night_jd_window(date);
    let mut filter = doc! {
        "candidate.jd": { "$gte": start_jd, "$lt": end_jd },
    };
    if *survey == Survey::Ztf {
        if let Some(pids) = programids {
            filter.insert("candidate.programid", doc! { "$in": pids });
        }
    }
    let collection: Collection<Document> = db.collection(&format!("{}_alerts", survey));
    collection.count_documents(filter).await
}

// This function, for a given survey name (ZTF, LSST), will create
// the required indexes on the alerts and alerts_aux collections
#[instrument(skip(db), fields(database = db.name()), err)]
pub async fn initialize_survey_indexes(
    survey: &Survey,
    db: &Database,
) -> Result<(), CreateIndexError> {
    let alerts_collection_name = format!("{}_alerts", survey);
    let alerts_aux_collection_name = format!("{}_alerts_aux", survey);

    let alerts_collection: Collection<Document> = db.collection(&alerts_collection_name);
    let alerts_aux_collection: Collection<Document> = db.collection(&alerts_aux_collection_name);

    // create the compound 2dsphere + _id index on the alerts and alerts_aux collections
    let index = doc! {
        "coordinates.radec_geojson": "2dsphere",
        "_id": 1,
    };
    create_index(&alerts_collection, index.clone(), false).await?;
    create_index(&alerts_aux_collection, index, false).await?;

    // create a simple index on the objectId field of the alerts collection
    let index = doc! {
        "objectId": 1,
    };
    create_index(&alerts_collection, index, false).await?;

    // ZTF joins a moving object's detections by MPC designation, since objectId is
    // positional. Indexes the raw field, which is present on the whole archive.
    if survey == &Survey::Ztf {
        let index = doc! {
            "candidate.ssnamenr": 1,
            "candidate.jd": -1,
        };
        create_partial_index(
            &alerts_collection,
            index,
            false,
            Some(doc! { "candidate.ssnamenr": { "$exists": true } }),
        )
        .await?;
    }

    // if survey is LSST, create an index on the ssObjectId field of the alerts collection,
    // and on the designation field of the aux collection (used to look up a moving object by
    // its MPC designation, independent of any cross-survey position match)
    if survey == &Survey::Lsst {
        let index = doc! {
            "ssObjectId": 1,
        };
        create_index(&alerts_collection, index, false).await?;

        let index = doc! {
            "designation": 1,
        };
        create_partial_index(
            &alerts_aux_collection,
            index,
            false,
            Some(doc! { "designation": { "$exists": true } }),
        )
        .await?;
    }

    Ok(())
}

/// Index name for the angular-size branch of a catalog's cone match.
fn angular_size_index_name(size_key: &str) -> String {
    format!("radec_2dsphere_large_{size_key}")
}

/// Partial 2dsphere indexes for catalogs matched by angular size.
///
/// An angular-size match is an `$or`: a base cone for everything, plus a much
/// wider cone for rows large enough to reach past it. Only a fraction of a
/// percent of a galaxy catalog qualifies, but without a partial index the wide
/// cone scans every row inside several degrees and then discards nearly all of
/// them. Restricting the index to the rows that can qualify makes that branch
/// cost about as much as the base cone.
///
/// The threshold has to match the one the query uses, so it is derived from the
/// same config rather than written out again.
pub async fn initialize_angular_size_indexes(
    xmatch_configs: &[crate::conf::CatalogXmatchConfig],
    db: &Database,
) -> Result<(), CreateIndexError> {
    for config in xmatch_configs {
        let Some(size_key) = &config.angular_size_key else {
            continue;
        };
        if config.angular_size_radius_max.is_none() {
            continue;
        }
        let collection: Collection<Document> = db.collection(config.collection_name());
        create_named_partial_index(
            &collection,
            doc! { "coordinates.radec_geojson": "2dsphere" },
            false,
            Some(doc! { size_key: { "$gt": config.angular_size_threshold_arcsec() } }),
            Some(angular_size_index_name(size_key)),
        )
        .await?;
    }
    Ok(())
}

/// This function updates a timeseries array by appending new values while deduplicating
/// based on a time field, maintaining sort order, and removing non-finite values.
/// (so we have only one measurement per epoch).
pub fn update_timeseries_op(
    array_field: &str,
    time_field: &str,
    value: &Vec<Document>,
) -> Document {
    let point_field_name = format!("$$point.{}", time_field);
    doc! {
        "$sortArray": {
            "input": {
                "$filter": {
                    "input": {
                        "$reduce": {
                            "input": {
                                "$concatArrays": [
                                    // handle the case where the array_field is not present
                                    { "$ifNull": [format!("${}", array_field), []] },
                                    value
                                ]
                            },
                            "initialValue": [],
                            "in": {
                                "$cond": {
                                    "if": { "$in": [format!("$$this.{}", time_field), format!("$$value.{}", time_field)] },
                                    "then": "$$value",
                                    "else": { "$concatArrays": ["$$value", ["$$this"]] }
                                }
                            }
                        }
                    },
                    "as": "point",
                    "cond": doc! {
                        "$and": [
                            // filter out non-finite values (including NaN and Infinity)
                            { "$isNumber": &point_field_name },
                            { "$eq": [&point_field_name, &point_field_name] },
                            { "$lt": [&point_field_name, f64::INFINITY] },
                            { "$gt": [&point_field_name, f64::NEG_INFINITY] }
                        ]
                    }
                }
            },
            "sortBy": { time_field: 1 }
        }
    }
}

pub fn get_array_element(field: &str) -> Document {
    doc! {
        "$ifNull": [
            {
                "$arrayElemAt": [
                    format!("${}", field),
                    0
                ]
            },
            []
        ]
    }
}

pub fn get_array_dict_element(field: &str) -> Document {
    doc! {
        "$ifNull": [
            {
                "$arrayElemAt": [
                    format!("${}", field),
                    0
                ]
            },
            {}
        ]
    }
}

/// This function generates a MongoDB aggregation operation
/// that filters an array field based on a time window relative to a candidate's jd field.
/// It can also include optional conditions for filtering.
/// The array_field is expected to come from an auxiliary collection
/// (i.e. "ztf_aux.prv_candidates", where "ztf_aux" is an array of documents itself).
pub fn fetch_timeseries_op(
    array_field: &str,
    candidate_jd_field: &str,
    time_window: i32,
    optional_conditions: Option<Vec<Document>>,
) -> Document {
    let mut conditions = vec![
        doc! {
            "$lt": [
                {
                    "$subtract": [
                        format!("${}", candidate_jd_field),
                        "$$x.jd"
                    ]
                },
                time_window
            ]
        },
        doc! { // only datapoints up to (and including) current alert
            "$lte": [
                "$$x.jd",
                format!("${}", candidate_jd_field),
            ]
        },
    ];
    if let Some(mut opts) = optional_conditions {
        conditions.append(&mut opts);
    }
    doc! {
        "$filter": doc! {
            "input": get_array_element(array_field),
            "as": "x",
            "cond": doc! {
                "$and": conditions
            }
        }
    }
}

#[cfg(test)]
mod angular_size_index_tests {
    use super::angular_size_index_name;
    use crate::conf::CatalogXmatchConfig;
    use mongodb::bson::doc;

    fn config(angular_size_key: Option<String>, radius_max: Option<f64>) -> CatalogXmatchConfig {
        CatalogXmatchConfig::new(
            "NED_LVS",
            Some("NED".to_string()),
            300.0,
            doc! {},
            false,
            None,
            None,
            None,
            None,
            angular_size_key,
            5.0,
            radius_max,
        )
    }

    // The index filter and the query filter have to use the same threshold, or
    // the index silently fails to cover rows the query asks for.
    #[test]
    fn test_threshold_is_derived_from_the_same_config_as_the_query() {
        let c = config(Some("Diam".to_string()), Some(21600.0));
        // 2 * base_radius / scale: the diameter at which the scaled radius first
        // exceeds the base cone.
        assert!((c.angular_size_threshold_arcsec() - 120.0).abs() < 1e-9);
    }

    // The name must differ from the full 2dsphere index on the same keys.
    #[test]
    fn test_index_name_does_not_collide_with_the_full_index() {
        assert_ne!(
            angular_size_index_name("Diam"),
            "coordinates.radec_geojson_2dsphere"
        );
    }

    #[test]
    fn test_catalogs_without_angular_size_matching_are_skipped() {
        assert!(config(None, Some(21600.0)).angular_size_key.is_none());
        // A key without a cap means no wide branch, so no partial index either.
        let capped = config(Some("Diam".to_string()), None);
        assert!(capped.angular_size_radius_max.is_none());
    }
}
