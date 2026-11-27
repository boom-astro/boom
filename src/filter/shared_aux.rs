//! Runs a batch's filters as one aggregation that joins the aux document once.

use futures::stream::{self, StreamExt, TryStreamExt};
use mongodb::bson::{doc, Bson, Document};
use tracing::instrument;

use crate::filter::{run_filter, FilterError, LoadedFilter};
use crate::utils::o11y::logging::{log_error, WARN};

/// Filters that cannot share the lookup are queried this many at a time.
const CONCURRENT_FILTER_QUERIES: usize = 8;

/// The survey's own aux `$lookup`, projected to what `aux_add_fields` reads:
/// each of its keys other than `aux` is computed from the aux field of that name.
pub(crate) fn aux_lookup(from: &str, aux_add_fields: &Document) -> Document {
    let projection: Document = aux_add_fields
        .keys()
        .filter(|key| key.as_str() != "aux")
        .map(|key| (key.clone(), Bson::Int32(1)))
        .collect();
    doc! {
        "$lookup": {
            "from": from,
            "localField": "objectId",
            "foreignField": "_id",
            "as": "aux",
            "pipeline": [{ "$project": projection }],
        }
    }
}

fn is_aux_lookup(stage: &Document) -> bool {
    stage.get_document("$lookup").is_ok_and(|lookup| {
        lookup.get_str("as").is_ok_and(|name| name == "aux")
            && lookup
                .get_str("localField")
                .is_ok_and(|field| field == "objectId")
    })
}

/// A filter pipeline split around its aux lookup.
struct SharedAuxBranch<'a> {
    /// The candid `$match` and the base `$project`.
    head: &'a [Document],
    /// The lookup without its projection.
    lookup: Document,
    /// The aux fields the filter reads.
    aux_fields: Document,
    /// Conditions of the `$match` stages ahead of the lookup.
    pre_lookup: Vec<Document>,
    /// Every stage after the head except the lookup.
    stages: Vec<Document>,
}

/// The filter as a branch of a shared lookup, if it can be one. Only `$match`
/// stages may precede its lookup: once the lookup runs ahead of them, any other
/// stage could drop `aux` or rewrite the `objectId` it joins on.
fn shared_aux_branch(pipeline: &[Document]) -> Option<SharedAuxBranch<'_>> {
    let at = 2 + pipeline.get(2..)?.iter().position(is_aux_lookup)?;
    let pre_lookup = pipeline[2..at]
        .iter()
        .map(|stage| stage.get_document("$match").ok().cloned())
        .collect::<Option<Vec<_>>>()?;
    let mut lookup = pipeline[at].get_document("$lookup").ok()?.clone();
    let aux_fields = match lookup.remove("pipeline")? {
        Bson::Array(inner) => match inner.as_slice() {
            [Bson::Document(stage)] => stage.get_document("$project").ok()?.clone(),
            _ => return None,
        },
        _ => return None,
    };
    let stages: Vec<Document> = pipeline[2..at]
        .iter()
        .chain(&pipeline[at + 1..])
        .cloned()
        .collect();
    // `$facet` cannot nest.
    if stages.iter().any(|stage| stage.contains_key("$facet")) {
        return None;
    }
    Some(SharedAuxBranch {
        head: &pipeline[..2],
        lookup,
        aux_fields,
        pre_lookup,
        stages,
    })
}

/// Joins the aux document once, onto the alerts some branch's pre-lookup cuts
/// admit, and runs each branch's own stages under `$facet`. All branches must
/// share their head and lookup.
fn shared_aux_pipeline(
    candids: &[i64],
    branches: &[&SharedAuxBranch],
) -> Result<Vec<Document>, FilterError> {
    let mut pipeline = branches[0].head.to_vec();
    pipeline[0]
        .get_document_mut("$match")?
        .insert("_id", doc! { "$in": candids });

    // A branch with no cut ahead of its lookup needs every alert joined.
    if branches.iter().all(|branch| !branch.pre_lookup.is_empty()) {
        let mut conditions: Vec<Document> = Vec::new();
        for branch in branches {
            let condition = doc! { "$and": branch.pre_lookup.clone() };
            if !conditions.contains(&condition) {
                conditions.push(condition);
            }
        }
        pipeline.push(doc! { "$match": { "$or": conditions } });
    }

    let mut aux_fields = Document::new();
    for branch in branches {
        for (field, value) in &branch.aux_fields {
            aux_fields.insert(field.clone(), value.clone());
        }
    }
    let mut lookup = branches[0].lookup.clone();
    lookup.insert("pipeline", vec![doc! { "$project": aux_fields }]);
    pipeline.push(doc! { "$lookup": lookup });

    let facet: Document = branches
        .iter()
        .enumerate()
        .map(|(i, branch)| (format!("f{i}"), Bson::from(branch.stages.clone())))
        .collect();
    pipeline.push(doc! { "$facet": facet });
    Ok(pipeline)
}

/// Each branch's output, in branch order.
#[instrument(skip_all, fields(n_filters = branches.len()))]
async fn run_shared_aux_filters(
    candids: &[i64],
    branches: &[&SharedAuxBranch<'_>],
    alert_collection: &mongodb::Collection<Document>,
) -> Result<Vec<Vec<Document>>, FilterError> {
    let pipeline = shared_aux_pipeline(candids, branches)?;
    let mut cursor = alert_collection.aggregate(pipeline).await?;
    let mut facets = cursor.next().await.transpose()?.unwrap_or_default();
    let malformed = |i: usize| {
        FilterError::FilterExecutionError(format!("$facet returned no documents for f{i}"))
    };
    (0..branches.len())
        .map(|i| match facets.remove(format!("f{i}")) {
            Some(Bson::Array(documents)) => documents
                .into_iter()
                .map(|document| match document {
                    Bson::Document(document) => Ok(document),
                    _ => Err(malformed(i)),
                })
                .collect(),
            _ => Err(malformed(i)),
        })
        .collect()
}

/// Runs `filters` on `candids` and returns each one's output, in filter order.
///
/// Filters whose pipelines agree up to their aux lookup run as one aggregation;
/// see `shared_aux_pipeline`. If that aggregation fails (its single output
/// document is capped at 16 MB), its filters run on their own.
#[instrument(skip_all, fields(n_filters = filters.len()))]
pub(crate) async fn run_filters(
    candids: &[i64],
    filters: &[&LoadedFilter],
    alert_collection: &mongodb::Collection<Document>,
) -> Result<Vec<Vec<Document>>, FilterError> {
    if candids.is_empty() {
        return Ok(vec![Vec::new(); filters.len()]);
    }
    let mut outputs: Vec<Option<Vec<Document>>> = vec![None; filters.len()];

    let branches: Vec<Option<SharedAuxBranch>> = filters
        .iter()
        .map(|filter| shared_aux_branch(&filter.pipeline))
        .collect();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (i, branch) in branches.iter().enumerate() {
        let Some(branch) = branch else {
            continue;
        };
        let joins_alike = |group: &&mut Vec<usize>| {
            branches[group[0]]
                .as_ref()
                .is_some_and(|first| first.head == branch.head && first.lookup == branch.lookup)
        };
        match groups.iter_mut().find(joins_alike) {
            Some(group) => group.push(i),
            None => groups.push(vec![i]),
        }
    }

    for group in groups.into_iter().filter(|group| group.len() > 1) {
        let shared: Vec<&SharedAuxBranch> =
            group.iter().filter_map(|&i| branches[i].as_ref()).collect();
        match run_shared_aux_filters(candids, &shared, alert_collection).await {
            Ok(results) => {
                for (i, documents) in group.into_iter().zip(results) {
                    outputs[i] = Some(documents);
                }
            }
            Err(error) => log_error!(
                WARN,
                error,
                "shared aux filter query failed; running its {} filters one by one",
                group.len()
            ),
        }
    }

    let alone: Vec<usize> = (0..filters.len())
        .filter(|&i| outputs[i].is_none())
        .collect();
    let results: Vec<(usize, Vec<Document>)> = stream::iter(alone)
        .map(|i| async move {
            let filter = filters[i];
            run_filter(
                candids,
                &filter.id,
                filter.pipeline.clone(),
                alert_collection,
            )
            .await
            .map(|documents| (i, documents))
        })
        .buffer_unordered(CONCURRENT_FILTER_QUERIES)
        .try_collect()
        .await?;
    for (i, documents) in results {
        outputs[i] = Some(documents);
    }

    Ok(outputs.into_iter().map(Option::unwrap_or_default).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::build_ztf_filter_pipeline;
    use crate::utils::enums::Survey;
    use std::collections::HashMap;

    async fn ztf_pipeline(filter: serde_json::Value) -> Vec<Document> {
        let filter: Vec<serde_json::Value> = serde_json::from_value(filter).unwrap();
        let permissions = HashMap::from([(Survey::Ztf, vec![1])]);
        build_ztf_filter_pipeline(&filter, &permissions)
            .await
            .unwrap()
    }

    fn stage_names(stages: &[Document]) -> Vec<&str> {
        stages
            .iter()
            .filter_map(|stage| stage.keys().next().map(String::as_str))
            .collect()
    }

    #[tokio::test]
    async fn test_branch_lifts_the_matches_ahead_of_the_lookup() {
        let pipeline = ztf_pipeline(serde_json::json!([
            {"$match": {"candidate.drb": {"$gt": 0.5}}},
            {"$match": {"prv_candidates.1": {"$exists": true}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        let branch = shared_aux_branch(&pipeline).expect("shares the lookup");

        assert_eq!(branch.aux_fields, doc! { "prv_candidates": 1 });
        assert_eq!(
            branch.pre_lookup,
            vec![doc! { "candidate.drb": { "$gt": 0.5 } }]
        );
        assert!(!branch.lookup.contains_key("pipeline"));
        // The branch keeps its own cuts, so its output does not depend on the others.
        assert_eq!(
            stage_names(&branch.stages),
            ["$match", "$addFields", "$match", "$project"]
        );
    }

    #[tokio::test]
    async fn test_filters_without_a_shareable_lookup_run_alone() {
        // No aux field read, so no lookup.
        let no_lookup = ztf_pipeline(serde_json::json!([
            {"$match": {"candidate.drb": {"$gt": 0.5}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        assert!(shared_aux_branch(&no_lookup).is_none());

        // A stage other than `$match` ahead of the lookup.
        let reshaped = ztf_pipeline(serde_json::json!([
            {"$addFields": {"bright": {"$lt": ["$candidate.magpsf", 18]}}},
            {"$match": {"bright": true}},
            {"$match": {"prv_candidates.1": {"$exists": true}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        assert!(shared_aux_branch(&reshaped).is_none());

        let nested_facet = ztf_pipeline(serde_json::json!([
            {"$match": {"prv_candidates.1": {"$exists": true}}},
            {"$facet": {"all": [{"$match": {}}]}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        assert!(shared_aux_branch(&nested_facet).is_none());
    }

    #[tokio::test]
    async fn test_shared_pipeline_joins_once_for_every_branch() {
        let a = ztf_pipeline(serde_json::json!([
            {"$match": {"candidate.drb": {"$gt": 0.5}}},
            {"$match": {"prv_candidates.1": {"$exists": true}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        let b = ztf_pipeline(serde_json::json!([
            {"$match": {"candidate.drb": {"$gt": 0.5}}},
            {"$match": {"cross_matches.AllWISE.0": {"$exists": true}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        let c = ztf_pipeline(serde_json::json!([
            {"$match": {"candidate.magpsf": {"$lt": 18}}},
            {"$match": {"fp_hists.0": {"$exists": true}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        let branches: Vec<SharedAuxBranch> = [&a, &b, &c]
            .into_iter()
            .map(|p| shared_aux_branch(p).unwrap())
            .collect();
        let refs: Vec<&SharedAuxBranch> = branches.iter().collect();
        let pipeline = shared_aux_pipeline(&[1, 2], &refs).unwrap();

        assert_eq!(
            stage_names(&pipeline),
            ["$match", "$project", "$match", "$lookup", "$facet"]
        );
        assert_eq!(
            pipeline[0],
            doc! { "$match": { "_id": { "$in": [1_i64, 2_i64] } } }
        );
        // a and b cut alike, so the union has two conditions.
        assert_eq!(
            pipeline[2],
            doc! { "$match": { "$or": [
                { "$and": [{ "candidate.drb": { "$gt": 0.5 } }] },
                { "$and": [{ "candidate.magpsf": { "$lt": 18_i64 } }] },
            ] } }
        );
        let lookup = pipeline[3].get_document("$lookup").unwrap();
        assert_eq!(
            lookup.get_array("pipeline").unwrap()[0],
            Bson::Document(doc! { "$project": {
                "prv_candidates": 1, "cross_matches": 1, "fp_hists": 1,
            } })
        );
        let facet = pipeline[4].get_document("$facet").unwrap();
        assert_eq!(facet.keys().collect::<Vec<_>>(), ["f0", "f1", "f2"]);
    }

    #[tokio::test]
    async fn test_a_branch_without_a_pre_lookup_cut_joins_every_alert() {
        let cut = ztf_pipeline(serde_json::json!([
            {"$match": {"candidate.drb": {"$gt": 0.5}}},
            {"$match": {"prv_candidates.1": {"$exists": true}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        let uncut = ztf_pipeline(serde_json::json!([
            {"$match": {"prv_candidates.1": {"$exists": true}}},
            {"$project": {"objectId": 1}},
        ]))
        .await;
        let branches = [
            shared_aux_branch(&cut).unwrap(),
            shared_aux_branch(&uncut).unwrap(),
        ];
        let refs: Vec<&SharedAuxBranch> = branches.iter().collect();
        let pipeline = shared_aux_pipeline(&[1], &refs).unwrap();
        assert_eq!(
            stage_names(&pipeline),
            ["$match", "$project", "$lookup", "$facet"]
        );
    }
}
