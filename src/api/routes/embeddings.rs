//! Endpoints backed by the Milvus vector store of CIDER fusion embeddings.
//!
//! Milvus is optional (see `milvus.enabled`), so these handlers receive the
//! client as `web::Data<Option<MilvusClient>>` and reply with a clear error
//! when it is switched off or was unreachable at startup. Every method on
//! [`MilvusClient`] takes `&mut self`, so each handler clones the shared client
//! (cheap — the tonic `Channel` shares one connection pool) to get a local
//! owned copy rather than serializing all requests through a lock.
//!
//! The logic lives in the `pub(crate)` helpers so that
//! [`crate::api::routes::babamul::embeddings`] can share it.

use crate::api::models::response;
use crate::enrichment::models::applecider_postprocess::AppleCiderFusion;
use crate::milvus::{MilvusClient, SearchHit};
use crate::utils::enums::ProgramId;

use std::collections::HashMap;

use actix_web::{get, post, web, HttpResponse};
use futures::TryStreamExt;
use mongodb::{bson::doc, Database};

/// What every caller may search until per-user access is derived from ACLs.
const PUBLIC_ONLY: &[i32] = &[ProgramId::Public as i32];

/// Request body for a similarity search seeded by a known object.
#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct SimilarObjectsQuery {
    /// The object whose stored embedding seeds the search.
    pub object_id: String,
    /// How many neighbors to return (default 10, capped at 100). The seed
    /// object itself is excluded, so at most `top_k` others come back.
    pub top_k: Option<i64>,
}

const DEFAULT_TOP_K: i64 = 10;
const MAX_TOP_K: i64 = 100;

/// Only ZTF alerts are embedded, so every stored candid is a ZTF one.
const ZTF_ALERTS: &str = "ZTF_alerts";

/// One neighbor from a similarity search.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct SimilarObject {
    pub object_id: String,
    /// Similarity score under the collection's metric; higher is closer.
    pub score: f32,
    /// AppleCiDEr's classification of the alert the stored embedding came
    /// from. Null when that alert has none.
    pub applecider_fusion: Option<AppleCiderFusion>,
}

#[derive(serde::Deserialize)]
struct ClassifiedAlert {
    #[serde(rename = "_id")]
    candid: i64,
    classifications: Option<StoredClassifications>,
}

#[derive(serde::Deserialize)]
struct StoredClassifications {
    applecider_fusion: Option<AppleCiderFusion>,
}

/// Grab the shared client, cloned for `&mut` use, or the "not enabled" reply.
pub(crate) fn client_or_unavailable(
    milvus: &web::Data<Option<MilvusClient>>,
) -> Result<MilvusClient, HttpResponse> {
    match milvus.get_ref() {
        Some(client) => Ok(client.clone()),
        None => Err(response::internal_error(
            "Milvus is not enabled or was unavailable at startup",
        )),
    }
}

/// Map each hit's candid to the AppleCiDEr classification stored on its alert.
async fn applecider_classifications(
    db: &Database,
    hits: &[SearchHit],
) -> Result<HashMap<i64, AppleCiderFusion>, mongodb::error::Error> {
    let candids: Vec<i64> = hits.iter().map(|hit| hit.candid).collect();
    let alerts: Vec<ClassifiedAlert> = db
        .collection::<ClassifiedAlert>(ZTF_ALERTS)
        .find(doc! { "_id": { "$in": candids } })
        .projection(doc! { "classifications.applecider_fusion": 1 })
        .await?
        .try_collect()
        .await?;
    Ok(alerts
        .into_iter()
        .filter_map(|alert| Some((alert.candid, alert.classifications?.applecider_fusion?)))
        .collect())
}

/// Resolve the seed object's embedding and return its nearest neighbors.
pub(crate) async fn similar_objects_response(
    milvus: &web::Data<Option<MilvusClient>>,
    db: &Database,
    object_id: &str,
    top_k: Option<i64>,
) -> HttpResponse {
    let mut client = match client_or_unavailable(milvus) {
        Ok(client) => client,
        Err(resp) => return resp,
    };

    let object_id = object_id.trim();
    if object_id.is_empty() {
        return response::bad_request("object_id must not be empty");
    }
    let top_k = top_k.unwrap_or(DEFAULT_TOP_K).clamp(1, MAX_TOP_K);

    // Fetch the seed object's embedding — the API caller supplies an id, not a
    // raw 384-float vector, so we resolve it here before searching.
    // TODO: replace PUBLIC_ONLY, here and in the search below, with the
    // caller's allowed programids from their ACLs.
    let seed = match client.get_embeddings(&[object_id], PUBLIC_ONLY).await {
        Ok(rows) => rows,
        Err(e) => return response::internal_error(&format!("Error fetching embedding: {}", e)),
    };
    let Some(seed) = seed.into_iter().next() else {
        return response::not_found(&format!("No embedding stored for object {}", object_id));
    };

    // Ask for one extra result: the seed object matches itself perfectly and is
    // stripped out below, so this keeps up to `top_k` genuine neighbors.
    let hits = match client
        .search_embedding(&seed.embedding, top_k + 1, PUBLIC_ONLY)
        .await
    {
        Ok(hits) => hits,
        Err(e) => return response::internal_error(&format!("Error running search: {}", e)),
    };

    let hits: Vec<_> = hits
        .into_iter()
        .filter(|hit| hit.object_id != object_id)
        .take(top_k as usize)
        .collect();

    // The classification only annotates the neighbors, so a failed lookup
    // drops it rather than the search results.
    let mut classes = applecider_classifications(db, &hits)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Error fetching AppleCiDEr classifications: {}", e);
            HashMap::new()
        });

    let neighbors: Vec<_> = hits
        .into_iter()
        .map(|hit| SimilarObject {
            applecider_fusion: classes.remove(&hit.candid),
            object_id: hit.object_id,
            score: hit.score,
        })
        .collect();

    response::ok_ser("success", neighbors)
}

/// Count the stored embeddings.
pub(crate) async fn embeddings_count_response(
    milvus: &web::Data<Option<MilvusClient>>,
) -> HttpResponse {
    let mut client = match client_or_unavailable(milvus) {
        Ok(client) => client,
        Err(resp) => return resp,
    };

    match client.count().await {
        Ok(count) => response::ok("success", serde_json::json!({ "count": count })),
        Err(e) => response::internal_error(&format!("Error counting embeddings: {}", e)),
    }
}

/// Find the objects most similar to a given object.
///
/// Looks up the seed object's stored embedding, then runs a nearest-neighbor
/// search with it. The seed object (which would otherwise be its own closest
/// match) is filtered out of the results. Each neighbor carries AppleCiDEr's
/// classification of the alert its embedding came from.
#[utoipa::path(
    post,
    path = "/similarity/objects",
    request_body = SimilarObjectsQuery,
    responses(
        (status = 200, description = "Nearest neighbors, best-first", body = serde_json::Value),
        (status = 404, description = "No embedding stored for the object"),
        (status = 500, description = "Milvus unavailable or search failed")
    ),
    tags=["Similarity"]
)]
#[post("/similarity/objects")]
pub async fn post_similar_objects(
    milvus: web::Data<Option<MilvusClient>>,
    db: web::Data<Database>,
    body: web::Json<SimilarObjectsQuery>,
) -> HttpResponse {
    similar_objects_response(&milvus, &db, &body.object_id, body.top_k).await
}

/// Get the number of embeddings currently stored.
#[utoipa::path(
    get,
    path = "/embeddings/count",
    responses(
        (status = 200, description = "Embedding count retrieved"),
        (status = 500, description = "Milvus unavailable or count failed")
    ),
    tags=["Similarity"]
)]
#[get("/embeddings/count")]
pub async fn get_embeddings_count(milvus: web::Data<Option<MilvusClient>>) -> HttpResponse {
    embeddings_count_response(&milvus).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrichment::models::applecider_postprocess::{derive, AppleCiderModalities};
    use crate::utils::db::mongify;
    use mongodb::bson::from_document;

    const MODALITIES: AppleCiderModalities = AppleCiderModalities {
        photometry: true,
        science_stamp: true,
        reference_stamp: true,
        metadata: true,
        redshift: false,
    };

    /// Enrichment writes the summary with `mongify`, so the lookup must read
    /// that shape back.
    #[test]
    fn reads_the_classification_enrichment_stores() {
        let alpha = [20.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let (fusion, _) = derive(&alpha, 10, MODALITIES).expect("8 alphas derive");
        let stored = doc! {
            "_id": 1234567890123_i64,
            "classifications": { "acai_h": 0.1, "applecider_fusion": mongify(&fusion) },
        };

        let alert: ClassifiedAlert = from_document(stored).expect("must deserialize");
        let read = alert.classifications.unwrap().applecider_fusion.unwrap();
        assert_eq!(alert.candid, 1234567890123);
        assert_eq!(read.decision_label, fusion.decision_label);
        assert_eq!(read.abstain_completely, fusion.abstain_completely);
        assert_eq!(
            read.class_probabilities.agn_like,
            fusion.class_probabilities.agn_like
        );
    }

    #[test]
    fn an_alert_without_a_classification_reads_as_none() {
        let alert: ClassifiedAlert = from_document(doc! { "_id": 1_i64 }).unwrap();
        assert!(alert.classifications.is_none());

        let alert: ClassifiedAlert =
            from_document(doc! { "_id": 1_i64, "classifications": { "acai_h": 0.1 } }).unwrap();
        assert!(alert.classifications.unwrap().applecider_fusion.is_none());
    }
}
