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
use crate::milvus::MilvusClient;
use crate::utils::enums::ProgramId;

use actix_web::{get, post, web, HttpResponse};

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

/// Resolve the seed object's embedding and return its nearest neighbors.
pub(crate) async fn similar_objects_response(
    milvus: &web::Data<Option<MilvusClient>>,
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

    let neighbors: Vec<_> = hits
        .into_iter()
        .filter(|hit| hit.object_id != object_id)
        .take(top_k as usize)
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
/// match) is filtered out of the results.
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
    body: web::Json<SimilarObjectsQuery>,
) -> HttpResponse {
    similar_objects_response(&milvus, &body.object_id, body.top_k).await
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
