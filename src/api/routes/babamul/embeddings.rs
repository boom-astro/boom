//! Babamul-facing similarity search over the AppleCiDEr fusion embeddings.
//!
//! These mirror [`crate::api::routes::embeddings`] onto the `/babamul` scope.
//! They exist separately because the two surfaces authenticate differently:
//! the internal API uses `auth_middleware` and [`crate::api::routes::users::User`],
//! while Babamul uses `babamul_auth_middleware` and [`BabamulUser`]. A Babamul
//! token is not accepted by the internal scope, so the web frontend could not
//! reach the similarity endpoints at all without these.
//!
//! The search logic itself is not duplicated — it lives in the shared helpers
//! in the internal module and these handlers only do auth and argument
//! plumbing.
//!
//! Search and count are public (listed in `BABAMUL_PUBLIC_ROUTES`), matching
//! the stats endpoints behind `/dashboard`: they are read-only and the web page
//! is reachable without signing in.

use crate::api::routes::embeddings::{
    embeddings_count_response, similar_objects_response, SimilarObjectsQuery,
};
use crate::milvus::MilvusClient;

use actix_web::{get, post, web, HttpResponse};

/// Find the objects most similar to a given object.
///
/// Looks up the seed object's stored embedding, then runs a nearest-neighbor
/// search with it. The seed object itself is excluded from the results.
#[utoipa::path(
    post,
    path = "/babamul/similarity/objects",
    request_body = SimilarObjectsQuery,
    responses(
        (status = 200, description = "Nearest neighbors, best-first", body = serde_json::Value),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "No embedding stored for the object"),
        (status = 500, description = "Milvus unavailable or search failed")
    ),
    tags=["Babamul"]
)]
#[post("/similarity/objects")]
pub async fn post_babamul_similar_objects(
    milvus: web::Data<Option<MilvusClient>>,
    body: web::Json<SimilarObjectsQuery>,
) -> HttpResponse {
    similar_objects_response(&milvus, &body.object_id, body.top_k).await
}

/// Get the number of embeddings currently stored.
///
/// Doubles as a health signal: enrichment pauses embedding uploads while Milvus
/// is unreachable, and a count that stops growing is the visible symptom.
#[utoipa::path(
    get,
    path = "/babamul/embeddings/count",
    responses(
        (status = 200, description = "Embedding count retrieved"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Milvus unavailable or count failed")
    ),
    tags=["Babamul"]
)]
#[get("/embeddings/count")]
pub async fn get_babamul_embeddings_count(milvus: web::Data<Option<MilvusClient>>) -> HttpResponse {
    embeddings_count_response(&milvus).await
}
