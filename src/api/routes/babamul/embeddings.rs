//! Babamul-facing similarity search over the AppleCiDEr fusion embeddings.
//!
//! Thin wrappers over the helpers in [`crate::api::routes::embeddings`]: the
//! internal scope rejects Babamul tokens, so clients need these routes. Both
//! are public (`BABAMUL_PUBLIC_ROUTES`), like the `/dashboard` stats.

use crate::api::routes::embeddings::{
    embeddings_count_response, similar_objects_response, SimilarObjectsQuery,
};
use crate::milvus::MilvusClient;

use actix_web::{get, post, web, HttpResponse};
use mongodb::Database;

/// Find the objects most similar to a given object.
///
/// Looks up the seed object's stored embedding, then runs a nearest-neighbor
/// search with it. The seed object itself is excluded from the results. Each
/// neighbor carries AppleCiDEr's classification of the alert its embedding came
/// from.
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
    db: web::Data<Database>,
    body: web::Json<SimilarObjectsQuery>,
) -> HttpResponse {
    similar_objects_response(&milvus, &db, &body.object_id, body.top_k).await
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
