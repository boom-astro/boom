//! Endpoints backed by the Milvus vector store of CIDER fusion embeddings.
//!
//! Milvus is optional (see `milvus.enabled`), so these handlers receive the
//! client as `web::Data<Option<MilvusClient>>` and reply with a clear error
//! when it is switched off or was unreachable at startup. Every method on
//! [`MilvusClient`] takes `&mut self`, so each handler clones the shared client
//! (cheap — the tonic `Channel` shares one connection pool) to get a local
//! owned copy rather than serializing all requests through a lock.

use crate::api::models::response;
use crate::api::routes::babamul::{BabamulAcl, BabamulUser};
use crate::api::routes::users::User;
use crate::milvus::MilvusClient;
use crate::utils::enums::ProgramId;

use actix_web::{delete, get, post, web, HttpResponse};

const PUBLIC_ONLY: &[i32] = &[ProgramId::Public as i32];

/// Programs whose embeddings a Babamul user may see; `None` means all.
/// `has_acl` is also true for admins.
fn babamul_programids(user: &BabamulUser) -> Option<&'static [i32]> {
    if user.has_acl(BabamulAcl::ZtfPartnership) || user.has_acl(BabamulAcl::ZtfCaltech) {
        None
    } else {
        Some(PUBLIC_ONLY)
    }
}

/// Main-API users have no ACLs, so only admins see past the public program.
fn boom_programids(user: &User) -> Option<&'static [i32]> {
    if user.is_admin {
        None
    } else {
        Some(PUBLIC_ONLY)
    }
}

/// Request body for a similarity search seeded by a known object.
#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct SimilarObjectsQuery {
    /// The object whose stored embedding seeds the search.
    object_id: String,
    /// How many neighbors to return (default 10, capped at 100). The seed
    /// object itself is excluded, so at most `top_k` others come back.
    top_k: Option<i64>,
}

const DEFAULT_TOP_K: i64 = 10;
const MAX_TOP_K: i64 = 100;

/// Grab the shared client, cloned for `&mut` use, or the "not enabled" reply.
fn client_or_unavailable(
    milvus: &web::Data<Option<MilvusClient>>,
) -> Result<MilvusClient, HttpResponse> {
    match milvus.get_ref() {
        Some(client) => Ok(client.clone()),
        None => Err(response::internal_error(
            "Milvus is not enabled or was unavailable at startup",
        )),
    }
}

/// Find the objects most similar to a given object.
///
/// Looks up the seed object's stored embedding, then runs a nearest-neighbor
/// search with it. The seed object (which would otherwise be its own closest
/// match) is filtered out of the results. Non-admins see public objects only.
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
    current_user: web::ReqData<User>,
    body: web::Json<SimilarObjectsQuery>,
) -> HttpResponse {
    similar_objects(&milvus, &body, boom_programids(&current_user)).await
}

/// Find the objects most similar to a given object.
///
/// As the main API's `/similarity/objects`. Users without the
/// `ztf_partnership` or `ztf_caltech` ACL (or admin) see public objects only.
#[utoipa::path(
    post,
    path = "/similarity/objects",
    request_body = SimilarObjectsQuery,
    responses(
        (status = 200, description = "Nearest neighbors, best-first", body = serde_json::Value),
        (status = 401, description = "Not logged in"),
        (status = 404, description = "No embedding stored for the object, or not visible to the user"),
        (status = 500, description = "Milvus unavailable or search failed")
    ),
    tags=["Similarity"]
)]
#[post("/similarity/objects")]
pub async fn post_babamul_similar_objects(
    milvus: web::Data<Option<MilvusClient>>,
    current_user: Option<web::ReqData<BabamulUser>>,
    body: web::Json<SimilarObjectsQuery>,
) -> HttpResponse {
    let Some(current_user) = current_user else {
        return HttpResponse::Unauthorized().body("Unauthorized");
    };
    similar_objects(&milvus, &body, babamul_programids(&current_user)).await
}

/// The seed lookup is filtered too: a seed the user cannot see is reported
/// missing, and its vector is never used to search.
async fn similar_objects(
    milvus: &web::Data<Option<MilvusClient>>,
    body: &SimilarObjectsQuery,
    programids: Option<&[i32]>,
) -> HttpResponse {
    let mut client = match client_or_unavailable(milvus) {
        Ok(client) => client,
        Err(resp) => return resp,
    };

    let object_id = body.object_id.trim();
    if object_id.is_empty() {
        return response::bad_request("object_id must not be empty");
    }
    let top_k = body.top_k.unwrap_or(DEFAULT_TOP_K).clamp(1, MAX_TOP_K);

    // Fetch the seed object's embedding — the API caller supplies an id, not a
    // raw 384-float vector, so we resolve it here before searching.
    let seed = match client.get_embeddings(&[object_id], programids).await {
        Ok(rows) => rows,
        Err(e) => return response::internal_error(&format!("Error fetching embedding: {}", e)),
    };
    let Some(seed) = seed.into_iter().next() else {
        return response::not_found(&format!("No embedding stored for object {}", object_id));
    };

    // Ask for one extra result: the seed object matches itself perfectly and is
    // stripped out below, so this keeps up to `top_k` genuine neighbors.
    let hits = match client
        .search_embedding(&seed.embedding, top_k + 1, programids)
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
    let mut client = match client_or_unavailable(&milvus) {
        Ok(client) => client,
        Err(resp) => return resp,
    };

    match client.count().await {
        Ok(count) => response::ok("success", serde_json::json!({ "count": count })),
        Err(e) => response::internal_error(&format!("Error counting embeddings: {}", e)),
    }
}

/// Delete the stored embedding for an object. Admin only.
///
/// Operational use — e.g. purging a retracted or spurious object so it stops
/// polluting similarity results, or keeping Milvus in sync with Mongo.
#[utoipa::path(
    delete,
    path = "/embeddings/{object_id}",
    params(("object_id" = String, Path, description = "Object whose embedding to delete")),
    responses(
        (status = 200, description = "Deletion issued; reports how many rows were removed"),
        (status = 403, description = "Admins only"),
        (status = 500, description = "Milvus unavailable or delete failed")
    ),
    tags=["Similarity"]
)]
#[delete("/embeddings/{object_id}")]
pub async fn delete_object_embedding(
    milvus: web::Data<Option<MilvusClient>>,
    current_user: web::ReqData<User>,
    object_id: web::Path<String>,
) -> HttpResponse {
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }

    let mut client = match client_or_unavailable(&milvus) {
        Ok(client) => client,
        Err(resp) => return resp,
    };

    let object_id = object_id.into_inner();
    match client.delete_embeddings(&[&object_id]).await {
        Ok(deleted) => response::ok("success", serde_json::json!({ "deleted": deleted })),
        Err(e) => response::internal_error(&format!("Error deleting embedding: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn babamul_user(is_admin: bool, acls: Vec<BabamulAcl>) -> BabamulUser {
        BabamulUser {
            id: "bbml-1".to_string(),
            username: "pete".to_string(),
            email: "pete@example.org".to_string(),
            password_hash: String::new(),
            activation_code: None,
            is_activated: true,
            created_at: 0,
            kafka_credentials: Vec::new(),
            tokens: Vec::new(),
            password_reset_token_hash: None,
            password_reset_token_expires_at: None,
            password_last_changed_at: None,
            identities: Vec::new(),
            orcid_id: None,
            name: None,
            is_admin,
            acls,
        }
    }

    fn boom_user(is_admin: bool) -> User {
        User {
            id: "boom-1".to_string(),
            username: "pete".to_string(),
            email: "pete@example.org".to_string(),
            password: String::new(),
            is_admin,
            watchlist_access: Vec::new(),
        }
    }

    #[test]
    fn a_babamul_user_without_ztf_acls_sees_public_only() {
        assert_eq!(
            babamul_programids(&babamul_user(false, vec![])),
            Some(PUBLIC_ONLY)
        );
        let winter_only = babamul_user(false, vec![BabamulAcl::Winter]);
        assert_eq!(babamul_programids(&winter_only), Some(PUBLIC_ONLY));
    }

    #[test]
    fn a_ztf_acl_or_admin_sees_every_program() {
        for user in [
            babamul_user(true, vec![]),
            babamul_user(false, vec![BabamulAcl::ZtfPartnership]),
            babamul_user(false, vec![BabamulAcl::ZtfCaltech]),
            babamul_user(
                false,
                vec![BabamulAcl::ZtfPartnership, BabamulAcl::ZtfCaltech],
            ),
        ] {
            assert_eq!(babamul_programids(&user), None);
        }
    }

    #[test]
    fn only_main_api_admins_see_past_the_public_program() {
        assert_eq!(boom_programids(&boom_user(true)), None);
        assert_eq!(boom_programids(&boom_user(false)), Some(PUBLIC_ONLY));
    }
}
