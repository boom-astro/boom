use crate::api::models::response;
use crate::api::routes::babamul::{BabamulAcl, BabamulUser};
use actix_web::{get, patch, web, HttpResponse};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

const MAX_LIMIT: u64 = 200;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BabamulAdminUser {
    pub id: String,
    pub username: String,
    pub email: String,
    pub name: Option<String>,
    pub created_at: i64,
    pub is_activated: bool,
    pub is_admin: bool,
    pub acls: Vec<BabamulAcl>,
}

impl From<BabamulUser> for BabamulAdminUser {
    fn from(user: BabamulUser) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            name: user.name,
            created_at: user.created_at,
            is_activated: user.is_activated,
            is_admin: user.is_admin,
            acls: user.acls,
        }
    }
}

#[derive(Deserialize, IntoParams)]
pub struct AdminUsersQuery {
    pub search: Option<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Serialize, ToSchema)]
pub struct AdminUsersResponse {
    pub users: Vec<BabamulAdminUser>,
    pub total: u64,
    pub available_acls: Vec<BabamulAcl>,
}

#[derive(Deserialize, ToSchema)]
pub struct AdminUserPatch {
    pub is_admin: Option<bool>,
    pub acls: Option<Vec<BabamulAcl>>,
}

fn require_admin(
    current_user: Option<web::ReqData<BabamulUser>>,
) -> Result<BabamulUser, HttpResponse> {
    let Some(user) = current_user else {
        return Err(HttpResponse::Unauthorized().body("Unauthorized"));
    };
    let user = user.into_inner();
    if !user.is_admin {
        return Err(response::forbidden("Only admins can manage users"));
    }
    Ok(user)
}

#[utoipa::path(
    get,
    path = "/babamul/admin/users",
    params(AdminUsersQuery),
    responses(
        (status = 200, description = "Users retrieved", body = AdminUsersResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Not an admin"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[get("/admin/users")]
pub async fn get_admin_users(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    query: web::Query<AdminUsersQuery>,
) -> HttpResponse {
    if let Err(resp) = require_admin(current_user) {
        return resp;
    }
    let filter = match query.search.as_deref().map(str::trim) {
        Some(search) if !search.is_empty() => {
            let pattern = regex::escape(search);
            doc! { "$or": [
                { "email": { "$regex": &pattern, "$options": "i" } },
                { "username": { "$regex": &pattern, "$options": "i" } },
                { "name": { "$regex": &pattern, "$options": "i" } },
            ] }
        }
        _ => Document::new(),
    };
    let limit = query.limit.unwrap_or(50).clamp(1, MAX_LIMIT);
    let offset = query.offset.unwrap_or(0);

    let collection: Collection<BabamulUser> = db.collection("babamul_users");
    let total = match collection.count_documents(filter.clone()).await {
        Ok(total) => total,
        Err(e) => {
            tracing::error!("Failed to count babamul users: {}", e);
            return response::internal_error("Failed to list users");
        }
    };
    let users: Vec<BabamulAdminUser> = match collection
        .find(filter)
        .sort(doc! { "created_at": -1, "_id": 1 })
        .skip(offset)
        .limit(limit as i64)
        .await
    {
        Ok(cursor) => match cursor.try_collect::<Vec<BabamulUser>>().await {
            Ok(users) => users.into_iter().map(BabamulAdminUser::from).collect(),
            Err(e) => {
                tracing::error!("Failed to read babamul users: {}", e);
                return response::internal_error("Failed to list users");
            }
        },
        Err(e) => {
            tracing::error!("Failed to list babamul users: {}", e);
            return response::internal_error("Failed to list users");
        }
    };

    response::ok_ser(
        "success",
        AdminUsersResponse {
            users,
            total,
            available_acls: BabamulAcl::ALL.to_vec(),
        },
    )
}

#[utoipa::path(
    patch,
    path = "/babamul/admin/users/{user_id}",
    request_body = AdminUserPatch,
    params(("user_id" = String, Path, description = "Babamul user id")),
    responses(
        (status = 200, description = "User updated", body = BabamulAdminUser),
        (status = 400, description = "An admin cannot remove their own admin status"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Not an admin"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[patch("/admin/users/{user_id}")]
pub async fn patch_admin_user(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    path: web::Path<String>,
    body: web::Json<AdminUserPatch>,
) -> HttpResponse {
    let admin = match require_admin(current_user) {
        Ok(admin) => admin,
        Err(resp) => return resp,
    };
    let user_id = path.into_inner();
    if user_id == admin.id && body.is_admin == Some(false) {
        return response::bad_request("You cannot remove your own admin status");
    }

    let mut set = Document::new();
    if let Some(is_admin) = body.is_admin {
        set.insert("is_admin", is_admin);
    }
    if let Some(acls) = &body.acls {
        if acls.contains(&BabamulAcl::ZtfCaltech) && !acls.contains(&BabamulAcl::ZtfPartnership) {
            return response::bad_request("ztf_caltech requires ztf_partnership");
        }
        let mut acls = acls.clone();
        acls.sort_by_key(|acl| BabamulAcl::ALL.iter().position(|a| a == acl));
        acls.dedup();
        match mongodb::bson::to_bson(&acls) {
            Ok(acls) => set.insert("acls", acls),
            Err(e) => {
                tracing::error!("Failed to serialize ACLs: {}", e);
                return response::internal_error("Failed to update user");
            }
        };
    }

    let collection: Collection<BabamulUser> = db.collection("babamul_users");
    let result = if set.is_empty() {
        collection.find_one(doc! { "_id": &user_id }).await
    } else {
        collection
            .find_one_and_update(doc! { "_id": &user_id }, doc! { "$set": set })
            .with_options(
                FindOneAndUpdateOptions::builder()
                    .return_document(ReturnDocument::After)
                    .build(),
            )
            .await
    };
    match result {
        Ok(Some(user)) => {
            tracing::info!(
                admin = %admin.id,
                user = %user.id,
                is_admin = user.is_admin,
                acls = ?user.acls,
                "Babamul user access updated"
            );
            response::ok_ser("success", BabamulAdminUser::from(user))
        }
        Ok(None) => response::not_found("User not found"),
        Err(e) => {
            tracing::error!("Failed to update babamul user {}: {}", user_id, e);
            response::internal_error("Failed to update user")
        }
    }
}
