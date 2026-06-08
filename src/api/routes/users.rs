use crate::api::models::response;
use actix_web::{delete, get, patch, post, web, HttpResponse};
use futures::stream::StreamExt;
use mongodb::{bson::doc, Collection, Database};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;

#[derive(Deserialize, Clone, ToSchema)]
pub struct UserPost {
    pub username: String,
    pub email: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct User {
    // Save in the database as _id, but we want to rename on the way out
    #[serde(rename = "_id")]
    pub id: String,
    pub username: String,
    pub email: String,
    pub password: String, // This will be hashed before insertion
    pub is_admin: bool,   // Indicates if the user is an admin
    /// Names of watchlist catalogs this user can read (and bind filters to).
    /// Admins bypass this list.
    #[serde(default)]
    pub watchlist_access: Vec<String>,
}

impl User {
    /// Whether this user can read the given catalog. Non-watchlist catalogs are
    /// always visible; watchlist catalogs require the user to have explicit access.
    pub fn can_access_catalog(&self, catalog: &str) -> bool {
        !catalog.starts_with(crate::api::catalogs::WATCHLIST_PREFIX)
            || self.is_admin
            || self.watchlist_access.iter().any(|w| w == catalog)
    }
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct UserPublic {
    #[serde(rename(serialize = "id", deserialize = "_id"))]
    pub id: String,
    pub username: String,
    pub email: String,
    pub is_admin: bool,
}

impl From<User> for UserPublic {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            is_admin: user.is_admin,
        }
    }
}

/// Add a new user (admin only)
#[utoipa::path(
    post,
    path = "/users",
    request_body = UserPost,
    responses(
        (status = 200, description = "User created successfully", body = UserPublic),
        (status = 409, description = "User already exists"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Users"]
)]
#[post("/users")]
pub async fn post_user(
    db: web::Data<Database>,
    body: web::Json<UserPost>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    if !current_user.is_admin {
        return HttpResponse::Forbidden().body("Only admins can create new users");
    }
    let user_collection: Collection<User> = db.collection("users");

    // Create a new user document
    // First, hash password
    // TODO: Permissions?
    let user_id = uuid::Uuid::new_v4().to_string();
    let hashed_password =
        bcrypt::hash(&body.password, bcrypt::DEFAULT_COST).expect("failed to hash password");
    let user_insert = User {
        id: user_id.clone(),
        username: body.username.clone(),
        email: body.email.clone(),
        password: hashed_password,
        is_admin: false,
        watchlist_access: Vec::new(),
    };

    // Save new user to database
    match user_collection.insert_one(user_insert.clone()).await {
        Ok(_) => response::ok_ser("success", UserPublic::from(user_insert)),
        // Catch unique index constraint error
        Err(e) if e.to_string().contains("E11000 duplicate key error") => HttpResponse::Conflict()
            .body(format!(
                "user with username '{}' already exists",
                body.username
            )),
        // Catch other errors
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to insert user into database. error: {}", e)),
    }
}

/// Get multiple users
#[utoipa::path(
    get,
    path = "/users",
    responses(
        (status = 200, description = "Users retrieved successfully", body = [UserPublic]),
        (status = 500, description = "Internal server error")
    ),
    tags=["Users"]
)]
#[get("/users")]
pub async fn get_users(db: web::Data<Database>) -> HttpResponse {
    let user_collection: Collection<UserPublic> = db.collection("users");
    let users = user_collection.find(doc! {}).await;

    match users {
        Ok(mut cursor) => {
            let mut user_list = Vec::<UserPublic>::new();
            while let Some(user) = cursor.next().await {
                match user {
                    Ok(user) => {
                        user_list.push(user);
                    }
                    Err(e) => {
                        return HttpResponse::InternalServerError()
                            .body(format!("error reading user: {}", e));
                    }
                }
            }
            response::ok_ser("success", user_list)
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to query users: {}", e)),
    }
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct WatchlistAccessPatch {
    /// New full list of watchlist catalog names this user can read.
    pub watchlist_access: Vec<String>,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct WatchlistAccessResponse {
    pub user_id: String,
    pub watchlist_access: Vec<String>,
}

/// Replace a user's watchlist access list (admin only)
#[utoipa::path(
    patch,
    path = "/users/{user_id}/watchlist_access",
    request_body = WatchlistAccessPatch,
    responses(
        (status = 200, description = "Watchlist access updated", body = WatchlistAccessResponse),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Users"]
)]
#[patch("/users/{user_id}/watchlist_access")]
pub async fn patch_watchlist_access(
    db: web::Data<Database>,
    path: web::Path<String>,
    body: web::Json<WatchlistAccessPatch>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    if !current_user.is_admin {
        return HttpResponse::Forbidden().body("Only admins can modify watchlist access");
    }
    let user_id = path.into_inner();
    let user_collection: Collection<User> = db.collection("users");
    let new_list: Vec<String> = body
        .watchlist_access
        .iter()
        .filter(|w| !w.is_empty())
        .cloned()
        .collect();
    let update = doc! { "$set": { "watchlist_access": &new_list } };
    match user_collection
        .find_one_and_update(doc! { "_id": &user_id }, update)
        .with_options(
            mongodb::options::FindOneAndUpdateOptions::builder()
                .return_document(mongodb::options::ReturnDocument::After)
                .build(),
        )
        .await
    {
        Ok(Some(updated)) => response::ok_ser(
            "watchlist access updated",
            WatchlistAccessResponse {
                user_id: updated.id,
                watchlist_access: updated.watchlist_access,
            },
        ),
        Ok(None) => HttpResponse::NotFound().body("user not found"),
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to update watchlist access: {}", e)),
    }
}

/// Delete a user by ID (admin only)
#[utoipa::path(
    delete,
    path = "/users/{user_id}",
    responses(
        (status = 200, description = "User deleted successfully"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Users"]
)]
#[delete("/users/{user_id}")]
pub async fn delete_user(
    db: web::Data<Database>,
    path: web::Path<String>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    if !current_user.is_admin {
        return HttpResponse::Forbidden().body("Only admins can delete users");
    }
    // TODO: Ensure the caller is authorized to delete this user
    let user_id = path.into_inner();
    let user_collection: Collection<UserPublic> = db.collection("users");

    match user_collection.delete_one(doc! { "_id": &user_id }).await {
        Ok(delete_result) => {
            if delete_result.deleted_count > 0 {
                HttpResponse::Ok().json(json!({
                    "status": "success",
                    "message": format!("user ID '{}' deleted successfully", user_id)
                }))
            } else {
                HttpResponse::NotFound().body("user not found")
            }
        }
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("failed to delete user ID: {}", e))
        }
    }
}
