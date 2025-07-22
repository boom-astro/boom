use crate::models::response;
use actix_web::{HttpResponse, delete, get, post, web};
use futures::stream::StreamExt;
use mongodb::{Collection, Database, bson::doc};
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
    pub id: String,
    pub username: String,
    pub email: String,
    pub password: String, // This will be hashed before insertion
    pub is_admin: bool,   // Indicates if the user is an admin
}

/// Add a new user (admin only)
#[utoipa::path(
    post,
    path = "/users",
    request_body = UserPost,
    responses(
        (status = 200, description = "User created successfully", body = User),
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
    let current_user = current_user.unwrap();
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
    };

    // Save new user to database
    match user_collection.insert_one(user_insert.clone()).await {
        Ok(_) => response::ok(
            "success",
            serde_json::to_value(UserGet {
                id: user_id,
                username: user_insert.username.clone(),
                email: user_insert.email.clone(),
                is_admin: user_insert.is_admin,
            })
            .unwrap(),
        ),
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

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct UserGet {
    pub id: String,
    pub username: String,
    pub email: String,
    pub is_admin: bool,
}

/// Get a list of users
#[utoipa::path(
    get,
    path = "/users",
    responses(
        (status = 200, description = "Users retrieved successfully", body = [User]),
        (status = 500, description = "Internal server error")
    ),
    tags=["Users"]
)]
#[get("/users")]
pub async fn get_users(db: web::Data<Database>) -> HttpResponse {
    let user_collection: Collection<UserGet> = db.collection("users");
    let users = user_collection.find(doc! {}).await;

    match users {
        Ok(mut cursor) => {
            let mut user_list = Vec::<UserGet>::new();
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
            response::ok("success", serde_json::to_value(&user_list).unwrap())
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to query users: {}", e)),
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
    let current_user = current_user.unwrap();
    if !current_user.is_admin {
        return HttpResponse::Forbidden().body("Only admins can delete users");
    }
    // TODO: Ensure the caller is authorized to delete this user
    let user_id = path.into_inner();
    let user_collection: Collection<UserGet> = db.collection("users");

    match user_collection.delete_one(doc! { "id": &user_id }).await {
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
