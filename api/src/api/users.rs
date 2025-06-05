use crate::models::response;
use actix_web::{HttpResponse, delete, get, post, web};
use futures::stream::StreamExt;
use mongodb::{Collection, Database, bson::doc};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Deserialize, Clone)]
pub struct UserPost {
    pub username: String,
    pub email: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserInsert {
    pub id: String,
    pub username: String,
    pub email: String,
    pub password: String, // This will be hashed before insertion
}

#[post("/users")]
pub async fn post_user(db: web::Data<Database>, body: web::Json<UserPost>) -> HttpResponse {
    let user_collection: Collection<UserInsert> = db.collection("users");

    // Create a new user document
    // First, hash password
    // TODO: Permissions?
    let user_id = uuid::Uuid::new_v4().to_string();
    let hashed_password =
        bcrypt::hash(&body.password, bcrypt::DEFAULT_COST).expect("failed to hash password");
    let user_insert = UserInsert {
        id: user_id.clone(),
        username: body.username.clone(),
        email: body.email.clone(),
        password: hashed_password,
    };

    // Save new user to database
    match user_collection.insert_one(user_insert).await {
        Ok(_) => response::ok(
            "success",
            serde_json::to_value(User {
                id: user_id,
                username: body.username.clone(),
                email: body.email.clone(),
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

#[derive(Serialize, Deserialize, Debug)]
pub struct User {
    pub id: String,
    pub username: String,
    pub email: String,
}

#[get("/users")]
pub async fn get_users(db: web::Data<Database>) -> HttpResponse {
    let user_collection: Collection<User> = db.collection("users");
    let users = user_collection.find(doc! {}).await;

    match users {
        Ok(mut cursor) => {
            let mut user_list = Vec::<User>::new();
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

#[delete("/users/{username}")]
pub async fn delete_user(db: web::Data<Database>, path: web::Path<String>) -> HttpResponse {
    // TODO: Ensure the caller is authorized to delete this user
    let username = path.into_inner();
    let user_collection: Collection<User> = db.collection("users");

    match user_collection
        .delete_one(doc! { "username": &username })
        .await
    {
        Ok(delete_result) => {
            if delete_result.deleted_count > 0 {
                HttpResponse::Ok().json(json!({
                    "status": "success",
                    "message": format!("user '{}' deleted successfully", username)
                }))
            } else {
                HttpResponse::NotFound().body("user not found")
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to delete user: {}", e)),
    }
}
