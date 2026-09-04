//! Authorization for the admin surface.
//!
//! BOOM has two login realms -- the original API's `users` and Babamul's
//! `babamul_users` -- and the admin page is reached from the web app, which
//! authenticates as the latter. Rather than merge the logins, both realms carry
//! an `is_admin` flag and both are accepted here, so there is one authorization
//! check and one shape of actor recorded on a run.

use crate::api::models::response;
use crate::api::routes::{babamul::BabamulUser, users::User};
use crate::tasks::Actor;
use actix_web::{web, HttpResponse};

/// An authenticated admin, whichever realm they came from.
pub struct AdminActor {
    /// `boom` or `babamul`. Recorded so a run can be traced back to an account
    /// in the right collection -- the two id spaces are unrelated.
    pub realm: &'static str,
    pub user_id: String,
    pub username: String,
}

impl AdminActor {
    /// How this admin is recorded on a task run.
    pub fn as_task_actor(&self) -> Actor {
        Actor {
            // Qualified by realm: a bare id is ambiguous across the two
            // collections, and the whole point of recording it is being able to
            // look the person up later.
            user_id: format!("{}:{}", self.realm, self.user_id),
            username: self.username.clone(),
        }
    }
}

/// Resolve an admin from whichever realm authenticated the request.
///
/// Returns the response to send when the caller is not an admin, so handlers
/// stay a single `match`.
pub fn require_admin(
    boom_user: &Option<web::ReqData<User>>,
    babamul_user: &Option<web::ReqData<BabamulUser>>,
) -> Result<AdminActor, HttpResponse> {
    if let Some(user) = boom_user {
        return if user.is_admin {
            Ok(AdminActor {
                realm: "boom",
                user_id: user.id.clone(),
                username: user.username.clone(),
            })
        } else {
            Err(response::forbidden("Admin access required"))
        };
    }
    if let Some(user) = babamul_user {
        return if user.is_admin {
            Ok(AdminActor {
                realm: "babamul",
                user_id: user.id.clone(),
                username: user.username.clone(),
            })
        } else {
            Err(response::forbidden("Admin access required"))
        };
    }
    Err(HttpResponse::Unauthorized().body("Unauthorized"))
}

/// Reconcile `is_admin` on every Babamul account against the configured list.
///
/// Runs at API startup. Deliberately two-way: accounts on the list are granted,
/// accounts not on it are revoked. A one-way grant would mean removing someone
/// from the config left them an admin until somebody noticed, which is the
/// failure mode that matters here.
#[tracing::instrument(skip(db, admin_emails))]
pub async fn reconcile_babamul_admins(
    db: &mongodb::Database,
    admin_emails: &[String],
) -> Result<(), mongodb::error::Error> {
    use mongodb::bson::doc;

    let collection = db.collection::<BabamulUser>("babamul_users");
    // Emails are compared case-insensitively because that is how they are
    // matched at sign-in; a config entry that differs only in case should not
    // silently fail to grant access.
    let emails: Vec<String> = admin_emails.iter().map(|e| e.to_lowercase()).collect();
    let matcher: Vec<mongodb::bson::Regex> = emails
        .iter()
        .map(|e| mongodb::bson::Regex {
            pattern: format!("^{}$", regex::escape(e)),
            options: "i".to_string(),
        })
        .collect();

    let granted = collection
        .update_many(
            doc! { "email": { "$in": &matcher }, "is_admin": { "$ne": true } },
            doc! { "$set": { "is_admin": true } },
        )
        .await?;
    let revoked = collection
        .update_many(
            doc! { "email": { "$nin": &matcher }, "is_admin": true },
            doc! { "$set": { "is_admin": false } },
        )
        .await?;

    if granted.modified_count > 0 || revoked.modified_count > 0 {
        tracing::info!(
            "babamul admins reconciled: {} granted, {} revoked ({} configured)",
            granted.modified_count,
            revoked.modified_count,
            emails.len()
        );
    }
    // Worth saying out loud: with no admins configured, nobody can reach the
    // admin page, and the symptom is a 403 that looks like a bug.
    if emails.is_empty() {
        tracing::warn!("no babamul.admin_emails configured; the admin page is closed to everyone");
    }
    Ok(())
}
