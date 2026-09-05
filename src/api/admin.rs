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
#[derive(Debug)]
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

/// Why a caller was refused.
///
/// Separated from the HTTP response so the decision itself can be tested
/// without constructing a request: this is the check standing between an
/// authenticated user and every data-mutating job, and it should not be
/// exercised only through handlers.
#[derive(Debug, PartialEq, Eq)]
pub enum AdminDenied {
    /// Authenticated, but not an admin.
    NotAnAdmin,
    /// No recognized credential in either realm.
    Unauthenticated,
}

/// Resolve an admin from whichever realm authenticated the request.
///
/// The middlewares inject one realm or the other, never both. If both are
/// somehow present the main-API user wins, and a non-admin there is refused
/// rather than falling through to the other realm -- an account that failed the
/// check must not get a second attempt at it.
pub fn resolve_admin(
    boom_user: Option<&User>,
    babamul_user: Option<&BabamulUser>,
) -> Result<AdminActor, AdminDenied> {
    if let Some(user) = boom_user {
        return if user.is_admin {
            Ok(AdminActor {
                realm: "boom",
                user_id: user.id.clone(),
                username: user.username.clone(),
            })
        } else {
            Err(AdminDenied::NotAnAdmin)
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
            Err(AdminDenied::NotAnAdmin)
        };
    }
    Err(AdminDenied::Unauthenticated)
}

/// Resolve an admin from the request, returning the response to send when the
/// caller is not one, so handlers stay a single `match`.
pub fn require_admin(
    boom_user: &Option<web::ReqData<User>>,
    babamul_user: &Option<web::ReqData<BabamulUser>>,
) -> Result<AdminActor, HttpResponse> {
    resolve_admin(
        boom_user.as_ref().map(|u| &**u),
        babamul_user.as_ref().map(|u| &**u),
    )
    .map_err(|denied| match denied {
        AdminDenied::NotAnAdmin => response::forbidden("Admin access required"),
        AdminDenied::Unauthenticated => HttpResponse::Unauthorized().body("Unauthorized"),
    })
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

#[cfg(test)]
mod tests {
    use super::*;

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

    fn babamul_user(is_admin: bool) -> BabamulUser {
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
        }
    }

    #[test]
    fn no_credential_is_unauthenticated_not_forbidden() {
        // The distinction is what lets a client know to log in rather than to
        // give up.
        assert_eq!(
            resolve_admin(None, None).unwrap_err(),
            AdminDenied::Unauthenticated
        );
    }

    #[test]
    fn a_non_admin_is_refused_in_either_realm() {
        assert_eq!(
            resolve_admin(Some(&boom_user(false)), None).unwrap_err(),
            AdminDenied::NotAnAdmin
        );
        assert_eq!(
            resolve_admin(None, Some(&babamul_user(false))).unwrap_err(),
            AdminDenied::NotAnAdmin
        );
    }

    #[test]
    fn an_admin_is_accepted_from_either_realm() {
        assert_eq!(
            resolve_admin(Some(&boom_user(true)), None).unwrap().realm,
            "boom"
        );
        assert_eq!(
            resolve_admin(None, Some(&babamul_user(true)))
                .unwrap()
                .realm,
            "babamul"
        );
    }

    #[test]
    fn a_refused_main_api_user_does_not_fall_through_to_the_other_realm() {
        // Both realms are never injected at once today, but if that ever
        // changed, an account that failed the check must not get a second
        // attempt at it through the other one.
        assert_eq!(
            resolve_admin(Some(&boom_user(false)), Some(&babamul_user(true))).unwrap_err(),
            AdminDenied::NotAnAdmin
        );
    }

    #[test]
    fn the_recorded_actor_is_qualified_by_realm() {
        // The two id spaces are unrelated, and the point of recording the actor
        // is being able to look the person up in the right collection later.
        let boom = resolve_admin(Some(&boom_user(true)), None)
            .unwrap()
            .as_task_actor();
        let babamul = resolve_admin(None, Some(&babamul_user(true)))
            .unwrap()
            .as_task_actor();
        assert_eq!(boom.user_id, "boom:boom-1");
        assert_eq!(babamul.user_id, "babamul:bbml-1");
        // Same username in both realms, so the id is what disambiguates.
        assert_eq!(boom.username, babamul.username);
        assert_ne!(boom.user_id, babamul.user_id);
    }
}
