//! Authorization for the admin surface.
//!
//! BOOM has two login realms -- the original API's `users` and Babamul's
//! `babamul_users` -- and the admin page is reached from the client, which
//! authenticates as the latter. Rather than merge the logins, both realms carry
//! an `is_admin` flag and both are accepted here, so there is one authorization
//! check and one shape of actor recorded on a run.
//!
//! One users collection with explicit permissions -- internal, external, admin
//! -- would be cleaner than two realms carrying a boolean each, and is the
//! direction to take whenever either login is next reworked.

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

/// Grant `is_admin` to the Babamul accounts named in the configured list.
///
/// Named "reconcile" for the layering's sake rather than its own: the call site
/// lives in `src/bin/api.rs`, which a different layer of this stack owns, so
/// renaming it here alone would break the layer that carries this file. It
/// seeds; it does not reconcile.
///
/// Runs at API startup, grants only, and only when the deployment has no
/// admins at all. This list solves one problem: admin is granted through
/// `PATCH /babamul/admin/users/{id}`, which only an admin may call, so a fresh
/// deployment has no way to appoint its first one.
///
/// Both halves of that matter, because the API is the source of truth and a
/// config list that keeps asserting itself would fight it. Revoking somebody
/// would last until the next restart if they were still named here, which is a
/// grant nobody made and nobody can see. And a two-way version would be worse:
/// every restart would un-admin everyone appointed since the last one. So once
/// an admin exists, this is inert, and admin is added and removed in one place.
///
/// Losing every admin is the one case it fires again, which is the recovery
/// path: put an address here and restart.
#[tracing::instrument(skip(db, admin_emails))]
pub async fn reconcile_babamul_admins(
    db: &mongodb::Database,
    admin_emails: &[String],
) -> Result<(), mongodb::error::Error> {
    use mongodb::bson::doc;

    let collection = db.collection::<BabamulUser>("babamul_users");

    // Inert once anyone is an admin, whoever made them one.
    let existing = collection
        .count_documents(doc! { "is_admin": true })
        .await?;
    if existing > 0 {
        tracing::debug!(existing, "babamul admins exist already; not seeding");
        return Ok(());
    }

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

    if granted.modified_count > 0 {
        tracing::info!(
            "no babamul admins existed: {} seeded from config ({} configured)",
            granted.modified_count,
            emails.len()
        );
    }
    // Worth saying out loud on a deployment that has no admins yet: with none
    // configured and none granted, nobody can reach the admin page, and the
    // symptom is a 403 that looks like a bug.
    if emails.is_empty() {
        tracing::debug!("no babamul.admin_emails configured; admins can only come from the API");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    /// Serializes the seeding tests: they share one `babamul_users` collection
    /// and each one cares how many admins are in it.
    static SEED_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    async fn insert_user(db: &mongodb::Database, email: &str, is_admin: bool) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let mut user = babamul_user(is_admin);
        user.id = id.clone();
        user.email = email.to_string();
        db.collection::<BabamulUser>("babamul_users")
            .insert_one(&user)
            .await
            .expect("inserts");
        id
    }

    async fn is_admin(db: &mongodb::Database, id: &str) -> bool {
        db.collection::<BabamulUser>("babamul_users")
            .find_one(doc! { "_id": id })
            .await
            .expect("reads")
            .expect("present")
            .is_admin
    }

    async fn clear(db: &mongodb::Database) {
        db.collection::<BabamulUser>("babamul_users")
            .delete_many(doc! {})
            .await
            .expect("clears");
    }

    #[tokio::test]
    async fn the_first_admin_comes_from_config() {
        let _seeding = SEED_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        clear(&db).await;
        let id = insert_user(&db, "first@example.org", false).await;

        reconcile_babamul_admins(&db, &["First@Example.org".to_string()])
            .await
            .expect("seeds");

        // Case-insensitively, because that is how sign-in matches an address.
        assert!(is_admin(&db, &id).await);
        clear(&db).await;
    }

    #[tokio::test]
    async fn a_revoked_admin_is_not_handed_it_back_at_the_next_restart() {
        let _seeding = SEED_LOCK.lock().await;
        // The hazard this guards: an address stays in config after an admin
        // revokes it through the API. Seeding again would be a grant nobody
        // made, undone only by editing config, and invisible until someone
        // noticed the admin page working for a person who should not have it.
        let db = crate::conf::get_test_db().await;
        clear(&db).await;
        let keeper = insert_user(&db, "keeper@example.org", true).await;
        let revoked = insert_user(&db, "revoked@example.org", false).await;

        reconcile_babamul_admins(
            &db,
            &[
                "keeper@example.org".to_string(),
                "revoked@example.org".to_string(),
            ],
        )
        .await
        .expect("seeds");

        assert!(is_admin(&db, &keeper).await, "untouched");
        assert!(
            !is_admin(&db, &revoked).await,
            "a revocation through the API has to outlast a restart"
        );
        clear(&db).await;
    }

    #[tokio::test]
    async fn an_empty_list_writes_nothing() {
        let _seeding = SEED_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        clear(&db).await;
        let id = insert_user(&db, "nobody@example.org", false).await;

        reconcile_babamul_admins(&db, &[]).await.expect("no-op");

        assert!(!is_admin(&db, &id).await);
        clear(&db).await;
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
            acls: Vec::new(),
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
