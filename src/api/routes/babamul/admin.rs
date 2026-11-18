use crate::api::models::response;
use crate::api::routes::babamul::{BabamulAcl, BabamulUser};
use actix_web::{get, patch, web, HttpResponse};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
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
    /// Replace the whole ACL list. Last write wins, so two admins editing one
    /// user at the same time lose one of the two edits; `grant_acls` and
    /// `revoke_acls` apply as a delta and do not.
    pub acls: Option<Vec<BabamulAcl>>,
    /// ACLs to add, leaving every other ACL the user has alone.
    pub grant_acls: Option<Vec<BabamulAcl>>,
    /// ACLs to remove, leaving every other ACL the user has alone.
    pub revoke_acls: Option<Vec<BabamulAcl>>,
}

/// Put an ACL list in a stable order, drop duplicates, and enforce that ZTF
/// Caltech access implies ZTF partnership access.
fn normalize_acls(acls: &[BabamulAcl]) -> Result<Vec<BabamulAcl>, HttpResponse> {
    if acls.contains(&BabamulAcl::ZtfCaltech) && !acls.contains(&BabamulAcl::ZtfPartnership) {
        return Err(response::bad_request(
            "ztf_caltech requires ztf_partnership",
        ));
    }
    let mut acls = acls.to_vec();
    acls.sort_by_key(|acl| BabamulAcl::ALL.iter().position(|a| a == acl));
    acls.dedup();
    Ok(acls)
}

/// Apply a grant/revoke delta to the ACLs a user actually has.
fn apply_acl_delta(
    current: &[BabamulAcl],
    grant: &[BabamulAcl],
    revoke: &[BabamulAcl],
) -> Result<Vec<BabamulAcl>, HttpResponse> {
    if let Some(acl) = grant.iter().find(|acl| revoke.contains(acl)) {
        return Err(response::bad_request(&format!(
            "{acl:?} is in both grant_acls and revoke_acls"
        )));
    }
    // The same contradiction reached through the interlock rather than
    // directly: Caltech access requires partnership access, so granting the
    // first while revoking the second asks for a state that cannot exist.
    // Resolving it either way silently drops half of what was asked for -- the
    // delta below would have removed the Caltech access it was just told to
    // grant and reported success.
    if grant.contains(&BabamulAcl::ZtfCaltech) && revoke.contains(&BabamulAcl::ZtfPartnership) {
        return Err(response::bad_request(
            "granting ztf_caltech while revoking ztf_partnership asks for a combination that \
             cannot exist: ztf_caltech requires ztf_partnership",
        ));
    }
    let mut next: Vec<BabamulAcl> = current
        .iter()
        .chain(grant)
        .filter(|acl| !revoke.contains(acl))
        .copied()
        .collect();
    // Revoking partnership access revokes the Caltech access that depends on
    // it, rather than refusing the request over a combination the caller did
    // not ask for.
    if revoke.contains(&BabamulAcl::ZtfPartnership) {
        next.retain(|acl| *acl != BabamulAcl::ZtfCaltech);
    }
    // Granting Caltech access grants the partnership access it requires.
    if next.contains(&BabamulAcl::ZtfCaltech) {
        next.push(BabamulAcl::ZtfPartnership);
    }
    normalize_acls(&next)
}

/// Attempts at the compare-and-swap below before the caller is told to retry.
///
/// Three is enough for contention between two admins on one user; past that the
/// conflict is not transient and a 409 is more use than a longer spin.
const ACL_WRITE_ATTEMPTS: usize = 3;

/// Match the ACL list a delta was computed from.
///
/// `BabamulUser` reads a missing `acls` field as an empty list, but MongoDB
/// does not consider the two equal -- so an equality on `[]` matches no
/// document at all for an account created before the field existed, and every
/// delta against one of those would have exhausted its retries and returned a
/// conflict that nothing could resolve.
fn matching_acls(
    observed: &[BabamulAcl],
) -> Result<mongodb::bson::Bson, mongodb::bson::ser::Error> {
    let stored = mongodb::bson::to_bson(observed)?;
    Ok(if observed.is_empty() {
        mongodb::bson::bson!({ "$in": [stored, mongodb::bson::Bson::Null] })
    } else {
        mongodb::bson::bson!({ "$eq": stored })
    })
}

async fn count_admins(collection: &Collection<BabamulUser>) -> Result<u64, mongodb::error::Error> {
    collection
        .count_documents(doc! { "is_admin": true })
        .max_time(QUERY_TIME)
        .await
}

/// Whether this patch would take the deployment's last admin away.
///
/// Demoting somebody who is not an admin is not a demotion, and demoting one of
/// several is fine; only the last one leaves a deployment that cannot appoint
/// another through this route.
fn would_orphan(patch_is_admin: Option<bool>, target_is_admin: bool, admins: u64) -> bool {
    patch_is_admin == Some(false) && target_is_admin && admins <= 1
}

pub const LOCK_COLLECTION: &str = "locks";
/// The lock every demotion is taken under.
const DEMOTION_LOCK: &str = "babamul_admin_demotion";
/// How long a holder may keep it.
///
/// Longer than [`DEMOTION_DEADLINE`] plus the driver's 30 s server selection
/// and a [`QUERY_TIME`], so a write that started in time lands before the lease
/// can pass to another demotion.
const DEMOTION_LOCK_SECONDS: f64 = 60.0;
/// A demotion that has not written by then gives up instead.
const DEMOTION_DEADLINE: Duration = Duration::from_secs(20);
/// Server-side budget of each query on this route.
const QUERY_TIME: Duration = Duration::from_secs(5);
/// How long a demotion waits for another one to finish.
const DEMOTION_LOCK_WAIT: Duration = Duration::from_secs(2);

fn seconds_since_epoch() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Take the demotion lock, or report that somebody else holds it.
///
/// "At least one admin remains" spans documents, so no conditional update can
/// express it, and the deployment runs a standalone MongoDB where a transaction
/// is not available either. Serializing the demotions themselves is what is
/// left: with only one in flight at a time, counting the admins and then
/// writing is sound, because the only thing that could invalidate the count is
/// another demotion.
///
/// Expressed as an upsert whose filter requires the lock to be free. If it is
/// held, the filter matches nothing, the upsert tries to insert, and the
/// duplicate `_id` is the refusal.
async fn take_demotion_lock(
    db: &Database,
    token: mongodb::bson::oid::ObjectId,
) -> Result<bool, mongodb::error::Error> {
    let free = doc! {
        "_id": DEMOTION_LOCK,
        "expires_at": { "$lt": seconds_since_epoch() },
    };
    let held = doc! {
        "$set": {
            "expires_at": seconds_since_epoch() + DEMOTION_LOCK_SECONDS,
            "token": token,
        },
    };
    match db
        .collection::<Document>(LOCK_COLLECTION)
        .update_one(free, held)
        .upsert(true)
        .await
    {
        Ok(_) => Ok(true),
        // The document exists and the filter did not match it, so the upsert
        // collided with the holder's `_id`. That is the lock being held, not a
        // failure to talk to the database.
        Err(e) if is_duplicate_key(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Release the lock, but only if we still hold it.
///
/// Matched on the token: a release that did not check could drop a lock the
/// lease had already handed to somebody else.
async fn release_demotion_lock(db: &Database, token: mongodb::bson::oid::ObjectId) {
    if let Err(e) = db
        .collection::<Document>(LOCK_COLLECTION)
        .update_one(
            doc! { "_id": DEMOTION_LOCK, "token": token },
            doc! { "$set": { "expires_at": 0.0 } },
        )
        .await
    {
        // Not worth failing the request it guarded: the lease expires on its
        // own, so the cost is a few seconds of contention.
        tracing::warn!("could not release the demotion lock: {e}");
    }
}

/// Take the demotion lock, waiting [`DEMOTION_LOCK_WAIT`] for its holder.
async fn wait_for_demotion_lock(
    db: &Database,
    token: mongodb::bson::oid::ObjectId,
) -> Result<bool, mongodb::error::Error> {
    let give_up = Instant::now() + DEMOTION_LOCK_WAIT;
    loop {
        if take_demotion_lock(db, token).await? {
            return Ok(true);
        }
        if Instant::now() >= give_up {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(
        *e.kind,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(ref w))
            if w.code == 11000
    )
}

fn require_admin(
    current_user: Option<web::ReqData<BabamulUser>>,
) -> Result<BabamulUser, HttpResponse> {
    let Some(user) = current_user else {
        return Err(response::unauthorized("Unauthorized"));
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
        (status = 400, description = "An admin cannot remove their own admin status, or the patch is invalid"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Not an admin"),
        (status = 404, description = "User not found"),
        (status = 409, description = "Another admin changed this user first, or this is the last admin"),
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

    let collection: Collection<BabamulUser> = db.collection("babamul_users");

    // A demotion holds the lock for the whole of its read-count-write. Without
    // it the count is only advice: two admins demoting each other both see two
    // admins, both write, and the deployment is left with none -- which nothing
    // can appoint its way out of, since this route requires already being an
    // admin and the startup reconciliation only grants.
    let lock = mongodb::bson::oid::ObjectId::new();
    let locked_at = if body.is_admin == Some(false) {
        match wait_for_demotion_lock(&db, lock).await {
            Ok(true) => Some(Instant::now()),
            Ok(false) => {
                return response::conflict(
                    "Another admin's access is being changed right now. Try again in a moment.",
                );
            }
            Err(e) => {
                tracing::error!("Failed to take the demotion lock: {}", e);
                return response::internal_error("Failed to update user");
            }
        }
    } else {
        None
    };

    let response = apply_patch(&collection, &admin, &user_id, &body, locked_at).await;

    if locked_at.is_some() {
        release_demotion_lock(&db, lock).await;
    }
    response
}

/// The body of the patch, with the demotion lock already held where one is
/// needed, so every exit from here releases it in the caller.
async fn apply_patch(
    collection: &Collection<BabamulUser>,
    admin: &BabamulUser,
    user_id: &str,
    body: &AdminUserPatch,
    locked_at: Option<Instant>,
) -> HttpResponse {
    // Read under the lock, so a demotion sees the target as it is when it
    // writes, and a delta knows what it is being applied to.
    let mut current = match collection
        .find_one(doc! { "_id": user_id })
        .max_time(QUERY_TIME)
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => return response::not_found("User not found"),
        Err(e) => {
            tracing::error!("Failed to read babamul user {}: {}", user_id, e);
            return response::internal_error("Failed to update user");
        }
    };
    let demoting = locked_at.is_some() && current.is_admin;

    let replace = body.acls.as_deref();
    let grant = body.grant_acls.as_deref().unwrap_or(&[]);
    let revoke = body.revoke_acls.as_deref().unwrap_or(&[]);
    if replace.is_some() && !(grant.is_empty() && revoke.is_empty()) {
        return response::bad_request(
            "send either acls or grant_acls/revoke_acls, not both: replacing the list and \
             editing it mean different things when two admins edit one user at once",
        );
    }
    let delta = !grant.is_empty() || !revoke.is_empty();

    // Counted under the lock, so nothing can demote another admin between here
    // and the write below. That is what makes this a guard rather than advice,
    // and why there is no compensating rollback afterwards.
    if demoting {
        match count_admins(collection).await {
            Ok(admins) if would_orphan(body.is_admin, current.is_admin, admins) => {
                return response::conflict(
                    "This is the deployment's last admin. Appoint another one first, or \
                     nobody will be able to manage users.",
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to count babamul admins: {}", e);
                return response::internal_error("Failed to update user");
            }
        }
    }

    for attempt in 1..=ACL_WRITE_ATTEMPTS {
        if locked_at.is_some_and(|at| at.elapsed() > DEMOTION_DEADLINE) {
            return response::request_timeout("The database is slow right now. Try again.");
        }
        let mut set = Document::new();
        if let Some(is_admin) = body.is_admin {
            set.insert("is_admin", is_admin);
        }
        let next_acls = match replace {
            Some(acls) => match normalize_acls(acls) {
                Ok(acls) => Some(acls),
                Err(resp) => return resp,
            },
            None if delta => match apply_acl_delta(&current.acls, grant, revoke) {
                Ok(acls) => Some(acls),
                Err(resp) => return resp,
            },
            None => None,
        };
        if let Some(acls) = &next_acls {
            match mongodb::bson::to_bson(acls) {
                Ok(acls) => set.insert("acls", acls),
                Err(e) => {
                    tracing::error!("Failed to serialize ACLs: {}", e);
                    return response::internal_error("Failed to update user");
                }
            };
        }

        // Compare and swap, for a delta only: the filter names the ACLs the
        // delta was computed from, so an edit that landed in between fails to
        // match and is recomputed against what is now there instead of
        // reverting it. A wholesale replacement is last-write-wins by
        // definition and has nothing to compare against.
        let mut filter = doc! { "_id": user_id };
        if delta {
            match matching_acls(&current.acls) {
                Ok(bson) => filter.insert("acls", bson),
                Err(e) => {
                    tracing::error!("Failed to serialize ACLs: {}", e);
                    return response::internal_error("Failed to update user");
                }
            };
        }

        let result = if set.is_empty() {
            Ok(Some(current.clone()))
        } else {
            collection
                .find_one_and_update(filter, doc! { "$set": set })
                .with_options(
                    FindOneAndUpdateOptions::builder()
                        .return_document(ReturnDocument::After)
                        .max_time(QUERY_TIME)
                        .build(),
                )
                .await
        };

        let user = match result {
            Ok(Some(user)) => user,
            // The filter did not match, so either the user is gone or another
            // admin wrote first. Re-reading tells us which.
            Ok(None) if delta && attempt < ACL_WRITE_ATTEMPTS => {
                match collection
                    .find_one(doc! { "_id": user_id })
                    .max_time(QUERY_TIME)
                    .await
                {
                    Ok(Some(fresh)) => {
                        current = fresh;
                        continue;
                    }
                    Ok(None) => return response::not_found("User not found"),
                    Err(e) => {
                        tracing::error!("Failed to re-read babamul user {}: {}", user_id, e);
                        return response::internal_error("Failed to update user");
                    }
                }
            }
            Ok(None) if delta => {
                return response::conflict(
                    "Another admin kept changing this user's access. Reload and try again.",
                );
            }
            Ok(None) => return response::not_found("User not found"),
            Err(e) => {
                tracing::error!("Failed to update babamul user {}: {}", user_id, e);
                return response::internal_error("Failed to update user");
            }
        };

        tracing::info!(
            admin = %admin.id,
            user = %user.id,
            is_admin = user.is_admin,
            acls = ?user.acls,
            "Babamul user access updated"
        );
        return response::ok_ser("success", BabamulAdminUser::from(user));
    }

    response::conflict("Another admin kept changing this user's access. Reload and try again.")
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINTER: BabamulAcl = BabamulAcl::Winter;
    const PARTNERSHIP: BabamulAcl = BabamulAcl::ZtfPartnership;
    const CALTECH: BabamulAcl = BabamulAcl::ZtfCaltech;

    #[test]
    fn a_delta_keeps_the_acls_it_does_not_mention() {
        // The point of applying a delta: the admin granting WINTER has no idea
        // the user was given partnership access since the page was loaded, and
        // sending the whole list back would revert it.
        assert_eq!(
            apply_acl_delta(&[PARTNERSHIP], &[WINTER], &[]).unwrap(),
            vec![WINTER, PARTNERSHIP]
        );
        assert_eq!(
            apply_acl_delta(&[WINTER, PARTNERSHIP], &[], &[WINTER]).unwrap(),
            vec![PARTNERSHIP]
        );
    }

    #[test]
    fn a_delta_carries_the_ztf_interlock_both_ways() {
        // Granting Caltech access grants the partnership access it requires,
        // and revoking partnership access takes Caltech access with it --
        // rather than refusing over a combination the caller never asked for.
        assert_eq!(
            apply_acl_delta(&[], &[CALTECH], &[]).unwrap(),
            vec![PARTNERSHIP, CALTECH]
        );
        assert_eq!(
            apply_acl_delta(&[PARTNERSHIP, CALTECH], &[], &[PARTNERSHIP]).unwrap(),
            vec![]
        );
    }

    #[test]
    fn granting_and_revoking_the_same_acl_is_refused() {
        // Ambiguous rather than harmless: whichever one won would be arbitrary.
        assert!(apply_acl_delta(&[], &[WINTER], &[WINTER]).is_err());
    }

    /// The same contradiction reached through the interlock instead of
    /// directly, which is the one that used to slip past.
    #[test]
    fn granting_caltech_while_revoking_partnership_is_refused() {
        // It passed the overlap check, and then the interlock removed the
        // Caltech access the request had just asked to grant -- reporting
        // success having done the opposite of half of what was asked.
        assert!(apply_acl_delta(&[], &[CALTECH], &[PARTNERSHIP]).is_err());
        assert!(apply_acl_delta(&[PARTNERSHIP], &[CALTECH], &[PARTNERSHIP]).is_err());
    }

    /// The compare-and-swap filter is the whole protection, so it is worth
    /// testing as a filter rather than inferring it from a race.
    #[test]
    fn the_swap_matches_only_the_list_the_delta_was_computed_from() {
        let eq = |acls: &[BabamulAcl]| mongodb::bson::bson!({ "$eq": mongodb::bson::to_bson(&acls.to_vec()).unwrap() });
        assert_eq!(matching_acls(&[WINTER]).unwrap(), eq(&[WINTER]));
        assert_ne!(matching_acls(&[WINTER]).unwrap(), eq(&[PARTNERSHIP]));
    }

    /// An account from before the field existed has no `acls` at all, and
    /// MongoDB does not consider a missing field equal to `[]`.
    ///
    /// Without this the swap could never match such a document: all three
    /// attempts would see the same empty list, and every delta against one of
    /// those accounts would end in a conflict nothing could resolve.
    #[test]
    fn the_swap_also_matches_a_list_that_was_never_written() {
        let matcher = matching_acls(&[]).unwrap();
        let alternatives = matcher
            .as_document()
            .and_then(|d| d.get_array("$in").ok())
            .expect("an empty list matches either shape");
        assert!(alternatives.contains(&mongodb::bson::Bson::Null));
        assert!(alternatives.contains(&mongodb::bson::Bson::Array(vec![])));
    }

    #[test]
    fn a_delta_is_idempotent_and_deduplicated() {
        assert_eq!(
            apply_acl_delta(&[WINTER], &[WINTER, WINTER], &[]).unwrap(),
            vec![WINTER]
        );
        assert_eq!(apply_acl_delta(&[], &[], &[WINTER]).unwrap(), vec![]);
    }

    #[test]
    fn a_wholesale_list_is_ordered_and_still_checks_the_interlock() {
        assert_eq!(
            normalize_acls(&[CALTECH, WINTER, PARTNERSHIP, WINTER]).unwrap(),
            vec![WINTER, PARTNERSHIP, CALTECH]
        );
        assert!(normalize_acls(&[CALTECH]).is_err());
    }

    /// Only the last admin is protected. The other cases are ordinary edits,
    /// and refusing them would make the admin page unusable.
    #[test]
    fn only_the_last_admin_is_protected() {
        assert!(would_orphan(Some(false), true, 1));
        assert!(would_orphan(Some(false), true, 0));
        assert!(!would_orphan(Some(false), true, 2));
        assert!(!would_orphan(Some(false), false, 1));
        assert!(!would_orphan(Some(true), true, 1));
        assert!(!would_orphan(None, true, 1));
    }
}
