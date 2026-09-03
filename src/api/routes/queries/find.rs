/// Endpoints for executing analytical queries.
use crate::api::catalogs::catalog_accessible;
use crate::api::filters::parse_filter;
use crate::api::models::response;
use crate::api::routes::users::User;

use crate::utils::mpcorb::{
    fetch_orbits, fill_geometry, normalize_ztf_ssnamenr, GEOMETRY_FIELDS, ORBITS_COLLECTION,
};

use actix_web::{post, web, HttpResponse};
use futures::StreamExt;
use mongodb::{
    bson::{doc, Bson, Document},
    Database,
};
use std::collections::{HashMap, HashSet};
use utoipa::ToSchema;

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
struct FindQuery {
    catalog_name: String,
    filter: serde_json::Value,
    projection: Option<serde_json::Value>,
    limit: u32,
    skip: Option<u64>,
    sort: Option<serde_json::Value>,
    max_time_ms: Option<u64>,
}
impl FindQuery {
    /// Convert to MongoDB Find options
    fn to_find_options(&self) -> Result<mongodb::options::FindOptions, String> {
        let mut options = mongodb::options::FindOptions::default();
        if let Some(projection) = &self.projection {
            options.projection = match mongodb::bson::to_document(projection) {
                Ok(doc) => Some(doc),
                Err(e) => {
                    return Err(format!(
                        "Error converting projection to BSON document: {:?}",
                        e
                    ));
                }
            }
        }
        // assert that limit is a positive integer < 100_000
        if self.limit == 0 || self.limit > 100_000 {
            return Err(
                "Limit must be a positive integer less than or equal to 100,000".to_string(),
            );
        }
        options.limit = Some(self.limit as i64);
        if let Some(skip) = self.skip {
            options.skip = Some(skip);
        }
        if let Some(sort) = &self.sort {
            options.sort = match mongodb::bson::to_document(sort) {
                Ok(doc) => Some(doc),
                Err(e) => {
                    return Err(format!("Error converting sort to BSON document: {:?}", e));
                }
            }
        }
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        } else {
            options.max_time = Some(std::time::Duration::from_secs(30)); // Default max time
        }
        Ok(options)
    }
}

/// Run a find query on a catalog
#[utoipa::path(
    post,
    path = "/queries/find",
    request_body = FindQuery,
    responses(
        (status = 200, description = "Documents found in the catalog", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/find")]
pub async fn post_find_query(
    db: web::Data<Database>,
    body: web::Json<FindQuery>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    let catalog_name = body.catalog_name.trim();
    if !catalog_accessible(&db, catalog_name, Some(&current_user)).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Find documents with the provided filter
    let filter = match parse_filter(&body.filter) {
        Ok(filter) => filter,
        Err(e) => return response::bad_request(&format!("Invalid filter: {}", e)),
    };
    let find_options = match body.to_find_options() {
        Ok(options) => options,
        Err(e) => return response::bad_request(&format!("Invalid find options: {}", e)),
    };
    let projection = find_options.projection.clone();
    // Deriving geometry needs the designation and the epoch, so ask for them
    // whenever a geometry field is wanted and strip them from the response after.
    let mut find_options = find_options;
    let injected = inject_geometry_inputs(&collection_name, projection.as_ref(), &mut find_options);
    let mut cursor = match collection.find(filter).with_options(find_options).await {
        Ok(cursor) => cursor,
        Err(e) => return response::internal_error(&format!("Error finding documents: {}", e)),
    };
    let mut docs = Vec::new();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => docs.push(doc),
            Err(e) => {
                tracing::error!("Error retrieving document from the database: {}", e);
                return response::internal_error("Error retrieving document from the database");
            }
        }
    }
    fill_sso_geometry(&db, &collection_name, projection.as_ref(), &mut docs).await;
    for doc in docs.iter_mut() {
        remove_injected(doc, &injected);
    }
    response::ok_ser("success", &docs)
}

/// Collection whose documents carry a derivable `properties.sso` block.
const ZTF_ALERTS: &str = "ZTF_alerts";

/// Derive `properties.sso` geometry for results that predate enrichment writing it.
///
/// The alternative to doing this on read is a backfill across the whole alert
/// collection; geometry is a pure function of designation and epoch, so reading
/// is enough. Only fields the projection asks for are written, and a result
/// without `properties.sso.designation` or `candidate.jd` is left alone.
async fn fill_sso_geometry(
    db: &Database,
    collection_name: &str,
    projection: Option<&Document>,
    docs: &mut [Document],
) {
    // LSST reads the equivalent quantities from vectors in its own packet.
    if collection_name != ZTF_ALERTS {
        return;
    }
    let requested = requested_geometry(projection);
    if requested.is_empty() {
        return;
    }

    // Resolve each designation once, and skip any with no MPCORB form.
    let mut keys: HashMap<String, String> = HashMap::new();
    for doc in docs.iter() {
        let Some(designation) = sso_designation_needing_geometry(doc, &requested) else {
            continue;
        };
        if keys.contains_key(&designation) {
            continue;
        }
        if let Some(key) = normalize_ztf_ssnamenr(&designation) {
            keys.insert(designation, key);
        }
    }
    if keys.is_empty() {
        return;
    }

    let wanted: Vec<String> = keys
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let elements = match fetch_orbits(&db.collection(ORBITS_COLLECTION), &wanted).await {
        Ok(elements) => elements,
        // Geometry is an enhancement on this endpoint: return the documents as
        // stored rather than failing the query.
        Err(e) => {
            tracing::warn!("could not read {} for find query: {}", ORBITS_COLLECTION, e);
            return;
        }
    };

    for doc in docs.iter_mut() {
        let Some(designation) = sso_designation_needing_geometry(doc, &requested) else {
            continue;
        };
        let Some(key) = keys.get(&designation) else {
            continue;
        };
        let Some(jd) = doc
            .get_document("candidate")
            .ok()
            .and_then(|c| c.get_f64("jd").ok())
        else {
            continue;
        };
        if let Ok(sso) = doc
            .get_document_mut("properties")
            .and_then(|p| p.get_document_mut("sso"))
        {
            if fill_geometry(sso, key, jd, &elements) {
                // The three are derived together, so drop the ones the caller
                // did not project.
                for field in GEOMETRY_FIELDS.iter().filter(|f| !requested.contains(f)) {
                    sso.remove(field);
                }
            }
        }
    }
}

/// Inputs `fill_sso_geometry` needs that a caller may not have projected.
const GEOMETRY_INPUTS: [&str; 2] = ["properties.sso.designation", "candidate.jd"];

/// Add the geometry inputs to an inclusion projection, returning the paths added.
///
/// An inclusion projection returns only what it names, so a request for
/// `helio_dist` alone would arrive with no designation or epoch to derive from.
/// An exclusion projection already returns them unless it names them, and
/// un-excluding a field the caller explicitly removed would override the request.
fn inject_geometry_inputs(
    collection_name: &str,
    projection: Option<&Document>,
    options: &mut mongodb::options::FindOptions,
) -> Vec<&'static str> {
    if collection_name != ZTF_ALERTS || requested_geometry(projection).is_empty() {
        return Vec::new();
    }
    let Some(projection) = projection else {
        return Vec::new();
    };
    if is_exclusion(projection) {
        return Vec::new();
    }

    let missing: Vec<&'static str> = GEOMETRY_INPUTS
        .iter()
        .copied()
        .filter(|path| !projected(projection, path).unwrap_or(false))
        .collect();
    if missing.is_empty() {
        return Vec::new();
    }
    let mut augmented = projection.clone();
    for path in &missing {
        augmented.insert(*path, 1_i32);
    }
    options.projection = Some(augmented);
    missing
}

/// Drop the paths added by [`inject_geometry_inputs`], and any subdocument left
/// empty by their removal.
fn remove_injected(doc: &mut Document, injected: &[&str]) {
    for path in injected {
        let mut parts: Vec<&str> = path.split('.').collect();
        let Some(leaf) = parts.pop() else { continue };

        let mut current: Option<&mut Document> = Some(doc);
        for part in &parts {
            current = current.and_then(|d| d.get_document_mut(*part).ok());
        }
        let Some(parent) = current else { continue };
        parent.remove(leaf);
        let parent_emptied = parent.is_empty();

        // A subdocument that exists only to carry an injected field is noise.
        if parent_emptied && parts.len() == 1 {
            doc.remove(parts[0]);
        }
    }
}

/// The designation of a document whose `properties.sso` is missing a requested
/// geometry field.
fn sso_designation_needing_geometry(doc: &Document, requested: &[&str]) -> Option<String> {
    let sso = doc
        .get_document("properties")
        .ok()?
        .get_document("sso")
        .ok()?;
    if requested.iter().all(|f| sso.get_f64(f).is_ok()) {
        return None;
    }
    sso.get_str("designation").ok().map(str::to_string)
}

/// Geometry fields the projection would have returned had they been stored.
///
/// Results come back already projected, so a caller that asked only for, say,
/// the designation must not be handed geometry alongside it.
fn requested_geometry(projection: Option<&Document>) -> Vec<&'static str> {
    let Some(projection) = projection else {
        return GEOMETRY_FIELDS.to_vec();
    };
    // An exclusion projection returns everything it does not name.
    let exclusion = is_exclusion(projection);
    GEOMETRY_FIELDS
        .iter()
        .copied()
        .filter(|field| {
            projected(projection, &format!("properties.sso.{}", field)).unwrap_or(exclusion)
        })
        .collect()
}

/// Whether every field a projection names is turned off. An empty projection
/// excludes nothing and so returns the whole document.
fn is_exclusion(projection: &Document) -> bool {
    projection
        .iter()
        .filter(|(key, _)| key.as_str() != "_id")
        .all(|(_, value)| match value {
            Bson::Document(sub) => is_exclusion(sub),
            other => !is_included(other),
        })
}

/// Whether the projection includes or excludes `path`, or `None` if it does not
/// name it. Handles both the dotted and the nested spelling.
fn projected(projection: &Document, path: &str) -> Option<bool> {
    for (key, value) in projection {
        let Some(rest) = strip_prefix_path(path, key) else {
            continue;
        };
        return match value {
            Bson::Document(sub) if !rest.is_empty() => projected(sub, rest),
            Bson::Document(_) => Some(true),
            other => Some(is_included(other)),
        };
    }
    None
}

/// The remainder of `path` below `key`, or `None` if `key` does not cover it.
fn strip_prefix_path<'a>(path: &'a str, key: &str) -> Option<&'a str> {
    if path == key {
        return Some("");
    }
    path.strip_prefix(key)?.strip_prefix('.')
}

/// Projection values MongoDB reads as "include".
fn is_included(value: &Bson) -> bool {
    match value {
        Bson::Boolean(b) => *b,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(d) => *d != 0.0,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alert(designation: &str, jd: f64, with_geometry: bool) -> Document {
        let mut sso = doc! { "is_sso": true, "designation": designation };
        if with_geometry {
            sso.insert("helio_dist", 1.0_f64);
            sso.insert("topo_dist", 2.0_f64);
            sso.insert("phase_angle", 3.0_f64);
        }
        doc! { "candidate": { "jd": jd }, "properties": { "sso": sso } }
    }

    fn all_fields() -> Vec<&'static str> {
        GEOMETRY_FIELDS.to_vec()
    }

    #[test]
    fn test_bare_sso_document_is_selected() {
        let doc = alert("9816", 2_461_272.5, false);
        assert_eq!(
            sso_designation_needing_geometry(&doc, &all_fields()).as_deref(),
            Some("9816")
        );
    }

    #[test]
    fn test_document_with_geometry_is_skipped() {
        let doc = alert("9816", 2_461_272.5, true);
        assert!(sso_designation_needing_geometry(&doc, &all_fields()).is_none());
    }

    // A block missing only some of the fields still needs the derivation.
    #[test]
    fn test_partially_populated_document_is_selected() {
        let mut doc = alert("9816", 2_461_272.5, true);
        doc.get_document_mut("properties")
            .unwrap()
            .get_document_mut("sso")
            .unwrap()
            .remove("phase_angle");
        assert_eq!(
            sso_designation_needing_geometry(&doc, &all_fields()).as_deref(),
            Some("9816")
        );
    }

    // A non-SSO alert, or one projected without properties.sso, must not be
    // selected -- the endpoint is generic and most traffic is neither.
    #[test]
    fn test_documents_without_an_sso_block_are_skipped() {
        for doc in [
            doc! { "candidate": { "jd": 2_461_272.5 } },
            doc! { "properties": { "rock": false } },
            doc! {},
        ] {
            assert!(sso_designation_needing_geometry(&doc, &all_fields()).is_none());
        }
    }

    // An SSO block with no designation cannot be resolved to elements.
    #[test]
    fn test_sso_block_without_a_designation_is_skipped() {
        let doc = doc! { "properties": { "sso": { "is_sso": false } } };
        assert!(sso_designation_needing_geometry(&doc, &all_fields()).is_none());
    }

    // Results arrive already projected, so an absent geometry field means the
    // caller did not ask for it rather than that it needs deriving.
    #[test]
    fn test_projection_that_omits_geometry_requests_none() {
        for projection in [
            doc! { "properties.sso.designation": 1, "candidate.jd": 1 },
            doc! { "objectId": 1 },
            doc! { "properties": { "sso": { "designation": 1 } } },
        ] {
            assert!(requested_geometry(Some(&projection)).is_empty());
        }
    }

    #[test]
    fn test_projection_that_covers_geometry_requests_all() {
        for projection in [
            doc! { "properties.sso": 1 },
            doc! { "properties": 1 },
            doc! { "properties": { "sso": 1 } },
            doc! { "candidate.jd": 0 },
            doc! {},
        ] {
            assert_eq!(requested_geometry(Some(&projection)), GEOMETRY_FIELDS);
        }
        assert_eq!(requested_geometry(None), GEOMETRY_FIELDS);
    }

    #[test]
    fn test_projection_selects_individual_fields() {
        let projection = doc! { "properties.sso.helio_dist": 1, "candidate.jd": 1 };
        assert_eq!(requested_geometry(Some(&projection)), vec!["helio_dist"]);

        let projection = doc! { "properties.sso.phase_angle": 0 };
        assert_eq!(
            requested_geometry(Some(&projection)),
            vec!["helio_dist", "topo_dist"]
        );
    }
}

#[cfg(test)]
mod injection_tests {
    use super::*;

    fn options(projection: Option<Document>) -> mongodb::options::FindOptions {
        let mut o = mongodb::options::FindOptions::default();
        o.projection = projection;
        o
    }

    // Geometry is derived from the designation and the epoch, so asking for a
    // geometry field alone must still fetch them.
    #[test]
    fn test_geometry_alone_pulls_in_its_inputs() {
        let proj = doc! { "properties.sso.helio_dist": 1 };
        let mut o = options(Some(proj.clone()));
        let injected = inject_geometry_inputs(ZTF_ALERTS, Some(&proj), &mut o);
        assert_eq!(injected, GEOMETRY_INPUTS.to_vec());
        let sent = o.projection.expect("projection augmented");
        assert!(sent.contains_key("properties.sso.designation"));
        assert!(sent.contains_key("candidate.jd"));
        assert!(sent.contains_key("properties.sso.helio_dist"));
    }

    #[test]
    fn test_inputs_the_caller_already_asked_for_are_not_injected() {
        let proj = doc! {
            "properties.sso.helio_dist": 1,
            "properties.sso.designation": 1,
            "candidate.jd": 1,
        };
        let mut o = options(Some(proj.clone()));
        assert!(inject_geometry_inputs(ZTF_ALERTS, Some(&proj), &mut o).is_empty());
    }

    #[test]
    fn test_no_geometry_requested_means_no_injection() {
        let proj = doc! { "objectId": 1 };
        let mut o = options(Some(proj.clone()));
        assert!(inject_geometry_inputs(ZTF_ALERTS, Some(&proj), &mut o).is_empty());
    }

    // An exclusion projection already returns the inputs unless it names them,
    // and un-excluding one would override what the caller asked for.
    #[test]
    fn test_exclusion_projection_is_left_alone() {
        let proj = doc! { "cutoutScience": 0 };
        let mut o = options(Some(proj.clone()));
        assert!(inject_geometry_inputs(ZTF_ALERTS, Some(&proj), &mut o).is_empty());
    }

    #[test]
    fn test_other_catalogs_are_left_alone() {
        let proj = doc! { "properties.sso.helio_dist": 1 };
        let mut o = options(Some(proj.clone()));
        assert!(inject_geometry_inputs("LSST_alerts", Some(&proj), &mut o).is_empty());
    }

    // What the caller did not ask for must not reach the response.
    #[test]
    fn test_injected_inputs_are_stripped_from_the_response() {
        let mut doc = doc! {
            "candidate": { "jd": 2_461_272.5 },
            "properties": { "sso": { "designation": "1", "helio_dist": 2.7 } },
        };
        remove_injected(&mut doc, &GEOMETRY_INPUTS);

        let sso = doc
            .get_document("properties")
            .unwrap()
            .get_document("sso")
            .unwrap();
        assert!(sso.contains_key("helio_dist"), "requested field is kept");
        assert!(
            !sso.contains_key("designation"),
            "injected field is dropped"
        );
        // `candidate` existed only to carry the injected epoch.
        assert!(!doc.contains_key("candidate"));
    }

    #[test]
    fn test_stripping_keeps_a_subdocument_the_caller_asked_for() {
        let mut doc = doc! {
            "candidate": { "jd": 2_461_272.5, "magpsf": 18.1 },
            "properties": { "sso": { "designation": "1", "helio_dist": 2.7 } },
        };
        remove_injected(&mut doc, &GEOMETRY_INPUTS);
        let candidate = doc.get_document("candidate").expect("candidate kept");
        assert!(candidate.contains_key("magpsf"));
        assert!(!candidate.contains_key("jd"));
    }
}
