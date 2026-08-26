/// Endpoints for executing analytical queries.
use crate::api::catalogs::catalog_accessible;
use crate::api::filters::parse_filter;
use crate::api::models::response;
use crate::api::routes::users::User;

use crate::utils::mpcorb::{
    fetch_orbits, fill_geometry, normalize_ztf_ssnamenr, ORBITS_COLLECTION,
};

use actix_web::{post, web, HttpResponse};
use futures::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Database,
};
use std::collections::HashSet;
use utoipa::ToSchema;

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
struct FindQuery {
    catalog_name: String,
    filter: serde_json::Value,
    projection: Option<serde_json::Value>,
    limit: Option<i64>,
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
        if let Some(limit) = self.limit {
            options.limit = Some(limit);
        }
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
    fill_sso_geometry(&db, &collection_name, &mut docs).await;
    response::ok_ser("success", &docs)
}

/// Collection whose documents carry a derivable `properties.sso` block.
const ZTF_ALERTS: &str = "ZTF_alerts";

/// Derive `properties.sso` geometry for results that predate enrichment writing it.
///
/// The alternative to doing this on read is a backfill across the whole alert
/// collection; geometry is a pure function of designation and epoch, so reading
/// is enough. Callers that did not project `properties.sso` and `candidate.jd`
/// are untouched.
async fn fill_sso_geometry(db: &Database, collection_name: &str, docs: &mut [Document]) {
    // LSST reads the equivalent quantities from vectors in its own packet.
    if collection_name != ZTF_ALERTS {
        return;
    }

    let keys: Vec<String> = docs
        .iter()
        .filter_map(sso_designation_needing_geometry)
        .filter_map(|d| normalize_ztf_ssnamenr(&d))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if keys.is_empty() {
        return;
    }

    let elements = match fetch_orbits(&db.collection(ORBITS_COLLECTION), &keys).await {
        Ok(elements) => elements,
        // Geometry is an enhancement on this endpoint: return the documents as
        // stored rather than failing the query.
        Err(e) => {
            tracing::warn!("could not read {} for find query: {}", ORBITS_COLLECTION, e);
            return;
        }
    };

    for doc in docs.iter_mut() {
        let Some(designation) = sso_designation_needing_geometry(doc) else {
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
            fill_geometry(sso, &designation, jd, &elements);
        }
    }
}

/// The designation of a document whose `properties.sso` lacks geometry.
fn sso_designation_needing_geometry(doc: &Document) -> Option<String> {
    let sso = doc
        .get_document("properties")
        .ok()?
        .get_document("sso")
        .ok()?;
    if sso.get_f64("helio_dist").is_ok() {
        return None;
    }
    sso.get_str("designation").ok().map(str::to_string)
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

    #[test]
    fn test_bare_sso_document_is_selected() {
        let doc = alert("9816", 2_461_272.5, false);
        assert_eq!(
            sso_designation_needing_geometry(&doc).as_deref(),
            Some("9816")
        );
    }

    #[test]
    fn test_document_with_geometry_is_skipped() {
        let doc = alert("9816", 2_461_272.5, true);
        assert!(sso_designation_needing_geometry(&doc).is_none());
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
            assert!(sso_designation_needing_geometry(&doc).is_none());
        }
    }

    // An SSO block with no designation cannot be resolved to elements.
    #[test]
    fn test_sso_block_without_a_designation_is_skipped() {
        let doc = doc! { "properties": { "sso": { "is_sso": false } } };
        assert!(sso_designation_needing_geometry(&doc).is_none());
    }
}
