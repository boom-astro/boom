//! Keeping credentials out of the places task parameters are read back from.
//!
//! Some tasks legitimately take a connection URI -- a one-off copy between two
//! clusters has to name both ends somehow. But a URI carries a password, and
//! task parameters are stored on the run, rendered on the admin page, and
//! copied into the append-only ledger.
//!
//! The worker needs the real value, so `task_runs.params` holds it as given.
//! Everywhere it is *read back* it goes through here first: the API responses
//! the admin page renders, and the ledger, which is never edited or deleted and
//! would otherwise archive a password forever.

use mongodb::bson::{Bson, Document};

/// What replaces a password in a redacted URI.
const MASK: &str = "***";

/// Mask the password in a `scheme://user:password@host/...` URI.
///
/// Leaves everything else intact, so a redacted URI still says which host and
/// database a run touched -- which is most of why anyone reads it back.
pub fn redact_uri(value: &str) -> String {
    // Only the authority section can carry credentials, and only before the
    // first '/' after the scheme.
    let Some((scheme, rest)) = value.split_once("://") else {
        return value.to_string();
    };
    let (authority, tail) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let Some((userinfo, host)) = authority.rsplit_once('@') else {
        return value.to_string();
    };
    let user = userinfo.split_once(':').map(|(u, _)| u).unwrap_or(userinfo);
    format!("{scheme}://{user}:{MASK}@{host}{tail}")
}

/// Whether a parameter name looks like it carries a connection string.
///
/// Matching on the name rather than the value: a value that merely looks like a
/// URI might be a catalog source URL, which is not a secret and is useful to
/// read back in full.
fn is_uri_field(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.ends_with("_uri") || key == "uri"
}

/// Redact every connection URI in a parameter document.
pub fn redact_params(params: &serde_json::Value) -> serde_json::Value {
    match params {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let value = match value {
                        serde_json::Value::String(s) if is_uri_field(key) => {
                            serde_json::Value::String(redact_uri(s))
                        }
                        other => redact_params(other),
                    };
                    (key.clone(), value)
                })
                .collect(),
        ),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(redact_params).collect())
        }
        other => other.clone(),
    }
}

/// The same, for the BSON documents the ledger stores.
pub fn redact_document(details: &Document) -> Document {
    details
        .iter()
        .map(|(key, value)| {
            let value = match value {
                Bson::String(s) if is_uri_field(key) => Bson::String(redact_uri(s)),
                Bson::Document(d) => Bson::Document(redact_document(d)),
                other => other.clone(),
            };
            (key.clone(), value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_is_masked_but_the_endpoint_survives() {
        // Which host and database a run touched is most of why anyone reads the
        // parameters back, so redaction must not remove that.
        assert_eq!(
            redact_uri("mongodb://alice:hunter2@db.example.org:27017/boom"),
            "mongodb://alice:***@db.example.org:27017/boom"
        );
    }

    #[test]
    fn a_uri_without_credentials_is_untouched() {
        for uri in [
            "mongodb://localhost:27017/boom",
            "redis://valkey:6379/",
            "https://quasars.org/milliquas.fits.zip",
        ] {
            assert_eq!(redact_uri(uri), uri);
        }
    }

    #[test]
    fn a_userinfo_with_no_password_keeps_its_shape() {
        assert_eq!(
            redact_uri("mongodb://alice@db.example.org/boom"),
            "mongodb://alice:***@db.example.org/boom"
        );
    }

    #[test]
    fn an_at_sign_in_the_path_is_not_mistaken_for_credentials() {
        // rsplit_once on the authority only, so a path can contain '@'.
        assert_eq!(
            redact_uri("mongodb://localhost:27017/db/a@b"),
            "mongodb://localhost:27017/db/a@b"
        );
    }

    #[test]
    fn only_uri_shaped_fields_are_redacted() {
        // A catalog's source URL is not a secret and is useful in full.
        let params = serde_json::json!({
            "src_uri": "mongodb://u:p@host/db",
            "url": "https://example.org/catalog.fits",
            "batch_size": 100,
        });
        let redacted = redact_params(&params);
        assert_eq!(redacted["src_uri"], "mongodb://u:***@host/db");
        assert_eq!(redacted["url"], "https://example.org/catalog.fits");
        assert_eq!(redacted["batch_size"], 100);
    }

    #[test]
    fn nested_parameters_are_redacted_too() {
        let params = serde_json::json!({
            "endpoints": { "dst_uri": "mongodb://u:p@host/db" },
            "list": [{ "uri": "mongodb://u:p@host/db" }],
        });
        let redacted = redact_params(&params);
        assert_eq!(redacted["endpoints"]["dst_uri"], "mongodb://u:***@host/db");
        assert_eq!(redacted["list"][0]["uri"], "mongodb://u:***@host/db");
    }

    #[test]
    fn ledger_details_are_redacted_by_the_same_rule() {
        let details = mongodb::bson::doc! {
            "src_uri": "mongodb://u:p@host/db",
            "nested": { "dst_uri": "mongodb://u:p@host/db" },
        };
        let redacted = redact_document(&details);
        assert_eq!(
            redacted.get_str("src_uri").unwrap(),
            "mongodb://u:***@host/db"
        );
        assert_eq!(
            redacted
                .get_document("nested")
                .unwrap()
                .get_str("dst_uri")
                .unwrap(),
            "mongodb://u:***@host/db"
        );
    }
}
