//! Conservative inventory, not a proof that a bounded window preserves a filter.
use crate::filter::uses_field_in_filter;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;

/// Read a pipeline/export JSON file, a JSONL export, or a directory of either.
/// Only summaries leave this function; pipeline bodies stay in the input files.
pub fn inventory(path: &std::path::Path) -> anyhow::Result<Vec<Value>> {
    use anyhow::Context;
    let mut paths = if path.is_dir() {
        std::fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?
    } else {
        vec![path.to_owned()]
    };
    paths.retain(|path| {
        path.extension()
            .is_some_and(|ext| ext == "json" || ext == "jsonl")
    });
    paths.sort();
    let mut result = Vec::new();
    for path in paths {
        let text = std::fs::read_to_string(&path)?;
        let values: Vec<Value> = if path.extension().is_some_and(|ext| ext == "jsonl") {
            text.lines()
                .enumerate()
                .filter(|(_, line)| !line.trim().is_empty())
                .map(|(index, line)| {
                    serde_json::from_str(line)
                        .with_context(|| format!("{}:{}", path.display(), index + 1))
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        } else {
            vec![serde_json::from_str(&text).with_context(|| path.display().to_string())?]
        };
        for value in values {
            for mut row in exported(&value)? {
                let name = path
                    .file_name()
                    .context("missing filter file name")?
                    .to_string_lossy();
                row["file"] = serde_json::json!(name);
                row["filter"] = serde_json::json!(name);
                result.push(row);
            }
        }
    }
    Ok(result)
}

#[derive(Debug, Serialize)]
pub struct FilterAudit {
    pub classification: &'static str,
    pub history_fields: Vec<(String, usize)>,
    pub dynamic_access: Vec<String>,
    pub automatic_window_approval: bool,
}

/// Accept a pipeline, one exported BOOM filter, or an array of exported filters.
/// Select active_fid from fv rather than auditing inactive historical versions.
pub fn exported(value: &Value) -> anyhow::Result<Vec<Value>> {
    use anyhow::{ensure, Context};
    fn stages(value: &Value) -> anyhow::Result<Vec<Value>> {
        let value = if let Some(text) = value.as_str() {
            serde_json::from_str(text)?
        } else {
            value.clone()
        };
        value
            .as_array()
            .cloned()
            .context("filter pipeline must be an array")
    }
    if let Some(array) = value.as_array() {
        if array.is_empty() {
            return Ok(Vec::new());
        }
        if array.iter().all(|v| {
            v.as_object()
                .is_some_and(|m| m.len() == 1 && m.keys().all(|k| k.starts_with('$')))
        }) {
            return Ok(vec![serde_json::json!({"audit":pipeline(array)})]);
        }
        let mut rows = Vec::new();
        for filter in array {
            rows.extend(exported(filter)?);
        }
        return Ok(rows);
    }
    let object = value
        .as_object()
        .context("expected a pipeline or exported filter document")?;
    if object.get("active") == Some(&Value::Bool(false)) {
        return Ok(Vec::new());
    }
    let active = if let Some(versions) = object.get("fv").and_then(Value::as_array) {
        let id = object
            .get("active_fid")
            .and_then(Value::as_str)
            .context("export has no active_fid")?;
        versions
            .iter()
            .find(|v| v["fid"].as_str() == Some(id))
            .context("active filter version is missing")?
    } else {
        value
    };
    let stages = stages(active.get("pipeline").context("export has no pipeline")?)?;
    ensure!(
        stages.iter().all(Value::is_object),
        "pipeline stages must be objects"
    );
    Ok(vec![
        serde_json::json!({"id":value.get("_id"),"name":value.get("name"),"active_fid":value.get("active_fid"),"audit":pipeline(&stages)}),
    ])
}

pub fn pipeline(stages: &[Value]) -> FilterAudit {
    fn dynamic(value: &Value, found: &mut BTreeSet<String>) {
        match value {
            Value::String(s)
                if s == "$$ROOT"
                    || s.starts_with("$$ROOT.")
                    || s == "$$CURRENT"
                    || s.starts_with("$$CURRENT.") =>
            {
                found.insert(s.clone());
            }
            Value::Object(map) => {
                for (key, value) in map {
                    if [
                        "$getField",
                        "$setField",
                        "$function",
                        "$accumulator",
                        "$objectToArray",
                        "$lookup",
                        "$unionWith",
                    ]
                    .contains(&key.as_str())
                    {
                        found.insert(key.clone());
                    }
                    dynamic(value, found);
                }
            }
            Value::Array(values) => {
                for value in values {
                    dynamic(value, found);
                }
            }
            _ => (),
        }
    }
    let mut fields = Vec::new();
    for prefix in ["", "ZTF.", "LSST.", "DECAM.", "WINTER."] {
        for field in ["prv_candidates", "prv_nondetections", "fp_hists"] {
            let path = format!("{prefix}{field}");
            if let Some(stage) = uses_field_in_filter(stages, &path) {
                fields.push((path, stage));
            }
        }
    }
    let mut access = BTreeSet::new();
    for stage in stages {
        dynamic(stage, &mut access);
    }
    let classification = if !access.is_empty() {
        "dynamic_review_required"
    } else if !fields.is_empty() {
        "history_review_required"
    } else {
        "no_history_reference_detected"
    };
    FilterAudit {
        classification,
        history_fields: fields,
        dynamic_access: access.into_iter().collect(),
        automatic_window_approval: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn jsonl_inventory_selects_active_versions_and_skips_inactive_filters() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("active.jsonl");
        let active = json!({"active":true,"active_fid":"current","fv":[
            {"fid":"old","pipeline":"[{\"$set\":{\"x\":\"$fp_hists\"}}]"},
            {"fid":"current","pipeline":"[{\"$match\":{\"candidate.drb\":{\"$gt\":0.9}}}]"}]});
        let inactive = json!({"active":false});
        std::fs::write(&path, format!("{active}\n\n{inactive}\n")).unwrap();
        let rows = inventory(&path).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]["audit"]["classification"],
            "no_history_reference_detected"
        );
        assert_eq!(rows[0]["file"], "active.jsonl");
        std::fs::write(&path, "invalid export\n").unwrap();
        assert!(inventory(&path)
            .unwrap_err()
            .to_string()
            .ends_with("active.jsonl:1"));
    }

    #[test]
    fn inventory_is_conservative_about_windows_and_dynamic_access() {
        let ordinary = pipeline(&[json!({"$match":{"candidate.drb":{"$gt":0.9}}})]);
        assert_eq!(ordinary.classification, "no_history_reference_detected");
        let history = pipeline(&[json!({"$set":{"n":{"$size":"$fp_hists"}}})]);
        assert_eq!(history.history_fields, vec![("fp_hists".into(), 0)]);
        assert!(!history.automatic_window_approval);
        let dynamic = pipeline(&[json!({"$project":{"all":"$$ROOT"}})]);
        assert_eq!(dynamic.classification, "dynamic_review_required");
    }

    #[test]
    fn exports_use_only_the_selected_active_version() {
        let filter = json!({"_id":"example","active_fid":"new","fv":[
            {"fid":"old","pipeline":"[{\"$set\":{\"n\":\"$fp_hists\"}}]"},
            {"fid":"new","pipeline":"[{\"$match\":{\"candidate.drb\":{\"$gt\":0.9}}}]"}
        ]});
        let rows = exported(&json!([filter])).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]["audit"]["classification"],
            "no_history_reference_detected"
        );
        assert_eq!(rows[0]["active_fid"], "new");
        assert!(exported(&json!({"fv":[],"active_fid":"missing"})).is_err());
        assert!(exported(&json!({"active":false})).unwrap().is_empty());
    }
}
