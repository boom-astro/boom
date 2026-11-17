use std::collections::HashMap;

use mongodb::bson::Bson;
use serde_json::{Map, Value};

use crate::{api::catalogs::WATCHLIST_PREFIX, conf::CatalogXmatchConfig};

const CHECKED_FIELDS: [&str; 7] = [
    "candidate",
    "classifications",
    "properties",
    "prv_candidates",
    "prv_nondetections",
    "fp_hists",
    "cross_matches",
];

static ANY: DocumentShape = DocumentShape::Any;

#[derive(Debug, Default)]
pub enum DocumentShape {
    #[default]
    Any,
    Record(HashMap<String, DocumentShape>),
}

type Scope<'a> = HashMap<String, &'a DocumentShape>;

impl DocumentShape {
    pub fn filter_document(schema: &Value, catalogs: &[CatalogXmatchConfig]) -> DocumentShape {
        let mut fields = match from_avro(schema, &mut HashMap::new(), &mut Vec::new()) {
            DocumentShape::Record(fields) => fields,
            DocumentShape::Any => HashMap::new(),
        };
        fields.retain(|name, _| CHECKED_FIELDS.contains(&name.as_str()));
        let catalogs = catalogs
            .iter()
            .filter(|catalog| !catalog.catalog.starts_with(WATCHLIST_PREFIX))
            .map(|catalog| (catalog.catalog.clone(), catalog_row(catalog)))
            .collect();
        fields.insert("cross_matches".to_string(), DocumentShape::Record(catalogs));
        DocumentShape::Record(fields)
    }

    fn has(&self, name: &str) -> bool {
        matches!(self, DocumentShape::Record(fields) if fields.contains_key(name))
    }

    fn at(&self, path: &str) -> Option<&DocumentShape> {
        let mut shape = self;
        for segment in path.split('.') {
            if segment.parse::<usize>().is_ok() {
                continue;
            }
            match shape {
                DocumentShape::Any => return Some(&ANY),
                DocumentShape::Record(fields) => shape = fields.get(segment)?,
            }
        }
        Some(shape)
    }
}

fn catalog_row(catalog: &CatalogXmatchConfig) -> DocumentShape {
    let mut fields = HashMap::new();
    for (key, value) in &catalog.projection {
        if key == "_id" {
            continue;
        }
        if matches!(
            value,
            Bson::Boolean(false) | Bson::Int32(0) | Bson::Int64(0)
        ) || matches!(value, Bson::Double(n) if *n == 0.0)
        {
            return DocumentShape::Any;
        }
        let name = key.split('.').next().unwrap_or(key);
        fields.insert(name.to_string(), DocumentShape::Any);
    }
    if fields.is_empty() {
        return DocumentShape::Any;
    }
    fields.insert("_id".to_string(), DocumentShape::Any);
    fields.insert("distance_arcsec".to_string(), DocumentShape::Any);
    if catalog.distance_key.is_some() {
        fields.insert("distance_kpc".to_string(), DocumentShape::Any);
    }
    DocumentShape::Record(fields)
}

fn from_avro<'a>(
    schema: &'a Value,
    names: &mut HashMap<String, &'a Value>,
    visiting: &mut Vec<String>,
) -> DocumentShape {
    match schema {
        Value::String(name) => match names.get(name).copied() {
            Some(named) if !visiting.contains(name) => from_avro(named, names, visiting),
            _ => DocumentShape::Any,
        },
        Value::Array(variants) => {
            let mut types = variants.iter().filter(|v| v.as_str() != Some("null"));
            match (types.next(), types.next()) {
                (Some(only), None) => from_avro(only, names, visiting),
                _ => DocumentShape::Any,
            }
        }
        Value::Object(object) => {
            let kind = object.get("type").unwrap_or(&Value::Null);
            match kind.as_str() {
                Some("record") => {
                    let fullname = register(object, schema, names);
                    visiting.push(fullname);
                    let mut fields = HashMap::new();
                    for field in object
                        .get("fields")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        if let (Some(name), Some(field_type)) =
                            (field.get("name").and_then(Value::as_str), field.get("type"))
                        {
                            fields.insert(name.to_string(), from_avro(field_type, names, visiting));
                        }
                    }
                    visiting.pop();
                    DocumentShape::Record(fields)
                }
                Some("array") => object.get("items").map_or(DocumentShape::Any, |items| {
                    from_avro(items, names, visiting)
                }),
                Some("enum" | "fixed") => {
                    register(object, schema, names);
                    DocumentShape::Any
                }
                Some("map") => DocumentShape::Any,
                _ => from_avro(kind, names, visiting),
            }
        }
        _ => DocumentShape::Any,
    }
}

fn register<'a>(
    object: &Map<String, Value>,
    schema: &'a Value,
    names: &mut HashMap<String, &'a Value>,
) -> String {
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let fullname = match object.get("namespace").and_then(Value::as_str) {
        Some(namespace) if !name.contains('.') => format!("{namespace}.{name}"),
        _ => name.to_string(),
    };
    names.insert(name.to_string(), schema);
    names.insert(fullname.clone(), schema);
    fullname
}

pub fn find_unknown_field(pipeline: &[Value], document: &DocumentShape) -> Option<String> {
    let mut walker = Walker {
        root: document,
        redefined: Vec::new(),
        unknown: None,
    };
    for stage in pipeline {
        if !walker.stage(stage) || walker.unknown.is_some() {
            break;
        }
    }
    walker.unknown
}

struct Walker<'a> {
    root: &'a DocumentShape,
    redefined: Vec<String>,
    unknown: Option<String>,
}

impl<'a> Walker<'a> {
    fn report(&mut self, path: &str) {
        if self.unknown.is_none() {
            self.unknown = Some(path.to_string());
        }
    }

    fn from_root(&self, path: &str) -> Option<&'a DocumentShape> {
        let first = path.split('.').next().unwrap_or_default();
        let redefined = self.redefined.iter().any(|field| {
            path.strip_prefix(field.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
        });
        // Fields the schema does not describe are the filter's own.
        if redefined || !self.root.has(first) {
            return Some(&ANY);
        }
        self.root.at(path)
    }

    fn check(&mut self, path: &str) {
        if self.from_root(path).is_none() {
            self.report(path);
        }
    }

    fn reference(&mut self, value: &str, scope: &Scope<'a>) {
        if let Some(variable) = value.strip_prefix("$$") {
            let Some((name, path)) = variable.split_once('.') else {
                return;
            };
            if name == "ROOT" || name == "CURRENT" {
                self.check(path);
            } else if let Some(shape) = scope.get(name).copied() {
                if shape.at(path).is_none() {
                    self.report(value);
                }
            }
        } else if let Some(path) = value.strip_prefix('$').filter(|path| !path.is_empty()) {
            self.check(path);
        }
    }

    fn shape_of(&self, value: &Value, scope: &Scope<'a>) -> Option<&'a DocumentShape> {
        match value {
            Value::String(reference) => match reference.strip_prefix("$$") {
                Some(variable) => match variable.split_once('.') {
                    Some(("ROOT" | "CURRENT", path)) => self.from_root(path),
                    Some((name, path)) => scope.get(name).copied()?.at(path),
                    None => scope.get(variable).copied(),
                },
                None => self.from_root(reference.strip_prefix('$')?),
            },
            Value::Object(object) if object.len() == 1 => {
                let (operator, argument) = object.iter().next()?;
                match operator.as_str() {
                    "$filter" | "$sortArray" => self.shape_of(argument.get("input")?, scope),
                    "$slice" | "$ifNull" => self.shape_of(argument.get(0)?, scope),
                    "$reverseArray" => self.shape_of(argument, scope),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn bind(&self, scope: &Scope<'a>, name: &str, value: &Value) -> Scope<'a> {
        let mut inner = scope.clone();
        inner.insert(
            name.to_string(),
            self.shape_of(value, scope).unwrap_or(&ANY),
        );
        inner
    }

    fn expression(&mut self, value: &Value, scope: &Scope<'a>) {
        match value {
            Value::String(reference) => self.reference(reference, scope),
            Value::Array(items) => {
                for item in items {
                    self.expression(item, scope);
                }
            }
            Value::Object(object) => {
                if object.contains_key("$literal") {
                    return;
                }
                let arguments = match object.iter().next() {
                    Some((operator, Value::Object(arguments))) if object.len() == 1 => {
                        Some((operator.as_str(), arguments))
                    }
                    _ => None,
                };
                let input = arguments
                    .and_then(|(_, arguments)| arguments.get("input"))
                    .unwrap_or(&Value::Null);
                match arguments {
                    Some(("$filter" | "$map", arguments)) => {
                        self.expression(input, scope);
                        let name = arguments
                            .get("as")
                            .and_then(Value::as_str)
                            .unwrap_or("this");
                        let inner = self.bind(scope, name, input);
                        for key in ["cond", "in", "limit"] {
                            if let Some(nested) = arguments.get(key) {
                                self.expression(nested, &inner);
                            }
                        }
                    }
                    Some(("$reduce", arguments)) => {
                        self.expression(input, scope);
                        if let Some(initial) = arguments.get("initialValue") {
                            self.expression(initial, scope);
                        }
                        let mut inner = self.bind(scope, "this", input);
                        inner.insert("value".to_string(), &ANY);
                        if let Some(nested) = arguments.get("in") {
                            self.expression(nested, &inner);
                        }
                    }
                    Some(("$let", arguments)) => {
                        let mut inner = scope.clone();
                        if let Some(Value::Object(variables)) = arguments.get("vars") {
                            for (name, nested) in variables {
                                self.expression(nested, scope);
                                let shape = self.shape_of(nested, scope).unwrap_or(&ANY);
                                inner.insert(name.clone(), shape);
                            }
                        }
                        if let Some(nested) = arguments.get("in") {
                            self.expression(nested, &inner);
                        }
                    }
                    _ => {
                        for nested in object.values() {
                            self.expression(nested, scope);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn query(&mut self, query: &Value, base: Option<(&'a DocumentShape, &str)>, scope: &Scope<'a>) {
        let Value::Object(conditions) = query else {
            return;
        };
        for (key, condition) in conditions {
            match key.as_str() {
                "$and" | "$or" | "$nor" => {
                    for item in condition.as_array().into_iter().flatten() {
                        self.query(item, base, scope);
                    }
                }
                "$expr" => self.expression(condition, scope),
                _ if key.starts_with('$') => {}
                _ => {
                    let (shape, path) = match base {
                        None => (self.from_root(key), key.clone()),
                        Some((base, prefix)) => (base.at(key), format!("{prefix}.{key}")),
                    };
                    if shape.is_none() {
                        self.report(&path);
                    }
                    if let Some(element) = condition.get("$elemMatch") {
                        self.query(element, Some((shape.unwrap_or(&ANY), path.as_str())), scope);
                    }
                }
            }
        }
    }

    fn define(&mut self, prefix: &str, fields: &Map<String, Value>, project: bool) -> Vec<String> {
        let mut defined = Vec::new();
        for (key, value) in fields {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            match value {
                Value::Bool(_) | Value::Number(_) if project => self.check(&path),
                Value::Object(nested)
                    if !nested.is_empty() && !nested.keys().any(|name| name.starts_with('$')) =>
                {
                    defined.extend(self.define(&path, nested, project));
                }
                _ => {
                    self.expression(value, &Scope::new());
                    if !keeps_shape(&path, value) {
                        defined.push(path);
                    }
                }
            }
        }
        defined
    }

    fn stage(&mut self, stage: &Value) -> bool {
        let Value::Object(stage) = stage else {
            return true;
        };
        let scope = Scope::new();
        for (operator, argument) in stage {
            match (operator.as_str(), argument) {
                ("$match", _) => self.query(argument, None, &scope),
                ("$project" | "$addFields" | "$set", Value::Object(fields)) => {
                    let defined = self.define("", fields, operator.as_str() == "$project");
                    self.redefined.extend(defined);
                }
                ("$replaceRoot" | "$replaceWith", _) => {
                    self.expression(argument, &scope);
                    return false;
                }
                ("$unset" | "$sort" | "$limit" | "$skip" | "$count" | "$sample", _) => {}
                _ => self.expression(argument, &scope),
            }
        }
        true
    }
}

fn keeps_shape(path: &str, value: &Value) -> bool {
    let source = value
        .get("$filter")
        .and_then(|filter| filter.get("input"))
        .unwrap_or(value);
    source.as_str().and_then(|input| input.strip_prefix('$')) == Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;
    use serde_json::json;

    fn document() -> DocumentShape {
        let schema = json!({
            "type": "record",
            "name": "AlertToFilter",
            "fields": [
                {"name": "candidate", "type": {"type": "record", "name": "Candidate", "fields": [
                    {"name": "jd", "type": "double"},
                    {"name": "magpsf", "type": "float"},
                ]}},
                {"name": "prv_candidates", "type": {"type": "array", "items": "Candidate"}},
                {"name": "fp_hists", "type": {"type": "array", "items": {
                    "type": "record", "name": "ForcedPhot", "fields": [
                        {"name": "jd", "type": "double"},
                        {"name": "magpsf", "type": ["null", "float"]},
                        {"name": "snr_psf", "type": ["null", "float"]},
                    ]}}},
                {"name": "properties", "type": {"type": "record", "name": "Properties", "fields": [
                    {"name": "stationary", "type": "boolean"},
                ]}},
                {"name": "coordinates", "type": {"type": "record", "name": "Coordinates", "fields": [
                    {"name": "l", "type": "double"},
                ]}},
            ],
        });
        let catalogs = [
            CatalogXmatchConfig {
                catalog: "AllWISE".to_string(),
                projection: doc! { "_id": 1, "ra": 1, "w3mpro": 1 },
                ..Default::default()
            },
            CatalogXmatchConfig {
                catalog: "NED".to_string(),
                projection: doc! { "z": 1, "DistMpc": 1 },
                distance_key: Some("z".to_string()),
                ..Default::default()
            },
        ];
        DocumentShape::filter_document(&schema, &catalogs)
    }

    fn unknown(pipeline: Value) -> Option<String> {
        find_unknown_field(pipeline.as_array().unwrap(), &document())
    }

    #[test]
    fn rejects_paths_the_document_does_not_have() {
        let cases = [
            (
                json!([{"$match": {"cross_matches.CLU_20190625.0": {"$exists": true}}}]),
                "cross_matches.CLU_20190625.0",
            ),
            (
                json!([{"$project": {"w3": "$cross_matches.AllWISE.w4mpro"}}]),
                "cross_matches.AllWISE.w4mpro",
            ),
            (
                json!([{"$project": {"fp": {"$filter": {"input": "$fp_hists", "as": "cand",
                    "cond": {"$lt": [{"$subtract": ["$$cand.mag", "$$cand.magerr"]}, 18.5]}}}}}]),
                "$$cand.mag",
            ),
            (
                json!([{"$match": {"fp_hists": {"$elemMatch": {"snr": {"$gt": 5}}}}}]),
                "fp_hists.snr",
            ),
            (
                json!([{"$project": {"prv_candidates.sigmagpsf": 1}}]),
                "prv_candidates.sigmagpsf",
            ),
            (
                json!([{"$match": {"$expr": {"$anyElementTrue": {"$map": {"input": "$prv_candidates",
                    "in": {"$gt": ["$$this.drb", 0.5]}}}}}}]),
                "$$this.drb",
            ),
            (
                json!([{"$match": {"properties.stationnary": true}}]),
                "properties.stationnary",
            ),
            (
                json!([{"$match": {"candidate.jd": {"$gt": 0}}}, {"$project": {"objectId": 1,
                    "bad": {"$ifNull": ["$fp_hists.snr", 0]}}}]),
                "fp_hists.snr",
            ),
        ];
        for (pipeline, path) in cases {
            assert_eq!(
                unknown(pipeline.clone()).as_deref(),
                Some(path),
                "{pipeline}"
            );
        }
    }

    #[test]
    fn accepts_paths_the_document_has_or_the_filter_defines() {
        let pipelines = [
            json!([{"$match": {"cross_matches.NED.0.distance_kpc": {"$lt": 50}}}]),
            json!([{"$project": {"d": "$cross_matches.AllWISE.distance_arcsec", "c": "$coordinates.hpx"}}]),
            json!([{"$addFields": {"fp_hists": {"$map": {"input": "$fp_hists", "as": "p",
                "in": {"mag": "$$p.magpsf"}}}}}, {"$match": {"fp_hists.mag": {"$lt": 19}}}]),
            json!([{"$addFields": {"fp_hists": {"$filter": {"input": "$fp_hists",
                "cond": {"$gt": ["$$this.snr_psf", 5]}}}}}, {"$match": {"fp_hists.jd": {"$gt": 0}}}]),
            json!([{"$project": {"prv_r": "$prv_candidates"}}, {"$match": {"prv_r.anything": 1}}]),
            json!([{"$match": {"$expr": {"$let": {"vars": {"c": "$candidate"}, "in": {"$gt": ["$$c.jd", 0]}}}}}]),
            json!([{"$project": {"annotations.mag": {"$round": ["$candidate.magpsf", 2]}, "properties.stationary": 1}}]),
        ];
        for pipeline in pipelines {
            assert_eq!(unknown(pipeline.clone()), None, "{pipeline}");
        }
    }
}
