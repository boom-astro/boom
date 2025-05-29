use mongodb::bson::doc;
use std::{collections::HashMap, fmt};

#[derive(serde::Deserialize, Clone)]
pub struct InfoQueryBody {
    pub command: Option<String>,
    pub catalogs: Option<Vec<String>>,
}

#[derive(serde::Deserialize, Clone)]
pub enum Unit {
    Degrees,
    Radians,
    Arcseconds,
    Arcminutes,
}

impl fmt::Debug for Unit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Unit::Degrees => {
                write!(f, "{}", "Degrees")
            }
            Unit::Radians => {
                write!(f, "{}", "Radians")
            }
            Unit::Arcseconds => {
                write!(f, "{}", "Arcseconds")
            }
            Unit::Arcminutes => {
                write!(f, "{}", "Arcminutes")
            }
        }
    }
}

// TODO: update to multi-catalog (later)
#[derive(serde::Deserialize, Clone)]
pub struct ConeSearchBody {
    pub radius: Option<f64>,
    pub unit: Option<Unit>,
    pub object_coordinates: Option<HashMap<String, [f64; 2]>>,
    pub catalog: Option<CatalogDetails>,
    pub kwargs: Option<QueryKwargs>,
}

#[derive(serde::Deserialize, Clone)]
pub struct CatalogDetails {
    pub catalog_name: Option<String>,
    pub filter: Option<mongodb::bson::Document>,
    pub projection: Option<mongodb::bson::Document>,
}

#[derive(serde::Deserialize, Clone)]
pub struct Query {
    pub object_coordinates: Option<HashMap<String, [f64; 2]>>,
    pub catalog: Option<String>,
    pub filter: Option<mongodb::bson::Document>,
    pub projection: Option<mongodb::bson::Document>,
    pub size: Option<i64>,
}

impl Default for Query {
    fn default() -> Query {
        Query {
            object_coordinates: None,
            catalog: None,
            filter: None,
            projection: None,
            size: None,
        }
    }
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?},\n{:?},\n{:?},\n{:?},\n{:?}",
            self.object_coordinates, self.catalog, self.filter, self.projection, self.size
        )
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct QueryKwargs {
    pub limit: Option<i64>,
    pub skip: Option<u64>,
    pub sort: Option<mongodb::bson::Document>,
    pub max_time_ms: Option<u64>,
}

impl Default for QueryKwargs {
    fn default() -> QueryKwargs {
        QueryKwargs {
            limit: None,
            skip: None,
            sort: None,
            max_time_ms: None,
        }
    }
}

impl fmt::Debug for QueryKwargs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?},\n{:?},\n{:?},\n{:?}\n",
            self.limit, self.skip, self.sort, self.max_time_ms
        )
    }
}

#[derive(serde::Deserialize)]
pub struct QueryBody {
    pub query: Option<Query>,
    pub kwargs: Option<QueryKwargs>,
}
