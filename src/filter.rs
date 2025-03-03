use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::{error::Error, fmt};
use tracing::error;

const ALLOWED_CATALOGS: [&str; 1] = ["ZTF_alerts"];

#[derive(Debug)]
pub struct FilterError {
    message: String,
}

impl Error for FilterError {}
impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Filter Error {}", self.message)
    }
}

pub struct Filter {
    pub pipeline: Vec<Document>,
    pub permissions: Vec<i64>,
    pub catalog: String,
    pub id: i32,
}

impl Filter {
    pub async fn build(filter_id: i32, db: &mongodb::Database) -> Result<Filter, Box<dyn Error>> {
        // grab filter from database
        // >> pipeline returns document with group_id, permissions, and filter pipeline
        let filter_obj = db
            .collection::<Document>("filters")
            .aggregate(vec![
                doc! {
                    "$match": doc! {
                        "filter_id": filter_id,
                        "active": true,
                    }
                },
                doc! {
                    "$project": doc! {
                        "fv": doc! {
                            "$filter": doc! {
                                "input": "$fv",
                                "as": "x",
                                "cond": doc! {
                                    "$eq": [
                                        "$$x.fid",
                                        "$active_fid"
                                    ]
                                }
                            }
                        },
                        "group_id": 1,
                        "permissions": 1,
                        "catalog": 1
                    }
                },
                doc! {
                    "$project": doc! {
                        "pipeline": doc! {
                            "$arrayElemAt": [
                                "$fv.pipeline",
                                0
                            ]
                        },
                        "group_id": 1,
                        "permissions": 1,
                        "catalog": 1
                    }
                },
            ])
            .await;

        if let Err(e) = filter_obj {
            error!("Got ERROR when retrieving filter from database: {}", e);
            return Result::Err(Box::new(e));
        }

        // get document from cursor
        let mut filter_obj = filter_obj.unwrap();
        let advance = filter_obj.advance().await.unwrap();
        let filter_obj = if advance {
            filter_obj.deserialize_current().unwrap()
        } else {
            return Err(Box::new(FilterError {
                message: format!("Filter {} not found in database", filter_id),
            }));
        };

        let catalog = filter_obj.get("catalog").unwrap().as_str().unwrap();

        // check if catalog is allowed
        if !ALLOWED_CATALOGS.contains(&catalog) {
            panic!("Catalog {} not allowed", catalog);
        }

        // get permissions
        let permissions = filter_obj
            .get("permissions")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_i32().unwrap() as i64)
            .collect();

        // filter prefix (with permissions)
        let mut out_filter = vec![
            doc! {
                "$match": doc! {
                    // during filter::run proper candis are inserted here
                }
            },
            doc! {
                "$project": doc! {
                    "cutoutScience": 0,
                    "cutoutDifference": 0,
                    "cutoutTemplate": 0,
                    "publisher": 0,
                    "schemavsn": 0
                }
            },
            doc! {
                "$lookup": doc! {
                    "from": format!("{}_aux", catalog),
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            },
            doc! {
                "$project": doc! {
                    "objectId": 1,
                    "candid": 1,
                    "candidate": 1,
                    "classifications": 1,
                    "coordinates": 1,
                    "cross_matches": doc! {
                        "$arrayElemAt": [
                            "$aux.cross_matches",
                            0
                        ]
                    },
                    "prv_candidates": doc! {
                        "$filter": doc! {
                            "input": doc! {
                                "$arrayElemAt": [
                                    "$aux.prv_candidates",
                                    0
                                ]
                            },
                            "as": "x",
                            "cond": doc! {
                                "$and": [
                                    {
                                        "$in": [
                                            "$$x.programid",
                                            &permissions
                                        ]
                                    },
                                    { // maximum 1 year of past data
                                        "$lt": [
                                            {
                                                "$subtract": [
                                                    "$candidate.jd",
                                                    "$$x.jd"
                                                ]
                                            },
                                            365
                                        ]
                                    },
                                    { // only datapoints up to (and including) current alert
                                        "$lte": [
                                            "$$x.jd",
                                            "$candidate.jd"
                                        ]
                                    }

                                ]
                            }
                        }
                    },
                }
            },
        ];

        // get filter pipeline as str and convert to Vec<Bson>
        let filter = filter_obj.get("pipeline").unwrap().as_str().unwrap();
        let filter = serde_json::from_str::<serde_json::Value>(filter).expect("Invalid input");
        let filter = filter.as_array().unwrap();

        // append stages to prefix
        for stage in filter {
            let x = mongodb::bson::to_document(stage).expect("invalid filter BSON");
            out_filter.push(x);
        }

        let out_filter = Filter {
            pipeline: out_filter,
            permissions: permissions,
            catalog: catalog.to_string(),
            id: filter_id,
        };

        Ok(out_filter)
    }

    // runs the built filter on the provided mongodb::Database and returns the resulting candids
    pub async fn run(
        &mut self,
        candids: Vec<i64>,
        db: &mongodb::Database,
    ) -> Result<Vec<Document>, Box<dyn Error>> {
        if candids.len() == 0 {
            return Ok(vec![]);
        }
        if self.pipeline.len() == 0 {
            panic!("filter pipeline is empty, ensure filter has been built before running");
        }

        // insert candids into filter
        self.pipeline[0].insert(
            "$match",
            doc! {
                "candid": doc! {
                    "$in": candids
                }
            },
        );

        // run filter
        let mut result = db
            .collection::<Document>(&self.catalog)
            .aggregate(self.pipeline.to_owned())
            .await?; // is to_owned the fastest way to access self fields?

        let mut out_documents: Vec<Document> = Vec::new();

        while let Some(doc) = result.next().await {
            let doc = doc.unwrap();
            out_documents.push(doc);
        }
        Ok(out_documents)
    }
}
