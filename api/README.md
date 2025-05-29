# Boom API

This repo contains the REST API for working with
[BOOM](https://github.com/boom-astro/boom),
and is largely based on the
[Kowalski](https://github.com/skyportal/kowalski) API.

## Development

Development environment requirements:

1. Active BOOM MongoDB instance
2. Postman (or some other way of making HTTP requests) for querying

## API documentation

### Table of contents

#### Filtering

- [Adding a new filter](#post-a-filter)
- [Adding a filter version](#add-a-new-filter-version)

#### Querying

- [Retrieve an object](#get-object)
- [Getting database & collection info](#get-database-info)
- [Cone search](#cone-search)
- [Count documents](#count-documents)
- [Sample alerts](#sample-alerts)
- [Find alerts](#find-alerts)

### Filtering

#### Post a filter

Adds a filter to the database.

**Endpoint**: `POST "/filters"`\
**Body**:

```
{
    "pipeline": aggregate pipeline (array of bson documents),
    "catalog": catalog name (string),
    "permissions": allowed permissions,
    "id": filter id (i32)
}
```

**Example Body**:

```
{
    "pipeline":
    [
        {
            "$project": {
                "cutoutScience": 0,
                "cutoutDifference": 0,
                "cutoutTemplate": 0,
                "publisher": 0,
                "schemavsn": 0
            }
        },
        {
            "$lookup": {
                "from": "alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        {
            "$project": {
                "objectId": 1,
                "candid": 1,
                "candidate": 1,
                "classifications": 1,
                "coordinates": 1,
                "prv_candidates": {
                    "$arrayElemAt": [
                        "$aux.prv_candidates",
                        0
                    ]
                },
                "cross_matches": {
                    "$arrayElemAt": [
                        "$aux.cross_matches",
                        0
                    ]
                }
            }
        },
        {
            "$match": {
                "candidate.drb": {
                    "$gt": 0.5
                },
                "candidate.ndethist": {
                    "$gt": 1.0
                },
                "candidate.magpsf": {
                    "$lte": 18.5
                }
            }
        }
    ],
    "catalog": "ZTF",
    "permissions": [1],
    "id": -3
}
```

#### Add a new filter version

Adds a new pipeline to a filter's pipeline array and sets the filter's active pipeline id to the new pipeline's id.

**Endpoint**: `PATCH "/filters/{filter_id}"`\
**Body**:

```
{
    "pipeline": aggregate pipeline (array of bson documents)
}
```

**Example Body**:

```
{
    "pipeline": [
        {
            "$project": {
                "cutoutScience": 0,
                "cutoutDifference": 0,
                "cutoutTemplate": 0,
                "publisher": 0,
                "schemavsn": 0
            }
        },
        {
            "$match": {
                "candidate.drb": {
                    "$gt": 0.5
                },
                "candidate.ndethist": {
                    "$gt": 1.0
                },
                "candidate.magpsf": {
                    "$lte": 18.5
                }
            }
        }
    ]
}
```

### Querying

#### Get object

Retrieves the most recent detection of an object with its lightcurve, crossmatches with archival catalogs, metadata, and images from the specified survey.

**Endpoint**: `Get "/alerts/{survey_name}/get_object/{object_id}"`\
**catalog_name**: String. e.g., "ZTF", "NED"\
**Example Query**: `Get "/alerts/ZTF/get_object/ZTF18aajpnun`

#### Get database info

Get database or catalog information / specs.

**Endpoint**: `Get "/query/info"`\
**command_types**: "db_info", "index_info", "catalog_info", "catalog_names"\
**catalog_names**: Array Strings. e.g., `["ZTF_alerts",...]` (not required for db_info, catalog_names)\
**Body**:

```
{
    "command": <command_type>,
    "catalogs": [catalog_names]
}
```

#### Cone search

Performs a cone search on a catalog and returns the resulting data.

**Endpoint**: `Get "/query/cone_search"`\
**Unit**: "Arcseconds", "Arcminutes", "Degrees", "Radians"\
**Body**:

```
{
    "radius": <float>,
    "unit": <Unit>,
    "object_coordinates": {
        <object_name>: [
            <ra>, <dec>
        ],
        <object2_name>: [
            <ra>, <dec>
        ]
    },
    "catalog": {
        "catalog_name": <catalog_name>,
        "filter": <bson>,
        "projection": <bson>
    },
    "kwargs": {<kwargs>}
}
```

**Example Body** (should return at least an object called `NGC 5162`):

```
{
    "radius": 1,
    "unit": "Arcseconds",
    "object_coordinates": {
        "object1": [
            202.366276, 11.006276
        ]
    },
    "catalog": {
        "catalog_name": "NED",
        "filter": {},
        "projection": {}
    }
}
```

#### Count documents

Gets the number of documents which pass through a filter.

**Endpoint**: `GET "/query/count_documents"`\
**catalog_name**: String. e.g., "ZTF_alerts"\
**Body:**

```
{
    "query": {
        "catalog": <catalog_name>,
        "filter": {},
        "projection": {},
    }
}
```

#### Sample alerts

Retrieves a sample of alerts from the database.

**Endpoint**: `GET "/query/sample"`\
**catalog_name**: String. e.g., "ZTF_alerts"\
**Body:**

```
{
    "query": {
        "catalog": <catalog_name>,
        "size": <int>
    }
}
```

#### Find alerts

Performs a find query on the database.

**Endpoint**: `GET "/query/find"`\
**catalog_name**: String. e.g., "ZTF_alerts"\
**Body:**

```
{
    "query": {
        "catalog": <catalog_name>,
        "filter": <bson filter (aggregate pipeline)>
    },
    "kwargs": {<kwargs>}
}
```

**Example Body**:

```
{
    "query": {
        "catalog": "ZTF_alerts",
        "filter": {}
    },
    "kwargs": {
        "limit": 1
    }
}
```
