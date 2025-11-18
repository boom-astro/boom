use crate::{conf, utils::o11y::logging::as_error};

use flare::spatial::{great_circle_distance, radec2lb};
use futures::stream::StreamExt;
use mongodb::bson::{doc, from_bson};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use std::str::FromStr;
use tracing::instrument;

#[derive(thiserror::Error, Debug)]
pub enum XmatchError {
    #[error("value access error from bson")]
    BsonValueAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("error from mongodb")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("distance_key field is null")]
    NullDistanceKey,
    #[error("distance_max field is null")]
    NullDistanceMax,
    #[error("distance_max_near field is null")]
    NullDistanceMaxNear,
    #[error("failed to convert the bson data into a document")]
    AsDocumentError,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct GeoJsonPoint {
    r#type: String,
    coordinates: Vec<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct Coordinates {
    radec_geojson: GeoJsonPoint,
    l: f64,
    b: f64,
}

impl Coordinates {
    pub fn new(ra: f64, dec: f64) -> Self {
        let (l, b) = radec2lb(ra, dec);
        Coordinates {
            radec_geojson: GeoJsonPoint {
                r#type: "Point".to_string(),
                coordinates: vec![ra - 180.0, dec],
            },
            l,
            b,
        }
    }
}

// implement a deserialize_coordinate function that we can use on a ra or dec field with serde's deserialize_with method
fn deserialize_coordinate<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = match mongodb::bson::Bson::deserialize(deserializer)? {
        mongodb::bson::Bson::Double(v) => Ok(v),
        mongodb::bson::Bson::Int32(v) => Ok(v as f64),
        mongodb::bson::Bson::Int64(v) => Ok(v as f64),
        _ => return Err(serde::de::Error::custom("invalid coordinate value")),
    }?;
    if value.is_nan() || value.is_infinite() {
        return Err(serde::de::Error::custom(
            "coordinate value is NaN or infinite",
        ));
    }
    Ok(value)
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Xmatches {
    pub gaia: Option<Vec<GaiaWithDistance>>,
    pub panstarrs: Option<Vec<PanSTARRSWithDistance>>,
    pub milliquas: Option<Vec<MilliquasWithDistance>>,
    pub ned: Option<Vec<NEDWithDistance>>,
}
#[instrument(skip(xmatch_configs, db), fields(database = db.name()), err)]
pub async fn xmatch(
    ra: f64,
    dec: f64,
    xmatch_configs: &Vec<conf::CatalogXmatchConfig>,
    db: &mongodb::Database,
) -> Result<Xmatches, XmatchError> {
    // TODO, make the xmatch config a hashmap for faster access
    // while looping over the xmatch results of the batched queries
    if xmatch_configs.len() == 0 {
        return Ok(Xmatches {
            gaia: None,
            panstarrs: None,
            milliquas: None,
            ned: None,
        });
    }
    let ra_geojson = ra - 180.0;
    let dec_geojson = dec;

    let mut x_matches_pipeline = vec![
        doc! {
            "$match": {
                "coordinates.radec_geojson": {
                    "$geoWithin": {
                        "$centerSphere": [[ra_geojson, dec_geojson], xmatch_configs[0].radius]
                    }
                }
            }
        },
        doc! {
            "$project": &xmatch_configs[0].projection
        },
        doc! {
            "$group": {
                "_id": mongodb::bson::Bson::Null,
                "matches": {
                    "$push": "$$ROOT"
                }
            }
        },
        doc! {
            "$project": {
                "_id": 0,
                "matches": 1,
                "catalog": &xmatch_configs[0].catalog.to_string()
            }
        },
    ];

    // then for all the other xmatch_configs, use a unionWith stage
    for xmatch_config in xmatch_configs.iter().skip(1) {
        x_matches_pipeline.push(doc! {
            "$unionWith": {
                "coll": &xmatch_config.collection_name,
                "pipeline": [
                    doc! {
                        "$match": {
                            "coordinates.radec_geojson": {
                                "$geoWithin": {
                                    "$centerSphere": [[ra_geojson, dec_geojson], xmatch_config.radius]
                                }
                            }
                        }
                    },
                    doc! {
                        "$project": &xmatch_config.projection
                    },
                    doc! {
                        "$group": {
                            "_id": mongodb::bson::Bson::Null,
                            "matches": {
                                "$push": "$$ROOT"
                            }
                        }
                    },
                    doc! {
                        "$project": {
                            "_id": 0,
                            "matches": 1,
                            "catalog": &xmatch_config.catalog.to_string()
                        }
                    }
                ]
            }
        });
    }

    let collection: mongodb::Collection<mongodb::bson::Document> =
        db.collection(&xmatch_configs[0].collection_name);
    let mut cursor = collection
        .aggregate(x_matches_pipeline)
        .await
        .inspect_err(as_error!("failed to aggregate"))?;

    let mut xmatch_results = Xmatches {
        gaia: None,
        panstarrs: None,
        milliquas: None,
        ned: None,
    };

    while let Some(result) = cursor.next().await {
        let doc = result.inspect_err(as_error!("failed to get next document"))?;
        let catalog_str = doc
            .get_str("catalog")
            .inspect_err(as_error!("failed to get catalog"))?;
        let catalog = CrossmatchCatalog::from_str(catalog_str).unwrap();
        println!("Processing catalog: {:?}", catalog);

        let matches = doc
            .get_array("matches")
            .inspect_err(as_error!("failed to get matches"))?;

        match catalog {
            CrossmatchCatalog::PanSTARRS => {
                let panstarrs: Vec<PanSTARRSWithDistance> = matches
                    .into_iter()
                    .filter_map(|doc| from_bson(doc.clone()).ok())
                    .map(|panstarrs| PanSTARRSWithDistance::new(panstarrs, ra, dec))
                    .collect();
                xmatch_results.panstarrs = Some(panstarrs);
            }
            CrossmatchCatalog::Gaia => {
                let gaias: Vec<GaiaWithDistance> = matches
                    .iter()
                    .filter_map(|doc| from_bson(doc.clone()).ok())
                    .map(|gaia| GaiaWithDistance::new(gaia, ra, dec))
                    .collect();
                xmatch_results.gaia = Some(gaias);
            }
            CrossmatchCatalog::Milliquas => {
                let milliquases: Vec<MilliquasWithDistance> = matches
                    .iter()
                    .filter_map(|doc| from_bson(doc.clone()).ok())
                    .map(|milliquas| MilliquasWithDistance::new(milliquas, ra, dec))
                    .collect();
                xmatch_results.milliquas = Some(milliquases);
            }
            CrossmatchCatalog::NED => {
                let xmatch_config = xmatch_configs
                    .iter()
                    .find(|x| x.catalog == CrossmatchCatalog::NED)
                    .expect("this should never panic, the doc was derived from the catalogs");
                let neds: Vec<NEDWithDistance> = matches
                    .iter()
                    .filter_map(|doc| from_bson(doc.clone()).ok())
                    .filter_map(|ned: NED| {
                        let xmatch_ra = ned.ra;
                        let xmatch_dec = ned.dec;
                        let doc_z = match ned.z {
                            Some(v) => v,
                            None => {
                                return None;
                            }
                        };

                        let cm_radius_arcsec = if doc_z < 0.01 {
                            xmatch_config.distance_max_near.unwrap_or(10.0) // in arcsec
                        } else {
                            xmatch_config.distance_max.unwrap_or(5.0) * (0.05 / doc_z)
                            // in arcsec
                        };
                        let distance_arcsec =
                            great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0; // convert to arcsec

                        if distance_arcsec < cm_radius_arcsec {
                            // calculate the distance between objs in kpc
                            let distance_kpc = if doc_z > 0.005 {
                                distance_arcsec * (doc_z / 0.05)
                            } else {
                                -1.0
                            };
                            Some(NEDWithDistance::new(ned, distance_arcsec, distance_kpc))
                        } else {
                            None
                        }
                    })
                    .filter_map(|doc| Some(doc))
                    .collect();
                xmatch_results.ned = Some(neds);
            }
        }
    }

    Ok(xmatch_results)
}

pub trait Projectable {
    fn projection() -> mongodb::bson::Document;
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct PanSTARRS {
    #[serde(rename = "_id")]
    pub id: i64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    pub ra: f64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    pub dec: f64,
    #[serde(rename = "gMeanPSFMag")]
    pub g_mean_psf_mag: Option<f64>,
    #[serde(rename = "gMeanPSFMagErr")]
    pub g_mean_psf_mag_err: Option<f64>,
    #[serde(rename = "rMeanPSFMag")]
    pub r_mean_psf_mag: Option<f64>,
    #[serde(rename = "rMeanPSFMagErr")]
    pub r_mean_psf_mag_err: Option<f64>,
    #[serde(rename = "iMeanPSFMag")]
    pub i_mean_psf_mag: Option<f64>,
    #[serde(rename = "iMeanPSFMagErr")]
    pub i_mean_psf_mag_err: Option<f64>,
    #[serde(rename = "zMeanPSFMag")]
    pub z_mean_psf_mag: Option<f64>,
    #[serde(rename = "zMeanPSFMagErr")]
    pub z_mean_psf_mag_err: Option<f64>,
    #[serde(rename = "yMeanPSFMag")]
    pub y_mean_psf_mag: Option<f64>,
    #[serde(rename = "yMeanPSFMagErr")]
    pub y_mean_psf_mag_err: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct Gaia {
    #[serde(rename = "_id")]
    id: i64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    ra: f64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    dec: f64,
    parallax: Option<f64>,
    parallax_error: Option<f64>,
    pm: Option<f64>,
    pmra: Option<f64>,
    pmra_error: Option<f64>,
    pmdec: Option<f64>,
    pmdec_error: Option<f64>,
    phot_g_mean_mag: Option<f64>,
    phot_bp_mean_mag: Option<f64>,
    phot_rp_mean_mag: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct Milliquas {
    #[serde(rename = "_id")]
    id: i64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    ra: f64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    dec: f64,
    #[serde(rename = "Name")]
    name: Option<String>,
    #[serde(rename = "Descrip")]
    description: Option<String>,
    #[serde(rename = "Qpct")]
    qpct: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct NED {
    #[serde(rename = "objname")]
    id: i64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    ra: f64,
    #[serde(deserialize_with = "deserialize_coordinate")]
    dec: f64,
    name: Option<String>,
    objtype: Option<String>,
    z: Option<f64>,
    z_unc: Option<f64>,
    z_tech: Option<String>,
    z_qual: Option<i32>,
    #[serde(rename = "DistMpc")]
    dist_mpc: Option<f64>,
    #[serde(rename = "DistMpc_unc")]
    dist_mpc_unc: Option<f64>,
    ebv: Option<f64>,
    #[serde(rename = "m_Ks")]
    m_ks: Option<f64>,
    #[serde(rename = "m_Ks_unc")]
    m_ks_unc: Option<f64>,
    #[serde(rename = "tMASSphot")]
    t_mass_phot: Option<f64>,
    #[serde(rename = "Mstar")]
    m_star: Option<f64>,
    #[serde(rename = "Mstar_unc")]
    m_star_unc: Option<f64>,
}

impl Projectable for PanSTARRS {
    fn projection() -> mongodb::bson::Document {
        doc! {
            "_id": 1,
            "ra": 1,
            "dec": 1,
            "gMeanPSFMag": 1,
            "gMeanPSFMagErr": 1,
            "rMeanPSFMag": 1,
            "rMeanPSFMagErr": 1,
            "iMeanPSFMag": 1,
            "iMeanPSFMagErr": 1,
            "zMeanPSFMag": 1,
            "zMeanPSFMagErr": 1,
            "yMeanPSFMag": 1,
            "yMeanPSFMagErr": 1,
        }
    }
}

impl Projectable for Gaia {
    fn projection() -> mongodb::bson::Document {
        doc! {
            "_id": 1,
            "ra": 1,
            "dec": 1,
            "parallax": 1,
            "parallax_error": 1,
            "pm": 1,
            "pmra": 1,
            "pmra_error": 1,
            "pmdec": 1,
            "pmdec_error": 1,
            "phot_g_mean_mag": 1,
            "phot_bp_mean_mag": 1,
            "phot_rp_mean_mag": 1,
        }
    }
}

impl Projectable for Milliquas {
    fn projection() -> mongodb::bson::Document {
        doc! {
            "_id": 1,
            "ra": 1,
            "dec": 1,
            "Name": 1,
            "Descrip": 1,
            "Qpct": 1,
        }
    }
}

impl Projectable for NED {
    fn projection() -> mongodb::bson::Document {
        doc! {
            "objname": 1,
            "ra": 1,
            "dec": 1,
            "name": 1,
            "objtype": 1,
            "z": 1,
            "z_unc": 1,
            "z_tech": 1,
            "z_qual": 1,
            "DistMpc": 1,
            "DistMpc_unc": 1,
            "ebv": 1,
            "m_Ks": 1,
            "m_Ks_unc": 1,
            "tMASSphot": 1,
            "Mstar": 1,
            "Mstar_unc": 1,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PanSTARRSWithDistance {
    pub panstarrs: PanSTARRS,
    pub distance_arcsec: f64,
}

impl PanSTARRSWithDistance {
    fn new(panstarrs: PanSTARRS, ra: f64, dec: f64) -> Self {
        let distance_arcsec = great_circle_distance(ra, dec, panstarrs.ra, panstarrs.dec) * 3600.0; // convert to arcsec
        PanSTARRSWithDistance {
            panstarrs,
            distance_arcsec,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GaiaWithDistance {
    gaia: Gaia,
    distance_arcsec: f64,
}
impl GaiaWithDistance {
    fn new(gaia: Gaia, ra: f64, dec: f64) -> Self {
        let distance_arcsec = great_circle_distance(ra, dec, gaia.ra, gaia.dec) * 3600.0; // convert to arcsec
        GaiaWithDistance {
            gaia,
            distance_arcsec,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MilliquasWithDistance {
    milliquas: Milliquas,
    distance_arcsec: f64,
}
impl MilliquasWithDistance {
    fn new(milliquas: Milliquas, ra: f64, dec: f64) -> Self {
        let distance_arcsec = great_circle_distance(ra, dec, milliquas.ra, milliquas.dec) * 3600.0; // convert to arcsec
        MilliquasWithDistance {
            milliquas,
            distance_arcsec,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct NEDWithDistance {
    ned: NED,
    distance_arcsec: f64,
    distance_kpc: f64,
}

impl NEDWithDistance {
    fn new(ned: NED, distance_arcsec: f64, distance_kpc: f64) -> Self {
        NEDWithDistance {
            ned,
            distance_arcsec,
            distance_kpc,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CrossmatchCatalog {
    PanSTARRS,
    Gaia,
    Milliquas,
    NED,
}
impl ToString for CrossmatchCatalog {
    fn to_string(&self) -> String {
        match self {
            CrossmatchCatalog::PanSTARRS => "panstarrs".to_string(),
            CrossmatchCatalog::Gaia => "gaia".to_string(),
            CrossmatchCatalog::Milliquas => "milliquas".to_string(),
            CrossmatchCatalog::NED => "ned".to_string(),
        }
    }
}

// implement a FromStr for CrossmatchCatalog
impl FromStr for CrossmatchCatalog {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "panstarrs" => Ok(CrossmatchCatalog::PanSTARRS),
            "gaia" => Ok(CrossmatchCatalog::Gaia),
            "milliquas" => Ok(CrossmatchCatalog::Milliquas),
            "ned" => Ok(CrossmatchCatalog::NED),
            _ => Err(()),
        }
    }
}
