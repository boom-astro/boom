use super::base::{nearest_match, AlertError, TimeSeries};
use super::lsst::{
    DiaForcedSource, DiaSource, LsstAlert, LsstAliases, LsstCandidate, LsstForcedPhot, LsstObject,
    LsstPrvCandidate, LSST_DECAM_XMATCH_RADIUS, LSST_ZTF_XMATCH_RADIUS,
};
use super::{decam, ztf};
use crate::{
    api::catalogs::WATCHLIST_PREFIX,
    conf::CatalogXmatchConfig,
    utils::{
        enums::Survey,
        host::{self, HostGalaxyAssociation, HostGalaxyConfig},
        lightcurves::Band,
        spatial::{xmatch, Coordinates},
    },
};
use mongodb::bson::Document;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

const SIMULATED_OBJECT_ID: &str = "simulated";

const DIA_SOURCE_FLAGS: &[&str] = &[
    "centroid_flag",
    "apFlux_flag",
    "apFlux_flag_apertureTruncated",
    "psfFlux_flag",
    "psfFlux_flag_edge",
    "psfFlux_flag_noGoodPixels",
    "trail_flag_edge",
    "trail_flag",
    "forced_PsfFlux_flag",
    "forced_PsfFlux_flag_edge",
    "forced_PsfFlux_flag_noGoodPixels",
    "shape_flag",
    "shape_flag_no_pixels",
    "shape_flag_not_contained",
    "shape_flag_parent_source",
    "isDipole",
    "pixelFlags",
    "pixelFlags_bad",
    "pixelFlags_cr",
    "pixelFlags_crCenter",
    "pixelFlags_edge",
    "pixelFlags_nodata",
    "pixelFlags_nodataCenter",
    "pixelFlags_interpolated",
    "pixelFlags_interpolatedCenter",
    "pixelFlags_offimage",
    "pixelFlags_saturated",
    "pixelFlags_saturatedCenter",
    "pixelFlags_spike",
    "pixelFlags_suspect",
    "pixelFlags_suspectCenter",
    "pixelFlags_streak",
    "pixelFlags_streakCenter",
    "pixelFlags_injected",
    "pixelFlags_injectedCenter",
    "pixelFlags_injected_template",
    "pixelFlags_injected_templateCenter",
    "glint_trail",
    "mixedInstruments",
];

pub struct SimulatedEpoch {
    pub mjd: f64,
    pub band: Band,
    pub flux: f32,
    pub flux_err: f32,
    pub detected: bool,
}

pub struct SimulatedSurroundings {
    pub cross_matches: HashMap<String, Vec<Document>>,
    pub host_galaxy: Option<HostGalaxyAssociation>,
    pub ztf: Vec<String>,
    pub decam: Vec<String>,
}

pub struct SimulatedLsstObject {
    pub alerts: Vec<LsstAlert>,
    pub aux: LsstObject,
}

pub async fn lsst_surroundings(
    ra: f64,
    dec: f64,
    xmatch_configs: &[CatalogXmatchConfig],
    host_galaxy_config: &HostGalaxyConfig,
    db: &mongodb::Database,
) -> Result<SimulatedSurroundings, AlertError> {
    // Matching a watchlist writes the object id into it.
    let xmatch_configs: Vec<CatalogXmatchConfig> = xmatch_configs
        .iter()
        .filter(|c| {
            !c.catalog.starts_with(WATCHLIST_PREFIX)
                && !c
                    .collection
                    .as_deref()
                    .is_some_and(|name| name.starts_with(WATCHLIST_PREFIX))
        })
        .cloned()
        .collect();
    let cross_matches = xmatch(
        ra,
        dec,
        SIMULATED_OBJECT_ID,
        &Survey::Lsst,
        &xmatch_configs,
        db,
    )
    .await?;
    let host_galaxy = host::associate_with_photometry(
        ra,
        dec,
        &cross_matches,
        host_galaxy_config,
        &xmatch_configs,
        &Survey::Lsst,
        SIMULATED_OBJECT_ID,
        db,
    )
    .await;
    let ztf = nearest_match(
        ra,
        dec,
        ztf::ZTF_DEC_RANGE,
        LSST_ZTF_XMATCH_RADIUS,
        &db.collection::<Document>(ztf::ALERT_AUX_COLLECTION),
    )
    .await?;
    let decam = nearest_match(
        ra,
        dec,
        decam::DECAM_DEC_RANGE,
        LSST_DECAM_XMATCH_RADIUS,
        &db.collection::<Document>(decam::ALERT_AUX_COLLECTION),
    )
    .await?;
    Ok(SimulatedSurroundings {
        cross_matches,
        host_galaxy,
        ztf,
        decam,
    })
}

fn invalid(message: String) -> AlertError {
    AlertError::InvalidTimeseriesInput(message)
}

pub fn simulated_lsst_object(
    dia_object_id: i64,
    ra: f64,
    dec: f64,
    epochs: &[SimulatedEpoch],
    detection_fields: &Map<String, Value>,
    surroundings: &SimulatedSurroundings,
    now: f64,
) -> Result<SimulatedLsstObject, AlertError> {
    let coordinates = Coordinates::try_new(ra, dec)
        .ok_or_else(|| invalid(format!("invalid position ({ra}, {dec})")))?;

    let mut fields: Map<String, Value> = DIA_SOURCE_FLAGS
        .iter()
        .map(|flag| (flag.to_string(), Value::Bool(false)))
        .collect();
    fields.insert("reliability".to_string(), json!(1.0));
    fields.extend(detection_fields.clone());
    fields.remove("candid");

    let mut candidates: Vec<LsstCandidate> = Vec::new();
    let mut fp_hists: Vec<LsstForcedPhot> = Vec::new();
    let mut detected_once = false;
    for (index, epoch) in epochs.iter().enumerate() {
        if !(epoch.flux.is_finite() && epoch.flux_err.is_finite() && epoch.flux_err > 0.0) {
            return Err(invalid(format!(
                "invalid flux at mjd {}: {} +/- {}",
                epoch.mjd, epoch.flux, epoch.flux_err
            )));
        }
        let id = dia_object_id * 1_000_000 - index as i64;
        let band = serde_json::to_value(&epoch.band).map_err(|e| invalid(e.to_string()))?;
        if epoch.detected {
            detected_once = true;
            let mut source = fields.clone();
            source.extend([
                ("diaSourceId".to_string(), json!(id)),
                ("visit".to_string(), json!(index)),
                ("detector".to_string(), json!(0)),
                ("diaObjectId".to_string(), json!(dia_object_id)),
                ("midpointMjdTai".to_string(), json!(epoch.mjd)),
                ("ra".to_string(), json!(ra)),
                ("dec".to_string(), json!(dec)),
                ("psfFlux".to_string(), json!(epoch.flux)),
                ("psfFluxErr".to_string(), json!(epoch.flux_err)),
                ("apFlux".to_string(), json!(epoch.flux)),
                ("apFluxErr".to_string(), json!(epoch.flux_err)),
                ("snr".to_string(), json!(epoch.flux.abs() / epoch.flux_err)),
                ("isNegative".to_string(), json!(epoch.flux < 0.0)),
                ("band".to_string(), band),
            ]);
            let dia_source: DiaSource = serde_json::from_value(Value::Object(source))
                .map_err(|e| invalid(format!("invalid detection fields: {e}")))?;
            candidates.push(LsstCandidate::new(dia_source, None, None, None)?);
        }
        if detected_once {
            fp_hists.push(LsstForcedPhot::try_from(DiaForcedSource {
                dia_forced_source_id: id,
                dia_object_id,
                ra,
                dec,
                visit: index as i64,
                detector: 0,
                psf_flux: Some(epoch.flux),
                psf_flux_err: Some(epoch.flux_err),
                midpoint_mjd_tai: epoch.mjd,
                band: Some(epoch.band.clone()),
                ..Default::default()
            })?);
        }
    }

    let mut prv_candidates = candidates
        .iter()
        .cloned()
        .map(LsstPrvCandidate::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    LsstPrvCandidate::sanitize_timeseries(&mut prv_candidates);
    LsstForcedPhot::sanitize_timeseries(&mut fp_hists);
    let detection_jds: Vec<f64> = prv_candidates.iter().map(|p| p.jd).collect();

    let alerts = candidates
        .into_iter()
        .map(|mut candidate| {
            let jd = candidate.jd;
            let earlier: Vec<f64> = detection_jds.iter().copied().filter(|d| *d <= jd).collect();
            candidate.jdstarthist = earlier.first().copied();
            candidate.ndethist = Some(earlier.len() as i32);
            LsstAlert {
                candid: candidate.dia_source.candid,
                object_id: candidate.object_id.clone(),
                ss_object_id: None,
                ss_source: None,
                candidate,
                coordinates: coordinates.clone(),
                created_at: now,
                updated_at: now,
            }
        })
        .collect();

    let aux = LsstObject {
        object_id: dia_object_id.to_string(),
        prv_candidates,
        fp_hists,
        is_sso: false,
        designation: None,
        cross_matches: Some(surroundings.cross_matches.clone()),
        host_galaxy: surroundings.host_galaxy.clone(),
        aliases: Some(LsstAliases {
            ztf: surroundings.ztf.clone(),
            decam: surroundings.decam.clone(),
        }),
        coordinates,
        created_at: now,
        updated_at: now,
    };

    Ok(SimulatedLsstObject { alerts, aux })
}
