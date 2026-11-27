use super::base::{nearest_match, AlertError, TimeSeries};
use super::decam::{
    self, DecamAlert, DecamAliases, DecamCandidate, DecamObject, DECAM_LSST_XMATCH_RADIUS,
    DECAM_ZTF_XMATCH_RADIUS,
};
use super::lsst::{
    self, DiaForcedSource, DiaSource, LsstAlert, LsstAliases, LsstCandidate, LsstForcedPhot,
    LsstObject, LsstPrvCandidate, LSST_DECAM_XMATCH_RADIUS, LSST_ZTF_XMATCH_RADIUS,
};
use super::ztf::{
    self, Candidate, FpHist, PrvCandidate, ZtfAlert, ZtfAliases, ZtfCandidate, ZtfForcedPhot,
    ZtfObject, ZtfPrvCandidate, ZTF_DECAM_XMATCH_RADIUS, ZTF_LSST_XMATCH_RADIUS,
};
use crate::{
    api::catalogs::WATCHLIST_PREFIX,
    conf::CatalogXmatchConfig,
    utils::{
        enums::Survey,
        host::{self, HostGalaxyAssociation, HostGalaxyConfig},
        lightcurves::{flux2mag, fluxerr2diffmaglim, Band, ZTF_ZP},
        spatial::{xmatch, Coordinates},
    },
};
use mongodb::bson::{to_document, Document};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

const SIMULATED_OBJECT_ID: &str = "simulated";
const DECAM_ZP_UJY: f32 = 23.9;
const ZTF_HISTORY_DAYS: f64 = 30.0;

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

const ZTF_NO_NEIGHBOR: &[&str] = &[
    "distnr",
    "magnr",
    "sigmagnr",
    "ssdistnr",
    "ssmagnr",
    "distpsnr1",
    "sgscore1",
    "distpsnr2",
    "sgscore2",
    "distpsnr3",
    "sgscore3",
    "neargaia",
    "maggaia",
    "neargaiabright",
    "maggaiabright",
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
    pub lsst: Vec<String>,
    pub decam: Vec<String>,
}

pub struct SimulatedAlert {
    pub candid: i64,
    pub mjd: f64,
    pub band: Band,
    pub document: Document,
}

pub struct SimulatedObject {
    pub alerts: Vec<SimulatedAlert>,
    pub aux: Document,
}

pub async fn simulated_surroundings(
    survey: &Survey,
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
    let cross_matches = xmatch(ra, dec, SIMULATED_OBJECT_ID, survey, &xmatch_configs, db).await?;
    let host_galaxy = host::associate_with_photometry(
        ra,
        dec,
        &cross_matches,
        host_galaxy_config,
        &xmatch_configs,
        survey,
        SIMULATED_OBJECT_ID,
        db,
    )
    .await;

    let (ztf_radius, lsst_radius, decam_radius) = match survey {
        Survey::Ztf => (
            None,
            Some(ZTF_LSST_XMATCH_RADIUS),
            Some(ZTF_DECAM_XMATCH_RADIUS),
        ),
        Survey::Lsst => (
            Some(LSST_ZTF_XMATCH_RADIUS),
            None,
            Some(LSST_DECAM_XMATCH_RADIUS),
        ),
        Survey::Decam => (
            Some(DECAM_ZTF_XMATCH_RADIUS),
            Some(DECAM_LSST_XMATCH_RADIUS),
            None,
        ),
        _ => (None, None, None),
    };
    Ok(SimulatedSurroundings {
        cross_matches,
        host_galaxy,
        ztf: match_in(
            db,
            ra,
            dec,
            ztf::ZTF_DEC_RANGE,
            ztf_radius,
            ztf::ALERT_AUX_COLLECTION,
        )
        .await?,
        lsst: match_in(
            db,
            ra,
            dec,
            lsst::LSST_DEC_RANGE,
            lsst_radius,
            lsst::ALERT_AUX_COLLECTION,
        )
        .await?,
        decam: match_in(
            db,
            ra,
            dec,
            decam::DECAM_DEC_RANGE,
            decam_radius,
            decam::ALERT_AUX_COLLECTION,
        )
        .await?,
    })
}

async fn match_in(
    db: &mongodb::Database,
    ra: f64,
    dec: f64,
    range: (f64, f64),
    radius: Option<f64>,
    collection: &str,
) -> Result<Vec<String>, AlertError> {
    match radius {
        Some(radius) => {
            nearest_match(
                ra,
                dec,
                range,
                radius,
                &db.collection::<Document>(collection),
            )
            .await
        }
        None => Ok(vec![]),
    }
}

fn bson_document<T: Serialize>(value: &T) -> Result<Document, AlertError> {
    to_document(value).map_err(|e| invalid(e.to_string()))
}

fn invalid(message: String) -> AlertError {
    AlertError::InvalidTimeseriesInput(message)
}

fn check_epochs(epochs: &[SimulatedEpoch]) -> Result<(), AlertError> {
    for epoch in epochs {
        if !(epoch.mjd.is_finite()
            && epoch.flux.is_finite()
            && epoch.flux_err.is_finite()
            && epoch.flux_err > 0.0)
        {
            return Err(invalid(format!(
                "invalid point at mjd {}: {} +/- {}",
                epoch.mjd, epoch.flux, epoch.flux_err
            )));
        }
    }
    Ok(())
}

fn with_overrides(
    defaults: Map<String, Value>,
    overrides: &Map<String, Value>,
) -> Map<String, Value> {
    let mut fields = defaults;
    fields.extend(overrides.clone());
    fields
}

#[allow(clippy::too_many_arguments)]
pub fn simulated_object(
    survey: &Survey,
    object_index: i64,
    ra: f64,
    dec: f64,
    programid: i32,
    epochs: &[SimulatedEpoch],
    detection_fields: &Map<String, Value>,
    surroundings: &SimulatedSurroundings,
    now: f64,
) -> Result<SimulatedObject, AlertError> {
    let coordinates = Coordinates::try_new(ra, dec)
        .ok_or_else(|| invalid(format!("invalid position ({ra}, {dec})")))?;
    check_epochs(epochs)?;
    let object_id = -(object_index + 1);
    let ids = |index: usize| object_id * 1_000_000 - index as i64;
    match survey {
        Survey::Lsst => simulated_lsst_object(
            object_id,
            ids,
            ra,
            dec,
            coordinates,
            epochs,
            detection_fields,
            surroundings,
            now,
        ),
        Survey::Ztf => simulated_ztf_object(
            object_id,
            ids,
            ra,
            dec,
            coordinates,
            programid,
            epochs,
            detection_fields,
            surroundings,
            now,
        ),
        Survey::Decam => simulated_decam_object(
            object_id,
            ids,
            ra,
            dec,
            coordinates,
            epochs,
            detection_fields,
            surroundings,
            now,
        ),
        _ => Err(invalid(format!("simulation is not supported for {survey}"))),
    }
}

fn detections_up_to(jds: &[f64], jd: f64) -> (Option<f64>, i32) {
    let earlier: Vec<f64> = jds.iter().copied().filter(|d| *d <= jd).collect();
    (earlier.first().copied(), earlier.len() as i32)
}

#[allow(clippy::too_many_arguments)]
fn simulated_lsst_object(
    dia_object_id: i64,
    ids: impl Fn(usize) -> i64,
    ra: f64,
    dec: f64,
    coordinates: Coordinates,
    epochs: &[SimulatedEpoch],
    detection_fields: &Map<String, Value>,
    surroundings: &SimulatedSurroundings,
    now: f64,
) -> Result<SimulatedObject, AlertError> {
    let mut defaults: Map<String, Value> = DIA_SOURCE_FLAGS
        .iter()
        .map(|flag| (flag.to_string(), Value::Bool(false)))
        .collect();
    defaults.insert("reliability".to_string(), json!(1.0));
    let mut fields = with_overrides(defaults, detection_fields);
    fields.remove("candid");

    let mut candidates: Vec<LsstCandidate> = Vec::new();
    let mut fp_hists: Vec<LsstForcedPhot> = Vec::new();
    let mut detected_once = false;
    for (index, epoch) in epochs.iter().enumerate() {
        let id = ids(index);
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
                ("band".to_string(), json!(epoch.band)),
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

    let mut alerts = Vec::with_capacity(candidates.len());
    for mut candidate in candidates {
        (candidate.jdstarthist, candidate.ndethist) = {
            let (first, count) = detections_up_to(&detection_jds, candidate.jd);
            (first, Some(count))
        };
        let alert = LsstAlert {
            candid: candidate.dia_source.candid,
            object_id: candidate.object_id.clone(),
            ss_object_id: None,
            ss_source: None,
            coordinates: coordinates.clone(),
            created_at: now,
            updated_at: now,
            candidate,
        };
        alerts.push(SimulatedAlert {
            candid: alert.candid,
            mjd: alert.candidate.dia_source.midpoint_mjd_tai,
            band: alert.candidate.dia_source.band.clone().unwrap_or(Band::R),
            document: bson_document(&alert)?,
        });
    }

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
    Ok(SimulatedObject {
        alerts,
        aux: bson_document(&aux)?,
    })
}

fn ztf_fid(band: &Band) -> Result<i32, AlertError> {
    match band {
        Band::G => Ok(1),
        Band::R => Ok(2),
        Band::I => Ok(3),
        other => Err(invalid(format!(
            "ZTF has no {} band",
            json!(other).as_str().unwrap_or("?")
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
fn simulated_ztf_object(
    object_id: i64,
    ids: impl Fn(usize) -> i64,
    ra: f64,
    dec: f64,
    coordinates: Coordinates,
    programid: i32,
    epochs: &[SimulatedEpoch],
    detection_fields: &Map<String, Value>,
    surroundings: &SimulatedSurroundings,
    now: f64,
) -> Result<SimulatedObject, AlertError> {
    let mut defaults: Map<String, Value> = ZTF_NO_NEIGHBOR
        .iter()
        .map(|field| (field.to_string(), json!(-999.0)))
        .collect();
    defaults.extend([
        ("rb".to_string(), json!(1.0)),
        ("drb".to_string(), json!(1.0)),
        ("nbad".to_string(), json!(0)),
        ("nneg".to_string(), json!(0)),
        ("fwhm".to_string(), json!(2.0)),
        ("elong".to_string(), json!(1.0)),
        ("seeratio".to_string(), json!(1.0)),
        ("nmtchps".to_string(), json!(0)),
    ]);
    let fields = with_overrides(defaults, detection_fields);

    let first_detection_jd = epochs
        .iter()
        .find(|e| e.detected)
        .map(|e| e.mjd + 2400000.5);
    let all_jds: Vec<f64> = epochs.iter().map(|e| e.mjd + 2400000.5).collect();
    let detection_jds: Vec<f64> = epochs
        .iter()
        .filter(|e| e.detected)
        .map(|e| e.mjd + 2400000.5)
        .collect();

    let mut candidates: Vec<ZtfCandidate> = Vec::new();
    let mut prv_nondetections: Vec<ZtfPrvCandidate> = Vec::new();
    let mut fp_hists: Vec<ZtfForcedPhot> = Vec::new();
    for (index, epoch) in epochs.iter().enumerate() {
        let id = ids(index);
        let jd = epoch.mjd + 2400000.5;
        let fid = ztf_fid(&epoch.band)?;
        let diffmaglim = fluxerr2diffmaglim(epoch.flux_err, ZTF_ZP);
        if epoch.detected {
            let (magpsf, sigmapsf) = flux2mag(epoch.flux.abs(), epoch.flux_err, ZTF_ZP);
            let (jdstarthist, ndethist) = detections_up_to(&detection_jds, jd);
            let mut candidate = fields.clone();
            candidate.extend([
                ("jd".to_string(), json!(jd)),
                ("fid".to_string(), json!(fid)),
                ("pid".to_string(), json!(id)),
                ("candid".to_string(), json!(id)),
                ("programid".to_string(), json!(programid)),
                ("isdiffpos".to_string(), json!(epoch.flux > 0.0)),
                ("ra".to_string(), json!(ra)),
                ("dec".to_string(), json!(dec)),
                ("ranr".to_string(), json!(ra)),
                ("decnr".to_string(), json!(dec)),
                ("magpsf".to_string(), json!(magpsf)),
                ("sigmapsf".to_string(), json!(sigmapsf)),
                ("magap".to_string(), json!(magpsf)),
                ("sigmagap".to_string(), json!(sigmapsf)),
                ("diffmaglim".to_string(), json!(diffmaglim)),
                ("magzpsci".to_string(), json!(ZTF_ZP)),
                ("ndethist".to_string(), json!(ndethist)),
                (
                    "ncovhist".to_string(),
                    json!(all_jds.iter().filter(|d| **d <= jd).count()),
                ),
                ("jdstarthist".to_string(), json!(jdstarthist)),
            ]);
            let candidate: Candidate = serde_json::from_value(Value::Object(candidate))
                .map_err(|e| invalid(format!("invalid detection fields: {e}")))?;
            candidates.push(ZtfCandidate::try_from(candidate)?);
        }
        let in_history = first_detection_jd.is_some_and(|first| jd >= first - ZTF_HISTORY_DAYS);
        if !in_history {
            continue;
        }
        if !epoch.detected {
            let nondetection = ZtfPrvCandidate::try_from(PrvCandidate {
                jd,
                fid,
                pid: id,
                diffmaglim: Some(diffmaglim),
                programid,
                ..Default::default()
            });
            if let Ok(nondetection) = nondetection {
                prv_nondetections.push(nondetection);
            }
        }
        fp_hists.push(ZtfForcedPhot::try_from(FpHist {
            fid,
            pid: id,
            magzpsci: Some(ZTF_ZP),
            diffmaglim: Some(diffmaglim),
            programid,
            jd,
            forcediffimflux: Some(epoch.flux),
            forcediffimfluxunc: Some(epoch.flux_err),
            procstatus: Some("0".to_string()),
            ..Default::default()
        })?);
    }

    let mut prv_candidates = candidates
        .iter()
        .map(ZtfPrvCandidate::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    ZtfPrvCandidate::sanitize_timeseries(&mut prv_candidates);
    ZtfPrvCandidate::sanitize_timeseries(&mut prv_nondetections);
    ZtfForcedPhot::sanitize_timeseries(&mut fp_hists);

    let mut alerts = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let alert = ZtfAlert {
            candid: candidate.candidate.candid,
            object_id: object_id.to_string(),
            coordinates: coordinates.clone(),
            created_at: now,
            updated_at: now,
            candidate,
        };
        alerts.push(SimulatedAlert {
            candid: alert.candid,
            mjd: alert.candidate.candidate.jd - 2400000.5,
            band: alert.candidate.band.clone(),
            document: bson_document(&alert)?,
        });
    }

    let aux = ZtfObject {
        object_id: object_id.to_string(),
        prv_candidates,
        prv_nondetections,
        fp_hists,
        cross_matches: Some(surroundings.cross_matches.clone()),
        host_galaxy: surroundings.host_galaxy.clone(),
        aliases: Some(ZtfAliases {
            lsst: surroundings.lsst.clone(),
            decam: surroundings.decam.clone(),
        }),
        coordinates,
        created_at: now,
        updated_at: now,
    };
    Ok(SimulatedObject {
        alerts,
        aux: bson_document(&aux)?,
    })
}

#[allow(clippy::too_many_arguments)]
fn simulated_decam_object(
    object_id: i64,
    ids: impl Fn(usize) -> i64,
    ra: f64,
    dec: f64,
    coordinates: Coordinates,
    epochs: &[SimulatedEpoch],
    detection_fields: &Map<String, Value>,
    surroundings: &SimulatedSurroundings,
    now: f64,
) -> Result<SimulatedObject, AlertError> {
    let reliability = match detection_fields.get("reliability") {
        Some(value) => Some(
            value
                .as_f64()
                .ok_or_else(|| invalid("reliability must be a number".to_string()))?,
        ),
        None => Some(1.0),
    };
    if let Some(field) = detection_fields.keys().find(|k| *k != "reliability") {
        return Err(invalid(format!(
            "DECam detections only accept reliability, not {field}"
        )));
    }

    let mut prv_candidates: Vec<DecamCandidate> = Vec::new();
    let mut candids: Vec<i64> = Vec::new();
    for (index, epoch) in epochs.iter().enumerate() {
        let flux = epoch.flux / 1000.0;
        let flux_err = epoch.flux_err / 1000.0;
        let (magap, sigmagap) = if flux > 0.0 {
            flux2mag(flux, flux_err, DECAM_ZP_UJY)
        } else {
            (f32::NAN, f32::NAN)
        };
        let diffmaglim = fluxerr2diffmaglim(flux_err, DECAM_ZP_UJY);
        if epoch.detected {
            prv_candidates.push(DecamCandidate::try_from(decam::Candidate {
                mjd: epoch.mjd,
                forcediffimflux: flux as f64,
                forcediffimfluxunc: flux_err as f64,
                magap: magap as f64,
                sigmagap: sigmagap as f64,
                band: epoch.band.clone(),
                diffmaglim: diffmaglim as f64,
                ra,
                dec,
                reliability,
            })?);
            candids.push(ids(index));
        }
    }

    let mut alerts = Vec::with_capacity(prv_candidates.len());
    for (candid, candidate) in candids.into_iter().zip(prv_candidates.iter().cloned()) {
        let alert = DecamAlert {
            candid,
            object_id: object_id.to_string(),
            coordinates: coordinates.clone(),
            created_at: now,
            updated_at: now,
            candidate,
        };
        alerts.push(SimulatedAlert {
            candid,
            mjd: alert.candidate.candidate.mjd,
            band: alert.candidate.candidate.band.clone(),
            document: bson_document(&alert)?,
        });
    }
    DecamCandidate::sanitize_timeseries(&mut prv_candidates);

    let aux = DecamObject {
        object_id: object_id.to_string(),
        prv_candidates,
        fp_hists: Vec::new(),
        cross_matches: Some(surroundings.cross_matches.clone()),
        host_galaxy: surroundings.host_galaxy.clone(),
        aliases: Some(DecamAliases {
            ztf: surroundings.ztf.clone(),
            lsst: surroundings.lsst.clone(),
        }),
        coordinates,
        created_at: now,
        updated_at: now,
    };
    Ok(SimulatedObject {
        alerts,
        aux: bson_document(&aux)?,
    })
}
