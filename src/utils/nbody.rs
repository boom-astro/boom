//! N-body propagation of catalog orbits with kete, the planets taken from JPL DE440.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use kete_core::constants::GMS_SQRT;
use kete_core::desigs::Desig;
use kete_core::elements::CometElements;
use kete_core::ephemeris::Ephemeris;
use kete_core::frames::{Ecliptic, Equatorial, SSB};
use kete_core::propagation::NBody;
use kete_core::state::State;
use kete_core::time::{Time, TDB};
use kete_spice::prelude::SpiceEphemeris;
use rayon::prelude::*;

use crate::utils::sso_geometry::OrbitalElements;

pub const PLANETS_KERNEL_URL: &str =
    "https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/de440s.bsp";
pub const PLANETS_KERNEL_PATH: &str = "data/kernels/de440s.bsp";
const SUN: i32 = 10;
const EARTH: i32 = 399;

#[derive(thiserror::Error, Debug)]
pub enum NbodyError {
    #[error("failed to download the planetary ephemeris: {0}")]
    Download(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("kete: {0}")]
    Kete(String),
}

static PLANETS_LOADED: AtomicBool = AtomicBool::new(false);
static LOADING: Mutex<()> = Mutex::new(());

pub async fn load_planets() -> Result<(), NbodyError> {
    if PLANETS_LOADED.load(Ordering::Acquire) {
        return Ok(());
    }
    let path = Path::new(PLANETS_KERNEL_PATH);
    if !path.exists() {
        download_planets(path).await?;
    }
    let _guard = LOADING.lock().unwrap_or_else(|e| e.into_inner());
    if !PLANETS_LOADED.load(Ordering::Acquire) {
        kete_spice::load_kernels(&[PLANETS_KERNEL_PATH.to_string()])
            .map_err(|e| NbodyError::Kete(e.to_string()))?;
        PLANETS_LOADED.store(true, Ordering::Release);
    }
    Ok(())
}

async fn download_planets(path: &Path) -> Result<(), NbodyError> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    crate::utils::data::download_to_file(tmp.as_file_mut(), PLANETS_KERNEL_URL, None, None, false)
        .await
        .map_err(|e| NbodyError::Download(e.to_string()))?;
    tmp.persist(path).map_err(|e| NbodyError::Io(e.error))?;
    Ok(())
}

/// `epochs` must be sorted. `threads` of 0 uses the global pool.
pub fn propagate_all(
    orbits: &[OrbitalElements],
    epochs: &[f64],
    threads: usize,
) -> Result<Vec<Vec<Option<OrbitalElements>>>, NbodyError> {
    let eph = SpiceEphemeris::loaded().map_err(|e| NbodyError::Kete(e.to_string()))?;
    for &jd in epochs {
        eph.try_get_state_with_center(EARTH, Time::<TDB>::new(jd), SUN)
            .map(|_| ())
            .map_err(|e| NbodyError::Kete(format!("no planets at JD {jd}: {e}")))?;
    }
    let run = || -> Vec<Vec<Option<OrbitalElements>>> {
        orbits
            .par_iter()
            .map(|orbit| propagate_orbit(&eph, orbit, epochs))
            .collect()
    };
    if threads == 0 {
        return Ok(run());
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|e| NbodyError::Kete(e.to_string()))?;
    Ok(pool.install(run))
}

fn propagate_orbit(
    eph: &SpiceEphemeris,
    orbit: &OrbitalElements,
    epochs: &[f64],
) -> Vec<Option<OrbitalElements>> {
    let mut out = vec![None; epochs.len()];
    let Some(start) = ssb_state(eph, orbit) else {
        return out;
    };
    let force = NBody::new(eph, false);
    let split = epochs.partition_point(|&jd| jd < orbit.epoch_jd);
    walk(
        eph,
        &force,
        start.clone(),
        epochs,
        split..epochs.len(),
        &mut out,
    );
    walk(eph, &force, start, epochs, (0..split).rev(), &mut out);
    out
}

fn walk(
    eph: &SpiceEphemeris,
    force: &NBody<'_, SpiceEphemeris>,
    mut state: State<Equatorial, SSB>,
    epochs: &[f64],
    indices: impl Iterator<Item = usize>,
    out: &mut [Option<OrbitalElements>],
) {
    for i in indices {
        match state.propagate_with(force, Time::<TDB>::new(epochs[i])) {
            Ok(next) => {
                out[i] = elements_of(eph, next.clone());
                state = next;
            }
            Err(_) => return,
        }
    }
}

fn ssb_state(eph: &SpiceEphemeris, orbit: &OrbitalElements) -> Option<State<Equatorial, SSB>> {
    let elements = CometElements {
        desig: Desig::Empty,
        epoch: Time::<TDB>::new(orbit.epoch_jd),
        eccentricity: orbit.e,
        inclination: orbit.incl.to_radians(),
        lon_of_ascending: orbit.node.to_radians(),
        peri_time: Time::<TDB>::new(orbit.tp),
        peri_arg: orbit.peri.to_radians(),
        peri_dist: orbit.q,
        center_id: SUN,
        gm_sqrt: GMS_SQRT,
    };
    let state = elements.try_to_state().ok()?.into_frame::<Equatorial>();
    eph.try_to_ssb(state).ok()
}

fn elements_of(eph: &SpiceEphemeris, state: State<Equatorial, SSB>) -> Option<OrbitalElements> {
    let mut helio = State::<Equatorial>::from(state);
    eph.try_change_center(&mut helio, SUN).ok()?;
    let c = CometElements::from_state(&helio.into_frame::<Ecliptic>()).ok()?;
    let elements = OrbitalElements {
        epoch_jd: c.epoch.jd(),
        a: c.semi_major(),
        e: c.eccentricity,
        incl: c.inclination.to_degrees(),
        node: c.lon_of_ascending.to_degrees().rem_euclid(360.0),
        peri: c.peri_arg.to_degrees().rem_euclid(360.0),
        mean_anomaly: c.mean_anomaly().to_degrees(),
        q: c.peri_dist,
        tp: c.peri_time.jd(),
    };
    [
        elements.e,
        elements.incl,
        elements.node,
        elements.peri,
        elements.q,
        elements.tp,
    ]
    .iter()
    .all(|v| v.is_finite())
    .then_some(elements)
}
