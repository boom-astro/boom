"""Legacy Survey DR9, from the NERSC sweep catalogs and their photo-z counterparts.

DR9 is not mirrored as HATS parquet the way DR10.1 is, so there is no column
projection to push down the wire: FITS binary tables are row-major, and the
whole file has to arrive to read the ~30 columns BOOM keeps. A chunk is
therefore one sweep file paired with its photo-z file -- fetched, merged,
converted and deleted before the next one, which keeps peak disk at a few
gigabytes rather than the ~0.2 TB dr9/north runs to in total.

Only `dr9/north` is listed. That is the BASS+MzLS reduction, covering the sky
DR10's DECam footprint does not reach; `dr9/south` is another terabyte
re-reducing sky DR10 already covers. Chunk ids carry the region, so adding the
south later leaves the north's ids -- and so a resumed ingest's record of which
chunks are done -- untouched.

The photo-z sweeps are row-matched to the sweeps rather than needing a join on
an object id: `sweep-X.fits` and `sweep-X-pz.fits` have the same rows in the
same order. That is checked rather than assumed, because a truncated download
would otherwise attach each source to a different source's redshift.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, list_index, log

ID = "lsdr9"

BASE_URL = "https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr9"

#: The reduction BOOM ingests. `south` exists and is not listed; see the module
#: docstring.
REGION = "north"

SWEEP_VERSION = "9.0"

#: The Zhou et al. (2023) photo-z rerun. Computed against the same 9.0 sweeps as
#: the older 9.0-photo-z release, so either pairs row-for-row with them.
PHOTOZ_VERSION = "9.1-photo-z"

#: sweep-<RAmin><p|m><Decmin>-<RAmax><p|m><Decmax>.fits, e.g.
#: sweep-000m005-010p000.fits is RA 0..10, Dec -5..0.
SWEEP_NAME = re.compile(r"sweep-(\d{3})([pm])(\d{3})-(\d{3})([pm])(\d{3})\.fits")

#: The Dec at which the Legacy Surveys switch from the DECam (south) reduction
#: to the BASS+MzLS (north) one.
RESOLVE_DEC = 32.375

#: Columns taken from the sweep catalog. release/brickid/objid are the `_id`
#: ingredients and are not stored; the rest are the DR10 column names, because
#: the crossmatch projections and the host-galaxy association are written
#: against those and a DR9 row has to answer to the same names.
#:
#: No `flux_i`: DR9 predates the i-band entirely, so the column does not exist.
SWEEP_COLUMNS = [
    "release", "brickid", "objid",
    "ra", "dec", "type", "ebv",
    "flux_g", "flux_r", "flux_z",
    "flux_w1", "flux_w2", "flux_w3", "flux_w4",
    # Tractor ellipse, plus the quality columns that separate a real galaxy from
    # a marginal or blended REX fit. g and z give those cuts a fallback when r
    # is missing.
    "shape_r", "shape_e1", "shape_e2", "sersic",
    "flux_ivar_g", "flux_ivar_r", "flux_ivar_z",
    "fracflux_g", "fracflux_r", "fracflux_z",
    # Exposures per band. A zero distinguishes "not observed in this band" from
    # "observed and not detected", which otherwise both read as a missing flux.
    "nobs_g", "nobs_r", "nobs_z",
]

#: Read from the photo-z sweep only to prove the two files are row-matched, then
#: dropped in favor of the sweep's copies.
PHOTOZ_KEY_COLUMNS = ["release", "brickid", "objid"]

PHOTOZ_COLUMNS = [
    "z_spec", "survey",
    "z_phot_mean", "z_phot_median", "z_phot_std", "z_phot_l95", "z_phot_u95",
]

STORED_COLUMNS = SWEEP_COLUMNS + PHOTOZ_COLUMNS

#: The photo-z sweeps write -99 where they have no value. Compared against -98
#: rather than -99 exactly because these are f32, and turned into nulls so the
#: ingest omits the field rather than storing a redshift of -99.
MISSING_SENTINEL = -98.0

#: Rows compared when checking that a sweep and its photo-z file are aligned.
#: Comparing every row costs more than it is worth, and a misalignment is never
#: confined to a handful of rows.
ALIGNMENT_PROBE_ROWS = 4096


def _dec_max(name: str) -> float | None:
    """Upper Dec bound of a sweep file's sky box, from its filename."""
    match = SWEEP_NAME.fullmatch(name)
    if match is None:
        return None
    return int(match.group(6)) * (1 if match.group(5) == "p" else -1)


def list_chunks() -> list[Chunk]:
    listing = f"{BASE_URL}/{REGION}/sweep/{SWEEP_VERSION}/"
    names = list_index(listing, SWEEP_NAME.pattern)
    if not names:
        raise RuntimeError(f"no files matching {SWEEP_NAME.pattern!r} at {listing}")

    # dr9/north reduces equatorial sky as well, which this catalog does not keep
    # (see `_resolve_mask`). The filename gives the Dec box, so a file that
    # would minify down to zero rows is decidable before any bytes move.
    # An unparseable name is kept: the row cut below handles it correctly
    # anyway, and guessing from a name nobody recognizes would silently drop
    # real sky.
    kept = [
        name
        for name in names
        if (dec_max := _dec_max(name)) is None or dec_max > RESOLVE_DEC
    ]
    skipped = len(names) - len(kept)
    note = f" ({skipped} below the boundary, skipped)" if skipped else ""
    log(f"Legacy Survey DR9: {len(kept)} sweep files in dr9/{REGION}{note}")
    return [Chunk(id=f"{REGION}/{name}", label=name) for name in kept]


def _parse_chunk_id(chunk_id: str) -> str:
    """The sweep filename a chunk id names, rejecting anything else.

    The id is a path component on the way to a file this module writes and then
    deletes, so it is validated here rather than trusted: BOOM hands back
    whatever it recorded in a previous run.
    """
    region, _, name = chunk_id.partition("/")
    if region != REGION or SWEEP_NAME.fullmatch(name) is None:
        raise RuntimeError(
            f"{chunk_id!r} is not a dr9/{REGION} sweep chunk; "
            "expected e.g. north/sweep-000p032-005p035.fits"
        )
    return name


def _read_columns(path: Path, columns: list[str]):
    """Read `columns` out of the first table HDU of `path` into a DataFrame.

    Column by column off a memory-mapped file rather than through
    `Table.read`: a sweep carries around 150 columns and this keeps roughly 30,
    so materializing the whole table would cost several times the memory for
    nothing.
    """
    import numpy as np
    import pandas as pd
    from astropy.io import fits

    with fits.open(path, memmap=True) as hdul:
        data = hdul[1].data
        # The published columns are uppercase (RA, FLUX_G); astropy's field
        # lookup ignores case but the schema it reports back does not.
        published = {name.upper() for name in data.columns.names}
        missing = [c for c in columns if c.upper() not in published]
        if missing:
            raise RuntimeError(
                f"{path.name} is missing expected columns {missing}; "
                "the published schema may have changed"
            )
        frame = {}
        for column in columns:
            values = data[column]
            # FITS is big-endian and pandas will not take that.
            if values.dtype.byteorder == ">":
                values = values.byteswap().view(values.dtype.newbyteorder())
            # `type` (3A) and `survey` (10A) arrive as fixed-width bytes padded
            # with spaces.
            if values.dtype.kind in ("S", "U"):
                values = np.char.strip(values.astype(str))
            frame[column] = values
        return pd.DataFrame(frame)


def _resolve_mask(frame):
    """Rows of a dr9/north sweep this catalog keeps.

    The survey's own rule for counting a source once is to take the northern
    reduction only where Dec > 32.375 **and** the source is north of the
    Galactic plane, handing the rest to dr9/south. BOOM does not ingest
    dr9/south, so applying the Galactic half here would leave the Dec > 32.375,
    b < 0 sky with no coverage at all rather than covering it from the other
    reduction. Those rows are kept: BASS+MzLS observed that sky too, it is
    simply not where the survey would pick its reduction from.
    """
    return frame["dec"].to_numpy() > RESOLVE_DEC


def _merge_pair(sweep_path: Path, photoz_path: Path, out_path: Path) -> Path:
    """Merge one sweep/photo-z FITS pair into a single parquet file."""
    import numpy as np
    import pandas as pd

    sweep = _read_columns(sweep_path, SWEEP_COLUMNS)
    photoz = _read_columns(photoz_path, PHOTOZ_KEY_COLUMNS + PHOTOZ_COLUMNS)

    if len(sweep) != len(photoz):
        raise RuntimeError(
            f"row count mismatch: {sweep_path.name} has {len(sweep)} rows, "
            f"{photoz_path.name} has {len(photoz)}"
        )
    step = max(1, len(sweep) // ALIGNMENT_PROBE_ROWS)
    probe = slice(None, None, step)
    for key in PHOTOZ_KEY_COLUMNS:
        if not np.array_equal(
            sweep[key].to_numpy()[probe], photoz[key].to_numpy()[probe]
        ):
            raise RuntimeError(
                f"{sweep_path.name} and {photoz_path.name} disagree on {key!r}; "
                "the files are not row-matched"
            )

    frame = pd.concat([sweep, photoz[PHOTOZ_COLUMNS]], axis=1)
    del sweep, photoz

    before = len(frame)
    frame = frame[_resolve_mask(frame)].reset_index(drop=True)
    log(f"Legacy Survey DR9: kept {len(frame)} of {before} rows in {sweep_path.name}")

    for column in PHOTOZ_COLUMNS:
        if frame[column].dtype.kind == "f":
            frame.loc[frame[column] <= MISSING_SENTINEL, column] = np.nan
    # Blank for a source with no photo-z entry at all. Declared as a string
    # dtype rather than left to inference, so a sweep where no row has one --
    # the far north has whole files like that -- still writes a string column
    # instead of parquet's untyped null.
    frame["survey"] = frame["survey"].replace("", None).astype("string")

    ensure_dir(out_path.parent)
    partial = out_path.with_suffix(out_path.suffix + ".part")
    frame[STORED_COLUMNS].to_parquet(partial, index=False)
    # Renamed only once it is complete, so an interrupted chunk leaves no
    # half-written parquet for the retry to mistake for finished work.
    os.replace(partial, out_path)
    log(f"Legacy Survey DR9: wrote {len(frame)} rows to {out_path.name}")
    return out_path


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    name = _parse_chunk_id(chunk_id)
    region_dir = ensure_dir(ensure_dir(dest) / REGION)
    parquet_path = region_dir / name.replace(".fits", ".parquet")
    if parquet_path.exists():
        log(f"Legacy Survey DR9: {name} already converted")
        return [parquet_path]

    photoz_name = name.replace(".fits", "-pz.fits")
    sweep_path = region_dir / name
    photoz_path = region_dir / photoz_name
    for url, path in (
        (f"{BASE_URL}/{REGION}/sweep/{SWEEP_VERSION}/{name}", sweep_path),
        (f"{BASE_URL}/{REGION}/sweep/{PHOTOZ_VERSION}/{photoz_name}", photoz_path),
    ):
        size = content_length(url)
        if already_complete(path, size):
            log(f"Legacy Survey DR9: {path.name} already downloaded")
            continue
        log(f"Legacy Survey DR9: downloading {path.name}")
        download(url, path, expected_size=size)

    try:
        _merge_pair(sweep_path, photoz_path, parquet_path)
    finally:
        # The FITS pair is an intermediate, not the chunk: BOOM deletes the
        # files this returns, so leaving these behind would grow the staging
        # directory by the whole catalog over a run.
        sweep_path.unlink(missing_ok=True)
        photoz_path.unlink(missing_ok=True)
    return [parquet_path]
