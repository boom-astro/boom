"""Prepare the eclipsing-binary/spider pilot for BOOM's catalog task."""

import argparse
import csv
import gzip
import json
import math
from collections import defaultdict
from pathlib import Path

from astropy.io import fits

SDSS = "https://dr20.sdss.org/sas/dr20/"
LAMOST = "https://www.lamost.org/"
DESI = "https://data.desi.lbl.gov/public/dr1/spectro/redux/iron/"
NUMBER_FIELDS = {"mjd", "snr", "rv_km_s", "rv_error_km_s", "match_arcsec", "exptime"}
LEGACY = {
    "26": "sdss2-dr8-sdss/spectro/redux/26",
    "103": "sdss2-dr8-sdss/spectro/redux/103",
    "104": "sdss2-dr8-sdss/spectro/redux/104",
    "v5_13_2": "sdss4-dr17-eboss/spectro/redux/v5_13_2",
}


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def number(value):
    if not value:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def desi_petals(directory):
    for path in directory.glob("*.fits"):
        with fits.open(path) as spectrum:
            for row in spectrum["FIBERMAP"].data:
                yield (str(row["TARGETID"]), str(row["EXPID"])), int(row["PETAL_LOC"])


def locator(row, boss, apogee, petals):
    survey, obs = row["survey"], row["obs"]
    if survey in {"lamost_lrs", "lamost_lrs_all"}:
        return {
            "format": "lamost",
            "url": f"{LAMOST}dr12/v2.0/spectrum/fits/{obs}",
            "extensions": ["COADD"],
        }
    if survey == "lamost_mrs":
        band = row["band"]
        ext = f"{band}-{row['lmjm']}" if row["coadd"] == "0" else f"COADD_{band}"
        return {
            "format": "lamost",
            "url": f"{LAMOST}openapi/dr12/v2.0/mrs/spectrum/fits?obsid={obs}",
            "extensions": [ext],
        }
    if survey == "sdss_bossnet" and obs in boss:
        visit = boss[obs]
        field = int(visit["fieldid"])
        path = f"spectro/boss/redux/v6_2_1/spectra/daily/full/{field // 1000:03}XXX/{field:06}/{visit['mjd']}/{visit['spec_file']}"
        return {"format": "boss", "url": SDSS + path}
    if survey == "sdss_legacy" and row["run2d"] in LEGACY:
        plate, fiber = int(row["plate"]), int(row["fiberid"])
        path = f"prior-surveys/{LEGACY[row['run2d']]}/spectra/lite/{plate:04}/spec-{plate:04}-{row['mjd']}-{fiber:04}.fits"
        return {"format": "boss", "url": SDSS + path}
    if survey == "sdss_apogee" and obs in apogee:
        return {"format": "apogee", "url": SDSS + apogee[obs]["uri"]}
    if survey == "desi_exp" and (row["targetid"], row["expid"]) in petals:
        petal = petals[(row["targetid"], row["expid"])]
        expid = int(row["expid"])
        root = f"{DESI}exposures/{row['night']}/{expid:08}/"
        return {
            "format": "desi",
            "urls": [
                f"{root}cframe-{arm}{petal}-{expid:08}.fits.gz?download=1"
                for arm in "brz"
            ],
            "target_id": row["targetid"],
        }
    return None


def stage(input_dir, boss_csv, apogee_csv, desi_dir, destination):
    boss = {x["spectrum_pk"]: x for x in read_csv(boss_csv)}
    apogee = {x["filename"]: x for x in read_csv(apogee_csv)}
    petals = dict(desi_petals(desi_dir))
    observations = defaultdict(dict)
    fermi = defaultdict(list)
    for row in read_csv(input_dir / "eb_spider_observations.csv"):
        obs_id = ":".join(
            filter(
                None,
                [
                    row["survey"],
                    row["obs"] or row["targetid"],
                    row["lmjm"],
                    row["band"],
                    row["program"],
                ],
            )
        )
        doc = {
            key: number(value) if key in NUMBER_FIELDS else value
            for key, value in row.items()
            if key != "target" and value != ""
        }
        doc.update(id=obs_id, used_for_rv=row["used_for_rv"] == "1")
        if not row["counterpart_check"]:
            doc["spectrum"] = locator(row, boss, apogee, petals)
        observations[row["target"]][obs_id] = doc

    for row in read_csv(input_dir / "eb_spider_fermi_matches.csv"):
        source = row.pop("id")
        row["inside_95"] = row["inside_95"] == "1"
        row["exact_literature_name"] = row["exact_literature_name"] == "1"
        row["ellipse_radius"] = number(row["ellipse_radius"])
        row["separation_arcmin"] = number(row["separation_arcmin"])
        fermi[source].append(row)

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    with gzip.open(temporary, "wt") as stream:
        for source in read_csv(input_dir / "eb_spider_sources.csv"):
            identifier = source["id"]
            document = {
                "_id": identifier,
                "ra": float(source["ra"]),
                "dec": float(source["dec"]),
                "sample": source["sample"],
                "kind": source["kind"],
                "aliases": sorted(
                    set(
                        filter(
                            None,
                            [
                                *source["alternate_name"].split("; "),
                                source["gaia_dr3_id"],
                                source["fermi_literature"],
                            ],
                        )
                    )
                    - {identifier}
                ),
                "period_min": number(source["period_min"]),
                "observations": list(observations[identifier].values()),
                "fermi_matches": fermi[identifier],
                "review": {
                    "status": source["spectrum_assessment"],
                    "span_km_s": source["supported_pixel_span_km_s"],
                    "detail": source["assessment_detail"],
                }
                if source["spectrum_assessment"]
                else None,
            }
            stream.write(
                json.dumps(document, allow_nan=False, separators=(",", ":")) + "\n"
            )
    temporary.replace(destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("input_dir", "boss_csv", "apogee_csv", "desi_dir", "destination"):
        parser.add_argument(arg, type=Path)
    stage(**vars(parser.parse_args()))
