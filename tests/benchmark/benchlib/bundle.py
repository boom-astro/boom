"""Replay bundles: a night of raw alert packets plus an index describing them.

A bundle directory holds:

    meta.json     survey, night label, provenance, counts
    payloads.bin  the packets, byte for byte as the survey published them
    index.csv     one row per packet, columns INDEX_COLUMNS

Packets are stored unmodified so BOOM decodes exactly what it would in
production. The index carries what the replay and the fixture slicer need
without decoding packets again: identifiers, position, exposure time and, for
captured streams, the broker timestamp the packet originally arrived with.
"""

from __future__ import annotations

import csv
import io
import json
import mmap
import time
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import fastavro
import pandas as pd

from . import normalize_survey
from .timeutil import mjd_to_jd

INDEX_COLUMNS = [
    "offset",
    "length",
    "topic",
    "alert_id",
    "object_id",
    "ra",
    "dec",
    "t_obs_jd",
    "t_kafka_ms",
    "programid",
]

ZTF_PROGRAM_NAMES = {1: "public", 2: "partnership", 3: "caltech"}

AVRO_CONTAINER_MAGIC = b"Obj\x01"


def topic_for(survey: str, night: str, programid: int | None = None) -> str:
    """Topic the BOOM consumer subscribes to for `night` (see src/kafka/*.rs)."""
    survey = normalize_survey(survey)
    if survey == "ZTF":
        return f"ztf_{night}_programid{int(programid or 1)}"
    if survey == "LSST":
        return "lsst-alerts-v11"
    if survey == "DECAM":
        return f"farts_v2_{night}"
    return f"winter_{night}"


class SchemaRegistry:
    """Confluent schema registry client with an on-disk cache.

    LSST packets are framed as a zero byte, a 4-byte schema id, then schemaless
    Avro. Fetched schemas are written into the bundle so it can be re-indexed
    offline.
    """

    def __init__(self, url: str | None, cache_dir: Path | None = None):
        self.url = url.rstrip("/") if url else None
        self.cache_dir = cache_dir
        self._parsed: dict[int, dict] = {}

    def get(self, schema_id: int):
        if schema_id in self._parsed:
            return self._parsed[schema_id]
        raw = None
        cached = self.cache_dir / f"{schema_id}.json" if self.cache_dir else None
        if cached and cached.exists():
            raw = cached.read_text()
        elif self.url:
            with urllib.request.urlopen(f"{self.url}/schemas/ids/{schema_id}", timeout=30) as resp:
                raw = json.loads(resp.read())["schema"]
            if cached:
                cached.parent.mkdir(parents=True, exist_ok=True)
                cached.write_text(raw)
        if raw is None:
            raise LookupError(f"schema {schema_id} is neither cached nor fetchable")
        parsed = fastavro.parse_schema(json.loads(raw))
        self._parsed[schema_id] = parsed
        return parsed


def _container_record(payload: bytes) -> dict:
    return next(iter(fastavro.reader(io.BytesIO(payload))))


def _confluent_record(payload: bytes, registry: SchemaRegistry) -> dict:
    if len(payload) < 5 or payload[0] != 0:
        raise ValueError("not a Confluent-framed Avro packet")
    schema_id = int.from_bytes(payload[1:5], "big")
    return fastavro.schemaless_reader(io.BytesIO(payload[5:]), registry.get(schema_id))


def describe_packet(payload: bytes, survey: str, registry: SchemaRegistry | None = None) -> dict:
    """Identifiers, position and exposure time of one packet.

    Field names follow the raw alert structs in src/alert/*.rs.
    """
    survey = normalize_survey(survey)
    if survey == "LSST":
        if payload[:4] == AVRO_CONTAINER_MAGIC:
            rec = _container_record(payload)
        else:
            if registry is None:
                raise ValueError("LSST packets need a schema registry to be described")
            rec = _confluent_record(payload, registry)
        src = rec["diaSource"]
        dia_object_id = src.get("diaObjectId")
        ss_object_id = src.get("ssObjectId")
        # BOOM's object id: the diaObject, else "sso<ssObjectId>" (src/alert/lsst.rs).
        if dia_object_id:
            object_id = str(dia_object_id)
        elif ss_object_id:
            object_id = f"sso{ss_object_id}"
        else:
            object_id = ""
        return {
            "alert_id": int(src["diaSourceId"]),
            "object_id": object_id,
            "ra": float(src["ra"]),
            "dec": float(src["dec"]),
            "t_obs_jd": mjd_to_jd(float(src["midpointMjdTai"])),
            "programid": None,
        }

    rec = _container_record(payload)
    cand = rec.get("candidate") or {}
    jd = cand.get("jd")
    if jd is None and cand.get("mjd") is not None:
        jd = mjd_to_jd(float(cand["mjd"]))
    object_id = rec.get("objectId") or rec.get("objectid")
    return {
        "alert_id": int(rec["candid"]),
        "object_id": str(object_id),
        "ra": float(cand["ra"]),
        "dec": float(cand["dec"]),
        "t_obs_jd": float(jd),
        "programid": int(cand["programid"]) if survey == "ZTF" and cand.get("programid") else None,
    }


class BundleWriter:
    def __init__(self, path: Path, survey: str, night: str, source: str, overwrite: bool = False):
        self.path = Path(path)
        self.survey = normalize_survey(survey)
        self.night = night
        self.source = source
        self.path.mkdir(parents=True, exist_ok=True)
        payloads = self.path / "payloads.bin"
        if payloads.exists() and not overwrite:
            raise FileExistsError(f"{payloads} exists; pass overwrite to replace it")
        self._payloads = open(payloads, "wb")
        self._index_file = open(self.path / "index.csv", "w", newline="")
        self._index = csv.writer(self._index_file)
        self._index.writerow(INDEX_COLUMNS)
        self._offset = 0
        self.count = 0
        self.skipped = 0

    def add(self, payload: bytes, *, topic: str, alert_id, object_id, ra, dec, t_obs_jd,
            t_kafka_ms=None, programid=None) -> None:
        self._payloads.write(payload)
        self._index.writerow([
            self._offset, len(payload), topic, alert_id, object_id, ra, dec,
            repr(float(t_obs_jd)) if t_obs_jd is not None else "",
            t_kafka_ms if t_kafka_ms is not None else "",
            programid if programid is not None else "",
        ])
        self._offset += len(payload)
        self.count += 1

    def close(self, extra: dict | None = None) -> None:
        self._payloads.close()
        self._index_file.close()
        meta = {
            "survey": self.survey,
            "night": self.night,
            "source": self.source,
            "n_packets": self.count,
            "n_skipped": self.skipped,
            "bytes": self._offset,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        meta.update(extra or {})
        (self.path / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if not self._payloads.closed:
            self.close()


@dataclass
class Bundle:
    path: Path
    meta: dict
    index: pd.DataFrame

    @classmethod
    def open(cls, path) -> "Bundle":
        path = Path(path)
        meta = json.loads((path / "meta.json").read_text())
        index = pd.read_csv(
            path / "index.csv",
            dtype={"topic": str, "object_id": str, "alert_id": "int64"},
            keep_default_na=False,
            na_values={"t_kafka_ms": [""], "programid": [""], "t_obs_jd": [""]},
        )
        # Packets a capture could not decode have blank fields; they replay but join nothing.
        for col in ("ra", "dec", "t_obs_jd", "t_kafka_ms", "programid"):
            index[col] = pd.to_numeric(index[col], errors="coerce")
        return cls(path=path, meta=meta, index=index)

    @property
    def survey(self) -> str:
        return self.meta["survey"]

    @property
    def night(self) -> str:
        return self.meta["night"]

    @contextmanager
    def payloads(self):
        with open(self.path / "payloads.bin", "rb") as f:
            if f.seek(0, 2) == 0:
                yield b""
                return
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                yield mm
            finally:
                mm.close()
