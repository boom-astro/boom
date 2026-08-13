"""HTTP wrapper around a TEMPO evidential model bundle.

boom's Rust API cannot load the TEMPO checkpoint itself -- it is a PyTorch
transformer, not an ONNX graph -- so the classify endpoint forwards the
photometry it has already gathered to this service and renders what comes back.

The request body is boom's own photometry block, unchanged, so the Rust side
never has to reshape a light curve; the ZTF-table -> event-array conversion
happens here, in `tempo_hyrax.raw_photometry`, next to the model that defines it.

    TEMPO_BUNDLE_DIR=/models/tempo uvicorn app:app --host 0.0.0.0 --port 8500
"""
from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from predictor import TempoBundle

logging.basicConfig(level=os.environ.get("TEMPO_LOG_LEVEL", "INFO").upper())
log = logging.getLogger("tempo")

BUNDLE_DIR = os.environ.get("TEMPO_BUNDLE_DIR", "/models/tempo")
MODEL_ID = os.environ.get("TEMPO_MODEL_ID", "tempo_evidential")

app = FastAPI(title="TEMPO inference", version="1.0")
_bundle: TempoBundle | None = None
_load_error: str | None = None


@app.on_event("startup")
def _load() -> None:
    # Loaded once at startup rather than per request: a cold torch load is seconds.
    # A failure is kept rather than re-raised so `/classify` can report why the model
    # is unusable; a dead container would only give the caller a connection error.
    global _bundle, _load_error
    try:
        _bundle = TempoBundle(BUNDLE_DIR)
    except Exception as error:  # noqa: BLE001 - surfaced by /classify instead
        log.exception("failed to load TEMPO bundle from %s", BUNDLE_DIR)
        _load_error = str(error)


class ClassifyRequest(BaseModel):
    object_id: str
    # boom's photometry block: {"prv_candidates": [...], "prv_nondetections": [...],
    # "fp_hists": [...]}. Only detections carry a magnitude, so only they are used.
    photometry: dict[str, Any] = Field(default_factory=dict)
    # The scored alert's candidate block, enrichment and cross-matches. boom sends
    # this to every model; TEMPO is photometry-only and ignores it. Declared anyway
    # so the field is part of this service's contract rather than silently dropped.
    metadata: dict[str, Any] = Field(default_factory=dict)
    model: str | None = None
    # Forced photometry is a different measurement of the same epochs; mixing it
    # with alert magnitudes changes what the model sees, so it stays opt-in.
    include_forced_photometry: bool = False


def _photometry_frame(photometry: dict[str, Any], include_forced: bool) -> pd.DataFrame:
    """boom's photometry block -> the flat ZTF table `raw_photometry` expects.

    Non-detections are dropped: they have no `magpsf`, and the upstream
    normalizer discards rows with a null magnitude anyway.
    """
    sources = ["prv_candidates"]
    if include_forced:
        sources.append("fp_hists")

    rows = []
    for source in sources:
        for point in photometry.get(source) or []:
            if not isinstance(point, dict):
                continue
            jd, mag, magerr, fid = (
                point.get("jd"),
                point.get("magpsf"),
                point.get("sigmapsf"),
                point.get("fid"),
            )
            if jd is None or mag is None or magerr is None or fid is None:
                continue
            rows.append({"jd": jd, "magpsf": mag, "sigmapsf": magerr, "fid": fid})

    return pd.DataFrame(rows, columns=["jd", "magpsf", "sigmapsf", "fid"])


def _ready() -> TempoBundle:
    if _bundle is None:
        detail = (
            f"TEMPO bundle failed to load: {_load_error}"
            if _load_error
            else "model bundle is not loaded yet"
        )
        raise HTTPException(status_code=503, detail=detail)
    return _bundle


@app.post("/classify")
def classify(request: ClassifyRequest) -> dict[str, Any]:
    bundle = _ready()
    if request.model is not None and request.model != MODEL_ID:
        raise HTTPException(status_code=400, detail=f"unknown model: {request.model}")

    frame = _photometry_frame(request.photometry, request.include_forced_photometry)
    if frame.empty:
        raise HTTPException(
            status_code=422,
            detail=f"no usable photometry for {request.object_id}: need detections with jd, magpsf, sigmapsf and fid",
        )

    try:
        result = bundle.classify(frame, request.object_id)
    except Exception as error:  # noqa: BLE001 - surfaced to the caller as a 500
        log.exception("inference failed for %s", request.object_id)
        raise HTTPException(status_code=500, detail=f"inference failed: {error}") from error

    if result is None:
        raise HTTPException(
            status_code=422,
            detail=f"photometry for {request.object_id} produced no usable events",
        )

    result["model"] = MODEL_ID
    return result
