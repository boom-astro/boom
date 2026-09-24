"""Legacy Survey Point Source Catalog (LSPSC).

Liu et al. 2025 (arXiv:2505.17174) publish these morphological
resolved/unresolved scores as a cone-search service at
https://ls-xgboost.lbl.gov/, not as files: `/getsources/{ra}/{dec}/{radius}`,
radius capped at 300 arcsec. Reconstructing 3.1e9 rows through that endpoint
would be around a million requests and hundreds of gigabytes against someone
else's research server, so BOOM does not.

Instead BOOM ingests an export of the copy it already holds. The
`export_catalog` task writes gzipped JSONL chunks plus a `manifest.json` from the
`LSPSC` collection, and this module stages them: BOOM is the provenance for its
own copy, and the manifest records which database and release produced it.

Stage the export directory at `$BOOM_LSPSC_DIR`, or leave it where
`export_catalog` puts it, at `<catalog data path>/export/LSPSC`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .base import Chunk
from .http import log

ID = "lspsc"

#: Where the exported artifact is staged.
DIR_ENV = "BOOM_LSPSC_DIR"
#: Falls back to the shared catalog data path, which is where `export_catalog`
#: writes and which the task worker already mounts.
DATA_PATH_ENV = "BOOM_CATALOG_DATA_PATH"


def staged_dir() -> Path:
    explicit = os.getenv(DIR_ENV)
    if explicit:
        return Path(explicit)
    return Path(os.getenv(DATA_PATH_ENV, "data/catalogs")) / "export" / "LSPSC"


def _missing(directory: Path, detail: str) -> RuntimeError:
    return RuntimeError(
        f"{detail} in {directory}. This catalog is an export of BOOM's own copy, not a "
        f"download: run the export_catalog task against a deployment that has LSPSC, "
        f"then stage the directory here or set {DIR_ENV}."
    )


def list_chunks() -> list[Chunk]:
    directory = staged_dir()
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        # Without the manifest the directory may be a half-written export, and
        # ingesting one silently loads a fraction of the catalog.
        raise _missing(directory, "no manifest.json")
    manifest = json.loads(manifest_path.read_text())
    files = sorted(p.name for p in directory.glob("*.jsonl.gz"))
    if not files:
        raise _missing(directory, "manifest.json lists an export but no .jsonl.gz files are")
    log(
        f"LSPSC: {len(files)} exported chunk(s), {manifest.get('rows', 'unknown')} rows, "
        f"from {manifest.get('source_database', 'unknown')}"
    )
    return [Chunk(id=name, label=name) for name in files]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    # `dest` is ignored: the file is already the artifact, and copying it would
    # double the disk for no benefit. BOOM never deletes a staged file.
    path = staged_dir() / chunk_id
    if not path.is_file():
        raise RuntimeError(f"exported chunk {chunk_id} is missing from {staged_dir()}")
    return [path]
