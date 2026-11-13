"""Shared code for the BOOM benchmark harness (tests/benchmark)."""

import os
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = BENCH_DIR.parents[1]

# Survey names as BOOM spells them in collection and queue names.
SURVEYS = ("ZTF", "LSST", "DECAM", "WINTER")

DB_NAME = "boom-benchmarking"
MONGO_USER = "mongoadmin"
MONGO_PASSWORD = "mongoadminsecret"

# Host-side ports, offset from the dev stack's so both can run at once.
MONGO_HOST_PORT = int(os.environ.get("MONGO_HOST_PORT", "27117"))
SEED_MONGO_PORT = int(os.environ.get("SEED_MONGO_PORT", "27217"))
VALKEY_HOST_PORT = int(os.environ.get("VALKEY_HOST_PORT", "16379"))
KAFKA_HOST_PORT = int(os.environ.get("KAFKA_HOST_PORT", "19092"))
API_HOST_PORT = int(os.environ.get("API_HOST_PORT", "14000"))
MILVUS_HOST_PORT = int(os.environ.get("MILVUS_HOST_PORT", "19631"))

# Large state (seed database, per-run copy, bundles) lives outside the repo.
DATA_ROOT = Path(os.environ.get("BENCH_DATA_ROOT", Path.home() / "boom-bench"))
RESULTS_ROOT = Path(os.environ.get("BENCH_RESULTS_ROOT", REPO_ROOT / "logs" / "benchmark"))

# The API's JWT secret inside the benchmark stack. Not a credential: the stack
# is local and torn down after every run.
API_SECRET_KEY = "boom-benchmark-secret"
API_ADMIN_PASSWORD = "boom-benchmark-admin"


def mongo_uri(port: int = MONGO_HOST_PORT, host: str = "localhost") -> str:
    return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{host}:{port}/?authSource=admin"


def normalize_survey(name: str) -> str:
    upper = name.strip().upper()
    if upper not in SURVEYS:
        raise ValueError(f"unknown survey {name!r}; expected one of {SURVEYS}")
    return upper
