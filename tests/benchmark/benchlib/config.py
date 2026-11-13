"""Build the BOOM config for one benchmark run.

The run config is the repository's base `config.yaml`, pointed at the benchmark
stack, then overlaid with profiles (`profiles/*.yaml`) and `--set` overrides in
that order. A profile may also carry a top-level `benchmark:` block of harness
directives (GPU image, Milvus, catalog subset); it is removed before the config
is written.
"""

from __future__ import annotations

import copy
from pathlib import Path

import yaml

from . import API_ADMIN_PASSWORD, API_SECRET_KEY, DB_NAME, MONGO_PASSWORD, MONGO_USER, REPO_ROOT

HARNESS_KEY = "benchmark"


def deep_merge(base: dict, overlay: dict) -> dict:
    """Merge `overlay` into a copy of `base`. Lists replace; `None` deletes the key."""
    out = copy.deepcopy(base)
    for key, value in (overlay or {}).items():
        if value is None:
            out.pop(key, None)
        elif isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def parse_set(expr: str) -> dict:
    """`a.b.c=value` as a nested overlay; the value is parsed as YAML."""
    if "=" not in expr:
        raise ValueError(f"--set expects key=value, got {expr!r}")
    key, raw = expr.split("=", 1)
    value = yaml.safe_load(raw)
    for part in reversed(key.strip().split(".")):
        value = {part: value}
    return value


def load_profile(name_or_path: str) -> dict:
    path = Path(name_or_path)
    if not path.exists():
        path = Path(__file__).resolve().parents[1] / "profiles" / f"{name_or_path}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"profile {name_or_path!r} not found")
    return yaml.safe_load(path.read_text()) or {}


def stack_settings() -> dict:
    """Point every client at the benchmark stack's services."""
    consumers = {s: {"server": "broker:29092", "group_id": f"boom-bench-{s}", "username": "", "password": ""}
                 for s in ("ztf", "lsst", "decam", "winter")}
    for name in ("ztf", "decam", "winter"):
        consumers[name]["security_protocol"] = ""
    return {
        "database": {"host": "mongo", "port": 27017, "name": DB_NAME,
                     "username": MONGO_USER, "password": MONGO_PASSWORD},
        "cutouts_storage": {"type": "mongo", "host": "mongo", "port": 27017, "name": DB_NAME,
                            "username": MONGO_USER, "password": MONGO_PASSWORD},
        "redis": {"host": "valkey", "port": 6379},
        "kafka": {"consumer": consumers, "producer": {"server": "broker:29092"}},
        "api": {"port": 4000, "auth": {"secret_key": API_SECRET_KEY, "admin_password": API_ADMIN_PASSWORD}},
        # Nothing in the stack collects these; leaving them on costs throughput.
        "posthog": {"project_api_key": ""},
        # Off unless a profile turns it on, so no run writes to the shared NRP instance.
        "milvus": {"read_embeddings": False, "write_embeddings": False},
    }


MILVUS_LOCAL = {
    "milvus": {
        "host": "milvus", "port": 19530, "tls": False, "username": "", "password": "",
        "database": "", "read_embeddings": True, "write_embeddings": True,
    }
}


def keep_catalogs(config: dict, keep: list[str] | None) -> dict:
    """Drop every crossmatch catalog not named in `keep` (None keeps all)."""
    if keep is None:
        return config
    wanted = set(keep)
    for survey, catalogs in (config.get("crossmatch") or {}).items():
        if isinstance(catalogs, dict):
            config["crossmatch"][survey] = {k: v for k, v in catalogs.items() if k in wanted}
    return config


def build_run_config(profiles: list[str], sets: list[str],
                     base_path: Path | None = None) -> tuple[dict, dict]:
    """Return (BOOM config, harness directives)."""
    base = yaml.safe_load((base_path or REPO_ROOT / "config.yaml").read_text())
    config = deep_merge(base, stack_settings())
    directives: dict = {}
    overlays = [load_profile(p) for p in profiles] + [parse_set(s) for s in sets]
    for overlay in overlays:
        overlay = copy.deepcopy(overlay)
        directives = deep_merge(directives, overlay.pop(HARNESS_KEY, {}) or {})
        config = deep_merge(config, overlay)
    if directives.get("milvus"):
        config = deep_merge(config, MILVUS_LOCAL)
    config.setdefault("gpu", {})["enabled"] = bool(directives.get("gpu", False))
    config = keep_catalogs(config, directives.get("catalogs"))
    return config, directives


def write_config(config: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, default_flow_style=False, sort_keys=False))


def crossmatch_catalogs(config: dict, surveys: list[str]) -> dict[str, float]:
    """Catalog name -> largest search radius (arcsec) over the given surveys."""
    out: dict[str, float] = {}
    for survey in surveys:
        for name, spec in ((config.get("crossmatch") or {}).get(survey.lower()) or {}).items():
            if not isinstance(spec, dict):
                continue
            radius = max(float(spec.get("radius") or 0.0), float(spec.get("angular_size_radius_max") or 0.0))
            out[name] = max(out.get(name, 0.0), radius)
    return out
