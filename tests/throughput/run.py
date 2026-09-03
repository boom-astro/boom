"""Script to benchmark BOOM."""
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pyyaml",
#     "pandas>2",
#     "astropy",
# ]
# ///

import argparse
import json
import os
import re
import subprocess

import pandas as pd
import yaml
from astropy.time import Time

# Match an ISO-8601 timestamp like `2026-05-07T18:00:00.000000Z` anywhere in
# the line. Boom log lines may now be prefixed with `trace_id=<hex> span_id=<hex>`
# from the OTel formatter when an active span is in scope, so positional
# splitting (`line.split()[2]`) is no longer reliable.
TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z")


def parse_log_timestamp(line: str) -> pd.Timestamp:
    # Strip any ANSI escapes that older log lines might still contain.
    cleaned = line.replace("\x1b[2m", "").replace("\x1b[0m", "")
    match = TIMESTAMP_RE.search(cleaned)
    if not match:
        raise ValueError(f"Could not find ISO timestamp in log line: {line!r}")
    return pd.to_datetime(match.group(0))

# First, create the config
parser = argparse.ArgumentParser(description="Benchmark BOOM")
parser.add_argument(
    "--n-alert-workers",
    type=int,
    default=4,
    help="Number of alert workers to use for benchmarking.",
)
parser.add_argument(
    "--n-enrichment-workers",
    type=int,
    default=4,
    help="Number of enrichment workers to use for benchmarking.",
)
parser.add_argument(
    "--n-filter-workers",
    type=int,
    default=2,
    help="Number of filter workers to use for benchmarking.",
)
parser.add_argument(
    "--keep-up",
    action="store_true",
    help="Whether to keep the BOOM services up after the benchmark completes.",
    default=False,
)
parser.add_argument(
    "--cutouts-storage-type",
    choices=["s3", "mongo"],
    default="mongo",
    help="Cutout storage backend to benchmark (default: mongo).",
)
parser.add_argument(
    "--cache-ttl-seconds",
    type=int,
    default=30,
    help="Cutout cache TTL in seconds, S3 only (default: 30).",
)
parser.add_argument(
    "--cache-max-memory",
    default="1gb",
    help="Cutout cache max memory, S3 only (default: 1gb).",
)
parser.add_argument(
    "--boom-repo-dir",
    help="Path to the BOOM repo directory.",
    default=".",
)
parser.add_argument(
    "--timeout",
    type=int,
    default=300,
    help="Number of seconds to wait before considering the benchmark a failure.",
)
args = parser.parse_args()
hosts = {
    "mongo": "mongo",
    "redis": "valkey",
    "kafka": "broker",
}
ports = {
    "mongo": 27017,
    "redis": 6379,
    "kafka": 29092,
}
with open(os.path.join(args.boom_repo_dir, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)
config["database"]["host"] = hosts["mongo"]
config["database"]["port"] = ports["mongo"]
config["database"]["name"] = "boom-benchmarking"
config["database"]["password"] = "mongoadminsecret"
config["redis"]["host"] = hosts["redis"]
config["redis"]["port"] = ports["redis"]
config["kafka"]["consumer"]["ztf"]["server"] = f"{hosts['kafka']}:{ports['kafka']}"
config["kafka"]["consumer"]["ztf"]["group_id"] = "throughput-benchmarking"
config["kafka"]["producer"]["server"] = f"{hosts['kafka']}:{ports['kafka']}"
config["api"]["port"] = 4000
config["api"]["auth"]["secret_key"] = "1234"
config["api"]["auth"]["admin_password"] = "adminsecret"
config["cutouts_storage"]["type"] = args.cutouts_storage_type
if args.cutouts_storage_type == "s3":
    config["cutouts_storage"]["access_key"] = "rustfsadmin"
    config["cutouts_storage"]["secret_key"] = "rustfsadminsecret"
    config["cutouts_storage"]["cache"]["host"] = "valkey-cutouts"
    config["cutouts_storage"]["cache"]["ttl_seconds"] = args.cache_ttl_seconds
    config["cutouts_storage"]["cache"]["max_memory"] = args.cache_max_memory
elif args.cutouts_storage_type == "mongo":
    config["cutouts_storage"]["host"] = "mongo"
    config["cutouts_storage"]["name"] = "boom-benchmarking"
    config["cutouts_storage"]["username"] = "mongoadmin"
    config["cutouts_storage"]["password"] = "mongoadminsecret"
config["babamul"]["enabled"] = True
config["workers"]["ztf"]["alert"]["n_workers"] = args.n_alert_workers
config["workers"]["ztf"]["enrichment"]["n_workers"] = args.n_enrichment_workers
config["workers"]["ztf"]["filter"]["n_workers"] = args.n_filter_workers
with open(
    os.path.join(args.boom_repo_dir, "tests", "throughput", "config.yaml"), "w"
) as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

# Reformat filter for insertion into database
with open(
    os.path.join(
        args.boom_repo_dir, "tests", "throughput", "cats150.pipeline.json"
    ),
    "r",
) as f:
    cats150 = json.load(f)

now_jd = Time.now().jd
for_insert = {
    "_id": "replaced-in-mongo-init-script",
    "name": "cats150-replaced-in-mongo-init-script",
    "survey": "ZTF",
    "user_id": "benchmarking",
    "permissions": {"ZTF": [1, 2, 3]},
    "active": True,
    "active_fid": "first",
    "fv": [
        {
            "fid": "first",
            "created_at": now_jd,
            "pipeline": json.dumps(cats150),
        }
    ],
    "created_at": now_jd,
    "updated_at": now_jd,
}
with open(
    os.path.join(
        args.boom_repo_dir, "tests", "throughput", "cats150.filter.json"
    ),
    "w",
) as f:
    json.dump(for_insert, f)

if os.environ.get("BOOM_GPU__ENABLED", "false").lower() == "true":
    gpus = len(
        [d for d in os.environ.get("BOOM_GPU__DEVICE_IDS", "0").split(",") if d.strip()]
    )
else:
    gpus = 0

logs_dir = os.path.join(
    f"{args.boom_repo_dir}/logs",
    "boom-"
    + (
        f"na={args.n_alert_workers}-"
        f"ne={args.n_enrichment_workers}-"
        f"nf={args.n_filter_workers}-"
        f"gpu={gpus}"
    ),
)

# Now run the benchmark
os.environ["BOOM_REPO_ROOT"] = os.path.abspath(args.boom_repo_dir)
os.environ["TIMEOUT_SECS"] = str(args.timeout)
os.environ["BOOM_CUTOUTS_STORAGE__TYPE"] = args.cutouts_storage_type
cmd = [
    "bash",
    os.path.join(args.boom_repo_dir, "tests", "throughput", "_run.sh"),
    logs_dir,
]
if args.keep_up:
    cmd.append("--keep-up")
subprocess.run(cmd, check=True)

# Now analyze the logs and raise an error if we're too slow
t1_b, t2_b = None, None

# To calculate BOOM wall time, take:
# - Start: timestamp of the first message received by the consumer
# - End: last timestamp in the scheduler log
with open(f"{logs_dir}/consumer.log") as f:
    lines = f.readlines()
    for line in lines:
        if "Consumer received first message, continuing..." in line:
            t1_b = parse_log_timestamp(line)
            break

if t1_b is None:
    raise ValueError("Could not find start time in consumer log")
with open(f"{logs_dir}/scheduler.log") as f:
    lines = f.readlines()
    if len(lines) < 3:
        raise ValueError(
            "Scheduler log has fewer than 3 lines; cannot determine end time."
        )
    t2_b = parse_log_timestamp(lines[-3])

wall_time_s = (t2_b - t1_b).total_seconds()
print(f"BOOM throughput test wall time: {wall_time_s:.1f} seconds")

# Save the wall time to a file
os.makedirs(logs_dir, exist_ok=True)
with open(os.path.join(logs_dir, "wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
