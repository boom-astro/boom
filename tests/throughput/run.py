"""Script to benchmark BOOM.
    requires:
        Python 3.13+,
        pyyaml,
        pandas>2,
        astropy,
        confluent-kafka
"""
import argparse
import json
import os
import subprocess

import pandas as pd
import yaml
from astropy.time import Time

# First, create the config
parser = argparse.ArgumentParser(description="Benchmark BOOM")
parser.add_argument(
    "--n-alert-workers",
    type=int,
    default=3,
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
parser.add_argument(
    "--apptainer",
    action="store_true",
    help="Run the benchmark in Apptainer instead of Docker.",
)
args = parser.parse_args()
use_apptainer = args.apptainer
hosts = {
    "mongo": "localhost" if use_apptainer else "mongo",
    "redis": "localhost" if use_apptainer else "valkey",
    "kafka": "localhost" if use_apptainer else "broker",
}
ports = {
    "mongo": 27018 if use_apptainer else 27017,
    "redis": 6380 if use_apptainer else 6379,
    "kafka": 29192 if use_apptainer else 29092,
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
config["api"]["auth"]["secret_key"] = "benchmark-secret-key-at-least-32-chars"
config["api"]["auth"]["admin_password"] = "benchmarkadmin"
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

logs_dir = os.path.join(
    f"{args.boom_repo_dir}/logs",
    "boom-"
    + (
        f"na={args.n_alert_workers}-"
        f"ne={args.n_enrichment_workers}-"
        f"nf={args.n_filter_workers}"
    ),
)

# Now run the benchmark
os.environ["BOOM_REPO_ROOT"] = os.path.abspath(args.boom_repo_dir)
os.environ["BENCHMARK_MONGO_PORT"] = str(ports["mongo"])
os.environ["BENCHMARK_REDIS_PORT"] = str(ports["redis"])
os.environ["BENCHMARK_KAFKA_PORT"] = str(ports["kafka"])
os.environ["TIMEOUT_SECS"] = str(args.timeout)
cmd = [
    "bash",
    os.path.join(args.boom_repo_dir, "tests", "throughput", "_run.sh"),
    logs_dir,
]
if args.keep_up:
    cmd.append("--keep-up")
if use_apptainer:
    cmd.append("--apptainer")
subprocess.run(cmd, check=True)

# Now analyze the logs and raise an error if we're too slow
t1_b, t2_b = None, None

def extract_date_from_log(line_to_process, is_on_apptainer):
    line_index = 0 if is_on_apptainer else 2 # Docker logs have two extra columns
    return pd.to_datetime(
        line_to_process.split()[line_index].replace("\x1b[2m", "").replace("\x1b[0m", "")
    )

# To calculate BOOM wall time, take:
# - Start: timestamp of the first message received by the consumer
# - End: last timestamp in the scheduler log
with open(f"{logs_dir}/consumer.log") as f:
    lines = f.readlines()
    for line in lines:
        if "Consumer received first message, continuing..." in line:
            t1_b = extract_date_from_log(line, use_apptainer)
            break

if t1_b is None:
    raise ValueError("Could not find start time in consumer log")
with open(f"{logs_dir}/scheduler.log") as f:
    lines = f.readlines()
    if len(lines) < 3:
        raise ValueError(
            "Scheduler log has fewer than 3 lines; cannot determine end time."
        )
    line = lines[-3]
    t2_b = extract_date_from_log(line, use_apptainer)

wall_time_s = (t2_b - t1_b).total_seconds()
print(f"BOOM throughput test wall time: {wall_time_s:.1f} seconds")

# Save the wall time to a file
os.makedirs(logs_dir, exist_ok=True)
with open(os.path.join(logs_dir, "wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
