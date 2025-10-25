"""Script to benchmark BOOM."""
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "pyyaml",
#     "pandas>2",
# ]
# ///

import argparse
import json
import os
import subprocess
import uuid

import pandas as pd
import yaml

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
    default=6,
    help="Number of machine learning workers to use for benchmarking.",
)
parser.add_argument(
    "--n-filter-workers",
    type=int,
    default=1,
    help="Number of filter workers to use for benchmarking.",
)
args = parser.parse_args()
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)
config["workers"]["ztf"]["alert"]["n_workers"] = args.n_alert_workers
config["workers"]["ztf"]["enrichment"]["n_workers"] = args.n_enrichment_workers
config["workers"]["ztf"]["filter"]["n_workers"] = args.n_filter_workers
config["database"]["name"] = "boom-benchmarking"
config["database"]["host"] = "mongo"
config["database"]["password"] = "mongoadminsecret"
config["kafka"]["consumer"]["ztf"]["server"] = "broker:29092"
config["kafka"]["consumer"]["ztf"]["group_id"] = "throughput-benchmarking"
config["kafka"]["producer"] = "broker:29092"
config["redis"]["host"] = "valkey"
config["api"]["auth"]["secret_key"] = "1234"
config["api"]["auth"]["admin_password"] = "adminsecret"
with open("tests/throughput/config.yaml", "w") as f:
    yaml.safe_dump(config, f, default_flow_style=False)

# Reformat filter for insertion into database
with open("tests/throughput/cats150.pipeline.json", "r") as f:
    cats150 = json.load(f)
for_insert = {
    "_id": str(uuid.uuid4()),
    "catalog": "ZTF_alerts",
    "permissions": [1, 2, 3],
    "active": True,
    "active_fid": "first",
    "fv": [
        {
            "fid": "first",
            "created_at": "2021-01-01T00:00:00",
            "pipeline": json.dumps(cats150),
        }
    ],
}
with open("tests/throughput/cats150.filter.json", "w") as f:
    json.dump(for_insert, f)

logs_dir = os.path.join(
    "logs",
    "boom-"
    + (
        f"na={args.n_alert_workers}-"
        f"ne={args.n_enrichment_workers}-"
        f"nf={args.n_filter_workers}"
    ),
)

# Now run the benchmark
subprocess.run(["bash", "tests/throughput/_run.sh", logs_dir], check=True)

# Now analyze the logs and raise an error if we're too slow
boom_config = (
    f"na={args.n_alert_workers}-"
    f"ne={args.n_enrichment_workers}-"
    f"nf={args.n_filter_workers}"
)
boom_consumer_log_fpath = f"logs/boom-{boom_config}/consumer.log"
boom_scheduler_log_fpath = f"logs/boom-{boom_config}/scheduler.log"
t1_b, t2_b = None, None
# To calculate BOOM wall time, take:
# - Start: timestamp of the first message received by the consumer
# - End: last timestamp in the scheduler log
with open(boom_consumer_log_fpath) as f:
    lines = f.readlines()
    for line in lines:
        if "Consumer received first message, continuing..." in line:
            t1_b = pd.to_datetime(
                line.split()[2].replace("\x1b[2m", "").replace("\x1b[0m", "")
            )
            break

if t1_b is None:
    raise ValueError("Could not find start time in consumer log")
with open(boom_scheduler_log_fpath) as f:
    lines = f.readlines()
    line = lines[-3]
    t2_b = pd.to_datetime(
        line.split()[2].replace("\x1b[2m", "").replace("\x1b[0m", "")
    )
if t2_b is None:
    raise ValueError("Could not find end time in scheduler log")

wall_time_s = (t2_b - t1_b).total_seconds()
print(f"BOOM throughput test wall time: {wall_time_s:.1f} seconds")

# Save the wall time to a file
os.makedirs(logs_dir, exist_ok=True)
with open(os.path.join(logs_dir, "wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
