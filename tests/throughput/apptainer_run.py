"""Script to benchmark BOOM. requires: Python 3.8+, pandas>2, pyyaml"""
import argparse
import json
import os
import subprocess
import uuid

import pandas as pd
import yaml

# First, create the config
parser = argparse.ArgumentParser(description="Benchmark BOOM with Apptainer.")
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
config["workers"]["ZTF"]["alert"]["n_workers"] = args.n_alert_workers
config["workers"]["ZTF"]["enrichment"]["n_workers"] = args.n_enrichment_workers
config["workers"]["ZTF"]["filter"]["n_workers"] = args.n_filter_workers
config["database"]["name"] = "boom-benchmarking"
with open("tests/throughput/config.yaml", "w") as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

# Reformat filter for insertion into the database
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

script_dir = os.path.dirname(os.path.abspath(__file__))
boom_dir = os.path.abspath(os.path.join(script_dir, "../../"))

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
subprocess.run(["bash", "tests/throughput/apptainer_run.sh", boom_dir, logs_dir], check=True)

# Now analyze the logs and raise an error if we're too slow
boom_config = (
    f"na={args.n_alert_workers}-"
    f"ne={args.n_enrichment_workers}-"
    f"nf={args.n_filter_workers}"
)
boom_consumer_log_fpath = f"logs/boom-{boom_config}/consumer.log"
boom_scheduler_log_fpath = f"logs/boom-{boom_config}/scheduler.log"
# To calculate BOOM wall time, take the first timestamp from the consumer log
# as the start and the last timestamp of the scheduler as the end
with open(boom_consumer_log_fpath) as f:
    line = f.readline()
    t1_b = pd.to_datetime(
        line.split()[0].replace("\x1b[2m", "").replace("\x1b[0m", "")
    )
with open(boom_scheduler_log_fpath) as f:
    lines = f.readlines()
    line = lines[-3]
    t2_b = pd.to_datetime(
        line.split()[0].replace("\x1b[2m", "").replace("\x1b[0m", "")
    )
wall_time_s = (t2_b - t1_b).total_seconds()
print(f"BOOM throughput test wall time: {wall_time_s:.1f} seconds")

# Save the wall time to a file
os.makedirs(logs_dir, exist_ok=True)
with open(os.path.join(logs_dir, "wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
