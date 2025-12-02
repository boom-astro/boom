"""Script to benchmark BOOM. requires: Python 3.8+, pyyaml, pandas>2, astropy"""
import argparse
import json
import os
import subprocess
import uuid

import pandas as pd
import yaml
from astropy.time import Time

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
    default=18,
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
config["database"]["host"] = "localhost"
config["database"]["password"] = "mongoadminsecret"
config["kafka"]["consumer"]["ztf"]["server"] = "localhost:29092"
config["kafka"]["consumer"]["ztf"]["group_id"] = "throughput-benchmarking"
config["kafka"]["producer"]["server"] = "localhost:29092"
config["redis"]["host"] = "localhost"
config["api"]["auth"]["secret_key"] = "1234"
config["api"]["auth"]["admin_password"] = "adminsecret"
with open("tests/throughput/config.yaml", "w") as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

# Reformat filter for insertion into the database
with open("tests/throughput/cats150.pipeline.json", "r") as f:
    cats150 = json.load(f)

now_jd = Time.now().jd
for_insert = {
    "_id": "replaced-in-mongo-init-script",
    "name": "cats150-replaced-in-mongo-init-script",
    "survey": "ZTF",
    "user_id": "benchmarking",
    "permissions": {
        "ZTF": [1, 2, 3]
    },
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
with open("tests/throughput/cats150.filter.json", "w") as f:
    json.dump(for_insert, f)

script_dir = os.path.dirname(os.path.abspath(__file__))
boom_dir = os.path.abspath(os.path.join(script_dir, "../../"))

logs_dir = os.path.join(
    f"{boom_dir}/tests/apptainer/logs",
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
t1_b, t2_b = None, None

# To calculate BOOM wall time, take:
# - Start: timestamp of the first message received by the consumer
# - End: last timestamp in the scheduler log
with open(f"{logs_dir}/consumer.log") as f:
    lines = f.readlines()
    for line in lines:
        if "Consumer received first message, continuing..." in line:
            t1_b = pd.to_datetime(
                line.split()[0].replace("\x1b[2m", "").replace("\x1b[0m", "")
            )
            break
if t1_b is None:
    raise ValueError("Could not find start time in consumer log")

with open(f"{logs_dir}/scheduler.log") as f:
    lines = f.readlines()
    if len(lines) < 3:
        raise ValueError("Scheduler log has fewer than 3 lines; cannot determine end time.")
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
