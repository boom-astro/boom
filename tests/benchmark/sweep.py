"""Run a plan: a list of run.py configurations, each repeated.

    uv run --project tests/benchmark tests/benchmark/sweep.py plans/ablation.yaml --dry-run
    uv run --project tests/benchmark tests/benchmark/sweep.py plans/ablation.yaml --only gpu,milvus

A plan file has `name`, `repeats`, `defaults` (run.py options shared by every
step) and `steps` (each a `name` plus options overriding the defaults; list
options such as `profile` and `set` are appended to the defaults'). Keys are
run.py's long options without the dashes.

Repeats are interleaved (every step once, then every step again), so slow drift
on the machine spreads across steps instead of landing on one.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

import yaml

from benchlib import BENCH_DIR, RESULTS_ROOT

APPENDED = {"profile", "set", "tag", "bundle"}


def to_cli(options: dict) -> list[str]:
    out = []
    for key, value in options.items():
        flag = f"--{key}"
        if value is None or value is False:
            continue
        if value is True:
            out.append(flag)
        elif isinstance(value, list):
            for v in value:
                out += [flag, os.path.expanduser(str(v))]
        else:
            out += [flag, os.path.expanduser(str(value))]
    return out


def merge(defaults: dict, step: dict) -> dict:
    out = dict(defaults)
    for k, v in step.items():
        if k == "name":
            continue
        if k in APPENDED and isinstance(v, list):
            out[k] = list(defaults.get(k, [])) + v
        else:
            out[k] = v
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("plan", type=Path)
    p.add_argument("--repeats", type=int, default=None)
    p.add_argument("--only", default=None, help="comma-separated step names")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stop-on-error", action="store_true")
    p.add_argument("--extra", default="", help="extra run.py arguments for every step, e.g. '--skip-build'")
    args = p.parse_args()

    plan = yaml.safe_load(args.plan.read_text())
    repeats = args.repeats or int(plan.get("repeats", 1))
    steps = plan["steps"]
    if args.only:
        wanted = set(args.only.split(","))
        steps = [s for s in steps if s["name"] in wanted]
    defaults = plan.get("defaults", {})

    log = RESULTS_ROOT / "sweeps" / f"{time.strftime('%Y%m%dT%H%M%S')}_{plan['name']}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    failures = 0
    built = False
    for r in range(1, repeats + 1):
        for step in steps:
            options = merge(defaults, step)
            options["tag"] = list(options.get("tag", [])) + [f"plan={plan['name']}", f"step={step['name']}",
                                                              f"repeat={r}"]
            options.setdefault("name", f"{plan['name']}-{step['name']}-r{r}")
            cmd = [sys.executable, str(BENCH_DIR / "run.py"), *to_cli(options), *shlex.split(args.extra)]
            # The image only needs building once per sweep.
            if built and "--skip-build" not in cmd:
                cmd.append("--skip-build")
            print(shlex.join(cmd))
            if args.dry_run:
                continue
            code = subprocess.run(cmd).returncode
            built = built or code == 0
            with open(log, "a") as f:
                f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {plan['name']} {step['name']} r{r} exit={code}\n")
            if code != 0:
                failures += 1
                if args.stop_on_error:
                    return code
    if not args.dry_run:
        print(f"sweep log: {log}; tabulate with report.py {RESULTS_ROOT}/*{plan['name']}-*")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
