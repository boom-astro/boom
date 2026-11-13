"""Background samplers that run alongside a replay."""

from __future__ import annotations

import csv
import json
import subprocess
import threading
import time
from pathlib import Path

import redis

STAGES = ("packets", "enrichment", "filter")


def queue_names(surveys: list[str]) -> dict[str, tuple[str, str]]:
    """Valkey key -> (survey, stage), as named in src/utils/enums.rs, src/alert and src/enrichment."""
    return {f"{s}_alerts_{stage}_queue": (s, stage) for s in surveys for stage in STAGES}


class QueueSampler(threading.Thread):
    """Records every stage's queue depth once per `interval` seconds."""

    def __init__(self, port: int, surveys: list[str], out: Path, interval: float = 1.0):
        super().__init__(daemon=True)
        self.client = redis.Redis(host="localhost", port=port)
        self.keys = queue_names(surveys)
        self.out = out
        self.interval = interval
        self.latest: dict[str, int] = {}
        self._halt = threading.Event()

    def total(self) -> int:
        return sum(self.latest.values())

    def run(self) -> None:
        with open(self.out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t", "survey", "stage", "length"])
            while not self._halt.is_set():
                t = time.time()
                try:
                    pipe = self.client.pipeline()
                    for key in self.keys:
                        pipe.llen(key)
                    lengths = pipe.execute()
                except redis.RedisError:
                    lengths = None
                if lengths is not None:
                    for key, n in zip(self.keys, lengths):
                        survey, stage = self.keys[key]
                        w.writerow([f"{t:.3f}", survey, stage, n])
                        self.latest[key] = int(n)
                    f.flush()
                self._halt.wait(self.interval)

    def stop(self) -> None:
        self._halt.set()


class DockerStatsSampler(threading.Thread):
    """CPU, memory and I/O of every container in the stack, every `interval` seconds."""

    def __init__(self, container_ids, out: Path, interval: float = 5.0):
        super().__init__(daemon=True)
        self.container_ids = container_ids
        self.out = out
        self.interval = interval
        self._halt = threading.Event()

    def run(self) -> None:
        with open(self.out, "w") as f:
            while not self._halt.is_set():
                ids = self.container_ids()
                t = time.time()
                if ids:
                    res = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                                         capture_output=True, text=True)
                    for line in res.stdout.splitlines():
                        try:
                            entry = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        entry["t"] = t
                        f.write(json.dumps(entry) + "\n")
                    f.flush()
                self._halt.wait(self.interval)

    def stop(self) -> None:
        self._halt.set()


class EventLog:
    """Timestamped harness events (replay start/end, injected outages) for the analysis."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(["t", "event", "detail"])

    def __call__(self, event: str, detail: str = "") -> None:
        with self._lock, open(self.path, "a", newline="") as f:
            csv.writer(f).writerow([f"{time.time():.3f}", event, detail])
