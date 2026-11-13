"""Thin wrappers over the docker CLI."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

from . import BENCH_DIR, REPO_ROOT

PROJECT = "boom-bench"
HELPER_IMAGE = "debian:trixie-slim"


def run(cmd: list[str], check: bool = True, capture: bool = False, env: dict | None = None,
        timeout: float | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, env=env, timeout=timeout,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.STDOUT if capture else None)


class Compose:
    def __init__(self, env: dict[str, str], gpu: bool = False, milvus: bool = False):
        self.files = [BENCH_DIR / "compose.yaml"]
        if gpu:
            self.files.append(BENCH_DIR / "compose.cuda.yaml")
        if milvus:
            self.files.append(BENCH_DIR / "compose.milvus.yaml")
        self.env = {**os.environ, "BOOM_REPO_ROOT": str(REPO_ROOT), **env}

    def _base(self) -> list[str]:
        cmd = ["docker", "compose", "-p", PROJECT]
        for f in self.files:
            cmd += ["-f", str(f)]
        return cmd

    def __call__(self, *args: str, check: bool = True, capture: bool = False,
                 timeout: float | None = None) -> subprocess.CompletedProcess:
        return run(self._base() + list(args), check=check, capture=capture, env=self.env, timeout=timeout)

    def up(self, *services: str) -> None:
        self("up", "-d", "--wait", *services)

    def start(self, *services: str) -> None:
        # No --wait: BOOM services have no healthcheck, and readiness is read from their logs.
        self("up", "-d", "--no-deps", *services)

    def down(self) -> None:
        # Without every profile enabled, `down` skips the survey services and leaves them running.
        self("--profile", "*", "down", "-v", "--remove-orphans", check=False)

    def logs(self, service: str) -> str:
        return self("logs", "--no-color", "-t", service, check=False, capture=True).stdout or ""

    def container_ids(self) -> list[str]:
        out = self("ps", "-q", check=False, capture=True).stdout or ""
        return [line for line in out.split() if line]

    def exited(self, service: str) -> int | None:
        """Exit code if the service's container has stopped, else None."""
        out = (self("ps", "-a", "--format", "json", service, check=False, capture=True).stdout or "").strip()
        # Older Compose prints one JSON array, newer prints one object per line.
        try:
            parsed = json.loads(out) if out else []
            entries = parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            entries = [json.loads(line) for line in out.splitlines() if line.startswith("{")]
        for info in entries:
            if info.get("State") == "exited":
                return int(info.get("ExitCode", 1))
        return None


def helper(args: list[str], volumes: dict[str, str] | None = None, privileged: bool = False,
           check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command in a throwaway root container.

    Mongo's files belong to the container's mongodb user, so copying, warming
    and deleting them is done as root inside a container.
    """
    cmd = ["docker", "run", "--rm"]
    if privileged:
        cmd.append("--privileged")
    for host, container in (volumes or {}).items():
        cmd += ["-v", f"{host}:{container}"]
    cmd += [HELPER_IMAGE, "sh", "-c", " ".join(args)]
    return run(cmd, check=check)


def copy_tree(src: Path, dst: Path) -> float:
    """Replace `dst` with a copy of `src`; reflinked where the filesystem allows."""
    parent = src.parent
    if dst.parent != parent:
        raise ValueError("seed and run directories must share a parent")
    t0 = time.time()
    helper([f"rm -rf /root_/{dst.name} && cp -a --reflink=auto /root_/{src.name} /root_/{dst.name}"],
           volumes={str(parent): "/root_"})
    return time.time() - t0


def remove_tree(path: Path) -> None:
    helper([f"rm -rf /root_/{path.name}"], volumes={str(path.parent): "/root_"}, check=False)


def drop_page_cache() -> None:
    """Empty the host's page cache, so the run starts with nothing in memory."""
    helper(["sync && echo 3 > /proc/sys/vm/drop_caches"], privileged=True)


def warm_page_cache(path: Path) -> float:
    """Read every file under `path` once, so the run starts with it all in memory."""
    t0 = time.time()
    helper(["find /d -type f -exec cat {} + > /dev/null"], volumes={str(path): "/d:ro"})
    return time.time() - t0


def build_image(dockerfile: str, tag: str, git_sha: str) -> None:
    run(["docker", "build", "-f", str(REPO_ROOT / dockerfile), "-t", tag,
         "--build-arg", f"BOOM_GIT_SHA={git_sha}", str(REPO_ROOT)])


def image_id(tag: str) -> str | None:
    res = run(["docker", "image", "inspect", "-f", "{{.Id}}", tag], check=False, capture=True)
    return res.stdout.strip() if res.returncode == 0 else None
