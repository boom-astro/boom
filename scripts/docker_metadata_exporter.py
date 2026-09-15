#!/usr/bin/env python3
"""Minimal Prometheus exporter for Docker container metadata.

Exposes container id/name/compose labels by querying the Docker Engine API over
its Unix socket. This is intended to be joined with cAdvisor metrics in PromQL.
"""

from __future__ import annotations

import json
import os
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urlencode

# Standard Docker Engine Unix socket path. Change to ~/.docker/run/docker.sock
# for rootless Docker.
DOCKER_SOCKET = "/var/run/docker.sock"
# Docker API base endpoint. Defaults to the local docker-socket-proxy service,
# and can be set to a unix:// URL for direct socket access in constrained dev
# setups.
DOCKER_API_BASE = os.getenv("DOCKER_API_BASE", "http://docker-socket-proxy:2375")
# Docker Engine API version (v1.41 = Docker Engine 20.10). Lower values have
# wider daemon compatibility; bump only if newer API features are needed.
DOCKER_API_VERSION = "v1.41"
# Prometheus community port allocation for this exporter (must match the
# targets entry in prometheus.yaml).
EXPORTER_PORT = 9158
# Docker API timeout. A failed scrape is not a neutral event here: Prometheus
# writes stale markers for every series a target exported the moment a scrape
# fails, so the series backing the api-down alert vanishes immediately rather
# than after the usual 5m staleness window. The daemon shares this host with
# the self-hosted Actions runner, so `/containers/json` can be slow during an
# image build; give it room rather than turning that into a gap. Capped at
# Prometheus' scrape_timeout (10s by default) -- waiting past that just holds a
# connection open for a scrape that has already been abandoned.
DOCKER_API_TIMEOUT_SECONDS = 10
# Prometheus metric name. The _info suffix follows the convention for
# label-only identity metrics (value is always 1).
METRIC_NAME = "docker_container_identity_info"
# Health/liveness metrics. cAdvisor reports resource usage but knows nothing
# about Docker healthchecks, so a container that is running yet failing its
# healthcheck (see PR #562: every request panicked its actix worker, so the
# `/` healthcheck failed forever while the process stayed alive) is invisible
# to every other exporter in the stack.
HEALTH_METRIC_NAME = "docker_container_health_status"
UP_METRIC_NAME = "docker_container_up"
# Emitted for every container so queries never have to guess which label
# values exist; exactly one is 1 per container.
HEALTH_STATES = ("healthy", "unhealthy", "starting", "none")


def _http_get_unix_socket(path: str, query: dict[str, Any] | None = None) -> Any:
    if query:
        path = f"{path}?{urlencode(query)}"

    request = (
        f"GET {path} HTTP/1.1\r\n"
        "Host: docker\r\n"
        "Connection: close\r\n"
        "Accept: application/json\r\n"
        "\r\n"
    ).encode("utf-8")

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(DOCKER_API_TIMEOUT_SECONDS)
        client.connect(DOCKER_SOCKET)
        client.sendall(request)

        chunks: list[bytes] = []
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)

    raw = b"".join(chunks)
    head, body = raw.split(b"\r\n\r\n", 1)
    status_line = head.split(b"\r\n", 1)[0].decode("utf-8", errors="replace")
    if " 200 " not in status_line:
        raise RuntimeError(f"Docker API request failed: {status_line}")

    if b"transfer-encoding: chunked" in head.lower():
        body = _decode_chunked(body)

    return json.loads(body.decode("utf-8"))


def _http_get_tcp(path: str, query: dict[str, Any] | None = None) -> Any:
    parsed = urlparse(DOCKER_API_BASE)
    if parsed.scheme not in ("http", "https"):
        raise RuntimeError(f"Unsupported DOCKER_API_BASE scheme: {parsed.scheme}")

    if query:
        path = f"{path}?{urlencode(query)}"

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    host = parsed.hostname
    if not host:
        raise RuntimeError("DOCKER_API_BASE must include a hostname")

    if parsed.scheme == "https":
        import http.client

        conn = http.client.HTTPSConnection(
            host, port, timeout=DOCKER_API_TIMEOUT_SECONDS
        )
    else:
        import http.client

        conn = http.client.HTTPConnection(
            host, port, timeout=DOCKER_API_TIMEOUT_SECONDS
        )

    conn.request("GET", path, headers={"Accept": "application/json"})
    response = conn.getresponse()
    body = response.read()
    status = response.status
    conn.close()
    if status != 200:
        raise RuntimeError(f"Docker API request failed: HTTP {status}")
    return json.loads(body.decode("utf-8"))


def _decode_chunked(body: bytes) -> bytes:
    out = bytearray()
    idx = 0
    while True:
        end = body.find(b"\r\n", idx)
        if end == -1:
            raise RuntimeError("Malformed chunked response")
        size = int(body[idx:end].split(b";", 1)[0], 16)
        idx = end + 2
        if size == 0:
            break
        out.extend(body[idx : idx + size])
        idx += size + 2
    return bytes(out)


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _container_name(container: dict[str, Any]) -> str:
    names = container.get("Names") or []
    if names:
        return names[0].lstrip("/")
    return container.get("Id", "")[:12]


def _health_state(container: dict[str, Any]) -> str:
    """Health of a container, as one of HEALTH_STATES.

    The container list endpoint has no dedicated health field; the daemon
    appends the healthcheck result to the human-readable Status string as
    "Up 3 hours (healthy)" / "(unhealthy)" / "(health: starting)", and omits
    the suffix entirely for containers that declare no healthcheck. Reading
    it here keeps the exporter to a single Docker API call per scrape, rather
    than an inspect request per container.
    """
    status = container.get("Status") or ""
    if "(healthy)" in status:
        return "healthy"
    if "(unhealthy)" in status:
        return "unhealthy"
    if "(health: starting)" in status:
        return "starting"
    return "none"


def render_metrics() -> str:
    if DOCKER_API_BASE.startswith("unix://"):
        containers = _http_get_unix_socket(
            f"/{DOCKER_API_VERSION}/containers/json", {"all": 1}
        )
    else:
        containers = _http_get_tcp(f"/{DOCKER_API_VERSION}/containers/json", {"all": 1})

    identity_lines = [
        f"# HELP {METRIC_NAME} Docker container identity metadata.",
        f"# TYPE {METRIC_NAME} gauge",
    ]
    health_lines = [
        f"# HELP {HEALTH_METRIC_NAME} Docker healthcheck state of a container "
        "(1 for the current state, 0 otherwise; 'none' means no healthcheck "
        "is defined).",
        f"# TYPE {HEALTH_METRIC_NAME} gauge",
    ]
    up_lines = [
        f"# HELP {UP_METRIC_NAME} 1 if the container is running and not "
        "failing its healthcheck, 0 otherwise.",
        f"# TYPE {UP_METRIC_NAME} gauge",
    ]

    for c in containers:
        full_id = c.get("Id", "")
        if not full_id:
            continue

        labels = c.get("Labels") or {}
        container_name = _container_name(c)
        compose_project = labels.get("com.docker.compose.project", "")
        compose_service = labels.get("com.docker.compose.service", "")

        prom_labels = {
            "container_id": full_id[:12],
            "container_full_id": full_id,
            "container_name": container_name,
            "compose_project": compose_project,
            "compose_service": compose_service,
        }

        label_text = ",".join(
            f'{k}="{_escape_label(v)}"' for k, v in prom_labels.items()
        )
        identity_lines.append(f"{METRIC_NAME}{{{label_text}}} 1")

        # The full id is 64 hex characters of pure churn on any dashboard or
        # alert that groups by it, and identity_info already maps it to the
        # names below, so the state metrics carry only the human-readable set.
        state_labels = {
            "container_name": container_name,
            "compose_project": compose_project,
            "compose_service": compose_service,
        }
        state_label_text = ",".join(
            f'{k}="{_escape_label(v)}"' for k, v in state_labels.items()
        )

        health = _health_state(c)
        for candidate in HEALTH_STATES:
            value = 1 if candidate == health else 0
            health_lines.append(
                f'{HEALTH_METRIC_NAME}{{{state_label_text},status="{candidate}"}} {value}'
            )

        # "starting" counts as up: it is what every container reports during
        # its healthcheck start_period, and Docker moves it to "unhealthy"
        # once the retries are exhausted, so treating it as down would fire
        # this alert on every deploy without catching anything extra.
        running = c.get("State") == "running"
        up = 1 if running and health != "unhealthy" else 0
        up_lines.append(f"{UP_METRIC_NAME}{{{state_label_text}}} {up}")

    return "\n".join(identity_lines + health_lines + up_lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/metrics", "/"):
            self.send_response(404)
            self.end_headers()
            return

        try:
            payload = render_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as exc:  # pragma: no cover
            error = f"# exporter error\n# {exc}\n".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(error)))
            self.end_headers()
            self.wfile.write(error)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


if __name__ == "__main__":
    # Threaded so one slow Docker API call can only delay its own scrape. The
    # single-threaded HTTPServer serialized them, so a stalled call blocked
    # every subsequent scrape until it returned, turning one slow response into
    # a run of failed scrapes.
    server = ThreadingHTTPServer(("0.0.0.0", EXPORTER_PORT), Handler)
    server.serve_forever()
