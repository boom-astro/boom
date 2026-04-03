#!/usr/bin/env python3
"""Minimal Prometheus exporter for Docker container metadata.

Exposes container id/name/compose labels by querying the Docker Engine API over
its Unix socket. This is intended to be joined with cAdvisor metrics in PromQL.
"""

from __future__ import annotations

import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import urlencode

DOCKER_SOCKET = "/var/run/docker.sock"
DOCKER_API_VERSION = "v1.41"
EXPORTER_PORT = 9158
METRIC_NAME = "docker_container_identity_info"


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


def render_metrics() -> str:
    containers = _http_get_unix_socket(
        f"/{DOCKER_API_VERSION}/containers/json", {"all": 1}
    )

    lines = [
        f"# HELP {METRIC_NAME} Docker container identity metadata.",
        f"# TYPE {METRIC_NAME} gauge",
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
        lines.append(f"{METRIC_NAME}{{{label_text}}} 1")

    return "\n".join(lines) + "\n"


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
    server = HTTPServer(("0.0.0.0", EXPORTER_PORT), Handler)
    server.serve_forever()
