"""Shared HTTP fetching for catalogs published as plain files.

Two things every archive needs and none of them provide the same way: reading
an Apache-style directory index, and getting a large file down over a link that
will drop partway.
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import requests

#: Read in 1 MiB blocks -- these files are hundreds of megabytes each, and the
#: default 1 KiB makes the syscall overhead visible.
BLOCK_SIZE = 1024 * 1024

DEFAULT_ATTEMPTS = 5

#: Ask before pulling a gigabyte, and let a hung archive fail rather than stall
#: an ingest for the rest of the day. Generous on read because these servers can
#: take a while to start a large transfer.
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 300


def log(message: str) -> None:
    """Progress goes to stderr; stdout carries the JSON result only."""
    print(message, file=sys.stderr, flush=True)


def list_index(url: str, pattern: str) -> list[str]:
    """Filenames in an Apache-style directory index matching `pattern`.

    Returned sorted so chunk ordering is stable between runs -- the ingest
    records progress by chunk id, and a shuffled listing would still be correct
    but would make a resumed run's logs impossible to follow.
    """
    response = requests.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    response.raise_for_status()
    matcher = re.compile(pattern)
    names = {
        href
        for href in re.findall(r'href="([^"]+)"', response.text)
        if matcher.fullmatch(href)
    }
    return sorted(names)


def content_length(url: str) -> int | None:
    """The size the archive advertises for `url`, if it advertises one."""
    try:
        response = requests.head(
            url, allow_redirects=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        response.raise_for_status()
    except requests.RequestException as e:
        log(f"HEAD {url} failed ({e}); downloading without a size check")
        return None
    size = response.headers.get("content-length")
    return int(size) if size else None


def download(
    url: str,
    dest: Path,
    expected_size: int | None = None,
    attempts: int = DEFAULT_ATTEMPTS,
) -> Path:
    """Download `url` to `dest`, resuming and retrying.

    Downloads into a `.part` file and renames on success, so an interrupted
    transfer is never mistaken for a complete file by the next run. Retries with
    a `Range` header where the server supports it, since re-fetching a gigabyte
    because the connection dropped at 90% is the common failure here.
    """
    partial = dest.with_suffix(dest.suffix + ".part")
    last_error: Exception | None = None

    for attempt in range(attempts):
        try:
            downloaded = partial.stat().st_size if partial.exists() else 0
            headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}
            response = requests.get(
                url,
                stream=True,
                allow_redirects=True,
                headers=headers,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            # A server that ignores Range answers 200 with the whole file, so
            # appending to what we have would corrupt it. Start over instead.
            if downloaded and response.status_code != requests.codes.partial_content:
                log(f"{url}: server ignored Range, restarting the download")
                downloaded = 0
            response.raise_for_status()

            with open(partial, "ab" if downloaded else "wb") as f:
                for block in response.iter_content(chunk_size=BLOCK_SIZE):
                    f.write(block)

            size = partial.stat().st_size
            if expected_size is not None and size != expected_size:
                raise OSError(f"got {size} bytes, expected {expected_size}")
            partial.replace(dest)
            return dest
        except (requests.RequestException, OSError) as e:
            last_error = e
            if attempt == attempts - 1:
                break
            delay = 2**attempt
            log(f"{url}: attempt {attempt + 1} failed ({e}); retrying in {delay}s")
            time.sleep(delay)

    partial.unlink(missing_ok=True)
    raise RuntimeError(f"failed to download {url} after {attempts} attempts: {last_error}")
