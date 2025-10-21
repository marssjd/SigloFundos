"""Networking helpers for downloading public datasets."""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Iterable

import requests
from requests import Response

LOGGER = logging.getLogger(__name__)


class DownloadError(RuntimeError):
    pass


def stream_download(url: str, *, chunk_size: int = 1024 * 1024) -> Iterable[bytes]:
    LOGGER.info("Downloading %s", url)
    try:
        response: Response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise DownloadError(f"Failed to download {url}: {exc}") from exc

    for chunk in response.iter_content(chunk_size=chunk_size):
        if chunk:
            yield chunk


def download_to_file(url: str, destination: Path | str) -> Path:
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("wb") as fh:
        for chunk in stream_download(url):
            fh.write(chunk)

    LOGGER.info("Saved download to %s", destination)
    return destination


def download_to_tempfile(url: str, suffix: str = "") -> Path:
    handle = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    path = Path(handle.name)
    with handle:
        for chunk in stream_download(url):
            handle.write(chunk)
    LOGGER.debug("Temporary file stored at %s", path)
    return path
