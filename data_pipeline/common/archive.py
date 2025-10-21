"""Helpers to extract compressed archives."""
from __future__ import annotations

import logging
import zipfile
from pathlib import Path

LOGGER = logging.getLogger(__name__)


def extract_zip(path: Path | str, destination: Path | str) -> Path:
    path = Path(path)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Extracting %s to %s", path, destination)
    with zipfile.ZipFile(path) as archive:
        archive.extractall(destination)
    return destination
