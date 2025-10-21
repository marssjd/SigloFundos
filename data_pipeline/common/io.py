"""File system helpers for the data pipeline."""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

LOGGER = logging.getLogger(__name__)


def ensure_directory(path: Path | str) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_dataframe_csv(df: pd.DataFrame, path: Path | str, *, index: bool = False) -> None:
    path = Path(path)
    ensure_directory(path.parent)
    LOGGER.info("Writing %s rows to %s", len(df), path)
    df.to_csv(path, index=index)


def read_dataframe_csv(path: Path | str) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    LOGGER.info("Loading dataframe from %s", path)
    return pd.read_csv(path)


def write_rows_csv(rows: Iterable[dict], path: Path | str, *, fieldnames: Iterable[str]) -> None:
    path = Path(path)
    ensure_directory(path.parent)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
