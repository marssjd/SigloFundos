"""Normalization helpers shared across different data providers."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Mapping

import pandas as pd

LOGGER = logging.getLogger(__name__)


def normalize_cnpj(value: str) -> str:
    """Normalize a CNPJ string removing punctuation."""
    digits = [char for char in value if char.isdigit()]
    if len(digits) != 14:
        LOGGER.warning("Unexpected CNPJ format: %s", value)
    return "".join(digits)


def parse_date(value: str, formats: Iterable[str]) -> datetime:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date '{value}' with formats {formats}")


def standardize_columns(df: pd.DataFrame, mapping: Mapping[str, str]) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns=mapping, inplace=True)
    return df
