"""Downloader and parser for CVM InfDiario datasets."""
from __future__ import annotations

import logging
import zipfile
from datetime import date
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from ..common import download, normalization

LOGGER = logging.getLogger(__name__)

BASE_URL_FI = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
BASE_URL_FIM = "https://dados.cvm.gov.br/dados/FIM/DOC/INF_DIARIO/DADOS"


def build_monthly_urls(reference_months: Iterable[date]) -> List[str]:
    urls = []
    for month in reference_months:
        ym = month.strftime("%Y%m")
        urls.append(f"{BASE_URL_FI}/inf_diario_fi_{ym}.zip")
        urls.append(f"{BASE_URL_FIM}/inf_diario_fim_{ym}.zip")
    return urls


COLUMN_MAPPING = {
    "CNPJ_FUNDO": "cnpj",
    "DT_COMPTC": "data_cotacao",
    "VL_TOTAL": "valor_total",
    "VL_QUOTA": "valor_cota",
    "VL_PATRIM_LIQ": "patrimonio_liquido",
    "CAPTC_DIA": "captacoes",
    "RESG_DIA": "resgates",
    "NR_COTST": "numero_cotistas",
}

DATE_COLUMNS = ["data_cotacao"]


def load_csv_from_archive(path: Path) -> pd.DataFrame:
    # There is a single CSV inside each archive. We list files and pick the first.
    with zipfile.ZipFile(path) as zf:
        inner_files = [info for info in zf.infolist() if info.filename.endswith(".csv")]
        if not inner_files:
            raise ValueError(f"No CSV file found inside archive {path}")
        with zf.open(inner_files[0]) as fh:
            df = pd.read_csv(fh, sep=";", decimal=",", dtype=str)
    return df


def parse_inf_diario(urls: Iterable[str], *, workdir: Path) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    staging_dir = workdir / "cvm" / "inf_diario"
    staging_dir.mkdir(parents=True, exist_ok=True)

    for url in urls:
        try:
            zip_path = download.download_to_file(url, staging_dir / Path(url).name)
        except download.DownloadError as exc:
            LOGGER.error("Could not download %s: %s", url, exc)
            continue
        try:
            df = load_csv_from_archive(zip_path)
        except Exception as exc:  # pragma: no cover - network data dependent
            LOGGER.error("Failed to parse %s: %s", zip_path, exc)
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("No InfDiario files were downloaded successfully")

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.rename(columns=COLUMN_MAPPING)
    merged["cnpj"] = merged["cnpj"].apply(normalization.normalize_cnpj)
    for column in DATE_COLUMNS:
        merged[column] = pd.to_datetime(merged[column], format="%Y-%m-%d", errors="coerce")
    merged["fonte"] = "CVM"
    return merged
