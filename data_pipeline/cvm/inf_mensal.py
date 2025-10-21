"""Downloader and parser for CVM InfMensal datasets."""
from __future__ import annotations

import logging
import zipfile
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..common import download, normalization

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_MENSAL/DADOS"


def build_monthly_urls(reference_months: Iterable[date]) -> List[str]:
    urls = []
    for month in reference_months:
        ym = month.strftime("%Y%m")
        urls.append(f"{BASE_URL}/inf_mensal_fi_{ym}.zip")
    return urls


CARTEIRA_COLUMNS = {
    "CNPJ_FUNDO": "cnpj",
    "DT_COMPTC": "data_referencia",
    "TP_APLIC": "tipo_ativo",
    "TP_ATIVO": "subtipo_ativo",
    "EMISSOR": "emissor",
    "SETOR": "setor",
    "COD_ISIN": "isin",
    "VL_MERC_POS_FINAL": "valor_mercado",
    "QT_POS_FINAL": "quantidade",
}

COTISTAS_COLUMNS = {
    "CNPJ_FUNDO": "cnpj",
    "DT_COMPTC": "data_referencia",
    "CLASSE_COTISTAS": "classe_cotistas",
    "QT_COTISTAS": "numero_cotistas",
    "VL_PATRIM_LIQ": "patrimonio_liquido",
}


def load_csv_from_archive(path: Path, *, pattern: str) -> pd.DataFrame:
    with zipfile.ZipFile(path) as zf:
        inner_files = [info for info in zf.infolist() if pattern in info.filename]
        if not inner_files:
            raise ValueError(f"No {pattern} file found inside {path}")
        with zf.open(inner_files[0]) as fh:
            df = pd.read_csv(fh, sep=";", decimal=",", dtype=str)
    return df


def parse_inf_mensal(urls: Iterable[str], *, workdir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    carteira_frames: List[pd.DataFrame] = []
    cotistas_frames: List[pd.DataFrame] = []
    staging_dir = workdir / "cvm" / "inf_mensal"
    staging_dir.mkdir(parents=True, exist_ok=True)

    for url in urls:
        try:
            zip_path = download.download_to_file(url, staging_dir / Path(url).name)
        except download.DownloadError as exc:
            LOGGER.error("Could not download %s: %s", url, exc)
            continue
        try:
            carteira_frames.append(
                load_csv_from_archive(zip_path, pattern="carteira").rename(
                    columns=CARTEIRA_COLUMNS
                )
            )
        except Exception as exc:  # pragma: no cover - depends on remote file
            LOGGER.warning("Failed to load carteira data from %s: %s", zip_path, exc)
        try:
            cotistas_frames.append(
                load_csv_from_archive(zip_path, pattern="cotist").rename(
                    columns=COTISTAS_COLUMNS
                )
            )
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Failed to load cotistas data from %s: %s", zip_path, exc)

    if not carteira_frames and not cotistas_frames:
        raise RuntimeError("No InfMensal files were downloaded successfully")

    carteira = pd.concat(carteira_frames, ignore_index=True) if carteira_frames else pd.DataFrame()
    cotistas = pd.concat(cotistas_frames, ignore_index=True) if cotistas_frames else pd.DataFrame()

    for df in (carteira, cotistas):
        if df.empty:
            continue
        df["cnpj"] = df["cnpj"].apply(normalization.normalize_cnpj)
        df["data_referencia"] = pd.to_datetime(
            df["data_referencia"], format="%Y-%m-%d", errors="coerce"
        )
        df["fonte"] = "CVM"

    return carteira, cotistas
