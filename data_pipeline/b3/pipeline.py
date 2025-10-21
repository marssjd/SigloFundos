"""Utilities to incorporate optional B3 datasets."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from ..common import normalization

LOGGER = logging.getLogger(__name__)


def load_planilhas(path_or_urls: Iterable[str], *, workdir: Path) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    destino = workdir / "b3"
    destino.mkdir(parents=True, exist_ok=True)

    for source in path_or_urls:
        try:
            if source.startswith("http://") or source.startswith("https://"):
                LOGGER.info("Baixando planilha da B3: %s", source)
                df = pd.read_excel(source)
            else:
                local_path = Path(source)
                LOGGER.info("Carregando planilha B3 local: %s", local_path)
                df = pd.read_excel(local_path)
        except Exception as exc:  # pragma: no cover - depends on remote availability
            LOGGER.error("Falha ao carregar planilha %s: %s", source, exc)
            continue
        frames.append(df)

    if not frames:
        LOGGER.warning("Nenhuma planilha da B3 foi carregada")
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    merged.columns = [col.strip().lower() for col in merged.columns]

    column_mapping = {
        "cnpj do fundo": "cnpj",
        "cnpj": "cnpj",
        "data": "data_referencia",
        "data de referência": "data_referencia",
        "valor da cota": "valor_cota",
        "patrimônio líquido": "patrimonio_liquido",
    }

    renamed = merged.rename(columns={col: column_mapping.get(col, col) for col in merged.columns})
    if "cnpj" in renamed.columns:
        renamed["cnpj"] = renamed["cnpj"].astype(str).apply(normalization.normalize_cnpj)
    if "data_referencia" in renamed.columns:
        renamed["data_referencia"] = pd.to_datetime(
            renamed["data_referencia"], dayfirst=True, errors="coerce"
        )
    renamed["fonte"] = "B3"
    return renamed


def map_to_fato_cota_diaria(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    expected_cols = {
        "cnpj",
        "data_referencia",
        "valor_cota",
        "patrimonio_liquido",
    }
    missing = expected_cols - set(df.columns)
    if missing:
        LOGGER.warning("Colunas ausentes na planilha da B3: %s", ", ".join(sorted(missing)))
    result = df.rename(columns={"data_referencia": "data_cotacao"})
    for col in ["captacoes", "resgates", "numero_cotistas"]:
        if col not in result.columns:
            result[col] = pd.NA
    ordered_cols = [
        "cnpj",
        "data_cotacao",
        "valor_cota",
        "patrimonio_liquido",
        "captacoes",
        "resgates",
        "numero_cotistas",
        "fonte",
    ]
    available = [col for col in ordered_cols if col in result.columns]
    return result[available].copy()
