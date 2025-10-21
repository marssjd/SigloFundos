"""Dimension tables derived from CVM metadata and configuration."""
from __future__ import annotations

import pandas as pd

from ..common.config import FundConfig, PipelineConfig


def build_dim_gestora(config: PipelineConfig) -> pd.DataFrame:
    data = {
        "gestora": sorted({fund.gestora for fund in config.fundos if fund.gestora}),
    }
    return pd.DataFrame(data)


def build_dim_categoria_cvm(config: PipelineConfig) -> pd.DataFrame:
    data = sorted({fund.categoria_cvm for fund in config.fundos if fund.categoria_cvm})
    return pd.DataFrame({"categoria_cvm": data})


def build_dim_classe_anbima(config: PipelineConfig) -> pd.DataFrame:
    data = sorted({fund.classe_anbima for fund in config.fundos if fund.classe_anbima})
    return pd.DataFrame({"classe_anbima": data})


def build_dim_fundo(config: PipelineConfig) -> pd.DataFrame:
    records = []
    for fund in config.fundos:
        records.append(
            {
                "cnpj": fund.cnpj,
                "nome": fund.nome,
                "categoria_cvm": fund.categoria_cvm,
                "gestora": fund.gestora,
                "classe_anbima": fund.classe_anbima,
                "grupo_looker": fund.grupo_looker,
            }
        )
    return pd.DataFrame(records)
