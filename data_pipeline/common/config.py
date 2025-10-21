"""Utilities for loading and validating pipeline configuration."""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import yaml


@dataclass
class FundConfig:
    cnpj: str
    nome: str
    categoria_cvm: str
    gestora: str
    classe_anbima: Optional[str] = None
    grupo_looker: Optional[str] = None


@dataclass
class PipelineConfig:
    meses_retroativos: int = 24
    fundos: List[FundConfig] = field(default_factory=list)
    categorias_looker: Dict[str, str] = field(default_factory=dict)
    bigquery_project: Optional[str] = None
    bigquery_dataset_staging: Optional[str] = None
    bigquery_dataset_curated: Optional[str] = None
    gcs_bucket: Optional[str] = None
    enable_b3_ingestion: bool = False
    enable_mais_retorno_fallback: bool = False
    b3_planilhas: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "PipelineConfig":
        fundos = [FundConfig(**fund) for fund in data.get("fundos", [])]
        return cls(
            meses_retroativos=int(data.get("meses_retroativos", 24)),
            fundos=fundos,
            categorias_looker=data.get("categorias_looker", {}),
            bigquery_project=data.get("bigquery_project"),
            bigquery_dataset_staging=data.get("bigquery_dataset_staging"),
            bigquery_dataset_curated=data.get("bigquery_dataset_curated"),
            gcs_bucket=data.get("gcs_bucket"),
            enable_b3_ingestion=bool(data.get("enable_b3_ingestion", False)),
            enable_mais_retorno_fallback=bool(
                data.get("enable_mais_retorno_fallback", False)
            ),
            b3_planilhas=list(data.get("b3_planilhas", [])),
        )


def load_config(path: Path | str) -> PipelineConfig:
    """Load pipeline configuration from a YAML file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    return PipelineConfig.from_dict(data)


def list_monitorados(config: PipelineConfig) -> Iterable[str]:
    """Return a human readable list of monitored funds."""
    for fund in config.fundos:
        yield f"{fund.nome} ({fund.cnpj})"
