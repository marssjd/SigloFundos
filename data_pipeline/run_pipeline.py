"""Command line entry-point for running the Siglo Fundos data pipeline."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict

import pandas as pd
import typer
from dotenv import load_dotenv

from .b3 import pipeline as b3_pipeline
from .common import bigquery, config, io, logging_utils
from .cvm.pipeline import CVMPipeline
from .mais_retorno import fallback as mais_retorno_fallback

APP = typer.Typer(help="Pipeline de ingestão de dados da CVM/B3 para BigQuery")


def load_environment() -> None:
    load_dotenv()
    logging_utils.configure_logging()


def get_config(config_path: Path | None = None) -> config.PipelineConfig:
    if config_path is None:
        config_path = Path("config/pipeline.yaml")
    cfg = config.load_config(config_path)
    cfg.bigquery_project = os.getenv("BIGQUERY_PROJECT", cfg.bigquery_project)
    cfg.bigquery_dataset_staging = os.getenv(
        "BIGQUERY_DATASET_STAGING", cfg.bigquery_dataset_staging
    )
    cfg.bigquery_dataset_curated = os.getenv(
        "BIGQUERY_DATASET_CURATED", cfg.bigquery_dataset_curated
    )
    cfg.gcs_bucket = os.getenv("GCS_BUCKET", cfg.gcs_bucket)
    return cfg


def collect_all_data(cfg: config.PipelineConfig, workdir: Path) -> Dict[str, pd.DataFrame]:
    cvm_runner = CVMPipeline(cfg, workdir=workdir)
    tables = cvm_runner.run()

    if cfg.enable_b3_ingestion and cfg.b3_planilhas:
        typer.echo("Carregando dados complementares da B3...")
        b3_df = b3_pipeline.load_planilhas(cfg.b3_planilhas, workdir=workdir)
        mapped = b3_pipeline.map_to_fato_cota_diaria(b3_df)
        if not mapped.empty:
            tables["fato_cota_diaria"] = pd.concat(
                [tables["fato_cota_diaria"], mapped], ignore_index=True
            )

    if cfg.enable_mais_retorno_fallback:
        mais_retorno_fallback.check_terms_of_use()

    return tables


def save_tables(tables: Dict[str, pd.DataFrame], destination: Path) -> Dict[str, Path]:
    destination = io.ensure_directory(destination)
    paths: Dict[str, Path] = {}
    for name, df in tables.items():
        path = destination / f"{name}.csv"
        io.write_dataframe_csv(df, path)
        paths[name] = path
    return paths


def build_curated_tables(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    curated: Dict[str, pd.DataFrame] = {}
    fato = tables.get("fato_cota_diaria")
    dim_fundo = tables.get("dim_fundo")
    if fato is not None and dim_fundo is not None and not fato.empty:
        enriched = fato.merge(dim_fundo, on="cnpj", how="left")
        curated["curated_cotas_por_categoria"] = (
            enriched.groupby(["data_cotacao", "categoria_cvm"], dropna=False)
            .agg({"valor_cota": "mean", "patrimonio_liquido": "sum"})
            .reset_index()
        )
        curated["curated_cotas_por_gestora"] = (
            enriched.groupby(["data_cotacao", "gestora"], dropna=False)
            .agg({"valor_cota": "mean", "patrimonio_liquido": "sum"})
            .reset_index()
        )
        curated["curated_cotas_por_grupo_looker"] = (
            enriched.groupby(["data_cotacao", "grupo_looker"], dropna=False)
            .agg({"valor_cota": "mean", "patrimonio_liquido": "sum"})
            .reset_index()
        )
    return curated


def create_bigquery_uploader(cfg: config.PipelineConfig) -> bigquery.BigQueryUploader:
    if not cfg.bigquery_project or not cfg.bigquery_dataset_staging or not cfg.bigquery_dataset_curated:
        raise RuntimeError("Configuração do BigQuery incompleta no config/pipeline.yaml")
    return bigquery.BigQueryUploader(
        project=cfg.bigquery_project,
        staging_dataset=cfg.bigquery_dataset_staging,
        curated_dataset=cfg.bigquery_dataset_curated,
    )


def upload_tables(
    uploader: bigquery.BigQueryUploader,
    csv_paths: Dict[str, Path],
    *,
    curated: bool = False,
) -> None:
    destination = "curated" if curated else "staging"
    for name, path in csv_paths.items():
        uploader.load_csv(path, table=name, destination=destination)


@APP.command()
def ingest(
    config_path: Path = typer.Option(Path("config/pipeline.yaml"), help="Arquivo de configuração YAML"),
    workdir: Path = typer.Option(Path(".tmp_pipeline"), help="Diretório temporário para downloads"),
    output_dir: Path = typer.Option(Path("output"), help="Diretório para salvar CSVs"),
    skip_bigquery: bool = typer.Option(False, help="Não realiza upload para o BigQuery"),
) -> None:
    """Executa a ingestão completa: download, limpeza, consolidação e upload."""

    load_environment()
    cfg = get_config(config_path)
    typer.echo("Iniciando ingestão completa...")
    tables = collect_all_data(cfg, workdir)

    staging_dir = output_dir / "staging"
    curated_dir = output_dir / "curated"

    staging_paths = save_tables(tables, staging_dir)
    curated_tables = build_curated_tables(tables)
    curated_paths = save_tables(curated_tables, curated_dir)

    if not skip_bigquery:
        uploader = create_bigquery_uploader(cfg)
        upload_tables(uploader, staging_paths, curated=False)
        upload_tables(uploader, curated_paths, curated=True)

    typer.echo("Ingestão concluída.")


@APP.command("export-local")
def export_local(
    config_path: Path = typer.Option(Path("config/pipeline.yaml"), help="Arquivo de configuração YAML"),
    workdir: Path = typer.Option(Path(".tmp_pipeline"), help="Diretório temporário"),
    output_dir: Path = typer.Option(Path("output"), help="Diretório de saída"),
) -> None:
    """Executa o pipeline e mantém os CSVs locais sem upload para o BigQuery."""

    load_environment()
    cfg = get_config(config_path)
    tables = collect_all_data(cfg, workdir)
    staging_paths = save_tables(tables, output_dir / "staging")
    curated_paths = save_tables(build_curated_tables(tables), output_dir / "curated")
    typer.echo("Arquivos salvos em:")
    typer.echo(json.dumps({"staging": [str(p) for p in staging_paths.values()], "curated": [str(p) for p in curated_paths.values()]}, indent=2))


@APP.command("upload-bigquery")
def upload_bigquery(
    config_path: Path = typer.Option(Path("config/pipeline.yaml"), help="Arquivo de configuração YAML"),
    output_dir: Path = typer.Option(Path("output"), help="Diretório com CSVs"),
) -> None:
    """Faz upload dos CSVs existentes no diretório de saída para o BigQuery."""

    load_environment()
    cfg = get_config(config_path)
    uploader = create_bigquery_uploader(cfg)

    staging_dir = output_dir / "staging"
    curated_dir = output_dir / "curated"

    staging_paths = {path.stem: path for path in staging_dir.glob("*.csv")}
    curated_paths = {path.stem: path for path in curated_dir.glob("*.csv")}

    if not staging_paths and not curated_paths:
        raise RuntimeError("Nenhum CSV encontrado no diretório de saída. Execute 'export-local' ou 'ingest' antes.")

    upload_tables(uploader, staging_paths, curated=False)
    upload_tables(uploader, curated_paths, curated=True)

    typer.echo("Upload concluído com sucesso.")


if __name__ == "__main__":
    APP()
