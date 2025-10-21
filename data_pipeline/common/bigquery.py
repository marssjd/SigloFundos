"""Wrapper around google-cloud-bigquery for uploads."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional dependency for tests
    bigquery = None  # type: ignore

LOGGER = logging.getLogger(__name__)


class BigQueryUploader:
    """Upload CSV files or DataFrames to BigQuery staging/curated datasets."""

    def __init__(
        self,
        *,
        project: str,
        staging_dataset: str,
        curated_dataset: str,
        location: Optional[str] = None,
    ) -> None:
        if bigquery is None:
            raise RuntimeError(
                "google-cloud-bigquery is required for uploads. Install the package first."
            )
        self.client = bigquery.Client(project=project, location=location)
        self.project = project
        self.staging_dataset = staging_dataset
        self.curated_dataset = curated_dataset

    def load_dataframe(
        self,
        df: pd.DataFrame,
        *,
        table: str,
        destination: str = "staging",
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        dataset = self._dataset_for(destination)
        table_id = f"{self.project}.{dataset}.{table}"
        LOGGER.info("Uploading %s rows to %s", len(df), table_id)
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    def load_csv(
        self,
        path: Path | str,
        *,
        table: str,
        destination: str = "staging",
        write_disposition: str = "WRITE_TRUNCATE",
        autodetect: bool = True,
    ) -> None:
        dataset = self._dataset_for(destination)
        table_id = f"{self.project}.{dataset}.{table}"
        LOGGER.info("Uploading CSV %s to %s", path, table_id)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            autodetect=autodetect,
            skip_leading_rows=1,
            write_disposition=write_disposition,
        )
        with open(path, "rb") as fh:
            job = self.client.load_table_from_file(fh, table_id, job_config=job_config)
        job.result()

    def _dataset_for(self, destination: str) -> str:
        if destination not in {"staging", "curated"}:
            raise ValueError("destination must be 'staging' or 'curated'")
        return self.staging_dataset if destination == "staging" else self.curated_dataset
