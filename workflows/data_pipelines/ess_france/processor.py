import logging
import os

import pandas as pd
from airflow.sdk import get_current_context

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageFile
from data_pipelines_annuaire.helpers.utils import fetch_file_urls_from_page
from data_pipelines_annuaire.workflows.data_pipelines.ess_france.config import (
    ESS_CONFIG,
    ESS_FRANCE_PAGE_URL,
)

logger = logging.getLogger(__name__)


class EssFranceProcessor(DataProcessor):
    SIREN_COLUMN = "N°SIREN"
    # The smallest workbook contains ~20% of the Sirens so a drop of more than 5%
    # likely means a workbook is missing or truncated.
    MAX_DROP_PERCENTAGE = 5.0
    URLS_XCOM_KEY = "excel_urls"

    def __init__(self):
        super().__init__(ESS_CONFIG)

    def _set_files_to_download(self, urls: list[str]) -> None:
        self.config.files_to_download = {
            os.path.basename(url): {
                "url": url,
                "destination": os.path.join(
                    self.config.tmp_folder, os.path.basename(url)
                ),
            }
            for url in urls
        }

    def _restore_files_to_download(self) -> None:
        """Config is dynamic so it needs to be rebuilt for each Airflow task."""
        urls = get_current_context()["ti"].xcom_pull(
            task_ids="download_data", key=self.URLS_XCOM_KEY
        )
        self._set_files_to_download(urls)

    def download_data(self) -> None:
        urls = fetch_file_urls_from_page(ESS_FRANCE_PAGE_URL, ".xlsx")
        self._set_files_to_download(urls)
        super().download_data()
        get_current_context()["ti"].xcom_push(key=self.URLS_XCOM_KEY, value=urls)

    def _get_unique_sirens(self) -> pd.DataFrame:
        """Returns the unique and sorted Siren of the first sheet of each workbook."""
        sirens = []
        for params in self.config.files_to_download.values():
            path = params["destination"]
            df = pd.read_excel(path, sheet_name=0, dtype=str, engine="openpyxl")
            if self.SIREN_COLUMN not in df.columns:
                raise ValueError(
                    f"Expected the Siren column '{self.SIREN_COLUMN}' in {path}, "
                    f"found {list(df.columns)}"
                )
            logger.info(f"{len(df)} rows found in {path}")
            sirens.append(
                df[[self.SIREN_COLUMN]].rename(columns={self.SIREN_COLUMN: "siren"})
            )

        df_ess = clean_sirent_column(
            pd.concat(sirens, ignore_index=True),
            column_type="siren",
            add_leading_zeros=True,
        )
        return (
            df_ess.drop_duplicates()
            .sort_values("siren", ignore_index=True)
            .assign(est_ess_france=1)
        )

    def _check_no_rows_lost(self, new_count: int) -> None:
        latest_path = (
            f"{self.config.object_storage_path}/latest/{self.config.file_name}.csv"
        )
        if not ObjectStorageFile.does_exist(latest_path):
            logger.info(f"No previous file at {latest_path}: row count check skipped")
            return
        local_file = ObjectStorageFile(latest_path).download_to(
            os.path.join(self.config.tmp_folder, "ess-latest.csv")
        )
        previous_count = len(pd.read_csv(local_file.path, dtype=str))
        if previous_count == 0:
            return

        drop_percentage = (previous_count - new_count) / previous_count * 100
        if drop_percentage > self.MAX_DROP_PERCENTAGE:
            error_message = (
                f"{new_count} Siren instead of {previous_count} in the previous file "
                f"(-{drop_percentage:.1f}%, max allowed: -{self.MAX_DROP_PERCENTAGE}%): "
                f"a source file may be missing or truncated"
            )
            get_current_context()["ti"].xcom_push(
                key=Notification.notification_xcom_key, value=error_message
            )
            raise ValueError(error_message)

    def preprocess_data(self):
        self._restore_files_to_download()
        df_ess = self._get_unique_sirens()
        self._check_no_rows_lost(len(df_ess))
        df_ess.to_csv(f"{self.config.tmp_folder}/ess.csv", index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_ess["siren"],
            description="unités légales",
        )

    def save_date_last_modified(self) -> None:
        self._restore_files_to_download()
        super().save_date_last_modified()
