import logging

import pandas as pd
from airflow.sdk import Variable

from data_pipelines_annuaire.helpers import DataProcessor, GristApiClient
from data_pipelines_annuaire.helpers.object_storage import File
from data_pipelines_annuaire.workflows.data_pipelines.administration.config import (
    ADMINISTRATION_CONFIG,
)

logger = logging.getLogger(__name__)


class AdministrationProcessor(DataProcessor):
    DOCUMENT_ID = "dkBFLyepK16PG1b7aYcmpG"

    # Grist table IDs (source) -> output filenames (without extension).
    TABLE_CODES_JURIDIQUES = "Codes_juridiques_to_Administrations"
    TABLE_WHITELIST = "Administration_whitelist_siren"
    # Trailing underscore is intentional: this is the actual table ID in
    # Grist, not a typo. Do not "fix" it without checking upstream first.
    TABLE_BLACKLIST = "Administration_blacklist_siren_"

    TABLES = {
        TABLE_CODES_JURIDIQUES: "administration_codes_juridiques",
        TABLE_WHITELIST: "administration_whitelist_siren",
        TABLE_BLACKLIST: "administration_blacklist_siren",
    }

    def __init__(self) -> None:
        super().__init__(ADMINISTRATION_CONFIG)

    @staticmethod
    def validate_table(
        df: pd.DataFrame,
        table_id: str,
    ) -> None:
        if table_id == AdministrationProcessor.TABLE_BLACKLIST:
            if df.empty:
                raise ValueError(
                    "Administration blacklist is empty: expected at least one row."
                )

        elif table_id == AdministrationProcessor.TABLE_WHITELIST:
            if "siren" not in df.columns:
                raise ValueError(
                    "Administration whitelist does not contain "
                    "the required 'siren' column."
                )

            # Cells are read with keep_default_na=False upstream, so blanks
            # are empty strings, not NaN - dropna() alone would miss them.
            if df["siren"].str.strip().eq("").all():
                raise ValueError("Administration whitelist contains no SIREN values.")

        elif table_id == AdministrationProcessor.TABLE_CODES_JURIDIQUES:
            # No validation currently required for this table.
            pass

        else:
            raise ValueError(f"Unknown table_id passed to validate_table: {table_id!r}")

    def preprocess_data(self) -> None:
        client = GristApiClient(
            api_token=Variable.get("GRIST_API_TOKEN"),
            # api_token="b80df61b8bc42451aa32f57eaae152e0b0a16c70",
        )

        for table_id, filename in self.TABLES.items():
            logger.info("Downloading Grist table %s...", table_id)

            output_path = f"{self.config.tmp_folder}/{filename}.csv"

            client.download_table(
                document_id=self.DOCUMENT_ID,
                table_id=table_id,
                output_path=output_path,
            )

            # keep_default_na=False matches GristApiClient.download_table,
            # so blank cells stay as empty strings on this re-read too.
            df = pd.read_csv(
                output_path,
                dtype=str,
                keep_default_na=False,
            )

            # Validate the downloaded data.
            self.validate_table(
                df=df,
                table_id=table_id,
            )

            logger.info(
                "Saved %s: %d rows -> %s",
                filename,
                len(df),
                output_path,
            )

    def send_file_to_object_storage(self) -> None:
        """Send administration CSVs to object storage."""

        files = [
            File(
                source_path=f"{self.config.tmp_folder}/",
                source_name=f"{filename}.csv",
                dest_path=f"{self.config.object_storage_path}/new/",
                dest_name=f"{filename}.csv",
                content_type=None,
            )
            for filename in self.TABLES.values()
        ]

        self.object_storage_client.send_files(
            list_files=files,
        )

    def compare_files_object_storage(self) -> bool:
        """Compare all administration CSVs with the latest files."""

        is_same = all(
            self.object_storage_client.compare_files(
                file_path_1=f"{self.config.object_storage_path}/new/",
                file_name_1=f"{filename}.csv",
                file_path_2=f"{self.config.object_storage_path}/latest/",
                file_name_2=f"{filename}.csv",
            )
            for filename in self.TABLES.values()
        )

        if not is_same:
            files = [
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name=f"{filename}.csv",
                    dest_path=f"{self.config.object_storage_path}/latest/",
                    dest_name=f"{filename}.csv",
                    content_type=None,
                )
                for filename in self.TABLES.values()
            ]
            self.object_storage_client.send_files(
                list_files=files,
            )

        return not is_same
