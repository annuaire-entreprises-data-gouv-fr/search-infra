import json
import logging
import os
from datetime import UTC, date, datetime, timedelta

from airflow.sdk import Variable, get_current_context

from data_pipelines_annuaire.helpers import DataProcessor, GristTable
from data_pipelines_annuaire.helpers.notification import Notification
from data_pipelines_annuaire.helpers.utils import save_to_metadata
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    GRIST_COLUMN_NAMES,
    GRIST_DOCUMENT_ID_VARIABLE,
    GRIST_TABLE_ID_VARIABLE,
    KEY_COLUMNS,
    SIREN_PROTEGE_CONFIG,
)

logger = logging.getLogger(__name__)


class SirenProtegeProcessor(DataProcessor):
    # We receive around 40 accepted requests per month so a long pause
    # suggests the Démarche Numérique form or its integration no longer works.
    MAX_DAYS_WITHOUT_NEW_INSERT = 14
    METADATA_KEY_LAST_INSERT = "date_dernier_ajout"

    def __init__(self) -> None:
        super().__init__(SIREN_PROTEGE_CONFIG)
        self.metadata_path = os.path.join(self.config.tmp_folder, "metadata.json")

    def preprocess_data(self) -> None:
        output_path = f"{self.config.tmp_folder}/{self.config.file_name}.csv"

        table = GristTable(
            Variable.get(GRIST_DOCUMENT_ID_VARIABLE),
            Variable.get(GRIST_TABLE_ID_VARIABLE),
        )
        df = table.read()

        missing = set(GRIST_COLUMN_NAMES) - set(df.columns)
        if missing:
            raise ValueError(
                f"Grist table {table.table_id} is missing the columns {missing}."
            )

        df = df[GRIST_COLUMN_NAMES]
        # Before the deduplication so the newer dossiers of an already protected
        # dirigeant also count as an insertion
        last_insertion_date = max(df["date_ajout"], default="")
        # Same format as in the RNE YYYY-MM
        df["date_de_naissance"] = df["date_de_naissance"].str.strip().str[:7]
        # One row per dirigeant instead of one per dossier
        # Grist keeps the rows in insertion order so the oldest dossier is kept
        df = (
            df[df["siren"] != ""]
            .drop_duplicates(subset=KEY_COLUMNS)
            .sort_values(KEY_COLUMNS)
            .reset_index(drop=True)
        )

        save_to_metadata(
            self.metadata_path, self.METADATA_KEY_LAST_INSERT, last_insertion_date
        )
        df.to_csv(output_path, index=False)

        logger.info(f"Saved {len(df)} protected dirigeants into {output_path}")

    def check_last_dossier_insertion_date(self) -> None:
        with open(self.metadata_path) as f:
            date_last_addition = json.load(f)[self.METADATA_KEY_LAST_INSERT]

        if not date_last_addition or (
            datetime.now(tz=UTC).date() - date.fromisoformat(date_last_addition)
        ) > timedelta(days=self.MAX_DAYS_WITHOUT_NEW_INSERT):
            message = (
                "No accepted dossier added to Grist for more than "
                f"{self.MAX_DAYS_WITHOUT_NEW_INSERT} days (last addition: "
                f"{date_last_addition})."
            )
            get_current_context()["ti"].xcom_push(
                key=Notification.notification_xcom_key, value=message
            )
            raise ValueError(message)
