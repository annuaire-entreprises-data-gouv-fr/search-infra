import json
import logging
import os
from datetime import UTC, datetime, timedelta

import pandas as pd
from airflow.sdk import Variable, get_current_context
from botocore.exceptions import ClientError

from data_pipelines_annuaire.helpers import DataProcessor, GristTable
from data_pipelines_annuaire.helpers.notification import Notification
from data_pipelines_annuaire.helpers.object_storage import File
from data_pipelines_annuaire.helpers.utils import (
    sanitize_free_text_for_tabular_format,
    save_to_metadata,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    GRIST_COLUMN_NAMES,
    GRIST_DOCUMENT_ID_VARIABLE,
    GRIST_TABLE_ID_VARIABLE,
    KEY_COLUMNS,
    MOTIF_MAX_LENGTH,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.object_storage.config import (
    SIREN_PROTEGE_CONFIG,
)

logger = logging.getLogger(__name__)


class SirenProtegeProcessor(DataProcessor):
    # Removing dirigeants from the list should be very rare so a larger drop
    # is more likely an editing mistake in Grist
    MAX_REMOVED_RATIO = 0.01
    # We receive around 40 accepted requests per month so a longer pause
    # suggests the form or its integration no longer works.
    MAX_DAYS_WITHOUT_NEW_INSERT = 14
    METADATA_KEY_LAST_INSERT = "date_dernier_ajout"
    PARAM_SKIP_REMOVAL_CHECK = "ignorer_controle_suppressions"

    def __init__(self) -> None:
        super().__init__(SIREN_PROTEGE_CONFIG)
        self.metadata_path = os.path.join(self.config.tmp_folder, "metadata.json")
        self.latest_folder = os.path.join(self.config.tmp_folder, "latest")

    @staticmethod
    def clean_list(df: pd.DataFrame) -> pd.DataFrame:
        missing = set(GRIST_COLUMN_NAMES) - set(df.columns)
        if missing:
            raise ValueError(
                f"Grist table {GRIST_TABLE_ID_VARIABLE} is missing the columns {missing}."
            )

        # The table is also edited by hand: spaces, blank rows and duplicates
        # are expected.
        df = df[GRIST_COLUMN_NAMES].copy()
        df["siren"] = df["siren"].str.replace(r"\s", "", regex=True)
        for column in ("prenoms", "nom", "date_de_naissance"):
            df[column] = df[column].str.split().str.join(" ").str.upper()
        df["legacy"] = df["legacy"].str.strip().str.lower()
        for column in (
            "date_ajout",
            "date_validation_instructeur",
            "date_depot_demande",
            "numero_dossier",
        ):
            df[column] = df[column].str.strip()
        motifs = df["motif"].map(
            lambda motif: sanitize_free_text_for_tabular_format(motif, MOTIF_MAX_LENGTH)
        )
        df["motif"] = motifs.str[0]
        # Only reachable through a hand edit in Grist: the motifs added from
        # Démarche Numérique are already truncated.
        truncated = df.loc[motifs.str[1].astype(bool), "siren"]
        if not truncated.empty:
            logger.warning(
                f"Motif longer than {MOTIF_MAX_LENGTH} characters in Grist, "
                f"truncated in the CSV, SIREN: {sorted(set(truncated))}"
            )
        # One row per dirigeant: Grist has one per request, and keeps them in
        # the order they were added, so the oldest request is kept.
        df = (
            df[df["siren"] != ""]
            .drop_duplicates(subset=KEY_COLUMNS)
            .sort_values(KEY_COLUMNS)
        )

        invalid = df.loc[~df["siren"].str.fullmatch(r"\d{9}"), "siren"]
        if not invalid.empty:
            logger.warning(f"Malformed SIREN in the protected list: {list(invalid)}")

        return df.reset_index(drop=True)

    @staticmethod
    def get_keys(df: pd.DataFrame) -> set[tuple[str, ...]]:
        return set(df[KEY_COLUMNS].itertuples(index=False, name=None))

    @classmethod
    def check_removed_rows(
        cls, previous: set[tuple[str, ...]], current: set[tuple[str, ...]]
    ) -> None:
        removed = previous - current
        if previous and len(removed) > cls.MAX_REMOVED_RATIO * len(previous):
            raise ValueError(
                f"{len(removed)} rows removed from the protected list "
                f"(more than {cls.MAX_REMOVED_RATIO:.0%} of {len(previous)}), "
                f"SIREN: {sorted({key[0] for key in removed})}. If intended, "
                f"trigger the DAG manually with the parameter "
                f"{cls.PARAM_SKIP_REMOVAL_CHECK}."
            )

    @classmethod
    def compute_date_last_addition(
        cls,
        previous: set[tuple[str, ...]] | None,
        current: set[tuple[str, ...]],
        previous_date: str | None,
        now: datetime,
    ) -> str:
        if previous is None or previous_date is None or current - previous:
            return now.isoformat()
        return previous_date

    def _get_previous_state(self) -> tuple[set[tuple[str, ...]] | None, str | None]:
        """Return the latest published list and its last addition date."""
        try:
            self.object_storage_client.get_files(
                list_files=[
                    File(
                        source_path=f"{self.config.object_storage_path}/latest/",
                        source_name=name,
                        dest_path=f"{self.latest_folder}/",
                        dest_name=name,
                        content_type=None,
                    )
                    for name in (f"{self.config.file_name}.csv", "metadata.json")
                ]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                logger.info("No previous protected list published.")
                return None, None
            raise

        previous = pd.read_csv(
            f"{self.latest_folder}/{self.config.file_name}.csv",
            dtype=str,
            keep_default_na=False,
        )
        with open(f"{self.latest_folder}/metadata.json") as f:
            metadata = json.load(f)

        return self.get_keys(previous), metadata.get(self.METADATA_KEY_LAST_INSERT)

    def preprocess_data(self) -> None:
        output_path = f"{self.config.tmp_folder}/{self.config.file_name}.csv"

        df = self.clean_list(
            GristTable(
                Variable.get(GRIST_DOCUMENT_ID_VARIABLE), GRIST_TABLE_ID_VARIABLE
            ).read()
        )
        current = self.get_keys(df)
        previous, previous_date = self._get_previous_state()

        if previous is not None:
            if get_current_context()["params"].get(self.PARAM_SKIP_REMOVAL_CHECK):
                logger.warning("Removal check skipped by DAG parameter.")
            else:
                try:
                    self.check_removed_rows(previous, current)
                except ValueError as e:
                    get_current_context()["ti"].xcom_push(
                        key=Notification.notification_xcom_key, value=str(e)
                    )
                    raise

        save_to_metadata(
            self.metadata_path,
            self.METADATA_KEY_LAST_INSERT,
            self.compute_date_last_addition(
                previous, current, previous_date, datetime.now(tz=UTC)
            ),
        )
        df.to_csv(output_path, index=False)

        logger.info(f"Saved {len(df)} protected dirigeants -> {output_path}")

    def check_last_dossier_insertion_date(self) -> None:
        with open(self.metadata_path) as f:
            date_last_addition = json.load(f)[self.METADATA_KEY_LAST_INSERT]

        if datetime.now(tz=UTC) - datetime.fromisoformat(
            date_last_addition
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
