import json
import logging
import os
from datetime import UTC, datetime, timedelta

import pandas as pd
from airflow.sdk import Variable, get_current_context
from botocore.exceptions import ClientError

from data_pipelines_annuaire.helpers import DataProcessor, GristApiClient
from data_pipelines_annuaire.helpers.notification import Notification
from data_pipelines_annuaire.helpers.object_storage import File
from data_pipelines_annuaire.helpers.utils import save_to_metadata
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    SIREN_PROTEGE_CONFIG,
)

logger = logging.getLogger(__name__)


class SirenProtegeProcessor(DataProcessor):
    DOCUMENT_ID = "uE2WGSjyBbSfiuSGbQiN9K"
    TABLE_ID = "Protected_siren"

    # SIREN are very rarely removed from the list: a larger drop is more likely
    # an editing mistake in Grist, which would unmask every removed company.
    MAX_REMOVED_RATIO = 0.01
    # SIREN are regularly added: a longer pause suggests the list is no longer maintained.
    MAX_DAYS_WITHOUT_NEW_SIREN = 10
    METADATA_KEY_LAST_ADDITION = "date_dernier_ajout"
    PARAM_SKIP_REMOVAL_CHECK = "ignorer_controle_suppressions"

    def __init__(self) -> None:
        super().__init__(SIREN_PROTEGE_CONFIG)
        self.metadata_path = os.path.join(self.config.tmp_folder, "metadata.json")
        self.previous_folder = os.path.join(self.config.tmp_folder, "previous")

    @staticmethod
    def clean_siren_list(df: pd.DataFrame) -> pd.DataFrame:
        if "siren" not in df.columns:
            raise ValueError(
                f"Grist table {SirenProtegeProcessor.TABLE_ID} does not contain "
                "the required 'siren' column."
            )

        # The list is edited by hand: spaces, blank rows and duplicates are expected.
        siren = df["siren"].str.replace(r"\s", "", regex=True)
        siren = siren[siren != ""].drop_duplicates().sort_values()

        invalid = siren[~siren.str.fullmatch(r"\d{9}")]
        if not invalid.empty:
            logger.warning("Malformed SIREN in the protected list: %s", list(invalid))

        return siren.to_frame(name="siren")

    @classmethod
    def check_removed_siren(cls, previous: set[str], current: set[str]) -> None:
        removed = previous - current
        if previous and len(removed) > cls.MAX_REMOVED_RATIO * len(previous):
            raise ValueError(
                f"{len(removed)} SIREN removed from the protected list "
                f"(more than {cls.MAX_REMOVED_RATIO:.0%} of {len(previous)}): "
                f"{sorted(removed)}. If intended, trigger the DAG manually with "
                f"the parameter {cls.PARAM_SKIP_REMOVAL_CHECK}."
            )

    @classmethod
    def compute_date_last_addition(
        cls,
        previous: set[str] | None,
        current: set[str],
        previous_date: str | None,
        now: datetime,
    ) -> str:
        if previous is None or previous_date is None or current - previous:
            return now.isoformat()
        return previous_date

    @classmethod
    def check_date_last_addition(cls, date_last_addition: str, now: datetime) -> None:
        if now - datetime.fromisoformat(date_last_addition) > timedelta(
            days=cls.MAX_DAYS_WITHOUT_NEW_SIREN
        ):
            raise ValueError(
                "No SIREN added to the protected list for more than "
                f"{cls.MAX_DAYS_WITHOUT_NEW_SIREN} days (last addition: "
                f"{date_last_addition})."
            )

    def _get_previous_state(self) -> tuple[set[str] | None, str | None]:
        """Return the latest published SIREN list and its last addition date."""
        try:
            self.object_storage_client.get_files(
                list_files=[
                    File(
                        source_path=f"{self.config.object_storage_path}/latest/",
                        source_name=name,
                        dest_path=f"{self.previous_folder}/",
                        dest_name=name,
                        content_type=None,
                    )
                    for name in (f"{self.config.file_name}.csv", "metadata.json")
                ]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                logger.info("No previous protected SIREN list published.")
                return None, None
            raise

        previous = pd.read_csv(
            f"{self.previous_folder}/{self.config.file_name}.csv",
            dtype=str,
            keep_default_na=False,
        )
        with open(f"{self.previous_folder}/metadata.json") as f:
            metadata = json.load(f)

        return set(previous["siren"]), metadata.get(self.METADATA_KEY_LAST_ADDITION)

    def preprocess_data(self) -> None:
        output_path = f"{self.config.tmp_folder}/{self.config.file_name}.csv"

        GristApiClient(api_token=Variable.get("GRIST_API_TOKEN")).download_table(
            document_id=self.DOCUMENT_ID,
            table_id=self.TABLE_ID,
            output_path=output_path,
        )

        df = self.clean_siren_list(
            pd.read_csv(output_path, dtype=str, keep_default_na=False)
        )
        current = set(df["siren"])
        previous, previous_date = self._get_previous_state()

        if previous is not None:
            if get_current_context()["params"].get(self.PARAM_SKIP_REMOVAL_CHECK):
                logger.warning("Removal check skipped by DAG parameter.")
            else:
                try:
                    self.check_removed_siren(previous, current)
                except ValueError as e:
                    get_current_context()["ti"].xcom_push(
                        key=Notification.notification_xcom_key, value=str(e)
                    )
                    raise

        save_to_metadata(
            self.metadata_path,
            self.METADATA_KEY_LAST_ADDITION,
            self.compute_date_last_addition(
                previous, current, previous_date, datetime.now(tz=UTC)
            ),
        )
        df.to_csv(output_path, index=False)

        logger.info("Saved %d protected SIREN -> %s", len(df), output_path)

    def check_new_siren_added(self) -> None:
        with open(self.metadata_path) as f:
            date_last_addition = json.load(f)[self.METADATA_KEY_LAST_ADDITION]

        try:
            self.check_date_last_addition(date_last_addition, datetime.now(tz=UTC))
        except ValueError as e:
            get_current_context()["ti"].xcom_push(
                key=Notification.notification_xcom_key, value=str(e)
            )
            raise
