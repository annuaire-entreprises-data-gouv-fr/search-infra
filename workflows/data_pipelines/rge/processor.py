import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.rge.config import RGE_CONFIG

logger = logging.getLogger(__name__)


class RgeProcessor(DataProcessor):
    def __init__(self):
        super().__init__(RGE_CONFIG)

    def download_data(self):
        list_rge = []
        url = self.config.files_to_download["rge"]["url"]
        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            list_rge.extend(data["results"])

            while "next" in data:
                r = requests.get(data["next"])
                r.raise_for_status()
                data = r.json()
                list_rge.extend(data["results"])
                logger.info("Fetched additional page data.")

            logger.info(
                f"Data downloaded successfully from {url}. "
                f"Total records: {len(list_rge)}."
            )
            return list_rge

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading data from {url}: {e}")
            raise

    def remove_expired_certificates(self, df: pd.DataFrame) -> pd.DataFrame:
        """The ADEME seems to remove most of expired certificates from its dataset,
        but some are still persisting and need to be removed."""
        end_dates = pd.to_datetime(
            df["lien_date_fin"], format="ISO8601", errors="coerce"
        )
        today = pd.Timestamp(datetime.now(ZoneInfo("Europe/Paris")).date())
        # We also remove certificates where the last validity date is today since the
        # elasticsearch index will be live most of the next day.
        is_expired = end_dates.notna() & (end_dates <= today)

        logger.info(f"Removed {int(is_expired.sum())} expired RGE certificates.")
        return df[~is_expired]

    def preprocess_data(self):
        list_rge = self.download_data()

        df_rge = pd.DataFrame(list_rge)
        df_rge = df_rge[df_rge["siret"].notna()]
        df_rge = self.remove_expired_certificates(df_rge)
        df_list_rge = (
            df_rge.groupby(["siret"])["code_qualification"]
            .apply(list)
            .reset_index(name="liste_rge")
        )
        df_list_rge = df_list_rge[["siret", "liste_rge"]]
        df_list_rge["liste_rge"] = df_list_rge["liste_rge"].astype(str)

        # Clean siren column and remove invalid rows
        df_list_rge = clean_sirent_column(
            df_list_rge,
            column_type="siret",
        )

        df_list_rge.to_csv(f"{self.config.tmp_folder}/rge.csv", index=False)
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_list_rge["siret"],
            description="établissements",
        )

        del df_rge
        del df_list_rge
