import logging

import pandas as pd
import requests

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.rge.config import RGE_CONFIG


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
                logging.info("Fetched additional page data.")

            logging.info(
                f"Data downloaded successfully from {url}."
                "Total records: {len(list_rge)}."
            )
            return list_rge

        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading data from {url}: {e}")
            raise e

    def preprocess_data(self):
        list_rge = self.download_data()

        df_rge = pd.DataFrame(list_rge)
        df_rge = df_rge[df_rge["siret"].notna()]
        df_list_rge = (
            df_rge.groupby(["siret"])["code_qualification"]
            .apply(list)
            .reset_index(name="liste_rge")
        )
        df_list_rge = df_list_rge[["siret", "liste_rge"]]
        df_list_rge["liste_rge"] = df_list_rge["liste_rge"].astype(str)

        df_list_rge.to_csv(f"{self.config.tmp_folder}/rge.csv", index=False)
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_list_rge["siret"],
            description="établissements",
        )

        del df_rge
        del df_list_rge
