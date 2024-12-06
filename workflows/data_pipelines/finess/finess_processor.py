import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.finess.config import FINESS_CONFIG


class FinessProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FINESS_CONFIG)

    def preprocess_data(self):
        destination_path = f"{self.config.tmp_folder}/finess-download.csv"
        self.download_data(destination_path)

        df_finess = pd.read_csv(
            destination_path,
            dtype=str,
            sep=";",
            encoding="Latin-1",
            skiprows=1,
            header=None,
        )
        df_finess = df_finess[[1, 18, 22]].rename(
            columns={1: "finess", 18: "cat_etablissement", 22: "siret"}
        )
        df_finess = df_finess[df_finess["siret"].notna()]
        df_list_finess = (
            df_finess.groupby(["siret"])["finess"]
            .apply(list)
            .reset_index(name="liste_finess")
        )
        df_list_finess = df_list_finess[["siret", "liste_finess"]]
        df_list_finess["liste_finess"] = df_list_finess["liste_finess"].astype(str)
        df_list_finess.to_csv(f"{self.config.tmp_folder}/finess.csv", index=False)

        DataProcessor._push_unique_count(
            df_list_finess["siret"],
            Notification.notification_xcom_key,
            "établissements",
        )

        del df_finess
        del df_list_finess
