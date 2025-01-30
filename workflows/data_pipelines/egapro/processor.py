import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG


class EgaproProcessor(DataProcessor):
    def __init__(self):
        super().__init__(EGAPRO_CONFIG)

    def preprocess_data(self):
        df_egapro = pd.read_excel(
            self.config.files_to_download["egapro"]["url"],
            dtype=str,
            engine="openpyxl",
        )
        df_egapro = df_egapro.drop_duplicates(subset=["SIREN"], keep="first")
        df_egapro = df_egapro[["SIREN"]]
        df_egapro["egapro_renseignee"] = 1
        df_egapro = df_egapro.rename(columns={"SIREN": "siren"})
        df_egapro.to_csv(f"{self.config.tmp_folder}/egapro.csv", index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_egapro["siren"],
            description="unités légales",
        )

        del df_egapro
