import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.ess_france.config import ESS_CONFIG


class EssFranceProcessor(DataProcessor):
    def __init__(self):
        super().__init__(ESS_CONFIG)

    def preprocess_data(self):
        df_ess = pd.read_csv(self.config.files_to_download["ess"]["url"], dtype=str)
        df_ess["SIREN"] = df_ess["SIREN"].str.zfill(9)
        df_ess.rename(columns={"SIREN": "siren"}, inplace=True)
        df_ess["est_ess_france"] = True
        df_ess = df_ess[["siren", "est_ess_france"]]

        df_ess.to_csv(f"{self.config.tmp_folder}/ess.csv", index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_ess["siren"],
            description="unités légales",
        )
        del df_ess
