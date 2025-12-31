import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG


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
        df_egapro = (
            df_egapro[["SIREN"]]
            .assign(egapro_renseignee=1)
            .rename(columns={"SIREN": "siren"})
        )

        # Clean siren column and remove invalid rows
        df_egapro = clean_sirent_column(df_egapro, column_type="siren")

        df_egapro.to_csv(f"{self.config.tmp_folder}/egapro.csv", index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_egapro["siren"],
            description="unités légales",
        )

        del df_egapro
