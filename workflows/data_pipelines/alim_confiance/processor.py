import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.alim_confiance.config import (
    ALIM_CONFIANCE_CONFIG,
)


class AlimConfianceProcessor(DataProcessor):
    def __init__(self):
        super().__init__(ALIM_CONFIANCE_CONFIG)

    def preprocess_data(self):
        df_alim = (
            pd.read_csv(
                self.config.files_to_download["alim_confiance"]["destination"],
                dtype="string",
                sep=";",
                usecols=["SIRET"],
            )
            .assign(
                est_alim_confiance=1,
                siren=lambda df: df["SIRET"].str.replace(" ", "").str[:9].str.zfill(9),
            )
            .loc[
                lambda x: x["siren"].notna() & x["siren"].str.isdigit(),
                ["siren", "est_alim_confiance"],
            ]
            .query("siren != ''")
            .drop_duplicates(subset=["siren"])
        )

        df_alim.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_alim.siren,
        )

        del df_alim
