import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.patrimoine_vivant.config import (
    PATRIMOINE_VIVANT_CONFIG,
)


class PatrimoineVivantProcessor(DataProcessor):
    def __init__(self):
        super().__init__(PATRIMOINE_VIVANT_CONFIG)

    def preprocess_data(self):
        df_patrimoine = (
            pd.read_csv(
                self.config.files_to_download["patrimoine_vivant"]["destination"],
                dtype="string",
                sep=";",
                usecols=["SIRET"],
            )
            .rename(
                columns={
                    "SIRET": "siret",
                }
            )
            .dropna(subset=["siret"])
            .assign(
                siret=lambda df: df["siret"].str.strip(),
                siren=lambda df: df["siret"].str[:9],
                est_patrimoine_vivant=1,
            )
            .query("siren != ''")
            .drop_duplicates(subset=["siren"])
            .drop(columns=["siret"])
        )

        df_patrimoine.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_patrimoine.siren,
        )

        del df_patrimoine
