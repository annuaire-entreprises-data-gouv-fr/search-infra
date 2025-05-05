import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.helpers.utils import clean_siren_column
from dag_datalake_sirene.workflows.data_pipelines.achats_responsables.config import (
    ACHATS_RESPONSABLES_CONFIG,
)


class AchatsResponsablesProcessor(DataProcessor):
    def __init__(self):
        super().__init__(ACHATS_RESPONSABLES_CONFIG)

    def preprocess_data(self):
        df_achats = (
            pd.read_csv(
                self.config.files_to_download["achats_responsables"]["destination"],
                dtype="string",
                sep=";",
                usecols=["SIREN", "PERIMETRE"],
            )
            .rename(
                columns={
                    "SIREN": "siren",
                    "PERIMETRE": "perimetre_label",
                }
            )
            .assign(
                est_achats_responsables=1,
                siren=lambda df: clean_siren_column(df["siren"]),
            )
            .dropna(subset=["siren"])
            .query("siren != ''")
            .drop_duplicates(subset=["siren"])
        )

        df_achats.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_achats.siren,
        )

        del df_achats
