import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.aides.config import AIDES_CONFIG


class AidesProcessor(DataProcessor):
    def __init__(self):
        super().__init__(AIDES_CONFIG)

    def preprocess_data(self):
        df_aides = pd.read_csv(
            self.config.files_to_download["aides"]["destination"],
            dtype="string",
            sep=";",
            usecols=["identifiant_beneficiaire"],
        )

        df_aides = df_aides.assign(
            identifiant_beneficiaire=lambda df: (
                df["identifiant_beneficiaire"]
                .str.replace(" ", "", regex=False)
                .str.strip()
            ),
        )

        df_aides = df_aides.assign(
            siren=lambda df: df["identifiant_beneficiaire"].str.extract(
                r"^(\d{9})$", expand=False
            )
        ).dropna(subset=["siren"])

        df_aides = df_aides.assign(aides_de_minimis_renseignee=1)[
            ["siren", "aides_de_minimis_renseignee"]
        ].drop_duplicates(subset=["siren"])

        df_aides = clean_sirent_column(
            df=df_aides,
            column_type="siren",
        )

        df_aides.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_aides.siren,
        )

        del df_aides
