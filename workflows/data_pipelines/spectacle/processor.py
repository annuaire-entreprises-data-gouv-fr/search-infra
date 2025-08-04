import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.helpers.utils import clean_siren_column


class SpectacleProcessor(DataProcessor):
    def __init__(self):
        from dag_datalake_sirene.workflows.data_pipelines.spectacle.config import (
            SPECTACLE_CONFIG,
        )

        super().__init__(SPECTACLE_CONFIG)

    def preprocess_data(self):
        df_spectacle = pd.read_csv(
            self.config.files_to_download["spectacle"]["destination"],
            dtype=str,
            sep=";",
            encoding="latin-1",
        )

        df_spectacle = (
            df_spectacle.assign(
                statut_du_recepisse=lambda x: x["statut_recepisse"],
                est_entrepreneur_spectacle=1,
                siren=lambda x: clean_siren_column(x["siren_siret"].str[:9]),
            )
            .loc[
                lambda x: (x["siren"].notna() & x["siren"].str.isdigit()),
                ["siren", "statut_recepisse", "est_entrepreneur_spectacle"],
            ]
            .groupby(["siren", "est_entrepreneur_spectacle"])
            # If at least one of `statut` values is valid, then the value we keep is `valide`
            .agg(
                statut_entrepreneur_spectacle=(
                    "statut_recepisse",
                    lambda x: "valide" if "Valide" in x.unique() else "non_valide",
                )
            )
            .reset_index()
        )

        df_spectacle[
            ["siren", "statut_entrepreneur_spectacle", "est_entrepreneur_spectacle"]
        ].to_csv(self.config.file_output, index=False)

        self.push_message(
            Notification.notification_xcom_key,
            column=df_spectacle["siren"],
            description="unités légales",
        )

        del df_spectacle
