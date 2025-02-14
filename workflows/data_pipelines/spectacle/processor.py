import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification


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
        )

        df_spectacle = (
            df_spectacle.assign(
                statut_du_recepisse=lambda x: x["Statut du récépissé"],
                est_entrepreneur_spectacle=1,
                siren=lambda x: x[
                    "SIRET (personne morale) / SIREN (personne physique)"
                ].str[:9],
            )
            .loc[
                lambda x: x["siren"].notna() & x["siren"].str.isdigit(),
                ["siren", "statut_du_recepisse", "est_entrepreneur_spectacle"],
            ]
            .groupby(["siren", "est_entrepreneur_spectacle"])
            # If at least one of `statut` values is valid, then the value we keep is `valide
            .agg(
                statut_entrepreneur_spectacle=(
                    "statut_du_recepisse",
                    lambda x: "valide" if "Valide" in x.unique() else "invalide",
                )
            )
            .reset_index()
        )

        df_spectacle[
            ["siren", "statut_entrepreneur_spectacle", "est_entrepreneur_spectacle"]
        ].to_csv(self.config.file_output, index=False)

        self.push_unique_count(
            df_spectacle["siren"], Notification.notification_xcom_key, "unités légales"
        )

        del df_spectacle
