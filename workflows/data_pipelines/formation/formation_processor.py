import logging

import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.formation.formation_config import (
    FORMATION_CONFIG,
)


class FormationProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FORMATION_CONFIG)

    def preprocess_data(self):
        df_organisme_formation = pd.read_csv(
            self.config.files_to_download["formation"]["destination"],
            dtype="string",
            sep=";",
            usecols=["Certifications", "Numéro Déclaration Activité", "Code SIREN"],
        )

        df_organisme_formation = (
            df_organisme_formation.assign(
                est_qualiopi=lambda x: x["Certifications"].notna(),
                id_nda=lambda x: x["Numéro Déclaration Activité"],
                siren=lambda x: x["Code SIREN"],
            )
            .groupby(["siren"], as_index=False)
            .agg(
                id_nda=("id_nda", list),
                est_qualiopi=("est_qualiopi", "max"),  # True takes priority over False
            )
            .rename(columns={"id_nda": "liste_id_organisme_formation"})
            .sort_values("siren")
        )

        df_organisme_formation.to_csv(self.config.file_output, index=False)
        logging.info(f"Formation dataset saved in {self.config.file_output}")

        DataProcessor.push_unique_count(
            df_organisme_formation["siren"], Notification.notification_xcom_key, "siren"
        )

        del df_organisme_formation
