import logging

import pandas as pd
import requests

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.marche_inclusion.config import (
    MARCHE_INCLUSION_CONFIG,
)


class MarcheInclusionProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__(MARCHE_INCLUSION_CONFIG)

    def call_api_marche_inclusion(self, number_of_structures: int) -> dict:
        query_params = f"token={self.config.auth_api}&limit={number_of_structures}"

        endpoint = f"{self.config.url_api}{query_params}"

        response = requests.get(endpoint)
        data = response.json()
        return data

    def preprocess_data(self) -> None:
        # There is around 10k records in early 2025
        number_of_structures = 20000

        response_data = self.call_api_marche_inclusion(number_of_structures)
        actual_number_of_structures = response_data.get("count", 0)
        logging.info(f"Number of structures: {actual_number_of_structures}")

        if actual_number_of_structures > number_of_structures:
            response_data = self.call_api_marche_inclusion(actual_number_of_structures)

        df_inclusion = pd.DataFrame(response_data.get("results", []))

        df_inclusion = (
            df_inclusion.assign(
                siren=lambda x: x.siret.str[:9],
            )
            .groupby("siren")["kind"]
            .agg(
                lambda x: str(list(set(x))),
            )
            .reset_index()
            .rename(columns={"kind": "type_siae"})
            .assign(
                est_siae=1,
            )
        )

        df_inclusion.to_csv(
            self.config.file_output,
            columns=["siren", "type_siae", "est_siae"],
            index=False,
        )

        DataProcessor.push_unique_count(
            df_inclusion.siren, Notification.notification_xcom_key
        )
