import logging

import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.helpers.api_client import ApiClient
from dag_datalake_sirene.workflows.data_pipelines.marche_inclusion.config import (
    MARCHE_INCLUSION_CONFIG,
)


class MarcheInclusionProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__(MARCHE_INCLUSION_CONFIG)

    def preprocess_data(self) -> None:
        if (
            not MARCHE_INCLUSION_CONFIG.url_api
            or not MARCHE_INCLUSION_CONFIG.endpoint_api
        ):
            raise ValueError(
                "The url_api and endpoint_api config values are mandatory to fetch data from Marché Inclusion!"
            )

        # There is around 11k records in 2025-02
        api_client = ApiClient(
            base_url=MARCHE_INCLUSION_CONFIG.url_api,
            headers={"Authorization": f"Bearer {MARCHE_INCLUSION_CONFIG.auth_api}"},
        )
        number_of_structures = 20000
        api_params = {
            "limit": number_of_structures,
        }

        response_data = api_client.get(
            endpoint=MARCHE_INCLUSION_CONFIG.endpoint_api,
            params=api_params,
        ).json()
        actual_number_of_structures = response_data.get("count", 0)
        logging.info(f"Number of structures: {actual_number_of_structures}")

        if actual_number_of_structures > number_of_structures:
            response_data = api_client.get(
                endpoint=MARCHE_INCLUSION_CONFIG.endpoint_api,
                params=api_params,
            ).json()

        df_inclusion = (
            pd.DataFrame(response_data["results"])
            .assign(
                siren=lambda x: x["siret"].str[:9],
            )
            .groupby("siren")["kind"]
            .agg(
                lambda x: str(sorted(list(set(x)))),
            )
            .reset_index()
            .rename(columns={"kind": "type_siae"})
            .assign(
                est_siae=1,
            )
            .sort_values(by=["siren"])
        )

        df_inclusion.to_csv(
            self.config.file_output,
            columns=["siren", "type_siae", "est_siae"],
            index=False,
        )

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_inclusion.siren,
        )
