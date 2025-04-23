import logging
from typing import Any

import pandas as pd

from dag_datalake_sirene.helpers.api_client import ApiClient
from dag_datalake_sirene.helpers.utils import (
    flatten_dict,
)


class SireneApiClient(ApiClient):
    def __init__(self, api_endpoint: str, bearer_token: str):
        super().__init__(
            base_url=api_endpoint,
            headers={"X-INSEE-Api-Key-Integration": f"{bearer_token}"},
        )

    def call_insee_api(self, endpoint: str, data_property: str) -> list[dict[str, Any]]:
        self.data_property = data_property
        return self.fetch_all(
            endpoint=endpoint,
            response_and_pagination_handler=self.process_response_and_pagination,
            batch_size=1_000,
            sleep_time=2.0,
        )

    def fetch_data(self, endpoint: str, data_property: str) -> pd.DataFrame:
        """Fetch data from the INSEE API, flatten it, and return it as a DataFrame."""
        data = self.call_insee_api(endpoint, data_property)
        # Process data in big chunks to avoid out of memory errors while being efficient
        chunk_size = 1_000_000
        logging.info("Download finished. Flattening the content..")

        if data_property == "unitesLegales":
            flux = [
                flatten_dict({**entry, **entry.get("periodesUniteLegale", [{}])[0]})
                for entry in data
            ]
            return pd.DataFrame(flux)
        elif data_property == "etablissements":
            logging.info(
                f"Processing {len(data)} etablissements in chunks of {chunk_size}"
            )
            dfs = []
            for i in range(0, len(data), chunk_size):
                chunk = data[i : i + chunk_size]
                chunk_df = pd.DataFrame(
                    [
                        flatten_dict(
                            {**entry, **entry.get("periodesEtablissement", [{}])[0]}
                        )
                        for entry in chunk
                    ]
                )
                dfs.append(chunk_df)
                logging.info(
                    f"Processed chunk {i // chunk_size + 1} of {(len(data) - 1) // chunk_size + 1}"
                )
            logging.info("Data has been flatten. Concatenating the chunks..")

            # Concatenate all chunks
            return pd.concat(dfs, ignore_index=True)

    def process_response_and_pagination(
        self, response: dict[str, Any] = None, current_params: dict[str, Any] = None
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        if current_params is None:
            initial_params = {"curseur": "*"}
            return None, initial_params

        header = response.get("header", {})
        next_cursor = header.get("curseurSuivant")
        current_cursor = header.get("curseur")

        data = response.get(self.data_property, [])

        if not next_cursor or next_cursor == current_cursor:
            return data, None

        new_params = {**current_params, "curseur": next_cursor}
        return data, new_params
