import logging
import time
from typing import Any
from dag_datalake_sirene.helpers.api_client import APIClient


class INSEEAPIClient:
    def __init__(self, api_endpoint: str, bearer_token: str):
        self.api_client = APIClient(
            api_endpoint, {"Authorization": f"Bearer {bearer_token}"}
        )

    def call_insee_api(self, endpoint, data_property: str) -> list[dict[str, Any]]:
        cursor: str | None = "*"
        api_data: list[dict[str, Any]] = []
        request_count = 0

        while cursor is not None:
            request_count += 1000
            if request_count % 10000 == 0:
                logging.info(f"Request count: {request_count}")

            response = self.api_client.get(endpoint, {"curseur": str(cursor)})
            response_json = response.json()

            header = response_json.get("header", {})
            next_cursor = header.get("curseurSuivant")
            current_cursor = header.get("curseur")

            cursor = (
                None
                if not next_cursor or next_cursor == current_cursor
                else next_cursor
            )
            api_data.extend(response_json.get(data_property, []))

            # Wait for 2 seconds after each call to avoid 429
            time.sleep(2)

        return api_data
